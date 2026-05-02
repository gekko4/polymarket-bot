const { ClobClient } = require('@polymarket/clob-client');
const fs = require('fs');

// --- STRATEGY CONFIG ---
const ENTRY_PRICE_MAX        = 0.35;   // Phase 1 ceiling
const ENTRY_PRICE_SECOND     = 0.25;   // Phase 2 ceiling
const TAKE_PROFIT_CENTS      = 0.05;   // Exit when bid is +5¢ from original entry
const STOP_LOSS              = 0.25;   // Used only for logging context at Grace Period
const ENTRY_TIME             = 210;    // Only enter new trades with this many seconds left
const TWO_MIN_MARK           = 120;
const GRACE_PERIOD_END       = 110;    // Force exit everything here
const PAPER_BET_SIZE         = 1.00;
const FEE_RATE               = 0.015;

// --- LIQUIDITY THRESHOLDS ---
const MAX_SPREAD_PERCENT     = 0.08;
const MIN_ASK_DEPTH_MULT     = 3;
const MIN_BID_DEPTH_MULT     = 5;
const MIN_VIABLE_BID         = 0.20;
const MIN_TOTAL_MARKET_VOL   = 50;

// --- TRADE LOGGING ---
const TRADES_LOG_FILE = 'trades.json';

function logTrade(tradeData) {
    try {
        let trades = [];
        if (fs.existsSync(TRADES_LOG_FILE)) {
            const data = fs.readFileSync(TRADES_LOG_FILE, 'utf8');
            trades = JSON.parse(data);
        }
        trades.push({
            timestamp: new Date().toISOString(),
            ...tradeData
        });
        fs.writeFileSync(TRADES_LOG_FILE, JSON.stringify(trades, null, 2));
    } catch (err) {
        console.error('[LOG ERROR]', err.message);
    }
}

// ─────────────────────────────────────────────────────────
// MARKET DISCOVERY
// ─────────────────────────────────────────────────────────
async function getActiveMarketTokens(clobClient) {
    const markets = await clobClient.getMarkets();

    const btcMarkets = markets.data.filter(m =>
        m.active &&
        !m.closed &&
        m.question.toLowerCase().includes('bitcoin') &&
        m.question.toLowerCase().includes('5')
    );

    if (!btcMarkets.length) return null;

    const sorted = btcMarkets.sort((a, b) =>
        new Date(a.end_date_iso) - new Date(b.end_date_iso)
    );

    const market   = sorted[0];
    const yesToken = market.tokens.find(t => t.outcome === 'Yes')?.token_id;
    const noToken  = market.tokens.find(t => t.outcome === 'No')?.token_id;

    if (!yesToken || !noToken) return null;

    return {
        yesToken,
        noToken,
        question: market.question,
        endDate:  market.end_date_iso
    };
}

function checkLiquidity(orderbook, entryPrice) {
    if (!orderbook || !Array.isArray(orderbook.asks) || !Array.isArray(orderbook.bids)) {
        return { ok: false, reason: "Invalid orderbook structure" };
    }

    const asks = orderbook.asks.map(a => ({ price: parseFloat(a.price), size: parseFloat(a.size) }));
    const bids = orderbook.bids.map(b => ({ price: parseFloat(b.price), size: parseFloat(b.size) }));

    if (!asks.length || !bids.length) return { ok: false, reason: "Empty orderbook" };

    const bestAsk   = asks[0].price;
    const bestBid   = bids[0].price;
    const spreadPct = (bestAsk - bestBid) / bestAsk;

    if (spreadPct > MAX_SPREAD_PERCENT)
        return { ok: false, reason: `Spread ${(spreadPct * 100).toFixed(1)}% > ${MAX_SPREAD_PERCENT * 100}% max` };

    const fee          = PAPER_BET_SIZE * FEE_RATE;
    const sharesNeeded = (PAPER_BET_SIZE - fee) / entryPrice;

    const askDepth = asks
        .filter(a => a.price <= entryPrice * 1.02)
        .reduce((sum, a) => sum + a.size, 0);

    if (askDepth < sharesNeeded * MIN_ASK_DEPTH_MULT)
        return { ok: false, reason: `Ask depth too thin` };

    const viableBidDepth = bids
        .filter(b => b.price >= MIN_VIABLE_BID)
        .reduce((sum, b) => sum + b.size, 0);

    if (viableBidDepth < sharesNeeded * MIN_BID_DEPTH_MULT)
        return { ok: false, reason: `Viable bid depth too thin` };

    const totalVol = asks.reduce((s, a) => s + a.size, 0) + bids.reduce((s, b) => s + b.size, 0);
    if (totalVol < MIN_TOTAL_MARKET_VOL)
        return { ok: false, reason: `Market too thin` };

    return {
        ok: true,
        stats: { spreadPct: (spreadPct * 100).toFixed(1), totalVol: totalVol.toFixed(0) }
    };
}

// ─────────────────────────────────────────────────────────
// OPEN SCANNER (NO LOCKS)
// Scans both YES and NO simultaneously. Returns cheapest valid option.
// ─────────────────────────────────────────────────────────
async function scanForEntry(clobClient, YES_TOKEN_ID, NO_TOKEN_ID, priceCeiling) {
    let yesBook, noBook;
    try {
        [yesBook, noBook] = await Promise.all([
            clobClient.getOrderBook(YES_TOKEN_ID),
            clobClient.getOrderBook(NO_TOKEN_ID)
        ]);
    } catch (err) {
        return null;
    }

    const candidates  = [];
    const rejections  = [];

    // Evaluate both sides purely on math
    const sidesToEvaluate = [
        { name: 'YES', book: yesBook, id: YES_TOKEN_ID },
        { name: 'NO',  book: noBook,  id: NO_TOKEN_ID }
    ];

    for (const side of sidesToEvaluate) {
        if (side.book && Array.isArray(side.book.asks) && side.book.asks.length > 0) {
            const bestAsk = parseFloat(side.book.asks[0].price);
            if (Number.isFinite(bestAsk) && bestAsk <= priceCeiling) {
                const liq = checkLiquidity(side.book, bestAsk);
                if (liq.ok) {
                    candidates.push({ side: side.name, tokenId: side.id, bestAsk, liq });
                } else {
                    rejections.push({ side: side.name, reason: liq.reason, bestAsk });
                }
            }
        }
    }

    if (candidates.length) {
        // If both happen to be valid (unlikely in reality), pick the cheaper one
        candidates.sort((a, b) => a.bestAsk - b.bestAsk);
        return candidates[0];
    }

    if (rejections.length) return { ...rejections[0], rejected: true };
    return null;
}

// ─────────────────────────────────────────────────────────
// BUILD POSITION
// ─────────────────────────────────────────────────────────
function buildPosition(scan, phaseLevel) {
    const fee    = PAPER_BET_SIZE * FEE_RATE;
    const shares = (PAPER_BET_SIZE - fee) / scan.bestAsk;
    return {
        phaseLevel,
        side:       scan.side,
        tokenId:    scan.tokenId,
        entryPrice: scan.bestAsk,
        takeProfit: parseFloat((scan.bestAsk + TAKE_PROFIT_CENTS).toFixed(2)),
        shares,
        cost:       PAPER_BET_SIZE
    };
}

// ─────────────────────────────────────────────────────────
// EXIT CHECK
// ─────────────────────────────────────────────────────────
function checkExit(position, bestBid, secondsLeft) {
    // 1. Profit Trigger (Independent of time, as long as before Grace)
    if (bestBid >= position.takeProfit)
        return { trigger: true, reason: `TAKE PROFIT — $${bestBid} >= $${position.takeProfit}` };

    // 2. Failsafe Exit (Force close)
    if (secondsLeft <= GRACE_PERIOD_END)
        return { trigger: true, reason: `GRACE ENDED — Force closing at $${bestBid}` };

    return null;
}

// ─────────────────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────────────────
async function runTrader() {
    console.log("Booting Independent Phase Engine...");
    console.log(`  Phase 1 ceiling : $${ENTRY_PRICE_MAX}`);
    console.log(`  Phase 2 ceiling : $${ENTRY_PRICE_SECOND}`);
    console.log(`  Take Profit     : Exact Entry + ${TAKE_PROFIT_CENTS * 100}¢\n`);

    const clobClient = new ClobClient('https://clob.polymarket.com', 137);

    let YES_TOKEN_ID  = null;
    let NO_TOKEN_ID   = null;
    let currentMarket = null;
    let marketEndTime = null;

    // Independent Phases
    let phase1 = null;
    let phase2 = null;
    let graceNotified = false;
    let secondsLeft = 0;
    let isLoadingMarket = false;

    async function loadMarket() {
        if (isLoadingMarket) return false;
        isLoadingMarket = true;
        try {
            const tokens = await getActiveMarketTokens(clobClient);
            if (!tokens) return false;

            YES_TOKEN_ID  = tokens.yesToken;
            NO_TOKEN_ID   = tokens.noToken;
            currentMarket = tokens.question;
            marketEndTime = new Date(tokens.endDate).getTime();

            phase1 = null;
            phase2 = null;
            graceNotified = false;

            const now = Date.now();
            secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));
            console.log(`\n[MARKET] Loaded: ${currentMarket}`);
            return true;
        } finally {
            isLoadingMarket = false;
        }
    }

    const loaded = await loadMarket();
    if (!loaded) setInterval(async () => { if (!YES_TOKEN_ID) await loadMarket(); }, 30000);

    setInterval(async () => {
        try {
            if (!YES_TOKEN_ID) return;

            if (marketEndTime) {
                const now = Date.now();
                secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));
            }

            // ── EXPIRED ──
            if (secondsLeft <= 0) {
                console.log('\n[MARKET] Timer expired — searching for next...');
                phase1 = phase2 = null;
                const ok = await loadMarket();
                if (!ok) YES_TOKEN_ID = null;
                return;
            }

            // ─────────────────────────────────────────────────────────
            // 1. CHECK EXITS (Independent Monitoring)
            // ─────────────────────────────────────────────────────────
            for (const currentPhase of [
                { pos: phase1, name: 'Phase 1', clear: () => phase1 = null },
                { pos: phase2, name: 'Phase 2', clear: () => phase2 = null }
            ]) {
                if (!currentPhase.pos) continue;

                let orderbook;
                try { orderbook = await clobClient.getOrderBook(currentPhase.pos.tokenId); } 
                catch (err) { continue; }

                if (!orderbook || !orderbook.bids.length) continue;
                const bestBid = parseFloat(orderbook.bids[0].price);
                if (!Number.isFinite(bestBid)) continue;

                const exit = checkExit(currentPhase.pos, bestBid, secondsLeft);
                if (exit) {
                    const gross = currentPhase.pos.shares * bestBid;
                    const fee   = gross * FEE_RATE;
                    const pnl   = (gross - fee) - currentPhase.pos.cost;
                    
                    console.log(`\n[${currentPhase.name} EXIT] ${exit.reason}`);
                    console.log(`  Side    : ${currentPhase.pos.side}`);
                    console.log(`  Entry   : $${currentPhase.pos.entryPrice} | TP: $${currentPhase.pos.takeProfit}`);
                    console.log(`  Net PnL : $${pnl.toFixed(4)}\n`);

                    logTrade({
                        event: 'EXIT',
                        phase: currentPhase.pos.phaseLevel,
                        side: currentPhase.pos.side,
                        entryPrice: currentPhase.pos.entryPrice,
                        exitPrice: bestBid,
                        pnl: parseFloat(pnl.toFixed(4)),
                        reason: exit.reason
                    });

                    // Clear this specific phase, allowing it to instantly reload
                    currentPhase.clear(); 
                } else {
                    process.stdout.write(`\r[HOLD ${currentPhase.name}] ${currentPhase.pos.side} | Bid: $${bestBid} | TP: $${currentPhase.pos.takeProfit}   `);
                }
            }

            // ─────────────────────────────────────────────────────────
            // 2. CHECK ENTRIES (Threshold Driven)
            // ─────────────────────────────────────────────────────────
            if (secondsLeft >= ENTRY_TIME) {
                let phase1JustFilled = false;

                // Evaluate Phase 1
                if (!phase1) {
                    const scan1 = await scanForEntry(clobClient, YES_TOKEN_ID, NO_TOKEN_ID, ENTRY_PRICE_MAX);
                    if (!scan1) {
                        process.stdout.write(`\r[SCAN] Time: ${secondsLeft}s | No setup at <= $${ENTRY_PRICE_MAX}   `);
                    } else if (!scan1.rejected) {
                        phase1 = buildPosition(scan1, 1);
                        phase1JustFilled = true;
                        console.log(`\n[ENTRY PHASE 1] ${phase1.side} @ $${phase1.entryPrice} | Target: $${phase1.takeProfit}`);
                        logTrade({ event: 'ENTRY', phase: 1, side: phase1.side, price: phase1.entryPrice });
                    }
                }

                // Evaluate Phase 2
                // (We require !phase1JustFilled to ensure a 1-second delay so it doesn't instantly 
                // double-buy the exact same API tick if the market is suddenly sitting at 0.20)
                if (!phase2 && !phase1JustFilled) {
                    const scan2 = await scanForEntry(clobClient, YES_TOKEN_ID, NO_TOKEN_ID, ENTRY_PRICE_SECOND);
                    if (scan2 && !scan2.rejected) {
                        phase2 = buildPosition(scan2, 2);
                        console.log(`\n[ENTRY PHASE 2] ${phase2.side} @ $${phase2.entryPrice} | Target: $${phase2.takeProfit}`);
                        logTrade({ event: 'ENTRY', phase: 2, side: phase2.side, price: phase2.entryPrice });
                    }
                }
            } else if (!phase1 && !phase2) {
                process.stdout.write(`\r[IDLE] Entry window closed. Time: ${secondsLeft}s   `);
            }

            // ── GRACE NOTIFICATION ──
            if (secondsLeft <= TWO_MIN_MARK && !graceNotified && (phase1 || phase2)) {
                graceNotified = true;
                console.log(`\n[GRACE WARNING] 2-min mark reached. Preparing for force-exit.`);
            }

        } catch (err) {
            console.error('[TICK ERROR]', err?.message || err);
        }
    }, 1000);
}

runTrader().catch(console.error);
