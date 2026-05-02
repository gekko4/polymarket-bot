const { ClobClient } = require('@polymarket/clob-client');
const fs = require('fs');

// --- STRATEGY CONFIG ---
const ENTRY_PRICE_MAX        = 0.35;   // Slot 1 ceiling
const ENTRY_PRICE_SECOND     = 0.25;   // Slot 2 ceiling (only opens if price drops here after slot 1)
const TAKE_PROFIT_CENTS      = 0.05;   // Exit when bid is 5 cents above entry price
const STOP_LOSS              = 0.25;
const ENTRY_TIME             = 210;    // Only enter with this many seconds left or more
const TWO_MIN_MARK           = 120;
const GRACE_PERIOD_END       = 110;
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

// ─────────────────────────────────────────────────────────
// TRADE LOGGING HELPERS
// ─────────────────────────────────────────────────────────
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

// ─────────────────────────────────────────────────────────
// LIQUIDITY CHECK
// ─────────────────────────────────────────────────────────
function checkLiquidity(orderbook, entryPrice) {
    if (!orderbook || !Array.isArray(orderbook.asks) || !Array.isArray(orderbook.bids)) {
        return { ok: false, reason: "Invalid orderbook structure" };
    }

    const asks = orderbook.asks.map(a => ({ price: parseFloat(a.price), size: parseFloat(a.size) }));
    const bids = orderbook.bids.map(b => ({ price: parseFloat(b.price), size: parseFloat(b.size) }));

    if (!asks.length || !bids.length)
        return { ok: false, reason: "Empty orderbook" };

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
        return { ok: false, reason: `Ask depth too thin: ${askDepth.toFixed(2)} shares (need ${(sharesNeeded * MIN_ASK_DEPTH_MULT).toFixed(2)})` };

    const viableBidDepth = bids
        .filter(b => b.price >= MIN_VIABLE_BID)
        .reduce((sum, b) => sum + b.size, 0);

    if (viableBidDepth < sharesNeeded * MIN_BID_DEPTH_MULT)
        return { ok: false, reason: `Viable bid depth too thin: ${viableBidDepth.toFixed(2)} (need ${(sharesNeeded * MIN_BID_DEPTH_MULT).toFixed(2)})` };

    const totalVol = asks.reduce((s, a) => s + a.size, 0) + bids.reduce((s, b) => s + b.size, 0);
    if (totalVol < MIN_TOTAL_MARKET_VOL)
        return { ok: false, reason: `Market too thin: ${totalVol.toFixed(0)} total shares` };

    return {
        ok: true,
        stats: {
            spreadPct:      (spreadPct * 100).toFixed(1),
            askDepth:       askDepth.toFixed(2),
            viableBidDepth: viableBidDepth.toFixed(2),
            totalVol:       totalVol.toFixed(0),
            sharesNeeded:   sharesNeeded.toFixed(2)
        }
    };
}

// ─────────────────────────────────────────────────────────
// SCAN BOTH SIDES
//
// sideFilter (optional): 'YES' | 'NO'
//   When provided, only that side is evaluated. Used to enforce
//   same-direction rule — both slots must trade the same side.
//
// FIX: Each side is evaluated independently. A liquidity failure
//   on one side no longer blocks the other side from being checked.
// ─────────────────────────────────────────────────────────
async function scanForEntry(clobClient, YES_TOKEN_ID, NO_TOKEN_ID, priceCeiling, sideFilter = null) {
    let yesBook, noBook;
    try {
        [yesBook, noBook] = await Promise.all([
            clobClient.getOrderBook(YES_TOKEN_ID),
            clobClient.getOrderBook(NO_TOKEN_ID)
        ]);
    } catch (err) {
        console.error('[SCAN ERROR] Failed to fetch orderbooks:', err?.message || err);
        return null;
    }

    const candidates  = [];
    const rejections  = [];

    // ── YES side ──────────────────────────────────────────
    // Skip entirely if caller locked us to NO only
    if (!sideFilter || sideFilter === 'YES') {
        if (!yesBook || !Array.isArray(yesBook.asks) || yesBook.asks.length === 0) {
            rejections.push({ side: 'YES', reason: 'Invalid / empty YES orderbook', bestAsk: NaN });
        } else {
            const yesBestAsk = parseFloat(yesBook.asks[0].price);
            if (Number.isFinite(yesBestAsk) && yesBestAsk <= priceCeiling) {
                const liq = checkLiquidity(yesBook, yesBestAsk);
                if (liq.ok) {
                    candidates.push({ side: 'YES', bestAsk: yesBestAsk, liq });
                } else {
                    rejections.push({ side: 'YES', reason: liq.reason, bestAsk: yesBestAsk });
                }
            }
        }
    }

    // ── NO side ───────────────────────────────────────────
    // Skip entirely if caller locked us to YES only
    if (!sideFilter || sideFilter === 'NO') {
        if (!noBook || !Array.isArray(noBook.asks) || noBook.asks.length === 0) {
            rejections.push({ side: 'NO', reason: 'Invalid / empty NO orderbook', bestAsk: NaN });
        } else {
            const noBestAsk = parseFloat(noBook.asks[0].price);
            if (Number.isFinite(noBestAsk) && noBestAsk <= priceCeiling) {
                const liq = checkLiquidity(noBook, noBestAsk);
                if (liq.ok) {
                    candidates.push({ side: 'NO', bestAsk: noBestAsk, liq });
                } else {
                    rejections.push({ side: 'NO', reason: liq.reason, bestAsk: noBestAsk });
                }
            }
        }
    }

    // Return the cheaper qualifying side (if any)
    if (candidates.length) {
        candidates.sort((a, b) => a.bestAsk - b.bestAsk);
        return candidates[0];
    }

    // No candidate — surface the most relevant rejection for logging
    if (rejections.length) {
        return { ...rejections[0], rejected: true };
    }

    return null; // Nothing in range
}

// ─────────────────────────────────────────────────────────
// BUILD POSITION
// ─────────────────────────────────────────────────────────
function buildPosition(scan, slot, YES_TOKEN_ID, NO_TOKEN_ID) {
    const fee    = PAPER_BET_SIZE * FEE_RATE;
    const shares = (PAPER_BET_SIZE - fee) / scan.bestAsk;
    return {
        slot,
        side:       scan.side,
        tokenId:    scan.side === 'YES' ? YES_TOKEN_ID : NO_TOKEN_ID,
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
    if (bestBid >= position.takeProfit)
        return { trigger: true, reason: `TAKE PROFIT — $${bestBid} >= $${position.takeProfit} (entry + 5¢)` };

    if (secondsLeft <= GRACE_PERIOD_END)
        return {
            trigger: true,
            reason: bestBid >= STOP_LOSS
                ? `GRACE ENDED — above stop loss ($${bestBid})`
                : `GRACE ENDED — below stop loss ($${bestBid} < $${STOP_LOSS})`
        };

    return null;
}

// ─────────────────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────────────────
async function runPaperTrader() {
    console.log("Booting dynamic two-slot paper engine...");
    console.log(`  Slot 1 entry ceiling : $${ENTRY_PRICE_MAX}`);
    console.log(`  Slot 2 entry ceiling : $${ENTRY_PRICE_SECOND}`);
    console.log(`  Take profit          : entry + ${TAKE_PROFIT_CENTS * 100}¢`);
    console.log(`  Stop loss            : $${STOP_LOSS}\n`);

    const clobClient = new ClobClient('https://clob.polymarket.com', 137);

    let YES_TOKEN_ID  = null;
    let NO_TOKEN_ID   = null;
    let currentMarket = null;
    let marketEndTime = null;

    let position1  = null;
    let position2  = null;
    let grace1     = false;
    let grace2     = false;
    let secondsLeft = 0;

    let isLoadingMarket = false;

    // ── MARKET LOADER ──
    async function loadMarket() {
        if (isLoadingMarket) return false;
        isLoadingMarket = true;

        try {
            const tokens = await getActiveMarketTokens(clobClient);
            if (!tokens) {
                console.log('[MARKET] No active BTC 5-min market found — retrying in 30s');
                return false;
            }

            YES_TOKEN_ID  = tokens.yesToken;
            NO_TOKEN_ID   = tokens.noToken;
            currentMarket = tokens.question;
            marketEndTime = new Date(tokens.endDate).getTime();

            position1 = position2 = null;
            grace1    = grace2    = false;

            const now = Date.now();
            secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));

            console.log(`\n[MARKET] Loaded: ${currentMarket}`);
            console.log(`  YES token: ${YES_TOKEN_ID}`);
            console.log(`  NO  token: ${NO_TOKEN_ID}`);
            console.log(`  Expires  : ${tokens.endDate}\n`);
            return true;
        } finally {
            isLoadingMarket = false;
        }
    }

    const loaded = await loadMarket();
    if (!loaded) {
        console.log('[BOOT] No market available on boot. Will retry every 30s.');
        setInterval(async () => {
            if (!YES_TOKEN_ID) await loadMarket();
        }, 30000);
    }

    setInterval(async () => {
        try {
            if (!YES_TOKEN_ID) return;

            if (marketEndTime) {
                const now = Date.now();
                secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));
            }

            // ── MARKET EXPIRED — load the next one ──
            if (secondsLeft <= 0) {
                console.log('\n[MARKET] Timer expired — searching for next market...');

                if (position1) {
                    logTrade({
                        event: 'MARKET_EXPIRED',
                        slot: 1,
                        side: position1.side,
                        entryPrice: position1.entryPrice,
                        status: 'ABANDONED'
                    });
                }
                if (position2) {
                    logTrade({
                        event: 'MARKET_EXPIRED',
                        slot: 2,
                        side: position2.side,
                        entryPrice: position2.entryPrice,
                        status: 'ABANDONED'
                    });
                }

                const ok = await loadMarket();
                if (!ok) {
                    YES_TOKEN_ID = null;
                    setTimeout(async () => {
                        const retry = await loadMarket();
                        if (!retry) console.log('[MARKET] Still no market — waiting...');
                    }, 30000);
                }
                return;
            }

            // ── ENTRY WINDOW ──────────────────────────────────────────────
            if (secondsLeft >= ENTRY_TIME) {

                // ── SLOT 1 ────────────────────────────────────────────────
                // Re-entry is allowed after a TP exit (position1 becomes null).
                // If slot 2 is still open from a previous cycle, lock slot 1
                // to the same side so we never hold opposing directions.
                if (!position1) {
                    // FIX: enforce same side as slot 2 when slot 2 is open
                    const sideFilter = position2 ? position2.side : null;

                    const scan = await scanForEntry(
                        clobClient, YES_TOKEN_ID, NO_TOKEN_ID,
                        ENTRY_PRICE_MAX,
                        sideFilter   // null = free to pick either side
                    );

                    if (!scan) {
                        process.stdout.write(`\r[SCAN S1] Time: ${secondsLeft}s | No setup at <= $${ENTRY_PRICE_MAX}${sideFilter ? ` (${sideFilter} only)` : ''}   `);
                    } else if (scan.rejected) {
                        process.stdout.write(`\r[SCAN S1] Time: ${secondsLeft}s | ${scan.side} $${scan.bestAsk} — LIQ FAIL: ${scan.reason}   `);
                    } else {
                        position1 = buildPosition(scan, 1, YES_TOKEN_ID, NO_TOKEN_ID);
                        console.log(`\n[SLOT 1 ENTRY] ${position1.side} | Entry: $${position1.entryPrice} | TP: $${position1.takeProfit} | Shares: ${position1.shares.toFixed(4)} | Time: ${secondsLeft}s`);
                        console.log(`[LIQ] Spread: ${scan.liq.stats.spreadPct}% | Ask: ${scan.liq.stats.askDepth} | Bids: ${scan.liq.stats.viableBidDepth} | Vol: ${scan.liq.stats.totalVol}`);

                        logTrade({
                            event: 'ENTRY',
                            slot: 1,
                            market: currentMarket,
                            side: position1.side,
                            entryPrice: position1.entryPrice,
                            shares: parseFloat(position1.shares.toFixed(4)),
                            cost: position1.cost
                        });
                    }
                }

                // ── SLOT 2 ────────────────────────────────────────────────
                // Opens only when:
                //   a) slot 1 is already filled (position1 is not null)
                //   b) slot 2 is not already filled (re-entry allowed after TP)
                //   c) best ask has dropped strictly below slot 1 entry
                //   d) best ask is within the $0.25 ceiling
                //   e) SAME SIDE as slot 1 (sideFilter = position1.side)
                if (position1 && !position2) {
                    const scan = await scanForEntry(
                        clobClient, YES_TOKEN_ID, NO_TOKEN_ID,
                        ENTRY_PRICE_SECOND,
                        position1.side   // FIX: always lock to slot 1's side
                    );

                    if (scan && !scan.rejected && scan.bestAsk < position1.entryPrice) {
                        position2 = buildPosition(scan, 2, YES_TOKEN_ID, NO_TOKEN_ID);
                        console.log(`\n[SLOT 2 ENTRY] ${position2.side} | Entry: $${position2.entryPrice} | TP: $${position2.takeProfit} | Shares: ${position2.shares.toFixed(4)} | Time: ${secondsLeft}s`);
                        console.log(`[LIQ] Spread: ${scan.liq.stats.spreadPct}% | Ask: ${scan.liq.stats.askDepth} | Bids: ${scan.liq.stats.viableBidDepth} | Vol: ${scan.liq.stats.totalVol}`);

                        logTrade({
                            event: 'ENTRY',
                            slot: 2,
                            market: currentMarket,
                            side: position2.side,
                            entryPrice: position2.entryPrice,
                            shares: parseFloat(position2.shares.toFixed(4)),
                            cost: position2.cost
                        });
                    }
                }

            } else {
                if (!position1 && !position2)
                    process.stdout.write(`\rEntry window closed. Time: ${secondsLeft}s   `);
            }

            // ── EXIT — checked every tick for each open slot ──────────────
            for (const [getPos, setPos, getGrace, setGrace] of [
                [() => position1, p => { position1 = p; }, () => grace1, v => { grace1 = v; }],
                [() => position2, p => { position2 = p; }, () => grace2, v => { grace2 = v; }]
            ]) {
                const pos = getPos();
                if (!pos) continue;

                let orderbook;
                try {
                    orderbook = await clobClient.getOrderBook(pos.tokenId);
                } catch (err) {
                    console.error(`[S${pos.slot} ERROR] Failed to fetch orderbook:`, err?.message || err);
                    continue;
                }

                if (!orderbook || !Array.isArray(orderbook.bids) || orderbook.bids.length === 0) {
                    process.stdout.write(`\r[S${pos.slot}] No bid data yet...   `);
                    continue;
                }

                const bestBid = parseFloat(orderbook.bids[0].price);

                if (!Number.isFinite(bestBid)) {
                    process.stdout.write(`\r[S${pos.slot}] No bid data yet...   `);
                    continue;
                }

                // 2-min grace notification
                if (secondsLeft <= TWO_MIN_MARK && !getGrace()) {
                    setGrace(true);
                    console.log(bestBid >= STOP_LOSS
                        ? `\n[GRACE S${pos.slot}] 2-min mark | $${bestBid} above stop loss`
                        : `\n[GRACE S${pos.slot}] 2-min mark | $${bestBid} BELOW stop loss`
                    );
                }

                const exit = checkExit(pos, bestBid, secondsLeft);
                if (exit) {
                    const gross  = pos.shares * bestBid;
                    const fee    = gross * FEE_RATE;
                    const pnl    = (gross - fee) - pos.cost;
                    const pnlPct = ((pnl / pos.cost) * 100).toFixed(2);

                    console.log(`\n[SLOT ${pos.slot} EXIT] ${exit.reason}`);
                    console.log(`  Side      : ${pos.side}`);
                    console.log(`  Time Left : ${secondsLeft}s`);
                    console.log(`  Entry     : $${pos.entryPrice}  |  TP was: $${pos.takeProfit}`);
                    console.log(`  Exit Bid  : $${bestBid}`);
                    console.log(`  Gross     : $${gross.toFixed(4)}`);
                    console.log(`  Fee       : $${fee.toFixed(4)}`);
                    console.log(`  Net PnL   : $${pnl.toFixed(4)} (${pnlPct}%)\n`);

                    logTrade({
                        event: 'EXIT',
                        slot: pos.slot,
                        market: currentMarket,
                        side: pos.side,
                        entryPrice: pos.entryPrice,
                        exitPrice: bestBid,
                        shares: parseFloat(pos.shares.toFixed(4)),
                        gross: parseFloat(gross.toFixed(4)),
                        fee: parseFloat(fee.toFixed(4)),
                        pnl: parseFloat(pnl.toFixed(4)),
                        pnlPercent: parseFloat(pnlPct),
                        reason: exit.reason
                    });

                    // Null out the slot — re-entry is now possible next tick
                    // if the entry window is still open and conditions are met
                    setPos(null);
                    setGrace(false);
                } else {
                    const inGrace = secondsLeft <= TWO_MIN_MARK;
                    process.stdout.write(`\r[S${pos.slot} ${inGrace ? 'GRACE' : 'HOLD'}] ${pos.side} | Time: ${secondsLeft}s | Bid: $${bestBid} | TP: $${pos.takeProfit}   `);
                }
            }

        } catch (err) {
            console.error('[TICK ERROR]', err?.message || err);
        }
    }, 1000);
}

runPaperTrader().catch(err => {
    console.error('[FATAL]', err);
    process.exit(1);
});
