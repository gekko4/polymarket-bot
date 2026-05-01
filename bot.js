const { ClobClient } = require('@polymarket/clob-client');

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

// ─────────────────────────────────────────────────────────
// MARKET DISCOVERY
// Finds the currently active BTC 5-min market and returns
// its YES and NO token IDs automatically — no hardcoding
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

    // Pick the market closest to expiry (most urgent / currently running)
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
// Checks spread, ask-side fill depth, and viable bid depth
// ─────────────────────────────────────────────────────────
function checkLiquidity(orderbook, entryPrice) {
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
// Returns whichever side (YES/NO) qualifies under priceCeiling
// ─────────────────────────────────────────────────────────
async function scanForEntry(clobClient, YES_TOKEN_ID, NO_TOKEN_ID, priceCeiling) {
    const [yesBook, noBook] = await Promise.all([
        clobClient.getOrderBook(YES_TOKEN_ID),
        clobClient.getOrderBook(NO_TOKEN_ID)
    ]);

    const yesBestAsk = parseFloat(yesBook.asks[0]?.price);
    const noBestAsk  = parseFloat(noBook.asks[0]?.price);

    if (yesBestAsk <= priceCeiling) {
        const liq = checkLiquidity(yesBook, yesBestAsk);
        if (liq.ok) return { side: 'YES', bestAsk: yesBestAsk, liq };
        return { side: 'YES', rejected: true, reason: liq.reason, bestAsk: yesBestAsk };
    }

    if (noBestAsk <= priceCeiling) {
        const liq = checkLiquidity(noBook, noBestAsk);
        if (liq.ok) return { side: 'NO', bestAsk: noBestAsk, liq };
        return { side: 'NO', rejected: true, reason: liq.reason, bestAsk: noBestAsk };
    }

    return null;
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

    let position1  = null;
    let position2  = null;
    let grace1     = false;
    let grace2     = false;
    let secondsLeft = 0;   // starts at 0 — loadMarket sets it

    // ── MARKET LOADER ──
    async function loadMarket() {
        const tokens = await getActiveMarketTokens(clobClient);
        if (!tokens) {
            console.log('[MARKET] No active BTC 5-min market found — retrying in 30s');
            return false;
        }

        YES_TOKEN_ID  = tokens.yesToken;
        NO_TOKEN_ID   = tokens.noToken;
        currentMarket = tokens.question;

        position1 = position2 = null;
        grace1    = grace2    = false;
        secondsLeft = 220;

        console.log(`\n[MARKET] Loaded: ${currentMarket}`);
        console.log(`  YES token: ${YES_TOKEN_ID}`);
        console.log(`  NO  token: ${NO_TOKEN_ID}`);
        console.log(`  Expires  : ${tokens.endDate}\n`);
        return true;
    }

    // Boot — load first market before starting the tick loop
    const loaded = await loadMarket();
    if (!loaded) {
        console.log('[BOOT] No market available on boot. Will retry every 30s.');
        setInterval(async () => {
            if (!YES_TOKEN_ID) await loadMarket();
        }, 30000);
    }

    setInterval(async () => {
        try {
            // No market loaded yet — wait
            if (!YES_TOKEN_ID) return;

            secondsLeft--;

            // ── MARKET EXPIRED — load the next one ──
            if (secondsLeft <= 0) {
                console.log('\n[MARKET] Timer expired — searching for next market...');
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

            // ── ENTRY WINDOW ──
            if (secondsLeft >= ENTRY_TIME) {

                // Slot 1 — enter at <= $0.35
                if (!position1) {
                    const scan = await scanForEntry(clobClient, YES_TOKEN_ID, NO_TOKEN_ID, ENTRY_PRICE_MAX);
                    if (!scan) {
                        process.stdout.write(`\r[SCAN S1] Time: ${secondsLeft}s | No setup at <= $${ENTRY_PRICE_MAX}   `);
                    } else if (scan.rejected) {
                        process.stdout.write(`\r[SCAN S1] Time: ${secondsLeft}s | ${scan.side} $${scan.bestAsk} — LIQ FAIL: ${scan.reason}   `);
                    } else {
                        position1 = buildPosition(scan, 1, YES_TOKEN_ID, NO_TOKEN_ID);
                        console.log(`\n[SLOT 1 ENTRY] ${position1.side} | Entry: $${position1.entryPrice} | TP: $${position1.takeProfit} | Shares: ${position1.shares.toFixed(4)} | Time: ${secondsLeft}s`);
                        console.log(`[LIQ] Spread: ${scan.liq.stats.spreadPct}% | Ask: ${scan.liq.stats.askDepth} | Bids: ${scan.liq.stats.viableBidDepth} | Vol: ${scan.liq.stats.totalVol}`);
                    }
                }

                // Slot 2 — only opens if:
                //   a) slot 1 is already filled
                //   b) price has dropped further to <= $0.25
                //   c) price is strictly lower than slot 1 entry (no double-entry at same level)
                if (position1 && !position2) {
                    const scan = await scanForEntry(clobClient, YES_TOKEN_ID, NO_TOKEN_ID, ENTRY_PRICE_SECOND);
                    if (scan && !scan.rejected && scan.bestAsk < position1.entryPrice) {
                        position2 = buildPosition(scan, 2, YES_TOKEN_ID, NO_TOKEN_ID);
                        console.log(`\n[SLOT 2 ENTRY] ${position2.side} | Entry: $${position2.entryPrice} | TP: $${position2.takeProfit} | Shares: ${position2.shares.toFixed(4)} | Time: ${secondsLeft}s`);
                    }
                }

            } else {
                if (!position1 && !position2)
                    process.stdout.write(`\rEntry window closed. Time: ${secondsLeft}s   `);
            }

            // ── EXIT — checked every tick for each open slot ──
            for (const [getPos, setPos, getGrace, setGrace] of [
                [() => position1, p => { position1 = p; }, () => grace1, v => { grace1 = v; }],
                [() => position2, p => { position2 = p; }, () => grace2, v => { grace2 = v; }]
            ]) {
                const pos = getPos();
                if (!pos) continue;

                const orderbook = await clobClient.getOrderBook(pos.tokenId);
                const bestBid   = parseFloat(orderbook.bids[0]?.price);

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

                    setPos(null);
                    setGrace(false);
                } else {
                    const inGrace = secondsLeft <= TWO_MIN_MARK;
                    process.stdout.write(`\r[S${pos.slot} ${inGrace ? 'GRACE' : 'HOLD'}] ${pos.side} | Time: ${secondsLeft}s | Bid: $${bestBid} | TP: $${pos.takeProfit}   `);
                }
            }

        } catch (err) {
            // Absorb API stutters silently
        }
    }, 1000);
}

runPaperTrader();