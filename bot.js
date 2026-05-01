const { ClobClient } = require('@polymarket/clob-client');

// --- STRATEGY CONFIG ---
const ENTRY_PRICE_MAX        = 0.35;
const TAKE_PROFIT_MIN        = 0.38;
const STOP_LOSS              = 0.25;
const ENTRY_TIME             = 210;
const TWO_MIN_MARK           = 120;
const GRACE_PERIOD_END       = 110;
const PAPER_BET_SIZE         = 1.00;
const FEE_RATE               = 0.015;

// --- LIQUIDITY THRESHOLDS ---
const MAX_SPREAD_PERCENT     = 0.08;   // Max 8% spread (tighter than before)
const MIN_ASK_DEPTH_MULT     = 3;      // Ask side must have 3x your share count at your entry price
const MIN_BID_DEPTH_MULT     = 5;      // Bid side must have 5x your shares AT VIABLE PRICES (>= stop loss)
const MIN_VIABLE_BID         = 0.20;   // Only count bids at $0.20 or above as "real" exit liquidity
const MIN_TOTAL_MARKET_VOL   = 50;     // Total shares across both sides must exceed this — low volume market = skip

// --- TOKEN IDS ---
// Each Polymarket market has a YES token and a NO token
// Fill in both — the bot will pick whichever side has a valid setup
const YES_TOKEN_ID = "2174263314346390629056905015582624153306716859246361910696144865184236377227";
const NO_TOKEN_ID  = "YOUR_NO_TOKEN_ID_HERE"; // Get this from the Polymarket market page

// ─────────────────────────────────────────────────────────
// LIQUIDITY CHECK
// Checks spread, ask-side fill depth, and VIABLE bid-side exit depth
// ─────────────────────────────────────────────────────────
function checkLiquidity(orderbook, entryPrice, direction) {
    const asks = orderbook.asks.map(a => ({ price: parseFloat(a.price), size: parseFloat(a.size) }));
    const bids = orderbook.bids.map(b => ({ price: parseFloat(b.price), size: parseFloat(b.size) }));

    if (!asks.length || !bids.length) {
        return { ok: false, reason: "Empty orderbook" };
    }

    const bestAsk = asks[0].price;
    const bestBid = bids[0].price;

    // Check 1: Spread
    const spread    = bestAsk - bestBid;
    const spreadPct = spread / bestAsk;
    if (spreadPct > MAX_SPREAD_PERCENT) {
        return { ok: false, reason: `Spread ${(spreadPct * 100).toFixed(1)}% > ${MAX_SPREAD_PERCENT * 100}% max` };
    }

    const fee          = PAPER_BET_SIZE * FEE_RATE;
    const sharesNeeded = (PAPER_BET_SIZE - fee) / entryPrice;

    // Check 2: Ask-side depth at or near your entry price
    const askDepthAtEntry = asks
        .filter(a => a.price <= entryPrice * 1.02) // within 2% of entry
        .reduce((sum, a) => sum + a.size, 0);

    if (askDepthAtEntry < sharesNeeded * MIN_ASK_DEPTH_MULT) {
        return {
            ok: false,
            reason: `Ask depth too thin: ${askDepthAtEntry.toFixed(2)} shares near entry (need ${(sharesNeeded * MIN_ASK_DEPTH_MULT).toFixed(2)})`
        };
    }

    // Check 3: VIABLE bid depth — only count bids at or above MIN_VIABLE_BID
    // This prevents the bot from thinking it can exit when all the bids are at $0.01
    const viableBidDepth = bids
        .filter(b => b.price >= MIN_VIABLE_BID)
        .reduce((sum, b) => sum + b.size, 0);

    if (viableBidDepth < sharesNeeded * MIN_BID_DEPTH_MULT) {
        return {
            ok: false,
            reason: `Viable bid depth too thin: only ${viableBidDepth.toFixed(2)} shares at >= $${MIN_VIABLE_BID} (need ${(sharesNeeded * MIN_BID_DEPTH_MULT).toFixed(2)})`
        };
    }

    // Check 4: Total market volume gate — both sides combined
    const totalAskVol = asks.reduce((sum, a) => sum + a.size, 0);
    const totalBidVol = bids.reduce((sum, b) => sum + b.size, 0);
    const totalVol    = totalAskVol + totalBidVol;

    if (totalVol < MIN_TOTAL_MARKET_VOL) {
        return {
            ok: false,
            reason: `Market too thin: ${totalVol.toFixed(0)} total shares across both sides (min ${MIN_TOTAL_MARKET_VOL})`
        };
    }

    return {
        ok: true,
        stats: {
            spreadPct:     (spreadPct * 100).toFixed(1),
            askDepth:      askDepthAtEntry.toFixed(2),
            viableBidDepth: viableBidDepth.toFixed(2),
            totalVol:      totalVol.toFixed(0),
            sharesNeeded:  sharesNeeded.toFixed(2)
        }
    };
}

// ─────────────────────────────────────────────────────────
// SCAN BOTH SIDES
// Checks YES and NO tokens each tick and returns whichever
// has a valid entry setup, or null if neither qualifies
// ─────────────────────────────────────────────────────────
async function scanForEntry(clobClient) {
    const [yesBook, noBook] = await Promise.all([
        clobClient.getOrderBook(YES_TOKEN_ID),
        clobClient.getOrderBook(NO_TOKEN_ID)
    ]);

    const yesBestAsk = parseFloat(yesBook.asks[0]?.price);
    const noBestAsk  = parseFloat(noBook.asks[0]?.price);

    // Check YES side
    if (yesBestAsk <= ENTRY_PRICE_MAX) {
        const liq = checkLiquidity(yesBook, yesBestAsk, 'YES');
        if (liq.ok) {
            return { side: 'YES', book: yesBook, bestAsk: yesBestAsk, liq };
        } else {
            return { side: 'YES', rejected: true, reason: liq.reason, bestAsk: yesBestAsk };
        }
    }

    // Check NO side (NO token cheap = YES token expensive, betting it resolves NO)
    if (noBestAsk <= ENTRY_PRICE_MAX) {
        const liq = checkLiquidity(noBook, noBestAsk, 'NO');
        if (liq.ok) {
            return { side: 'NO', book: noBook, bestAsk: noBestAsk, liq };
        } else {
            return { side: 'NO', rejected: true, reason: liq.reason, bestAsk: noBestAsk };
        }
    }

    return null; // Neither side has a valid setup
}

// ─────────────────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────────────────
async function runPaperTrader() {
    console.log("Booting two-sided time-decay paper engine...");
    const clobClient = new ClobClient('https://clob.polymarket.com', 137);

    let position      = null;
    let graceNotified = false;
    let secondsLeft   = 220;

    setInterval(async () => {
        try {
            secondsLeft--;

            // ── ENTRY ──
            if (!position) {
                if (secondsLeft >= ENTRY_TIME) {
                    const scan = await scanForEntry(clobClient);

                    if (!scan) {
                        process.stdout.write(`\rScanning both sides... Time Left: ${secondsLeft}s | No valid setup   `);
                    } else if (scan.rejected) {
                        process.stdout.write(`\rTime Left: ${secondsLeft}s | ${scan.side} ask $${scan.bestAsk} — LIQ FAIL: ${scan.reason}   `);
                    } else {
                        // Valid entry found
                        const fee    = PAPER_BET_SIZE * FEE_RATE;
                        const shares = (PAPER_BET_SIZE - fee) / scan.bestAsk;
                        position = {
                            side:       scan.side,
                            tokenId:    scan.side === 'YES' ? YES_TOKEN_ID : NO_TOKEN_ID,
                            entryPrice: scan.bestAsk,
                            shares,
                            cost:       PAPER_BET_SIZE
                        };
                        console.log(`\n[ENTRY] ${scan.side} side | Bought ${shares.toFixed(4)} shares at $${scan.bestAsk} | Time Left: ${secondsLeft}s`);
                        console.log(`[LIQ]   Spread: ${scan.liq.stats.spreadPct}% | Ask depth: ${scan.liq.stats.askDepth} | Viable bids: ${scan.liq.stats.viableBidDepth} | Market vol: ${scan.liq.stats.totalVol}`);
                    }
                } else {
                    process.stdout.write(`\rNo entry — window closed. Time Left: ${secondsLeft}s   `);
                }
            }

            // ── EXIT (runs every tick while holding) ──
            if (position) {
                // Fetch the book for whichever side we're holding
                const orderbook = await clobClient.getOrderBook(position.tokenId);
                const bestBid   = parseFloat(orderbook.bids[0]?.price);

                let triggerExit = false;
                let exitReason  = "";

                if (bestBid >= TAKE_PROFIT_MIN) {
                    triggerExit = true;
                    exitReason  = `TAKE PROFIT — $${bestBid} >= $${TAKE_PROFIT_MIN}`;
                } else if (secondsLeft <= GRACE_PERIOD_END) {
                    triggerExit = true;
                    exitReason  = bestBid >= STOP_LOSS
                        ? `GRACE ENDED — above stop loss ($${bestBid})`
                        : `GRACE ENDED — below stop loss ($${bestBid} < $${STOP_LOSS})`;
                } else if (secondsLeft <= TWO_MIN_MARK && !graceNotified) {
                    graceNotified = true;
                    console.log(bestBid >= STOP_LOSS
                        ? `\n[GRACE] 2-min mark. $${bestBid} above stop loss — 10s grace started...`
                        : `\n[GRACE] 2-min mark. $${bestBid} BELOW stop loss — 10s to recover...`
                    );
                }

                if (triggerExit) {
                    const gross     = position.shares * bestBid;
                    const fee       = gross * FEE_RATE;
                    const netReturn = gross - fee;
                    const pnl       = netReturn - position.cost;
                    const pnlPct    = ((pnl / position.cost) * 100).toFixed(2);

                    console.log(`\n[EXIT]  ${exitReason}`);
                    console.log(`  Side      : ${position.side}`);
                    console.log(`  Time Left : ${secondsLeft}s`);
                    console.log(`  Entry     : $${position.entryPrice}`);
                    console.log(`  Exit Bid  : $${bestBid}`);
                    console.log(`  Gross     : $${gross.toFixed(4)}`);
                    console.log(`  Fee       : $${fee.toFixed(4)}`);
                    console.log(`  Net PnL   : $${pnl.toFixed(4)} (${pnlPct}%)\n`);

                    position      = null;
                    graceNotified = false;
                    secondsLeft   = 220;
                } else {
                    const inGrace = secondsLeft <= TWO_MIN_MARK;
                    process.stdout.write(`\r[${inGrace ? 'GRACE' : 'HOLDING'}] ${position.side} | Time Left: ${secondsLeft}s | Bid: $${bestBid}   `);
                }
            }

        } catch (err) {
            // Absorb API stutters silently
        }
    }, 1000);
}

runPaperTrader();