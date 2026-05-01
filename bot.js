// ─────────────────────────────────────────
// 1. ENTRY LOGIC
//    Enter at ANY point while >= 3:30 remains
//    (secondsLeft >= 210 means at least 3.5 mins left)
// ─────────────────────────────────────────
if (!position) {
    if (secondsLeft >= ENTRY_TIME) {
        // Clock has enough time — check price
        if (bestAsk <= ENTRY_PRICE_MAX) {
            // Check liquidity before committing
            const liq = checkLiquidity(orderbook, bestAsk);

            if (!liq.ok) {
                process.stdout.write(`\rScanning... Time Left: ${secondsLeft}s | Ask: $${bestAsk} | LIQ FAIL: ${liq.reason}   `);
            } else {
                const fee    = PAPER_BET_SIZE * FEE_RATE;
                const shares = (PAPER_BET_SIZE - fee) / bestAsk;
                position = { entryPrice: bestAsk, shares, cost: PAPER_BET_SIZE };
                console.log(`\n[ENTRY] Bought ${shares.toFixed(4)} shares at $${bestAsk} | Time Left: ${secondsLeft}s`);
                console.log(`[LIQ] Spread: ${liq.stats.spreadPct}% | Ask depth: ${liq.stats.askDepth} | Bid depth: ${liq.stats.bidDepth}`);
            }
        } else {
            process.stdout.write(`\rScanning... Time Left: ${secondsLeft}s | Ask: $${bestAsk} (waiting for <= $${ENTRY_PRICE_MAX})   `);
        }
    } else {
        // Clock is below 3:30 — entry window is permanently closed for this cycle
        process.stdout.write(`\rNo entry. Time Left: ${secondsLeft}s | Entry window closed at 210s   `);
    }
}
