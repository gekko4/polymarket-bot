require('dotenv').config();
const { ClobClient, OrderType } = require('@polymarket/clob-client');
const { createWalletClient, http: viemHttp } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { polygon } = require('viem/chains');
const WebSocket = require('ws'); 
const fs = require('fs'); 
const http = require('http'); // Required for the Mobile Web Dashboard

// --- TERMINAL COLORS ---
const colors = {
    reset: "\x1b[0m",
    green: "\x1b[32m",
    red: "\x1b[31m",
    yellow: "\x1b[33m",
    brightYellow: "\x1b[93m",
    cyan: "\x1b[36m",
    magenta: "\x1b[35m",
    gray: "\x1b[90m"
};

// --- SECURITY & AUTH ---
let rawKey = process.env.PRIVATE_KEY;
if (!rawKey) throw new Error(`${colors.red}CRITICAL: PRIVATE_KEY is missing from .env file!${colors.reset}`);
if (!rawKey.startsWith('0x')) rawKey = '0x' + rawKey; 

const CHAIN_ID = 137; 
const HOST = 'https://clob.polymarket.com';

// --- MAKER ARBITRAGE CONFIG ---
const MAKER_BID_PRICE = 0.48;     // Discounted limit bids (must be < 0.50 to make profit)
const SCRATCH_TIME_SEC = 60;      // Cancel unfilled orders and dump single legs at 60s remaining
const BET_SIZE_USD = 1.00;        // Dollar size per leg
const TAKER_FEE_BPS = 180;        // Taker fee is 1.8%. Maker fee is 0%.

// --- PAPER TRADING STATE & STATS ---
let trades = { YES: null, NO: null };
let virtualOrders = { YES: null, NO: null }; // Holds our resting limit bids (explicit states)

let stats = {
    totalTrades: 0,
    wins: 0,
    losses: 0,
    startingBalance: 100.00,
    currentBalance: 100.00
};

let lastMidpoint = { YES: 0, NO: 0 };
let currentPrices = { YES: 0, NO: 0 }; 
let recentTrades = []; 

let isExecuting = { YES: false, NO: false };
let isExiting   = { YES: false, NO: false };
let lastBook    = { YES: null, NO: null };
let isSearchingNextMarket = false;
let searchCooldownTimer = 0; 

let currentYesToken = null;
let currentNoToken = null;
let marketEndTime = 0;

let clobClient;

// --- ASYNC LOGGING STREAMS (ZERO LAG) ---
const tradeLogFile = 'paper_trades_log.csv';
const priceLogFile = 'price_history.csv';

const tradeStream = fs.createWriteStream(tradeLogFile, { flags: 'a' });
const priceStream = fs.createWriteStream(priceLogFile, { flags: 'a' });

const terminalLogFile = 'terminal_logs.txt';
const terminalStream = fs.createWriteStream(terminalLogFile, { flags: 'a' });

const originalLog = console.log;
console.log = function (...args) {
    originalLog.apply(console, args);
    const message = args.map(arg => typeof arg === 'object' ? JSON.stringify(arg) : arg).join(' ');
    const cleanMessage = message.replace(/\x1b\[[0-9;]*m/g, '');
    terminalStream.write(`[${new Date().toISOString()}] ${cleanMessage}\n`);
};

if (!fs.existsSync(tradeLogFile) || fs.statSync(tradeLogFile).size === 0) {
    tradeStream.write("Date,Market,Action,Entry_Price,Exit_Price,Shares,PnL_USD,Balance_USD,Win_Rate_Pct\n");
}

const PRICE_LOG_INTERVAL_MS = 1000; 
const PRICE_PRE_MINUTES = 2;        
const PRICE_POST_MINUTES = 2;       
const PRICE_BUFFER_MAX = Math.max(1, Math.floor((PRICE_PRE_MINUTES * 60 * 1000) / PRICE_LOG_INTERVAL_MS));

let priceBuffer = [];              
let priceCaptureActive = false;    
let priceCaptureUntilTs = 0;       
let priceCaptureLastTickTs = 0;    
let pricePreBufferDumped = false;  

function ensurePriceHeader() {
    if (!fs.existsSync(priceLogFile) || fs.statSync(priceLogFile).size === 0) {
        priceStream.write("Timestamp,YES_Ask,NO_Ask,Phase\n");
    }
}
ensurePriceHeader();

function addToPriceBuffer(ts, yesAsk, noAsk) {
    priceBuffer.push({ ts, yesAsk, noAsk });
    if (priceBuffer.length > PRICE_BUFFER_MAX) priceBuffer.shift();
}

function anyTradeActive() {
    return !!(trades.YES || trades.NO || (virtualOrders.YES && virtualOrders.YES.status === 'PLACED') || (virtualOrders.NO && virtualOrders.NO.status === 'PLACED'));
}

function startPriceCaptureIfNeeded() {
    if (priceCaptureActive) return;
    priceCaptureActive = true;
    pricePreBufferDumped = false;
    priceCaptureUntilTs = 0; 
    console.log(`${colors.cyan}[PRICE LOG] Capture session started.${colors.reset}`);
}

function armPostCaptureWindow() {
    priceCaptureUntilTs = Date.now() + (PRICE_POST_MINUTES * 60 * 1000);
}

function dumpPreBufferOnce() {
    if (pricePreBufferDumped) return;
    pricePreBufferDumped = true;
    if (!priceBuffer.length) return;
    for (const row of priceBuffer) {
        priceStream.write(`${new Date(row.ts).toISOString()},${row.yesAsk.toFixed(3)},${row.noAsk.toFixed(3)},PRE\n`);
    }
}

function maybeLogPriceTick() {
    const now = Date.now();
    if (now - priceCaptureLastTickTs < PRICE_LOG_INTERVAL_MS) return;

    const yes = currentPrices.YES;
    const no  = currentPrices.NO;

    if (!(yes > 0) || !(no > 0)) return;

    addToPriceBuffer(now, yes, no);

    if (anyTradeActive()) startPriceCaptureIfNeeded();

    if (priceCaptureActive) {
        dumpPreBufferOnce();
        let phase = "LIVE";
        if (!anyTradeActive() && priceCaptureUntilTs > 0) phase = "POST";

        priceStream.write(`${new Date(now).toISOString()},${yes.toFixed(3)},${no.toFixed(3)},${phase}\n`);
        priceCaptureLastTickTs = now;

        if (!anyTradeActive() && priceCaptureUntilTs > 0 && now >= priceCaptureUntilTs) {
            priceCaptureActive = false;
            priceCaptureUntilTs = 0;
            pricePreBufferDumped = false;
            priceBuffer = [];
            console.log(`${colors.gray}[PRICE LOG] Capture session ended.${colors.reset}`);
        }
    } else {
        priceCaptureLastTickTs = now;
    }
}

function logCompletedTrade(t, exitReason, exitPrice) {
    // Single canonical place to update stats and logs for a closed leg
    const grossReturn = exitPrice * t.shares;
    const entryCost = t.entryPrice * t.shares;
    
    // Maker entries pay 0 fees. Exits (Taker) pay fees unless ARBITRAGE SETTLED (we treat settlement as fee-free).
    let entryFee = t.isMaker ? 0 : entryCost * (TAKER_FEE_BPS / 10000); 
    let exitFee = (exitReason !== "ARBITRAGE SETTLED") ? (grossReturn * (TAKER_FEE_BPS / 10000)) : 0;
    
    let totalFees = entryFee + exitFee;
    const netPnL = (grossReturn - entryCost) - totalFees;

    // Update stats only here
    stats.totalTrades++;
    if (netPnL > 0) stats.wins++;
    else stats.losses++;

    stats.currentBalance += netPnL;

    const winRate = ((stats.wins / stats.totalTrades) * 100).toFixed(1);
    const roi = (((stats.currentBalance - stats.startingBalance) / stats.startingBalance) * 100).toFixed(2);

    const c = netPnL > 0 ? colors.brightYellow : colors.red;

    console.log(`\n${colors.gray}========================================${colors.reset}`);
    console.log(`[TRADE CLOSED - ${t.side}] Reason: ${c}${exitReason}${colors.reset}`);
    console.log(`Entry: $${t.entryPrice.toFixed(3)} | Exit: $${exitPrice.toFixed(3)}`);
    console.log(`Gross PnL: $${(grossReturn - entryCost).toFixed(4)} | Fees Paid: $${totalFees.toFixed(4)}`);
    console.log(`NET PnL: ${c}$${netPnL > 0 ? '+' : ''}${netPnL.toFixed(4)}${colors.reset}`);
    console.log(`${colors.gray}---${colors.reset}`);
    console.log(`[STATS] Balance: $${stats.currentBalance.toFixed(2)} (${roi > 0 ? '+' : ''}${roi}% ROI)`);
    console.log(`[STATS] Win Rate: ${winRate}% (${stats.wins}W / ${stats.losses}L)`);
    console.log(`${colors.gray}========================================\n${colors.reset}`);

    const logEntry = `${new Date().toISOString()},BTC-5M-${t.side},${exitReason},${t.entryPrice},${exitPrice},${t.shares},${netPnL.toFixed(4)},${stats.currentBalance.toFixed(2)},${winRate}%\n`;
    tradeStream.write(logEntry); 

    recentTrades.unshift({ 
        time: new Date().toLocaleTimeString(), side: t.side,
        reason: exitReason, entry: t.entryPrice.toFixed(3), 
        exit: exitPrice.toFixed(3), pnl: netPnL 
    });
    if (recentTrades.length > 10) recentTrades.pop();

    trades[t.side] = null;
    isExiting[t.side] = false; 
    armPostCaptureWindow();
}

/**
 * Simulated FOK execution for paper trading.
 * - Checks available size (sizeAvailable)
 * - Applies simple slippage model based on depth and requested size
 * - Applies taker fee on fills that are taker exits
 * - Returns { success, sharesFilled, executedPrice }
 */
async function executeFOK(tokenId, price, buySell, marketSide, sizeNeeded, actionLog) {
    if (isExecuting[marketSide]) return { success: false };
    isExecuting[marketSide] = true;

    try {
        const t = trades[marketSide];
        // Determine reference price for share calculation
        const refPrice = buySell === 'BUY' ? price : (t ? t.entryPrice : price);
        const desiredShares = parseFloat((BET_SIZE_USD / refPrice).toFixed(2));

        // sizeNeeded is the book size at that price; if not provided, assume small liquidity
        const availableSize = parseFloat(sizeNeeded) || 0.01;

        // If not enough liquidity, fail or partially fill depending on availableSize
        if (availableSize <= 0) {
            console.log(`${colors.red}[EXEC SIM] No liquidity available for ${marketSide} at $${price.toFixed(3)}.${colors.reset}`);
            return { success: false };
        }

        // If available size smaller than desired, simulate partial fill
        const sharesFilled = Math.min(desiredShares, availableSize);

        // Simple slippage: larger fills suffer more slippage (linear)
        const slippagePct = Math.min(0.01, (sharesFilled / Math.max(0.01, availableSize)) * 0.002); // up to 0.1%
        const executedPrice = buySell === 'BUY' ? price * (1 + slippagePct) : price * (1 - slippagePct);

        // Simulate taker fee for non-maker exits (we'll apply fees in logCompletedTrade)
        console.log(`[PAPER SIMULATION] ${colors.cyan}${actionLog} ${buySell} ${marketSide} @ $${executedPrice.toFixed(3)} (${sharesFilled} shares)${colors.reset}`);
        return { success: true, sharesFilled: parseFloat(sharesFilled.toFixed(2)), executedPrice };

    } catch (err) {
        console.error(`${colors.red}[EXECUTION ERROR]:${colors.reset}`, err.message);
        return { success: false };
    } finally {
        isExecuting[marketSide] = false;
    }
}

function handleMarketUpdate(data) {
    if (!data) return;

    const bestAsk = data.bestAsk;
    const bestAskSize = data.bestAskSize;
    const bestBid = data.bestBid;
    const bestBidSize = data.bestBidSize;
    
    if (isNaN(bestAsk) || isNaN(bestBid)) return;
    
    const tokenId = data.asset_id;
    const side = tokenId === currentYesToken ? 'YES' : (tokenId === currentNoToken ? 'NO' : null);
    if (!side) return; 

    lastBook[side] = { tokenId, bestAsk, bestAskSize, bestBid, bestBidSize, ts: Date.now() };
    currentPrices[side] = bestAsk;

    const now = Date.now();
    const secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));

    // Wait for the initial 30 seconds of the market before placing limits
    if (secondsLeft > 270) return; 

    // ---------------------------------------------------------
    // 1. PLACE MAKER LIMIT ORDERS (explicit order state)
    // ---------------------------------------------------------
    if ((!virtualOrders.YES || virtualOrders.YES.status !== 'PLACED') && (!virtualOrders.NO || virtualOrders.NO.status !== 'PLACED') && !trades.YES && !trades.NO && secondsLeft > SCRATCH_TIME_SEC) {
        const shares = parseFloat((BET_SIZE_USD / MAKER_BID_PRICE).toFixed(2));
        virtualOrders.YES = { side: 'YES', tokenId: currentYesToken, price: MAKER_BID_PRICE, shares: shares, status: 'PLACED', placedAt: now, filled: false };
        virtualOrders.NO = { side: 'NO', tokenId: currentNoToken, price: MAKER_BID_PRICE, shares: shares, status: 'PLACED', placedAt: now, filled: false };
        console.log(`\n${colors.cyan}[MAKER] Placed resting Limit Bids at $${MAKER_BID_PRICE} for YES and NO.${colors.reset}`);
        startPriceCaptureIfNeeded(); 
    }

    // ---------------------------------------------------------
    // 2. CHECK FOR MAKER FILLS (corrected logic)
    // ---------------------------------------------------------
    if (virtualOrders[side] && virtualOrders[side].status === 'PLACED' && !virtualOrders[side].filled) {
        // Correct fill detection:
        // - If bestAsk <= ourBid then someone is offering at or below our bid (we can buy)
        // - If bestBid >= ourBid then someone is bidding at or above our bid (they would have to lift a sell to hit our bid)
        // Use >= for bestBid comparison (previous code used < which was inverted)
        if (bestAsk <= virtualOrders[side].price || bestBid >= virtualOrders[side].price) {
            // Check available liquidity at that price before claiming a fill
            const availableSize = (bestAsk <= virtualOrders[side].price) ? bestAskSize : bestBidSize;
            const desiredShares = virtualOrders[side].shares;

            // If availableSize is less than desiredShares, simulate partial fill
            const filledShares = Math.min(desiredShares, parseFloat(availableSize || 0));
            if (filledShares <= 0) {
                // No real liquidity; do not mark filled
            } else {
                virtualOrders[side].filled = true;
                virtualOrders[side].status = 'FILLED';
                
                trades[side] = { 
                    active: true, side: side, tokenId: virtualOrders[side].tokenId, 
                    entryPrice: virtualOrders[side].price, shares: parseFloat(filledShares.toFixed(2)), 
                    entryTime: now, isMaker: true // 0 Fees for maker entry
                };
                
                console.log(`${colors.green}[FILLED] Maker Bid on ${side} filled at $${virtualOrders[side].price} for ${filledShares} shares!${colors.reset}`);

                if (virtualOrders.YES && virtualOrders.NO && virtualOrders.YES.filled && virtualOrders.NO.filled) {
                    console.log(`\n${colors.brightYellow}[ARBITRAGE LOCKED] Both sides filled at $${MAKER_BID_PRICE}! Risk-free profit locked. Holding to Expiry.${colors.reset}`);
                }
            }
        }
    }

    // ---------------------------------------------------------
    // 3. ADVERSE SELECTION MANAGER ("THE SCRATCH")
    // ---------------------------------------------------------
    if (secondsLeft <= SCRATCH_TIME_SEC && virtualOrders.YES && virtualOrders.NO) {
        const yesFilled = virtualOrders.YES.filled;
        const noFilled = virtualOrders.NO.filled;

        if (yesFilled !== noFilled) { 
            const filledSide = yesFilled ? 'YES' : 'NO';
            const unfilledSide = yesFilled ? 'NO' : 'YES';

            if (!isExiting[filledSide]) {
                console.log(`\n${colors.magenta}[DANGER] ${unfilledSide} never filled! Canceling unfilled order.${colors.reset}`);
                virtualOrders[unfilledSide].status = 'CANCELLED';
                virtualOrders[unfilledSide].filled = false; 
                trades[unfilledSide] = null; 
                
                // Market-sell the side we are stuck holding
                const filledBook = lastBook[filledSide];
                if (filledBook && filledBook.bestBid > 0 && trades[filledSide]) {
                    console.log(`${colors.yellow}[SCRATCH] Dumping ${filledSide} to avoid directional risk into expiry.${colors.reset}`);
                    isExiting[filledSide] = true;
                    executeFOK(trades[filledSide].tokenId, filledBook.bestBid, 'SELL', filledSide, filledBook.bestBidSize, 'SCRATCH EXIT').then(res => {
                        if (res.success) {
                            // Use executedPrice if provided
                            const exitPrice = res.executedPrice || filledBook.bestBid;
                            // Mark as taker exit (isMaker false for exit)
                            trades[filledSide].isMaker = trades[filledSide].isMaker || false;
                            logCompletedTrade(trades[filledSide], "SCRATCH EXIT", exitPrice);
                        } else {
                            isExiting[filledSide] = false;
                        }
                    });
                }
            }
        } 
        else if (!yesFilled && !noFilled) {
            // Neither filled, market was too stable
            if (virtualOrders.YES.status === 'PLACED') { // Just use status as a flag so we only log once
                console.log(`\n${colors.gray}[CANCEL] Time expired. Neither side filled. Canceling both limit orders.${colors.reset}`);
                virtualOrders.YES.status = 'CANCELLED'; 
                virtualOrders.NO.status = 'CANCELLED'; 
                virtualOrders.YES.filled = false; 
                virtualOrders.NO.filled = false; 
                armPostCaptureWindow();
            }
        }
    }
}

function connectWebsocket() {
    if (global.wsMarket) {
        try { global.wsMarket.terminate(); } catch(e) {}
    }

    console.log(`${colors.yellow}[WS] Booting fresh market connection...${colors.reset}`);
    const wsMarket = new WebSocket('wss://ws-subscriptions-clob.polymarket.com/ws/market');
    global.wsMarket = wsMarket; 

    wsMarket.on('open', () => {
        if (currentYesToken && currentNoToken) {
            wsMarket.send(JSON.stringify({ type: "market", assets_ids: [currentYesToken, currentNoToken] }));
        }
    });

    wsMarket.on('message', (msg) => {
        const textMsg = msg.toString();
        if (textMsg === "PONG") return; 
        
        try {
            const data = JSON.parse(textMsg);
            if (data.event_type === 'book' && data.asks.length > 0 && data.bids.length > 0) {
                handleMarketUpdate({
                    asset_id: data.asset_id,
                    bestAsk: parseFloat(data.asks[0].price),
                    bestAskSize: parseFloat(data.asks[0].size),
                    bestBid: parseFloat(data.bids[0].price),
                    bestBidSize: parseFloat(data.bids[0].size)
                });
            } 
            else if (data.event_type === 'price_change' && data.price_changes && data.price_changes.length > 0) {
                for (const pc of data.price_changes) {
                    handleMarketUpdate({
                        asset_id: pc.asset_id,
                        bestAsk: parseFloat(pc.best_ask),
                        bestAskSize: 9999, 
                        bestBid: parseFloat(pc.best_bid),
                        bestBidSize: 9999
                    });
                }
            }
        } catch (err) {}
    });
}

async function loadNextMarket() {
    if (isSearchingNextMarket) return;
    isSearchingNextMarket = true;
    console.log(`\n${colors.yellow}[SCANNER] Calculating the CURRENT active 5-Min BTC Market...${colors.reset}`);
    
    try {
        const nowSec = Math.floor(Date.now() / 1000);
        const remainder = nowSec % 300;
        const currentIntervalStartSec = nowSec - remainder;
        const currentIntervalEndSec = currentIntervalStartSec + 300;
        
        const eventSlug = `btc-updown-5m-${currentIntervalStartSec}`;

        const response = await fetch(`https://gamma-api.polymarket.com/events?slug=${eventSlug}`);
        const events = await response.json();

        if (!events || events.length === 0 || !events[0].markets || events[0].markets.length === 0) {
            console.log(`${colors.gray}[SCANNER] Market ${eventSlug} not fully indexed yet. Retrying in 5s...${colors.reset}`);
            searchCooldownTimer = Date.now() + 5000;
            return;
        }

        const validEvent = events[0];
        const validMarket = validEvent.markets[0]; 

        let parsedTokens = typeof validMarket.clobTokenIds === 'string' 
            ? JSON.parse(validMarket.clobTokenIds) 
            : validMarket.clobTokenIds;

        let yesTokenId = parsedTokens[0];
        let noTokenId = parsedTokens[1];

        if (yesTokenId && noTokenId) {
            currentYesToken = yesTokenId;
            currentNoToken  = noTokenId;
            marketEndTime   = currentIntervalEndSec * 1000; 
            
            lastMidpoint = { YES: 0, NO: 0 };
            currentPrices = { YES: 0, NO: 0 };
            trades = { YES: null, NO: null };
            virtualOrders = { YES: null, NO: null }; // Reset maker state
            isExecuting = { YES: false, NO: false };
            isExiting   = { YES: false, NO: false };
            lastBook    = { YES: null, NO: null };

            console.log(`${colors.brightYellow}[MARKET LOADED] Subscribing to: ${validEvent.title}${colors.reset}`);
            connectWebsocket();

        } else {
            searchCooldownTimer = Date.now() + 5000;
        }
    } catch (err) {
        searchCooldownTimer = Date.now() + 5000; 
    } finally {
        isSearchingNextMarket = false;
    }
}

// --- MOBILE WEB DASHBOARD (ZERO DEPENDENCIES) ---
const path = require('path');

http.createServer((req, res) => {
    if (req.url === '/api/live') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ stats, trades, currentPrices, recentTrades }));
        return;
    }

    fs.readFile(path.join(__dirname, 'dashboard.html'), 'utf8', (err, data) => {
        if (err) {
            res.writeHead(500, { 'Content-Type': 'text/plain' });
            res.end('Error loading dashboard UI. Make sure dashboard.html exists.');
            return;
        }
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(data);
    });
}).listen(3000, '0.0.0.0', () => {
    console.log(`${colors.cyan}[DASHBOARD] Web UI running on port 3000${colors.reset}`);
});

/**
 * Settlement: determine winner and close both legs via logCompletedTrade
 * - We no longer directly mutate stats here; logCompletedTrade handles stats updates.
 * - Winner selection in paper mode: use lastBook bestBid comparison as a deterministic proxy.
 */
function settleArbitrageWin() {
    const tYES = trades['YES'];
    const tNO = trades['NO'];

    if (tYES && tNO) {
        // Determine winner deterministically from lastBook snapshot
        let winner = 'YES';
        if (lastBook.YES && lastBook.NO) {
            winner = (lastBook.YES.bestBid > lastBook.NO.bestBid) ? 'YES' : 'NO';
        } else if (currentPrices.YES && currentPrices.NO) {
            winner = (currentPrices.YES > currentPrices.NO) ? 'YES' : 'NO';
        }

        const payoutPrice = 1.00;

        // Close both legs at payoutPrice; logCompletedTrade will update stats and balance
        // Mark both trades as settled (no exit fees per original assumption)
        logCompletedTrade(tYES, "ARBITRAGE SETTLED", payoutPrice);
        logCompletedTrade(tNO, "ARBITRAGE SETTLED", payoutPrice);

        console.log(`\n${colors.brightYellow}========================================${colors.reset}`);
        console.log(`[ARBITRAGE SETTLED] Market Expired! Pair successfully held.`);
        console.log(`Payout: $${payoutPrice.toFixed(2)} per winning share`);
        console.log(`${colors.gray}---${colors.reset}`);
        console.log(`[STATS] Balance: $${stats.currentBalance.toFixed(2)}`);
        console.log(`${colors.gray}========================================\n${colors.reset}`);

        // Reset state
        trades = { YES: null, NO: null };
        virtualOrders = { YES: null, NO: null };
        armPostCaptureWindow();
    }
}

async function runLiveTrader() {
    console.log(`${colors.magenta}Booting MAKER ARBITRAGE Engine in PAPER TRADING MODE...${colors.reset}`);
    
    const account = privateKeyToAccount(rawKey);
    const walletClient = createWalletClient({ account, chain: polygon, transport: viemHttp() });
    clobClient = new ClobClient(HOST, CHAIN_ID, walletClient);

    let creds;
    try { creds = await clobClient.deriveApiKey(); } 
    catch (e) { creds = await clobClient.createApiKey(); }

    await loadNextMarket();

    setInterval(async () => {
        if (marketEndTime === 0) {
            if (Date.now() > searchCooldownTimer) loadNextMarket();
            return;
        }

        const now = Date.now();
        // Check if market expired!
        if (now >= marketEndTime && !isSearchingNextMarket) {
            if (trades.YES && trades.NO) {
                settleArbitrageWin(); // We successfully held both to expiry
            } else {
                // If we are partially filled, attempt to exit remaining positions
                if (trades.YES && !trades.NO && lastBook.YES) {
                    const book = lastBook.YES;
                    if (!isExiting.YES) {
                        isExiting.YES = true;
                        executeFOK(trades.YES.tokenId, book.bestBid, 'SELL', 'YES', book.bestBidSize, 'EXPIRY EXIT').then(res => {
                            if (res.success) {
                                const exitPrice = res.executedPrice || book.bestBid;
                                logCompletedTrade(trades.YES, "EXPIRY EXIT", exitPrice);
                            } else isExiting.YES = false;
                        });
                    }
                }
                if (trades.NO && !trades.YES && lastBook.NO) {
                    const book = lastBook.NO;
                    if (!isExiting.NO) {
                        isExiting.NO = true;
                        executeFOK(trades.NO.tokenId, book.bestBid, 'SELL', 'NO', book.bestBidSize, 'EXPIRY EXIT').then(res => {
                            if (res.success) {
                                const exitPrice = res.executedPrice || book.bestBid;
                                logCompletedTrade(trades.NO, "EXPIRY EXIT", exitPrice);
                            } else isExiting.NO = false;
                        });
                    }
                }
            }
            loadNextMarket();
        }

        maybeLogPriceTick();

        if (Math.floor(now / 1000) % 10 === 0) {
            let status = 'WAITING FOR MAKER FILLS';
            if (trades.YES && trades.NO) status = `${colors.brightYellow}LOCKED HEDGE - WAITING FOR EXPIRY${colors.reset}`;
            else if (trades.YES) status = `HOLDING YES - WAITING FOR NO`;
            else if (trades.NO) status = `HOLDING NO - WAITING FOR YES`;
            
            console.log(`${colors.cyan}[LIVE] Status: ${status} | Balance: $${stats.currentBalance.toFixed(2)} | YES Ask: $${currentPrices.YES.toFixed(3)} | NO Ask: $${currentPrices.NO.toFixed(3)}${colors.reset}`);
        }
    }, 250);
}

runLiveTrader().catch(console.error);
