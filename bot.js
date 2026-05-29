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
    brightYellow: "\x1b[93m", // Striking Gold/Yellow for Parrot OS visibility
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

// --- STRIKE PROXIMITY CONFIG ---
const ENTRY_VOLATILITY_THRESHOLD = 0.95; 
const MAX_ALLOWED_SPREAD = 0.02; // Prevents entering on bad spreads

// --- EXIT / BAILOUT TUNING (LOOSENED PER REQUEST) ---
const MAX_EXIT_SPREAD = 0.08;    // Loosened from 0.04 to allow exits when MMs widen spread
const COLLAPSE_THRESHOLD = 0.10; // Trigger panic handling earlier (not heavily used here but available)
const COLLAPSE_WINDOW_SEC = 25;  // Wider detection window for collapse logic
const MIN_BID_SIZE_FOR_EXIT = 0.15; // Allow exits into thinner bids during crash

// Wider TP to let winners run; SL kept conservative
const MIN_TP_CENTS = 0.06;       // Raised base TP to 6 cents
const MAX_TP_CENTS = 0.24;       // Let winners run up to 18 cents in high volatility
const MIN_SL_CENTS = 0.03; 
const MAX_SL_CENTS = 0.08; 

const BET_SIZE_USD = 1.00;   
const TAKER_FEE_BPS = 180; 

// --- GLOBAL ABSOLUTE EXIT LEVELS (locked at entry)
const GLOBAL_TP_PRICE = 0.713; // absolute take profit price for both sides
const GLOBAL_SL_PRICE = 0.402; // absolute stop loss price for both sides

// --- PAPER TRADING STATE & STATS ---
let trades = { YES: null, NO: null };

let stats = {
    totalTrades: 0,
    wins: 0,
    losses: 0,
    startingBalance: 100.00,
    currentBalance: 100.00
};

let lastMidpoint = { YES: 0, NO: 0 };
let trend = { YES: 0, NO: 0 }; 
let currentPrices = { YES: 0, NO: 0 }; 
let recentTrades = []; // Keeps track of dashboard history

let isExecuting = { YES: false, NO: false };
let isExiting   = { YES: false, NO: false };
let lastBook    = { YES: null, NO: null };
let isSearchingNextMarket = false;
let searchCooldownTimer = 0; 
let postTradeCooldown = 0; // Timer to prevent immediate re-entry after losses

let currentYesToken = null;
let currentNoToken = null;
let marketEndTime = 0;

let clobClient;

// --- ASYNC LOGGING STREAMS (ZERO LAG) ---
const tradeLogFile = 'paper_trades_log.csv';
const priceLogFile = 'price_history.csv';

const tradeStream = fs.createWriteStream(tradeLogFile, { flags: 'a' });
const priceStream = fs.createWriteStream(priceLogFile, { flags: 'a' });

// --- ZERO-LAG TERMINAL LOGGING ---
const terminalLogFile = 'terminal_logs.txt';
const terminalStream = fs.createWriteStream(terminalLogFile, { flags: 'a' });

const originalLog = console.log;
console.log = function (...args) {
    originalLog.apply(console, args);
    const message = args.map(arg => typeof arg === 'object' ? JSON.stringify(arg) : arg).join(' ');
    // Strip ANSI colors so the text file remains clean and readable
    const cleanMessage = message.replace(/\x1b\[[0-9;]*m/g, '');
    // Write asynchronously (Non-Blocking)
    terminalStream.write(`[${new Date().toISOString()}] ${cleanMessage}\n`);
};

if (!fs.existsSync(tradeLogFile) || fs.statSync(tradeLogFile).size === 0) {
    tradeStream.write("Date,Market,Action,Entry_Price,Exit_Price,Shares,PnL_USD,Balance_USD,Win_Rate_Pct\n");
}
if (!fs.existsSync(priceLogFile) || fs.statSync(priceLogFile).size === 0) {
    priceStream.write("Timestamp,YES_Ask,NO_Ask\n");
}

function logCompletedTrade(t, exitReason, exitPrice) {
    const grossReturn = exitPrice * t.shares;
    const entryCost = t.entryPrice * t.shares;
    
    let totalFees = entryCost * (TAKER_FEE_BPS / 10000); 
    if (exitReason !== "TAKE PROFIT") {
        totalFees += (grossReturn * (TAKER_FEE_BPS / 10000));
    }

    const netPnL = (grossReturn - entryCost) - totalFees;

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

    // Feed the mobile dashboard
    recentTrades.unshift({ 
        time: new Date().toLocaleTimeString(),
        side: t.side,
        reason: exitReason, 
        entry: t.entryPrice.toFixed(3), 
        exit: exitPrice.toFixed(3), 
        pnl: netPnL 
    });
    if (recentTrades.length > 10) recentTrades.pop();

    trades[t.side] = null;
    isExiting[t.side] = false; 
    
    if (exitReason === "STOP LOSS") {
        postTradeCooldown = Date.now() + 30000; 
    } else {
        postTradeCooldown = 0;
    }
}

async function executeFOK(tokenId, price, buySell, marketSide, sizeNeeded, actionLog) {
    if (isExecuting[marketSide]) return false;
    isExecuting[marketSide] = true;

    try {
        const t = trades[marketSide];
        const refPrice = buySell === 'BUY' ? price : (t ? t.entryPrice : price);
        const shares = (BET_SIZE_USD / refPrice).toFixed(2);
        
        if (parseFloat(sizeNeeded) < parseFloat(shares)) {
            return false;
        }

        const logColor = buySell === 'BUY' ? colors.cyan : (actionLog === 'TAKE PROFIT' ? colors.brightYellow : colors.red);
        console.log(`[PAPER SIMULATION] ${logColor}${actionLog} ${buySell} ${marketSide} @ $${price.toFixed(3)}...${colors.reset}`);
        console.log(`[PAPER SUCCESS] ${logColor}${actionLog} filled instantly.${colors.reset}`);
        return { success: true, sharesFilled: shares };

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

    const currentMid = (bestAsk + bestBid) / 2;
    const spread = bestAsk - bestBid; 
    
    const tokenId = data.asset_id;
    const side = tokenId === currentYesToken ? 'YES' : (tokenId === currentNoToken ? 'NO' : null);
    if (!side) return; 

    // Store live book for this side so the opposite side can read it at entry time
    lastBook[side] = { tokenId, bestAsk, bestAskSize, bestBid, bestBidSize, ts: Date.now() };

    currentPrices[side] = bestAsk;

    // --- SINGLE-TICK MEMORY ---
    if (lastMidpoint[side] !== 0) {
        trend[side] = currentMid - lastMidpoint[side];
    }
    lastMidpoint[side] = currentMid;

    const now = Date.now();
    const secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));

    if (secondsLeft > 270) {
        return; 
    }

    // --- ENTRY: signal fires on this side, enter BOTH sides simultaneously ---
    if (!trades[side] && !isExecuting[side] && !isExiting[side] && secondsLeft > 60 && Date.now() > postTradeCooldown) {
        if (spread > MAX_ALLOWED_SPREAD || bestBid === 0) return;

        const distanceToCenterAsk = Math.abs(0.50 - bestAsk);
        const volatilityMultiplierAsk = 1 - (distanceToCenterAsk / 0.50);

        const isTrendingCorrectly = trend[side] > 0 && trend[side] < 0.05;

        if (volatilityMultiplierAsk >= ENTRY_VOLATILITY_THRESHOLD && isTrendingCorrectly && bestAsk <= 0.50) {
            console.log(`\n${colors.cyan}[VOLATILITY SPIKE] Multiplier at ${volatilityMultiplierAsk.toFixed(2)} | Ask: $${bestAsk.toFixed(2)} | Spread: $${spread.toFixed(2)}${colors.reset}`);

            // Enter triggered side and lock absolute TP/SL
            executeFOK(tokenId, bestAsk, 'BUY', side, bestAskSize, 'ENTRY').then(res => {
                if (res.success) {
                    trades[side] = { 
                        active: true, 
                        side, 
                        tokenId, 
                        entryPrice: bestAsk, 
                        shares: res.sharesFilled, 
                        entryTime: Date.now(), 
                        _firstBreachTime: null, 
                        _consecBreaches: 0,
                        tpPrice: GLOBAL_TP_PRICE,
                        slPrice: GLOBAL_SL_PRICE
                    };
                    console.log(`[ENTRY LOCK] side=${side} entry=${bestAsk.toFixed(3)} tp=${GLOBAL_TP_PRICE} sl=${GLOBAL_SL_PRICE}`);
                }
            });

            // Enter opposite side using its live book — only if price is fresh (within 2 seconds)
            const oppSide  = side === 'YES' ? 'NO' : 'YES';
            const oppToken = side === 'YES' ? currentNoToken : currentYesToken;
            const opp = lastBook[oppSide];
            if (opp && (Date.now() - opp.ts) < 2000 && !trades[oppSide] && !isExecuting[oppSide]) {
                executeFOK(oppToken, opp.bestAsk, 'BUY', oppSide, opp.bestAskSize, 'ENTRY').then(res => {
                    if (res.success) {
                        trades[oppSide] = { 
                            active: true, 
                            side: oppSide, 
                            tokenId: oppToken, 
                            entryPrice: opp.bestAsk, 
                            shares: res.sharesFilled, 
                            entryTime: Date.now(), 
                            _firstBreachTime: null, 
                            _consecBreaches: 0,
                            tpPrice: GLOBAL_TP_PRICE,
                            slPrice: GLOBAL_SL_PRICE
                        };
                        console.log(`[ENTRY LOCK] side=${oppSide} entry=${opp.bestAsk.toFixed(3)} tp=${GLOBAL_TP_PRICE} sl=${GLOBAL_SL_PRICE}`);
                    }
                });
            }
        }
        return; 
    }

    // --- EXIT: each side watches its own price feed and exits at its own locked TP/SL ---
    const t = trades[side];
    if (t && t.tokenId === tokenId && !isExecuting[side] && !isExiting[side]) {
        
        if (secondsLeft <= 8) {
            console.log(`\n${colors.magenta}[EXPIRATION BAILOUT - ${side}] Market ending. Attempting controlled exit before resolution!${colors.reset}`);
            isExiting[side] = true;
            const bailoutPrice = Math.max(0.01, bestBid - 0.01);
            const effectiveBidSize = Math.max(bestBidSize, MIN_BID_SIZE_FOR_EXIT);
            executeFOK(tokenId, bailoutPrice, 'SELL', side, effectiveBidSize, 'BAILOUT').then(res => {
                if (res.success) logCompletedTrade(t, "EXPIRATION BAILOUT", bailoutPrice);
                else isExiting[side] = false; 
            });
            return;
        }

        // Use locked absolute TP/SL set at entry
        const targetProfitPrice = t.tpPrice;
        const stopLossPrice = t.slPrice;

        if (bestBid >= targetProfitPrice) {
            isExiting[side] = true;
            executeFOK(tokenId, bestBid, 'SELL', side, bestBidSize, 'TAKE PROFIT').then(res => {
                if (res.success) logCompletedTrade(t, "TAKE PROFIT", bestBid);
                else isExiting[side] = false;
            });
            return;
        }

        const minReasonableBid = stopLossPrice - 0.03; 

        if (bestBid > minReasonableBid && bestBid <= stopLossPrice) {
            if (spread > MAX_EXIT_SPREAD) {
                console.log(`${colors.yellow}[SHAKEOUT AVOIDED - ${side}] Bid crashed to $${bestBid.toFixed(2)} but spread is wide ($${spread.toFixed(2)}). Holding position.${colors.reset}`);
                return; 
            }

            isExiting[side] = true;
            const effectiveBidSize = Math.max(bestBidSize, MIN_BID_SIZE_FOR_EXIT);
            executeFOK(tokenId, bestBid, 'SELL', side, effectiveBidSize, 'STOP LOSS').then(res => {
                if (res.success) logCompletedTrade(t, "STOP LOSS", bestBid); 
                else isExiting[side] = false;
            });
            return;
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
            trend = { YES: 0, NO: 0 };
            currentPrices = { YES: 0, NO: 0 };
            trades = { YES: null, NO: null };
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

    // Read the HTML file dynamically so you can edit it without restarting the bot!
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

async function runLiveTrader() {
    console.log(`${colors.magenta}Booting Pure Dynamic Near-Strike Engine in PAPER TRADING MODE...${colors.reset}`);
    
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
        if (now >= marketEndTime && !isSearchingNextMarket) loadNextMarket();

        if (currentPrices.YES > 0 && currentPrices.NO > 0) {
            priceStream.write(`${new Date().toISOString()},${currentPrices.YES.toFixed(3)},${currentPrices.NO.toFixed(3)}\n`);
        }

        if (Math.floor(now / 1000) % 10 === 0) {
            const activeList = ['YES','NO'].filter(s => trades[s]).map(s => `${s}@$${trades[s].entryPrice.toFixed(2)}`).join(' + ');
            const statusColor = activeList ? colors.cyan : colors.gray;
            const status = activeList ? `HOLDING ${activeList}` : 'HUNTING STRIKE VOLATILITY';
            console.log(`${statusColor}[LIVE] Status: ${status} | Balance: $${stats.currentBalance.toFixed(2)} | YES Ask: $${currentPrices.YES.toFixed(3)} | NO Ask: $${currentPrices.NO.toFixed(3)}${colors.reset}`);
        }
    }, 1000);
}

runLiveTrader().catch(console.error);
