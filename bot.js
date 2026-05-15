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
const MAX_ALLOWED_SPREAD = 0.02; 
const MIN_TP_CENTS = 0.03;  // UPDATED
const MAX_TP_CENTS = 0.08;  // UPDATED
const MIN_SL_CENTS = 0.03;  // UPDATED
const MAX_SL_CENTS = 0.08;  // UPDATED

const BET_SIZE_USD = 1.00;   
const TAKER_FEE_BPS = 180; 

// --- PAPER TRADING STATE & STATS ---
let trade = { active: false, side: null, tokenId: null, entryPrice: 0, shares: 0, entryTime: 0 };

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

let isExecuting = false;
let isExiting = false; 
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

function logCompletedTrade(exitReason, exitPrice) {
    const isWin = exitPrice > trade.entryPrice;
    
    const grossReturn = exitPrice * trade.shares;
    const entryCost = trade.entryPrice * trade.shares;
    
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
    console.log(`[TRADE CLOSED] Reason: ${c}${exitReason}${colors.reset}`);
    console.log(`Entry: $${trade.entryPrice.toFixed(3)} | Exit: $${exitPrice.toFixed(3)}`);
    console.log(`Gross PnL: $${(grossReturn - entryCost).toFixed(4)} | Fees Paid: $${totalFees.toFixed(4)}`);
    console.log(`NET PnL: ${c}$${netPnL > 0 ? '+' : ''}${netPnL.toFixed(4)}${colors.reset}`);
    console.log(`${colors.gray}---${colors.reset}`);
    console.log(`[STATS] Balance: $${stats.currentBalance.toFixed(2)} (${roi > 0 ? '+' : ''}${roi}% ROI)`);
    console.log(`[STATS] Win Rate: ${winRate}% (${stats.wins}W / ${stats.losses}L)`);
    console.log(`${colors.gray}========================================\n${colors.reset}`);

    const logEntry = `${new Date().toISOString()},BTC-5M,${exitReason},${trade.entryPrice},${exitPrice},${trade.shares},${netPnL.toFixed(4)},${stats.currentBalance.toFixed(2)},${winRate}%\n`;
    tradeStream.write(logEntry); 

    // Feed the mobile dashboard
    recentTrades.unshift({ 
        time: new Date().toLocaleTimeString(), 
        reason: exitReason, 
        entry: trade.entryPrice.toFixed(3), 
        exit: exitPrice.toFixed(3), 
        pnl: netPnL 
    });
    if (recentTrades.length > 10) recentTrades.pop();

    trade = { active: false, side: null, tokenId: null, entryPrice: 0, shares: 0, entryTime: 0 };
    isExiting = false; 
    
    // FIX: Only apply the 5-second breather if the trade was a Stop Loss
    if (exitReason === "STOP LOSS") {
        postTradeCooldown = Date.now() + 5000; 
    } else {
        postTradeCooldown = 0; // Instantly ready for the next setup if it was a win
    }
}

async function executeFOK(tokenId, price, side, sizeNeeded, actionLog) {
    if (isExecuting) return false;
    isExecuting = true;

    try {
        const refPrice = side === 'BUY' ? price : trade.entryPrice;
        const shares = (BET_SIZE_USD / refPrice).toFixed(2);
        
        if (parseFloat(sizeNeeded) < parseFloat(shares)) {
            return false;
        }

        const logColor = side === 'BUY' ? colors.cyan : (actionLog === 'TAKE PROFIT' ? colors.brightYellow : colors.red);
        console.log(`[PAPER SIMULATION] ${logColor}${actionLog} ${side} @ $${price.toFixed(3)}...${colors.reset}`);
        console.log(`[PAPER SUCCESS] ${logColor}${actionLog} filled instantly.${colors.reset}`);
        return { success: true, sharesFilled: shares };

    } catch (err) {
        console.error(`${colors.red}[EXECUTION ERROR]:${colors.reset}`, err.message);
        return { success: false };
    } finally {
        isExecuting = false;
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

    // Checking the postTradeCooldown lock (only affects STOP LOSS events)
    if (!trade.active && !isExecuting && !isExiting && secondsLeft > 60 && Date.now() > postTradeCooldown) {
        if (spread > MAX_ALLOWED_SPREAD || bestBid === 0) return;

        const distanceToCenterAsk = Math.abs(0.50 - bestAsk);
        const volatilityMultiplierAsk = 1 - (distanceToCenterAsk / 0.50);

        // --- SINGLE-TICK AGGRESSIVE ENTRY ---
        const isTrendingCorrectly = trend[side] > 0 && trend[side] < 0.05;

        if (volatilityMultiplierAsk >= ENTRY_VOLATILITY_THRESHOLD && isTrendingCorrectly && bestAsk <= 0.50) {
            console.log(`\n${colors.cyan}[VOLATILITY SPIKE] Multiplier at ${volatilityMultiplierAsk.toFixed(2)} | Ask: $${bestAsk.toFixed(2)} | Spread: $${spread.toFixed(2)}${colors.reset}`);
            executeFOK(tokenId, bestAsk, 'BUY', bestAskSize, 'ENTRY').then(res => {
                if (res.success) trade = { active: true, side: side, tokenId: tokenId, entryPrice: bestAsk, shares: res.sharesFilled, entryTime: Date.now() };
            });
        }
        return; 
    }

    if (trade.active && trade.tokenId === tokenId && !isExecuting && !isExiting) {
        
        if (secondsLeft <= 5) {
            console.log(`\n${colors.magenta}[EXPIRATION BAILOUT] Market ending. Dumping bag to avoid resolution!${colors.reset}`);
            isExiting = true;
            const bailoutPrice = Math.max(0.01, bestBid - 0.02);
            executeFOK(tokenId, bestBid, 'SELL', bestBidSize, 'BAILOUT').then(res => {
                if (res.success) logCompletedTrade("EXPIRATION BAILOUT", bestBid);
                else isExiting = false; 
            });
            return;
        }

        const distanceToCenterBid = Math.abs(0.50 - bestBid);
        const volatilityMultiplierBid = 1 - (distanceToCenterBid / 0.50);

        const dynamicTP_Gap = MIN_TP_CENTS + ((MAX_TP_CENTS - MIN_TP_CENTS) * volatilityMultiplierBid);
        const dynamicSL_Gap = MIN_SL_CENTS + ((MAX_SL_CENTS - MIN_SL_CENTS) * volatilityMultiplierBid);

        const entryFeeCost = trade.entryPrice * (TAKER_FEE_BPS / 10000);
        const targetProfitPrice = trade.entryPrice + dynamicTP_Gap + entryFeeCost;
        const stopLossPrice = trade.entryPrice - dynamicSL_Gap;

        if (bestBid >= targetProfitPrice) {
            isExiting = true;
            executeFOK(tokenId, bestBid, 'SELL', bestBidSize, 'TAKE PROFIT').then(res => {
                if (res.success) logCompletedTrade("TAKE PROFIT", bestBid);
                else isExiting = false;
            });
            return;
        }

        const minReasonableBid = stopLossPrice - 0.03; 

        if (bestBid > minReasonableBid && bestBid <= stopLossPrice) {
            isExiting = true;
            executeFOK(tokenId, bestBid, 'SELL', bestBidSize, 'STOP LOSS').then(res => {
                if (res.success) logCompletedTrade("STOP LOSS", bestBid); 
                else isExiting = false;
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
http.createServer((req, res) => {
    if (req.url === '/api/live') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ stats, trade, currentPrices, recentTrades }));
        return;
    }

    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(`
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=0">
        <meta name="apple-mobile-web-app-capable" content="yes">
        <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
        <title>PolyBot Live</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #0d1117; color: #c9d1d9; margin: 0; padding: 20px; }
            .card { background-color: #161b22; border: 1px solid #30363d; border-radius: 12px; padding: 20px; margin-bottom: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
            h2 { margin-top: 0; font-size: 1.2rem; color: #8b949e; border-bottom: 1px solid #30363d; padding-bottom: 10px; }
            .metric-row { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
            .metric-value { font-size: 1.8rem; font-weight: bold; }
            .green { color: #3fb950; } .red { color: #f85149; } .gold { color: #d29922; } .cyan { color: #58a6ff; }
            .trade-row { display: flex; justify-content: space-between; font-size: 0.9rem; padding: 10px 0; border-bottom: 1px solid #21262d; }
            .trade-row:last-child { border-bottom: none; }
            .badge { padding: 4px 8px; border-radius: 12px; font-size: 0.75rem; font-weight: bold; }
            .bg-green { background: rgba(63, 185, 80, 0.1); color: #3fb950; }
            .bg-red { background: rgba(248, 81, 73, 0.1); color: #f85149; }
        </style>
    </head>
    <body>
        <div class="card">
            <h2>Live Balance</h2>
            <div class="metric-row">
                <div class="metric-value" id="balance">$0.00</div>
                <div id="roi" class="badge">0.00%</div>
            </div>
            <div style="font-size: 0.9rem; color: #8b949e;">Win Rate: <span id="winrate">0%</span></div>
        </div>

        <div class="card">
            <h2>Current Engine Status</h2>
            <div id="status" class="metric-value" style="font-size: 1.2rem; margin-bottom: 15px;">Initializing...</div>
            <div class="metric-row" style="font-size: 0.9rem;">
                <div>YES Ask: <span id="yes-price" class="cyan">$0.00</span></div>
                <div>NO Ask: <span id="no-price" class="gold">$0.00</span></div>
            </div>
        </div>

        <div class="card">
            <h2>Recent Trades</h2>
            <div id="history">Waiting for trades...</div>
        </div>

        <script>
            async function updateDashboard() {
                try {
                    const res = await fetch('/api/live');
                    const data = await res.json();
                    
                    document.getElementById('balance').innerText = '$' + data.stats.currentBalance.toFixed(2);
                    const roi = (((data.stats.currentBalance - data.stats.startingBalance) / data.stats.startingBalance) * 100);
                    const roiEl = document.getElementById('roi');
                    roiEl.innerText = (roi >= 0 ? '+' : '') + roi.toFixed(2) + '%';
                    roiEl.className = 'badge ' + (roi >= 0 ? 'bg-green' : 'bg-red');
                    
                    const wr = data.stats.totalTrades > 0 ? ((data.stats.wins / data.stats.totalTrades) * 100).toFixed(1) : 0;
                    document.getElementById('winrate').innerText = wr + '% (' + data.stats.wins + 'W / ' + data.stats.losses + 'L)';

                    const statusEl = document.getElementById('status');
                    if (data.trade.active) {
                        statusEl.innerHTML = '<span class="cyan">HOLDING ' + data.trade.side + '</span> @ $' + data.trade.entryPrice.toFixed(3);
                    } else {
                        statusEl.innerHTML = '<span class="gold">HUNTING VOLATILITY</span>';
                    }

                    document.getElementById('yes-price').innerText = '$' + data.currentPrices.YES.toFixed(3);
                    document.getElementById('no-price').innerText = '$' + data.currentPrices.NO.toFixed(3);

                    if (data.recentTrades.length > 0) {
                        let html = '';
                        data.recentTrades.forEach(t => {
                            const pnlColor = t.pnl > 0 ? 'green' : 'red';
                            const pnlSign = t.pnl > 0 ? '+' : '';
                            html += \`<div class="trade-row">
                                        <div><span style="color:#8b949e">\${t.time}</span> <br> \${t.reason}</div>
                                        <div style="text-align: right;">E: $\${t.entry} &rarr; $\${t.exit} <br> <span class="\${pnlColor}">\${pnlSign}$\${t.pnl.toFixed(4)}</span></div>
                                      </div>\`;
                        });
                        document.getElementById('history').innerHTML = html;
                    }
                } catch (e) { console.error("Sync error"); }
            }
            setInterval(updateDashboard, 1500);
            updateDashboard();
        </script>
    </body>
    </html>
    `);
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
            const statusColor = trade.active ? colors.cyan : colors.gray;
            const status = trade.active ? `HOLDING ${trade.side} @ $${trade.entryPrice.toFixed(2)}` : 'HUNTING STRIKE VOLATILITY';
            console.log(`${statusColor}[LIVE] Status: ${status} | Balance: $${stats.currentBalance.toFixed(2)} | YES Ask: $${currentPrices.YES.toFixed(3)} | NO Ask: $${currentPrices.NO.toFixed(3)}${colors.reset}`);
        }
    }, 1000);
}

runLiveTrader().catch(console.error);
