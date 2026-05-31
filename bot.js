require('dotenv').config();
const { ClobClient } = require('@polymarket/clob-client');
const { createWalletClient, http: viemHttp } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { polygon } = require('viem/chains');
const WebSocket = require('ws'); 
const fs = require('fs'); 
const http = require('http');

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

// --- ARBITRAGE CONFIGURATION ---
const ARB_TARGET_PRICE = 0.48; // Target price to place LIMIT bids for both YES and NO
const BET_SIZE_USD = 1.00;     // Capital allocated per side
const TAKER_FEE_BPS = 180;     // Only applied if we have to market-sell an unmatched leg

// --- PAPER TRADING STATE & STATS ---
let arbState = { 
    active: false, 
    yesFilled: false, 
    noFilled: false, 
    yesFillPrice: 0, 
    noFillPrice: 0, 
    shares: 0, 
    entryTime: 0 
};

let stats = {
    totalTrades: 0,
    wins: 0,
    losses: 0,
    startingBalance: 100.00,
    currentBalance: 100.00
};

let currentAsks = { YES: 0, NO: 0 }; 
let currentBids = { YES: 0, NO: 0 };
let recentTrades = [];

let isSearchingNextMarket = false;
let searchCooldownTimer = 0; 

let currentYesToken = null;
let currentNoToken = null;
let marketEndTime = 0;

let clobClient;

// --- ASYNC LOGGING STREAMS ---
const tradeLogFile = 'paper_trades_log.csv';
const priceLogFile = 'price_history.csv';
const terminalLogFile = 'terminal_logs.txt';

const tradeStream = fs.createWriteStream(tradeLogFile, { flags: 'a' });
const priceStream = fs.createWriteStream(priceLogFile, { flags: 'a' });
const terminalStream = fs.createWriteStream(terminalLogFile, { flags: 'a' });

const originalLog = console.log;
console.log = function (...args) {
    originalLog.apply(console, args);
    const message = args.map(arg => typeof arg === 'object' ? JSON.stringify(arg) : arg).join(' ');
    const cleanMessage = message.replace(/\x1b\[[0-9;]*m/g, '');
    terminalStream.write(`[${new Date().toISOString()}] ${cleanMessage}\n`);
};

if (!fs.existsSync(tradeLogFile) || fs.statSync(tradeLogFile).size === 0) {
    tradeStream.write("Date,Market,Action,PnL_USD,Balance_USD,Win_Rate_Pct\n");
}
if (!fs.existsSync(priceLogFile) || fs.statSync(priceLogFile).size === 0) {
    priceStream.write("Timestamp,YES_Ask,NO_Ask\n");
}

function logCompletedArb(exitReason, netPnL) {
    stats.totalTrades++;
    if (netPnL > 0) stats.wins++;
    else if (netPnL < 0) stats.losses++; // Break-evens don't count as losses

    stats.currentBalance += netPnL;

    const winRate = stats.totalTrades > 0 ? ((stats.wins / stats.totalTrades) * 100).toFixed(1) : 0;
    const roi = (((stats.currentBalance - stats.startingBalance) / stats.startingBalance) * 100).toFixed(2);
    const c = netPnL > 0 ? colors.brightYellow : colors.red;

    console.log(`\n${colors.gray}========================================${colors.reset}`);
    console.log(`[ARB RESOLVED] Outcome: ${c}${exitReason}${colors.reset}`);
    console.log(`NET PnL: ${c}$${netPnL > 0 ? '+' : ''}${netPnL.toFixed(4)}${colors.reset}`);
    console.log(`${colors.gray}---${colors.reset}`);
    console.log(`[STATS] Balance: $${stats.currentBalance.toFixed(2)} (${roi > 0 ? '+' : ''}${roi}% ROI)`);
    console.log(`[STATS] Win Rate: ${winRate}% (${stats.wins}W / ${stats.losses}L)`);
    console.log(`${colors.gray}========================================\n${colors.reset}`);

    const logEntry = `${new Date().toISOString()},BTC-5M,${exitReason},${netPnL.toFixed(4)},${stats.currentBalance.toFixed(2)},${winRate}%\n`;
    tradeStream.write(logEntry); 

    recentTrades.unshift({ 
        time: new Date().toLocaleTimeString(), 
        reason: exitReason, 
        pnl: netPnL.toFixed(4)
    });
    if (recentTrades.length > 10) recentTrades.pop();

    // Reset state for the next market
    arbState = { active: false, yesFilled: false, noFilled: false, yesFillPrice: 0, noFillPrice: 0, shares: 0, entryTime: 0 };
}

function handleMarketUpdate(data) {
    if (!data || !arbState.active) return;

    const bestAsk = data.bestAsk;
    const bestBid = data.bestBid;
    
    if (isNaN(bestAsk) || isNaN(bestBid)) return;

    const tokenId = data.asset_id;
    const side = tokenId === currentYesToken ? 'YES' : (tokenId === currentNoToken ? 'NO' : null);
    if (!side) return; 

    currentAsks[side] = bestAsk;
    currentBids[side] = bestBid;

    const now = Date.now();
    const secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));

    // --- ARBITRAGE FILL SIMULATION ---
    // If our limit bid at ARB_TARGET_PRICE gets touched by the market ask, we consider it filled.
    if (secondsLeft > 5) {
        if (!arbState.yesFilled && side === 'YES' && bestAsk <= ARB_TARGET_PRICE) {
            arbState.yesFilled = true;
            arbState.yesFillPrice = ARB_TARGET_PRICE; // Filled at our limit price
            console.log(`\n${colors.cyan}[ARB FILL] YES Side limit order filled at $${ARB_TARGET_PRICE.toFixed(2)}${colors.reset}`);
        }

        if (!arbState.noFilled && side === 'NO' && bestAsk <= ARB_TARGET_PRICE) {
            arbState.noFilled = true;
            arbState.noFillPrice = ARB_TARGET_PRICE; // Filled at our limit price
            console.log(`\n${colors.cyan}[ARB FILL] NO Side limit order filled at $${ARB_TARGET_PRICE.toFixed(2)}${colors.reset}`);
        }
    }

    // --- EXPIRATION / RESOLUTION BAILOUT ---
    // 5 seconds before the market closes, resolve the positions.
    if (secondsLeft <= 5 && arbState.active) {
        arbState.active = false; // Lock out further updates

        if (arbState.yesFilled && arbState.noFilled) {
            // Both sides filled: Guaranteed Arbitrage Win
            // Assuming Maker fees are 0% for limit orders placed.
            const totalCost = (arbState.shares * arbState.yesFillPrice) + (arbState.shares * arbState.noFillPrice);
            const grossReturn = arbState.shares * 1.00; // One side mathematically has to win $1.00
            const netPnL = grossReturn - totalCost;
            
            console.log(`${colors.green}[LOCKED ARBITRAGE] Both legs filled. Guaranteed payout incoming.${colors.reset}`);
            logCompletedArb("FULL ARBITRAGE CAPTURE", netPnL);

        } else if (arbState.yesFilled) {
            // Unmatched Leg: Sell YES at market bid to avoid directional risk
            console.log(`${colors.magenta}[UNMATCHED LEG] Only YES filled. Bailing out before expiration to prevent directional risk.${colors.reset}`);
            const entryCost = arbState.shares * arbState.yesFillPrice;
            const exitValue = arbState.shares * currentBids.YES;
            const takerFee = exitValue * (TAKER_FEE_BPS / 10000); // Pay taker fee on market sell
            const netPnL = exitValue - entryCost - takerFee;
            
            logCompletedArb("BAILOUT: SOLD UNMATCHED YES", netPnL);

        } else if (arbState.noFilled) {
            // Unmatched Leg: Sell NO at market bid to avoid directional risk
            console.log(`${colors.magenta}[UNMATCHED LEG] Only NO filled. Bailing out before expiration to prevent directional risk.${colors.reset}`);
            const entryCost = arbState.shares * arbState.noFillPrice;
            const exitValue = arbState.shares * currentBids.NO;
            const takerFee = exitValue * (TAKER_FEE_BPS / 10000); // Pay taker fee on market sell
            const netPnL = exitValue - entryCost - takerFee;
            
            logCompletedArb("BAILOUT: SOLD UNMATCHED NO", netPnL);

        } else {
            // Neither side filled
            console.log(`${colors.gray}[NO FILLS] Market expired without hitting our bids. No capital risked.${colors.reset}`);
            // Reset for next market without logging a trade
            arbState = { active: false, yesFilled: false, noFilled: false, yesFillPrice: 0, noFillPrice: 0, shares: 0, entryTime: 0 };
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
            
            currentAsks = { YES: 0, NO: 0 }; 
            currentBids = { YES: 0, NO: 0 };

            console.log(`${colors.brightYellow}[MARKET LOADED] Subscribing to: ${validEvent.title}${colors.reset}`);
            
            // Immediately activate the arbitrage parameters on market load
            arbState = { 
                active: true, 
                yesFilled: false, 
                noFilled: false, 
                yesFillPrice: 0, 
                noFillPrice: 0, 
                shares: BET_SIZE_USD / ARB_TARGET_PRICE, 
                entryTime: Date.now() 
            };
            console.log(`${colors.magenta}[MAKER MODE] Placed LIMIT BIDS at $${ARB_TARGET_PRICE.toFixed(2)} for YES and NO.${colors.reset}`);

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

// --- MOBILE WEB DASHBOARD ---
const path = require('path');

http.createServer((req, res) => {
    if (req.url === '/api/live') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        // Exposing arbState instead of trade for the frontend
        res.end(JSON.stringify({ stats, arbState, currentAsks, currentBids, recentTrades }));
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

async function runLiveTrader() {
    console.log(`${colors.magenta}Booting Dual-Sided Arbitrage Engine in PAPER TRADING MODE...${colors.reset}`);
    
    const account = privateKeyToAccount(rawKey);
    const walletClient = createWalletClient({ account, chain: polygon, transport: viemHttp() });
    clobClient = new ClobClient(HOST, CHAIN_ID, walletClient);

    try { await clobClient.deriveApiKey(); } 
    catch (e) { await clobClient.createApiKey(); }

    await loadNextMarket();

    setInterval(async () => {
        if (marketEndTime === 0) {
            if (Date.now() > searchCooldownTimer) loadNextMarket();
            return;
        }

        const now = Date.now();
        if (now >= marketEndTime && !isSearchingNextMarket) loadNextMarket();

        if (currentAsks.YES > 0 && currentAsks.NO > 0) {
            priceStream.write(`${new Date().toISOString()},${currentAsks.YES.toFixed(3)},${currentAsks.NO.toFixed(3)}\n`);
        }

        if (Math.floor(now / 1000) % 10 === 0) {
            let status = 'WAITING FOR FILLS';
            if (arbState.yesFilled && arbState.noFilled) status = 'ARB LOCKED (BOTH FILLED)';
            else if (arbState.yesFilled) status = 'HOLDING YES (WAITING NO)';
            else if (arbState.noFilled) status = 'HOLDING NO (WAITING YES)';

            const statusColor = arbState.yesFilled || arbState.noFilled ? colors.cyan : colors.gray;
            console.log(`${statusColor}[LIVE] ${status} | Bal: $${stats.currentBalance.toFixed(2)} | YES Ask: $${currentAsks.YES.toFixed(3)} | NO Ask: $${currentAsks.NO.toFixed(3)}${colors.reset}`);
        }
    }, 1000);
}

runLiveTrader().catch(console.error);
