require('dotenv').config();
const { ClobClient, OrderType } = require('@polymarket/clob-client');
const { createWalletClient, http } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { polygon } = require('viem/chains');
const WebSocket = require('ws'); 
const fs = require('fs'); 

// --- TERMINAL COLORS ---
const colors = {
    reset: "\x1b[0m",
    green: "\x1b[32m",
    red: "\x1b[31m",
    yellow: "\x1b[33m",
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
const ENTRY_VOLATILITY_THRESHOLD = 0.90; 
const MAX_ALLOWED_SPREAD = 0.02; 
const MIN_TP_CENTS = 0.02; 
const MAX_TP_CENTS = 0.05; 
const MIN_SL_CENTS = 0.03; 
const MAX_SL_CENTS = 0.12; 

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

let isExecuting = false;
let isExiting = false; 
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

    const c = netPnL > 0 ? colors.green : colors.red;

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
    tradeStream.write(logEntry); // Async write, no lag

    trade = { active: false, side: null, tokenId: null, entryPrice: 0, shares: 0, entryTime: 0 };
    isExiting = false; 
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

        const logColor = side === 'BUY' ? colors.cyan : (actionLog === 'TAKE PROFIT' ? colors.green : colors.red);
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

    if (lastMidpoint[side] !== 0) {
        trend[side] = currentMid - lastMidpoint[side];
    }
    lastMidpoint[side] = currentMid;

    const now = Date.now();
    const secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));

    if (secondsLeft > 270) {
        return; 
    }

    if (!trade.active && !isExecuting && !isExiting && secondsLeft > 60) {
        if (spread > MAX_ALLOWED_SPREAD || bestBid === 0) return;

        const distanceToCenterAsk = Math.abs(0.50 - bestAsk);
        const volatilityMultiplierAsk = 1 - (distanceToCenterAsk / 0.50);

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

        const timeInTradeSec = (Date.now() - trade.entryTime) / 1000;
        if (timeInTradeSec > 20) {
            if (bestBid <= trade.entryPrice) {
                console.log(`\n${colors.red}[STAGNATION BAILOUT] Momentum died. 20s elapsed. Exiting early to prevent Stop Loss.${colors.reset}`);
                isExiting = true;
                executeFOK(tokenId, bestBid, 'SELL', bestBidSize, 'TIME STOP').then(res => {
                    if (res.success) logCompletedTrade("TIME STOP", bestBid);
                    else isExiting = false;
                });
                return;
            }
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

            console.log(`${colors.green}[MARKET LOADED] Subscribing to: ${validEvent.title}${colors.reset}`);
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

async function runLiveTrader() {
    console.log(`${colors.magenta}Booting Pure Dynamic Near-Strike Engine in PAPER TRADING MODE...${colors.reset}`);
    
    const account = privateKeyToAccount(rawKey);
    const walletClient = createWalletClient({ account, chain: polygon, transport: http() });
    clobClient = new ClobClient(HOST, CHAIN_ID, walletClient);

    let creds;
    try { creds = await clobClient.deriveApiKey(); } 
    catch (e) { creds = await clobClient.createApiKey(); }

    await loadNextMarket();

    // The Interval Loop: Status updates and async price logging
    setInterval(async () => {
        if (marketEndTime === 0) {
            if (Date.now() > searchCooldownTimer) loadNextMarket();
            return;
        }

        const now = Date.now();
        if (now >= marketEndTime && !isSearchingNextMarket) loadNextMarket();

        // Log the exact YES and NO prices to the hard drive every 1 second
        if (currentPrices.YES > 0 && currentPrices.NO > 0) {
            priceStream.write(`${new Date().toISOString()},${currentPrices.YES.toFixed(3)},${currentPrices.NO.toFixed(3)}\n`);
        }

        // Print to the terminal every 10 seconds
        if (Math.floor(now / 1000) % 10 === 0) {
            const statusColor = trade.active ? colors.cyan : colors.gray;
            const status = trade.active ? `HOLDING ${trade.side} @ $${trade.entryPrice.toFixed(2)}` : 'HUNTING STRIKE VOLATILITY';
            console.log(`${statusColor}[LIVE] Status: ${status} | Balance: $${stats.currentBalance.toFixed(2)} | YES Ask: $${currentPrices.YES.toFixed(3)} | NO Ask: $${currentPrices.NO.toFixed(3)}${colors.reset}`);
        }
    }, 1000);
}

runLiveTrader().catch(console.error);
