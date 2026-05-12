require('dotenv').config();
const { ClobClient, OrderType } = require('@polymarket/clob-client');
const { createWalletClient, http } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { polygon } = require('viem/chains');
const WebSocket = require('ws'); 
const fs = require('fs'); // Added for local logging

// --- SECURITY & AUTH ---
let rawKey = process.env.PRIVATE_KEY;
if (!rawKey) throw new Error("CRITICAL: PRIVATE_KEY is missing from .env file!");
if (!rawKey.startsWith('0x')) rawKey = '0x' + rawKey; 

const CHAIN_ID = 137; 
const HOST = 'https://clob.polymarket.com';

// --- STRIKE PROXIMITY CONFIG ---
const ENTRY_VOLATILITY_THRESHOLD = 0.80; // Tightened from 0.70 to require $0.40 - $0.60 range
const MIN_TP_CENTS = 0.03; 
const MAX_TP_CENTS = 0.12; 
const MIN_SL_CENTS = 0.03; 
const MAX_SL_CENTS = 0.20; 

const BET_SIZE_USD = 1.00;   
const TAKER_FEE_BPS = 180; // 1.8% Fee

// --- PAPER TRADING STATE & STATS ---
let trade = { active: false, side: null, tokenId: null, entryPrice: 0, shares: 0 };

let stats = {
    totalTrades: 0,
    wins: 0,
    losses: 0,
    startingBalance: 100.00, // Simulated $100 starting bankroll
    currentBalance: 100.00
};

// Independent momentum trackers for YES and NO
let lastMidpoint = { YES: 0, NO: 0 };
let trend = { YES: 0, NO: 0 }; 

let isExecuting = false;
let isSearchingNextMarket = false;
let searchCooldownTimer = 0; 

let currentYesToken = null;
let currentNoToken = null;
let marketEndTime = 0;

let clobClient;

// Initialize CSV Log File
const logFile = 'paper_trades_log.csv';
if (!fs.existsSync(logFile)) {
    fs.writeFileSync(logFile, "Date,Market,Action,Entry_Price,Exit_Price,Shares,PnL_USD,Balance_USD,Win_Rate_Pct\n");
}

// ─────────────────────────────────────────────────────────
// REPORTING ENGINE
// ─────────────────────────────────────────────────────────
function logCompletedTrade(exitReason, exitPrice) {
    const isWin = exitPrice > trade.entryPrice;
    
    // Calculate exact PnL including fees for both entry and exit
    const grossReturn = exitPrice * trade.shares;
    const entryCost = trade.entryPrice * trade.shares;
    const totalFees = (entryCost * (TAKER_FEE_BPS / 10000)) + (grossReturn * (TAKER_FEE_BPS / 10000));
    const netPnL = (grossReturn - entryCost) - totalFees;

    stats.totalTrades++;
    if (netPnL > 0) stats.wins++;
    else stats.losses++;

    stats.currentBalance += netPnL;

    const winRate = ((stats.wins / stats.totalTrades) * 100).toFixed(1);
    const roi = (((stats.currentBalance - stats.startingBalance) / stats.startingBalance) * 100).toFixed(2);

    console.log(`\n========================================`);
    console.log(`[TRADE CLOSED] Reason: ${exitReason}`);
    console.log(`Entry: $${trade.entryPrice.toFixed(3)} | Exit: $${exitPrice.toFixed(3)}`);
    console.log(`Gross PnL: $${(grossReturn - entryCost).toFixed(4)} | Fees Paid: $${totalFees.toFixed(4)}`);
    console.log(`NET PnL: $${netPnL > 0 ? '+' : ''}${netPnL.toFixed(4)}`);
    console.log(`---`);
    console.log(`[STATS] Balance: $${stats.currentBalance.toFixed(2)} (${roi > 0 ? '+' : ''}${roi}% ROI)`);
    console.log(`[STATS] Win Rate: ${winRate}% (${stats.wins}W / ${stats.losses}L)`);
    console.log(`========================================\n`);

    // Write to CSV
    const logEntry = `${new Date().toISOString()},BTC-5M,${exitReason},${trade.entryPrice},${exitPrice},${trade.shares},${netPnL.toFixed(4)},${stats.currentBalance.toFixed(2)},${winRate}%\n`;
    fs.appendFileSync(logFile, logEntry);

    // Reset Trade State
    trade = { active: false, side: null, tokenId: null, entryPrice: 0, shares: 0 };
}

// ─────────────────────────────────────────────────────────
// PAPER TRADING EXECUTION ENGINE
// ─────────────────────────────────────────────────────────
async function executeFOK(tokenId, price, side, sizeNeeded, actionLog) {
    if (isExecuting) return false;
    isExecuting = true;

    try {
        const refPrice = side === 'BUY' ? price : trade.entryPrice;
        const shares = (BET_SIZE_USD / refPrice).toFixed(2);
        
        if (parseFloat(sizeNeeded) < parseFloat(shares)) {
            console.log(`[DEPTH WARNING] Insufficient liquidity for ${actionLog}. Required: ${shares}, Available: ${sizeNeeded}`);
            return false;
        }

        console.log(`\n[PAPER SIMULATION] ${actionLog} ${side} @ $${price.toFixed(3)}...`);
        
        // SIMULATED EXECUTION: Assuming a 1ms ping, if liquidity is verified above, we assume a clean fill.
        console.log(`[PAPER SUCCESS] ${actionLog} filled instantly.`);
        return { success: true, sharesFilled: shares };

    } catch (err) {
        console.error(`[EXECUTION ERROR]:`, err.message);
        return { success: false };
    } finally {
        isExecuting = false;
    }
}

// ─────────────────────────────────────────────────────────
// DYNAMIC NEAR-STRIKE LOGIC
// ─────────────────────────────────────────────────────────
function handleMarketUpdate(data) {
    if (!data || !data.asks || !data.bids) return;

    const bestAsk = parseFloat(data.asks[0].price);
    const bestAskSize = data.asks[0].size;
    const bestBid = parseFloat(data.bids[0].price);
    const bestBidSize = data.bids[0].size;
    const currentMid = (bestAsk + bestBid) / 2;
    
    const tokenId = data.asset_id;
    const side = data.asset_id === currentYesToken ? 'YES' : 'NO';

    if (lastMidpoint[side] !== 0) {
        trend[side] = currentMid - lastMidpoint[side];
    }
    lastMidpoint[side] = currentMid;

    const now = Date.now();
    const secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));

    // --- 1. DYNAMIC ENTRY LOGIC ---
    if (!trade.active && !isExecuting && secondsLeft > 10) {
        const distanceToCenterAsk = Math.abs(0.50 - bestAsk);
        const volatilityMultiplierAsk = 1 - (distanceToCenterAsk / 0.50);

        const isTrendingCorrectly = trend[side] > 0;

        if (volatilityMultiplierAsk >= ENTRY_VOLATILITY_THRESHOLD && isTrendingCorrectly) {
            console.log(`\n[VOLATILITY SPIKE] Multiplier at ${volatilityMultiplierAsk.toFixed(2)}`);
            executeFOK(tokenId, bestAsk, 'BUY', bestAskSize, 'ENTRY').then(res => {
                if (res.success) trade = { active: true, side: side, tokenId: tokenId, entryPrice: bestAsk, shares: res.sharesFilled };
            });
        }
        return; 
    }

    // --- 2. DYNAMIC EXIT LOGIC (TP, SL & EJECT SEAT) ---
    if (trade.active && trade.tokenId === tokenId && !isExecuting) {
        
        if (secondsLeft <= 5) {
            console.log(`\n[⚠️ EXPIRATION BAILOUT] Market ending. Dumping bag to avoid resolution!`);
            const bailoutPrice = Math.max(0.01, bestBid - 0.02);
            executeFOK(tokenId, bailoutPrice, 'SELL', bestBidSize, 'BAILOUT').then(res => {
                if (res.success) logCompletedTrade("EXPIRATION BAILOUT", bailoutPrice);
            });
            return;
        }

        const distanceToCenterBid = Math.abs(0.50 - bestBid);
        const volatilityMultiplierBid = 1 - (distanceToCenterBid / 0.50);

        const dynamicTP_Gap = MIN_TP_CENTS + ((MAX_TP_CENTS - MIN_TP_CENTS) * volatilityMultiplierBid);
        const dynamicSL_Gap = MIN_SL_CENTS + ((MAX_SL_CENTS - MIN_SL_CENTS) * volatilityMultiplierBid);

        const feeCost = trade.entryPrice * (TAKER_FEE_BPS / 10000) * 2;
        const targetProfitPrice = trade.entryPrice + dynamicTP_Gap + feeCost;
        const stopLossPrice = trade.entryPrice - dynamicSL_Gap;

        // TAKE PROFIT TRIGGER
        if (bestBid >= targetProfitPrice) {
            executeFOK(tokenId, bestBid, 'SELL', bestBidSize, 'TAKE PROFIT').then(res => {
                if (res.success) logCompletedTrade("TAKE PROFIT", bestBid);
            });
            return;
        }

        // STOP LOSS TRIGGER
        if (bestBid <= stopLossPrice) {
            const slipPrice = Math.max(0.01, bestBid - 0.01); 
            executeFOK(tokenId, slipPrice, 'SELL', bestBidSize, 'STOP LOSS').then(res => {
                if (res.success) logCompletedTrade("STOP LOSS", slipPrice);
            });
            return;
        }
    }
}

// ─────────────────────────────────────────────────────────
// MARKET ROLLOVER ENGINE
// ─────────────────────────────────────────────────────────
async function loadNextMarket() {
    if (isSearchingNextMarket) return;
    isSearchingNextMarket = true;
    console.log('\n[SCANNER] Searching for the next active 5-Min BTC Market...');
    
    try {
        const markets = await clobClient.getMarkets();
        const btcMarkets = markets.data.filter(m => 
            m.active && !m.closed && m.event_slug && m.event_slug.toLowerCase().includes('btc-updown-5m')
        );

        if (!btcMarkets.length) {
            searchCooldownTimer = Date.now() + 30000;
            return;
        }

        const now = Date.now();
        const sorted = btcMarkets.sort((a, b) => new Date(a.end_date_iso) - new Date(b.end_date_iso));
        const validMarket = sorted.find(m => new Date(m.end_date_iso).getTime() > now + 10000); 

        if (validMarket) {
            currentYesToken = validMarket.tokens.find(t => t.outcome === 'Yes').token_id;
            currentNoToken  = validMarket.tokens.find(t => t.outcome === 'No').token_id;
            marketEndTime   = new Date(validMarket.end_date_iso).getTime();
            
            lastMidpoint = { YES: 0, NO: 0 };
            trend = { YES: 0, NO: 0 };

            console.log(`[MARKET LOADED] Subscribing to: ${validMarket.question}`);
            
            if (global.wsMarket && global.wsMarket.readyState === WebSocket.OPEN) {
                global.wsMarket.send(JSON.stringify({ type: "market", assets_ids: [currentYesToken, currentNoToken] }));
            }
        } else {
            searchCooldownTimer = Date.now() + 30000;
        }
    } catch (err) {
        console.error('[MARKET ERROR]', err.message);
        searchCooldownTimer = Date.now() + 30000; 
    } finally {
        isSearchingNextMarket = false;
    }
}

// ─────────────────────────────────────────────────────────
// BOOT SEQUENCE & TIMERS
// ─────────────────────────────────────────────────────────
async function runLiveTrader() {
    console.log("Booting Pure Dynamic Near-Strike Engine in PAPER TRADING MODE...");
    
    const account = privateKeyToAccount(rawKey);
    const walletClient = createWalletClient({ account, chain: polygon, transport: http() });
    clobClient = new ClobClient(HOST, CHAIN_ID, walletClient);

    let creds;
    try { creds = await clobClient.deriveApiKey(); } 
    catch (e) { creds = await clobClient.createApiKey(); }

    const wsMarket = new WebSocket('wss://ws-subscriptions-clob.polymarket.com/ws/market');
    global.wsMarket = wsMarket; 
    
    wsMarket.on('open', () => console.log("[WS] Market Stream Connected."));
    wsMarket.on('message', (msg) => {
        const data = JSON.parse(msg);
        if (data.event === 'book' || data.event === 'price_change') handleMarketUpdate(data);
    });

    await loadNextMarket();

    setInterval(async () => {
        if (marketEndTime === 0) {
            if (Date.now() > searchCooldownTimer) loadNextMarket();
            return;
        }

        const now = Date.now();
        if (now >= marketEndTime && !isSearchingNextMarket) loadNextMarket();

        if (Math.floor(now / 1000) % 10 === 0) {
            const status = trade.active ? `HOLDING ${trade.side} @ $${trade.entryPrice.toFixed(2)}` : 'HUNTING STRIKE VOLATILITY';
            console.log(`[LIVE] Status: ${status} | Balance: $${stats.currentBalance.toFixed(2)}`);
        }
    }, 1000);
}

runLiveTrader().catch(console.error);
