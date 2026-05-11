require('dotenv').config();
const { ClobClient, OrderType } = require('@polymarket/clob-client');
const { createWalletClient, http } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { polygon } = require('viem/chains');
const WebSocket = require('ws'); 

// --- SECURITY & AUTH ---
let rawKey = process.env.PRIVATE_KEY;
if (!rawKey) throw new Error("CRITICAL: PRIVATE_KEY is missing from .env file!");
if (!rawKey.startsWith('0x')) rawKey = '0x' + rawKey; 

const CHAIN_ID = 137; 
const HOST = 'https://clob.polymarket.com';

// --- STRIKE PROXIMITY CONFIG ---
const ENTRY_VOLATILITY_THRESHOLD = 0.70; // Volatility multiplier threshold
const MIN_TP_CENTS = 0.03; // Increased to offset 1.8% fees safely
const MAX_TP_CENTS = 0.12; 
const MIN_SL_CENTS = 0.03; 
const MAX_SL_CENTS = 0.20; 

const BET_SIZE_USD = 1.00;   
const TAKER_FEE_BPS = 180; // Peak crypto network fee safety buffer (1.8%)

// --- STATE & TREND MANAGEMENT ---
let trade = { active: false, side: null, tokenId: null, entryPrice: 0 };

// Independent momentum trackers for YES and NO
let lastMidpoint = { YES: 0, NO: 0 };
let trend = { YES: 0, NO: 0 }; // Positive = Price Rising, Negative = Price Falling

let isExecuting = false;
let isSearchingNextMarket = false;
let searchCooldownTimer = 0; 

let currentYesToken = null;
let currentNoToken = null;
let marketEndTime = 0;

let clobClient;

// ─────────────────────────────────────────────────────────
// UNIVERSAL EXECUTION ENGINE (With Liquidity Check)
// ─────────────────────────────────────────────────────────
async function executeFOK(tokenId, price, side, sizeNeeded, actionLog) {
    if (isExecuting) return false;
    isExecuting = true;

    try {
        const refPrice = side === 'BUY' ? price : trade.entryPrice;
        const shares = (BET_SIZE_USD / refPrice).toFixed(2);
        
        // Depth Check: Ensure book has enough liquidity to actually fill our size
        if (parseFloat(sizeNeeded) < parseFloat(shares)) {
            console.log(`[DEPTH WARNING] Insufficient liquidity for ${actionLog}. Required: ${shares}, Available: ${sizeNeeded}`);
            return false;
        }

        console.log(`\n[${actionLog}] Firing FOK ${side} @ $${price.toFixed(2)}...`);

        const order = await clobClient.createOrder({
            tokenID: tokenId, price: price, side: side, size: shares, feeRateBps: TAKER_FEE_BPS, orderType: OrderType.FOK 
        });
        const response = await clobClient.postOrder(order);

        if (response && response.success && response.status !== 'CANCELED') {
            console.log(`[SUCCESS] ${actionLog} filled!`);
            return true;
        } else {
            console.log(`[REJECTED] Liquidity shifted. Order killed.`);
            return false;
        }
    } catch (err) {
        console.error(`[EXECUTION ERROR]:`, err.message);
        return false;
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

    // Track independent momentum for this specific token
    if (lastMidpoint[side] !== 0) {
        trend[side] = currentMid - lastMidpoint[side];
    }
    lastMidpoint[side] = currentMid;

    // Time calculations for the Eject Seat
    const now = Date.now();
    const secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));

    // --- 1. DYNAMIC ENTRY LOGIC ---
    if (!trade.active && !isExecuting && secondsLeft > 10) {
        // Calculate how close the Ask is to the $0.50 meat-grinder
        const distanceToCenterAsk = Math.abs(0.50 - bestAsk);
        const volatilityMultiplierAsk = 1 - (distanceToCenterAsk / 0.50);

        // Only enter if volatility is high AND this specific token's price is rising
        const isTrendingCorrectly = trend[side] > 0;

        if (volatilityMultiplierAsk >= ENTRY_VOLATILITY_THRESHOLD && isTrendingCorrectly) {
            console.log(`\n[VOLATILITY SPIKE] Multiplier at ${volatilityMultiplierAsk.toFixed(2)} (Threshold: ${ENTRY_VOLATILITY_THRESHOLD})`);
            executeFOK(tokenId, bestAsk, 'BUY', bestAskSize, 'ENTRY').then(success => {
                if (success) trade = { active: true, side: side, tokenId: tokenId, entryPrice: bestAsk };
            });
        }
        return; 
    }

    // --- 2. DYNAMIC EXIT LOGIC (TP, SL & EJECT SEAT) ---
    if (trade.active && trade.tokenId === tokenId && !isExecuting) {
        
        // -- THE EJECT SEAT (NEVER WAIT FOR RESOLUTION) --
        if (secondsLeft <= 5) {
            console.log(`\n[⚠️ EXPIRATION BAILOUT] Market ending. Dumping bag to avoid resolution!`);
            // Undercut the bid by 2 cents to guarantee an instant FOK fill before the buzzer
            const bailoutPrice = Math.max(0.01, bestBid - 0.02);
            executeFOK(tokenId, bailoutPrice, 'SELL', bestBidSize, 'BAILOUT').then(success => {
                if (success) trade = { active: false, side: null, tokenId: null, entryPrice: 0 };
            });
            return;
        }

        // Calculate how close the Bid is to the $0.50 meat-grinder
        const distanceToCenterBid = Math.abs(0.50 - bestBid);
        const volatilityMultiplierBid = 1 - (distanceToCenterBid / 0.50);

        // Dynamically scale TP and SL based on live volatility
        const dynamicTP_Gap = MIN_TP_CENTS + ((MAX_TP_CENTS - MIN_TP_CENTS) * volatilityMultiplierBid);
        const dynamicSL_Gap = MIN_SL_CENTS + ((MAX_SL_CENTS - MIN_SL_CENTS) * volatilityMultiplierBid);

        // Include entry and exit taker fees in the TP calculation to ensure actual profitability
        const feeCost = trade.entryPrice * (TAKER_FEE_BPS / 10000) * 2;
        const targetProfitPrice = trade.entryPrice + dynamicTP_Gap + feeCost;
        const stopLossPrice = trade.entryPrice - dynamicSL_Gap;

        // TAKE PROFIT TRIGGER
        if (bestBid >= targetProfitPrice) {
            console.log(`\n[DYNAMIC TP HIT] Volatility Mult: ${volatilityMultiplierBid.toFixed(2)} | Target: $${targetProfitPrice.toFixed(2)}`);
            executeFOK(tokenId, bestBid, 'SELL', bestBidSize, 'TAKE PROFIT').then(success => {
                if (success) trade = { active: false, side: null, tokenId: null, entryPrice: 0 };
            });
            return;
        }

        // STOP LOSS TRIGGER
        if (bestBid <= stopLossPrice) {
            console.log(`\n[DYNAMIC SL HIT] Volatility Mult: ${volatilityMultiplierBid.toFixed(2)} | Floor: $${stopLossPrice.toFixed(2)}`);
            // Sell at bestBid - 0.01 to ensure execution without doing a full 0.01 market dump
            const bailOutPrice = Math.max(0.01, bestBid - 0.01); 
            executeFOK(tokenId, bailOutPrice, 'SELL', bestBidSize, 'STOP LOSS').then(success => {
                if (success) trade = { active: false, side: null, tokenId: null, entryPrice: 0 };
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
        const validMarket = sorted.find(m => new Date(m.end_date_iso).getTime() > now + 10000); // 10s buffer

        if (validMarket) {
            currentYesToken = validMarket.tokens.find(t => t.outcome === 'Yes').token_id;
            currentNoToken  = validMarket.tokens.find(t => t.outcome === 'No').token_id;
            marketEndTime   = new Date(validMarket.end_date_iso).getTime();
            
            // Reset state trackers for the new market
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
    console.log("Booting Pure Dynamic Near-Strike Engine...");
    
    const account = privateKeyToAccount(rawKey);
    const walletClient = createWalletClient({ account, chain: polygon, transport: http() });
    clobClient = new ClobClient(HOST, CHAIN_ID, walletClient);

    let creds;
    try { creds = await clobClient.deriveApiKey(); } 
    catch (e) { creds = await clobClient.createApiKey(); }
    const apiKey = creds.apiKey || creds.key;

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
        // Time is strictly used to roll over into the next market
        if (now >= marketEndTime && !isSearchingNextMarket) loadNextMarket();

        // Print alive status every 10 seconds
        if (Math.floor(now / 1000) % 10 === 0) {
            const status = trade.active ? `HOLDING ${trade.side} @ $${trade.entryPrice.toFixed(2)}` : 'HUNTING STRIKE VOLATILITY';
            console.log(`[LIVE] Status: ${status}`);
        }
    }, 1000);
}

runLiveTrader().catch(console.error);
