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

// --- STRATEGY CONFIG ---
const ENTRY_PRICE_MAX        = 0.35;   
const ENTRY_PRICE_SECOND     = 0.25;   
const TAKE_PROFIT_CENTS      = 0.05;   
const ENTRY_TIME             = 210;    
const GRACE_PERIOD_END       = 110;    
const BET_SIZE_USD           = 1.00;   // Safest size for live testing
const TAKER_FEE_BPS          = 150;    // 1.5% fee

// --- STATE MANAGEMENT ---
let phase1 = { active: false, side: null, tokenId: null, entryPrice: 0, sellOrderId: null };
let phase2 = { active: false, side: null, tokenId: null, entryPrice: 0, sellOrderId: null };

let isExecutingP1 = false;
let isExecutingP2 = false;
let isSearchingNextMarket = false;
let searchCooldownTimer = 0; 

let currentYesToken = null;
let currentNoToken = null;
let marketEndTime = 0;

let clobClient;

// ─────────────────────────────────────────────────────────
// EXECUTION: ENTRY & EXIT
// ─────────────────────────────────────────────────────────
async function executeTradeSequence(side, tokenId, askPrice, phaseLevel) {
    const phaseLock = phaseLevel === 1 ? isExecutingP1 : isExecutingP2;
    if (phaseLock) return; 

    if (phaseLevel === 1) isExecutingP1 = true;
    else isExecutingP2 = true;

    try {
        const shares = (BET_SIZE_USD / askPrice).toFixed(2);
        console.log(`\n[PHASE ${phaseLevel} TRIGGER] ${side} Ask at $${askPrice}. Sending FOK...`);

        const buyOrder = await clobClient.createOrder({
            tokenID: tokenId, price: askPrice, side: 'BUY', size: shares, feeRateBps: TAKER_FEE_BPS, orderType: OrderType.FOK 
        });
        const buyResponse = await clobClient.postOrder(buyOrder);

        if (buyResponse && buyResponse.success && buyResponse.status !== 'CANCELED') {
            console.log(`[PHASE ${phaseLevel} BUY FILLED] Bought ${shares} ${side} @ $${askPrice}`);

            const feeCostPerShare = askPrice * (TAKER_FEE_BPS / 10000);
            const targetPriceRaw = askPrice + TAKE_PROFIT_CENTS + feeCostPerShare;
            const takeProfitPrice = parseFloat(targetPriceRaw.toFixed(2));

            console.log(`[PHASE ${phaseLevel} EXIT SET] Placing Maker Limit Sell @ $${takeProfitPrice}...`);

            const sellOrder = await clobClient.createOrder({
                tokenID: tokenId, price: takeProfitPrice, side: 'SELL', size: shares, feeRateBps: TAKER_FEE_BPS, orderType: OrderType.GTC 
            });
            const sellResponse = await clobClient.postOrder(sellOrder);

            if (sellResponse && sellResponse.success) {
                console.log(`[PHASE ${phaseLevel} LIVE] Limit Sell resting (ID: ${sellResponse.orderID})`);
                const stateObj = phaseLevel === 1 ? phase1 : phase2;
                stateObj.active = true; stateObj.side = side; stateObj.tokenId = tokenId; stateObj.entryPrice = askPrice; stateObj.sellOrderId = sellResponse.orderID;
            } else {
                console.error(`[CRITICAL] Phase ${phaseLevel} Limit Sell failed! You are holding shares.`);
            }
        } else {
            console.log(`[PHASE ${phaseLevel} REJECTED] Ghost liquidity or insufficient funds. Order killed.`);
        }
    } catch (err) {
        console.error(`[EXECUTION ERROR] Phase ${phaseLevel}:`, err.message);
    } finally {
        if (phaseLevel === 1) isExecutingP1 = false; else isExecutingP2 = false;
    }
}

// ─────────────────────────────────────────────────────────
// MARKET ROLLOVER ENGINE
// ─────────────────────────────────────────────────────────
async function loadNextMarket() {
    if (isSearchingNextMarket) return;
    if (Date.now() < searchCooldownTimer) return; 

    isSearchingNextMarket = true;
    console.log('\n[SCANNER] Searching for the next 5-Min BTC Market...');
    
    try {
        const markets = await clobClient.getMarkets();
        const activeMarkets = markets.data.filter(m => m.active && !m.closed);

        // Targeted search using the exact URL slug
        const btcMarkets = activeMarkets.filter(m => 
            m.event_slug && m.event_slug.toLowerCase().includes('btc-updown-5m')
        );

        if (!btcMarkets.length) {
            console.log('[SCANNER ALERT] I cannot find the exact slug. Here is what Polymarket is sending me:');
            activeMarkets.slice(0, 3).forEach(m => console.log(` -> TITLE: "${m.question}" | SLUG: "${m.event_slug}"`));
            console.log('[SCANNER] Retrying in 30 seconds...');
            searchCooldownTimer = Date.now() + 30000;
            return;
        }

        const sorted = btcMarkets.sort((a, b) => new Date(a.end_date_iso) - new Date(b.end_date_iso));
        const now = Date.now();
        const validMarket = sorted.find(m => {
            const timeRemaining = Math.floor((new Date(m.end_date_iso).getTime() - now) / 1000);
            return timeRemaining > ENTRY_TIME;
        });

        if (validMarket) {
            currentYesToken = validMarket.tokens.find(t => t.outcome === 'Yes').token_id;
            currentNoToken  = validMarket.tokens.find(t => t.outcome === 'No').token_id;
            marketEndTime   = new Date(validMarket.end_date_iso).getTime();
            
            console.log(`[MARKET LOADED] Subscribing to: ${validMarket.question}`);
            
            if (global.wsMarket && global.wsMarket.readyState === WebSocket.OPEN) {
                global.wsMarket.send(JSON.stringify({
                    type: "market", assets_ids: [currentYesToken, currentNoToken]
                }));
            }
        } else {
            console.log('[SCANNER] Found markets, but they expire too soon. Retrying in 30s...');
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
// WEBSOCKET HANDLERS
// ─────────────────────────────────────────────────────────
function handleMarketUpdate(data) {
    if (!data || !data.asks || data.asks.length === 0) return;

    const now = Date.now();
    const secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));

    if (secondsLeft < ENTRY_TIME) return;

    const bestAsk = parseFloat(data.asks[0].price);
    const side = data.asset_id === currentYesToken ? 'YES' : 'NO';
    const tokenId = data.asset_id;

    if (!phase1.active && bestAsk <= ENTRY_PRICE_MAX) {
        executeTradeSequence(side, tokenId, bestAsk, 1);
    }

    if (!phase2.active && bestAsk <= ENTRY_PRICE_SECOND) {
        executeTradeSequence(side, tokenId, bestAsk, 2);
    }
}

// ─────────────────────────────────────────────────────────
// BOOT SEQUENCE & TIMERS
// ─────────────────────────────────────────────────────────
async function runLiveTrader() {
    console.log("Booting HFT Live Engine (Viem + Anti-Spam Optimized)...");
    
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

    const wsUser = new WebSocket('wss://ws-subscriptions-clob.polymarket.com/ws/user');
    wsUser.on('open', () => {
        console.log("[WS] User Stream Connected.");
        wsUser.send(JSON.stringify({
            type: "user", auth: { apikey: apiKey, apiKey: apiKey, secret: creds.secret, passphrase: creds.passphrase }
        }));
    });

    wsUser.on('message', (msg) => {
        const data = JSON.parse(msg);
        if (Array.isArray(data)) {
            for (const order of data) {
                if (order.status === 'FILLED') {
                    if (phase1.active && order.orderID === phase1.sellOrderId) {
                        console.log(`\n[$$$ PHASE 1 PROFIT] Target Hit! Sold at $${order.price}`);
                        phase1 = { active: false, side: null, tokenId: null, entryPrice: 0, sellOrderId: null };
                    }
                    if (phase2.active && order.orderID === phase2.sellOrderId) {
                        console.log(`\n[$$$ PHASE 2 PROFIT] Target Hit! Sold at $${order.price}`);
                        phase2 = { active: false, side: null, tokenId: null, entryPrice: 0, sellOrderId: null };
                    }
                }
            }
        }
    });

    await loadNextMarket();

    setInterval(async () => {
        if (marketEndTime === 0) {
            loadNextMarket();
            return;
        }

        const now = Date.now();
        const secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));

        // Background scan for the next market
        if (secondsLeft <= ENTRY_TIME && !isSearchingNextMarket) loadNextMarket();

        // --- THE MARKET DUMP LOGIC ---
        if (secondsLeft <= GRACE_PERIOD_END) {
            for (const p of [phase1, phase2]) {
                if (p.active) {
                    console.log(`\n[GRACE PERIOD] Canceling Maker Sell to avoid trap...`);
                    try { await clobClient.cancelOrder({ orderID: p.sellOrderId }); } catch (e) {}

                    console.log(`[GRACE PERIOD DUMP] Firing Market Sell to liquidate shares...`);
                    try {
                        const shares = (BET_SIZE_USD / p.entryPrice).toFixed(2);
                        const dumpOrder = await clobClient.createOrder({
                            tokenID: p.tokenId, price: 0.01, side: 'SELL', size: shares, feeRateBps: TAKER_FEE_BPS, orderType: OrderType.FOK 
                        });
                        await clobClient.postOrder(dumpOrder);
                        console.log(`[LIQUIDATED] Dumped bag to the highest bidder.`);
                    } catch (e) { console.error(`[DUMP FAILED]`, e.message); }

                    p.active = false; 
                }
            }
        } else {
            if (secondsLeft % 10 === 0) {
                console.log(`[LIVE] Time: ${secondsLeft}s | P1: ${phase1.active ? 'HOLD' : 'HUNT'} | P2: ${phase2.active ? 'HOLD' : 'HUNT'}`);
            }
        }
    }, 1000);
}

runLiveTrader().catch(console.error);
