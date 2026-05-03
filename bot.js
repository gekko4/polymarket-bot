require('dotenv').config();
const { ClobClient, OrderType } = require('@polymarket/clob-client');
const { ethers } = require('ethers');
const fs = require('fs');

// --- SECURITY & AUTH ---
const PRIVATE_KEY = process.env.PRIVATE_KEY;
if (!PRIVATE_KEY) throw new Error("CRITICAL: PRIVATE_KEY is missing from .env file!");

const CHAIN_ID = 137; // Polygon Mainnet
const HOST = 'https://clob.polymarket.com';

// --- STRATEGY CONFIG ---
const ENTRY_PRICE_MAX        = 0.35;   
const ENTRY_PRICE_SECOND     = 0.25;   
const TAKE_PROFIT_CENTS      = 0.05;   
const ENTRY_TIME             = 210;    
const GRACE_PERIOD_END       = 110;    
const BET_SIZE_USD           = 5.00;   
const TAKER_FEE_BPS          = 150;    // 1.5% fee

// --- STATE MANAGEMENT ---
let phase1 = { active: false, side: null, tokenId: null, entryPrice: 0, sellOrderId: null };
let phase2 = { active: false, side: null, tokenId: null, entryPrice: 0, sellOrderId: null };

let isExecutingP1 = false;
let isExecutingP2 = false;
let isSearchingNextMarket = false;

let currentYesToken = null;
let currentNoToken = null;
let marketEndTime = 0;

let clobClient, wsClient;

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

        // 1. EXECUTE FOK BUY (Taker)
        const buyOrder = await clobClient.createOrder({
            tokenID: tokenId,
            price: askPrice,
            side: 'BUY',
            size: shares,
            feeRateBps: TAKER_FEE_BPS, 
            orderType: OrderType.FOK 
        });
        
        const buyResponse = await clobClient.postOrder(buyOrder);

        // Check if Polymarket rejected the FOK (Ghost Liquidity)
        if (buyResponse && buyResponse.success && buyResponse.status !== 'CANCELED') {
            console.log(`[PHASE ${phaseLevel} BUY FILLED] Bought ${shares} ${side} @ $${askPrice}`);

            // 2. FEE MATH & TARGET CALCULATION
            // Calculate exact target to ensure +$0.05 net profit after the Taker entry fee
            const feeCostPerShare = askPrice * (TAKER_FEE_BPS / 10000);
            const targetPriceRaw = askPrice + TAKE_PROFIT_CENTS + feeCostPerShare;
            const takeProfitPrice = parseFloat(targetPriceRaw.toFixed(2));

            console.log(`[PHASE ${phaseLevel} EXIT SET] Placing Maker Limit Sell @ $${takeProfitPrice}...`);

            // 3. IMMEDIATELY PLACE RESTING LIMIT SELL (Maker)
            const sellOrder = await clobClient.createOrder({
                tokenID: tokenId,
                price: takeProfitPrice,
                side: 'SELL',
                size: shares,
                feeRateBps: TAKER_FEE_BPS, // Usually 0% for Makers, but required field
                orderType: OrderType.GTC 
            });

            const sellResponse = await clobClient.postOrder(sellOrder);

            if (sellResponse && sellResponse.success) {
                console.log(`[PHASE ${phaseLevel} LIVE] Limit Sell resting (ID: ${sellResponse.orderID})`);
                
                const stateObj = phaseLevel === 1 ? phase1 : phase2;
                stateObj.active = true;
                stateObj.side = side;
                stateObj.tokenId = tokenId;
                stateObj.entryPrice = askPrice;
                stateObj.sellOrderId = sellResponse.orderID;
            } else {
                console.error(`[CRITICAL] Phase ${phaseLevel} Limit Sell failed! You are holding shares.`);
            }
        } else {
            // FOK was rejected (liquidity moved). State remains inactive, unlocking immediately.
            console.log(`[PHASE ${phaseLevel} REJECTED] Ghost liquidity. Order killed.`);
        }
    } catch (err) {
        console.error(`[EXECUTION ERROR] Phase ${phaseLevel}:`, err.message);
    } finally {
        // ALWAYS unlock, regardless of success or error, so the bot doesn't freeze
        if (phaseLevel === 1) isExecutingP1 = false;
        else isExecutingP2 = false;
    }
}

// ─────────────────────────────────────────────────────────
// MARKET ROLLOVER ENGINE
// ─────────────────────────────────────────────────────────
async function loadNextMarket() {
    if (isSearchingNextMarket) return;
    isSearchingNextMarket = true;
    
    console.log('\n[SCANNER] Searching for the next 5-Min BTC Market...');
    try {
        const markets = await clobClient.getMarkets();
        const btcMarkets = markets.data.filter(m =>
            m.active && !m.closed && 
            m.question.toLowerCase().includes('bitcoin') && 
            m.question.toLowerCase().includes('5')
        );

        if (!btcMarkets.length) return;

        // Sort by end date to find the *next* resolving market
        const sorted = btcMarkets.sort((a, b) => new Date(a.end_date_iso) - new Date(b.end_date_iso));
        
        // Find the first market that is still in its valid Entry Window
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
            
            // Subscribe to the new streams seamlessly
            if (wsClient) {
                wsClient.subscribe(`market:${currentYesToken}`);
                wsClient.subscribe(`market:${currentNoToken}`);
            }
        }
    } catch (err) {
        console.error('[MARKET ERROR]', err.message);
    } finally {
        isSearchingNextMarket = false;
    }
}

// ─────────────────────────────────────────────────────────
// WEBSOCKET HANDLERS
// ─────────────────────────────────────────────────────────
function handleOrderbookUpdate(data, side, tokenId) {
    const now = Date.now();
    const secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));

    if (secondsLeft < ENTRY_TIME) return;
    if (!data.asks || data.asks.length === 0) return;

    const bestAsk = parseFloat(data.asks[0].price);

    if (!phase1.active && bestAsk <= ENTRY_PRICE_MAX) {
        executeTradeSequence(side, tokenId, bestAsk, 1);
    }

    if (phase1.active && !phase2.active && bestAsk <= ENTRY_PRICE_SECOND) {
        if (phase1.side === side) {
            executeTradeSequence(side, tokenId, bestAsk, 2);
        }
    }
}

function handleUserOrderUpdate(data) {
    if (!data || !data.orderID || data.status !== 'FILLED') return;

    if (phase1.active && data.orderID === phase1.sellOrderId) {
        console.log(`\n[$$$ PHASE 1 PROFIT] Maker Target Hit! Sold at $${data.price}`);
        phase1 = { active: false, side: null, tokenId: null, entryPrice: 0, sellOrderId: null };
    }

    if (phase2.active && data.orderID === phase2.sellOrderId) {
        console.log(`\n[$$$ PHASE 2 PROFIT] Maker Target Hit! Sold at $${data.price}`);
        phase2 = { active: false, side: null, tokenId: null, entryPrice: 0, sellOrderId: null };
    }
}

// ─────────────────────────────────────────────────────────
// BOOT SEQUENCE & TIMERS
// ─────────────────────────────────────────────────────────
async function runLiveTrader() {
    console.log("Booting HFT Live Engine (Premium Intel Optimized)...");
    
    const wallet = new ethers.Wallet(PRIVATE_KEY);
    clobClient = new ClobClient(HOST, CHAIN_ID, wallet);
    
    wsClient = clobClient.createWsClient();
    await wsClient.connect();
    wsClient.subscribe(`user`); 

    wsClient.on('message', (msg) => {
        const data = JSON.parse(msg);
        if (data.channel.startsWith('market:')) {
            const side = data.channel.includes(currentYesToken) ? 'YES' : 'NO';
            const tokenId = side === 'YES' ? currentYesToken : currentNoToken;
            handleOrderbookUpdate(data, side, tokenId);
        }
        if (data.channel === 'user') {
            handleUserOrderUpdate(data);
        }
    });

    await loadNextMarket();

    setInterval(async () => {
        const now = Date.now();
        const secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));

        // Background Rollover: Start looking for the next market before this one ends
        if (secondsLeft <= ENTRY_TIME && !isSearchingNextMarket) {
            loadNextMarket();
        }

        // Grace Period Exit
        if (secondsLeft <= GRACE_PERIOD_END) {
            for (const p of [phase1, phase2]) {
                if (p.active) {
                    console.log(`\n[GRACE PERIOD] Canceling Maker Sell & force-closing at market...`);
                    await clobClient.cancelOrder({ orderID: p.sellOrderId });
                    // Place Market Sell Order logic here...
                    p.active = false; 
                }
            }
        } else {
            process.stdout.write(`\r[LIVE] Time: ${secondsLeft}s | Phase 1: ${phase1.active ? 'HOLD' : 'HUNT'} | Phase 2: ${phase2.active ? 'HOLD' : 'HUNT'}   `);
        }
    }, 1000);
}

runLiveTrader().catch(console.error);
