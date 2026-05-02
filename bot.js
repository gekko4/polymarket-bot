const { ClobClient, OrderType } = require('@polymarket/clob-client');
const { ethers } = require('ethers');
const fs = require('fs');

// --- WALLET & AUTH CONFIG ---
const PRIVATE_KEY = 'YOUR_PRIVATE_KEY_HERE'; 
const CHAIN_ID = 137; // Polygon Mainnet
const HOST = 'https://clob.polymarket.com';

// --- STRATEGY CONFIG ---
const ENTRY_PRICE_MAX        = 0.35;   // Phase 1 ceiling
const ENTRY_PRICE_SECOND     = 0.25;   // Phase 2 ceiling
const TAKE_PROFIT_CENTS      = 0.05;   // Target = Entry + 0.05
const ENTRY_TIME             = 210;    // Entry window closing time
const GRACE_PERIOD_END       = 110;    // Force exit time
const BET_SIZE_USD           = 5.00;   // Dollar amount per trade

// --- STATE MANAGEMENT ---
let phase1 = { active: false, side: null, tokenId: null, entryPrice: 0, sellOrderId: null };
let phase2 = { active: false, side: null, tokenId: null, entryPrice: 0, sellOrderId: null };

// Locks to prevent spamming orders while API requests are in flight
let isExecutingP1 = false;
let isExecutingP2 = false;

let marketEndTime = 0;
let clobClient, wsClient;

// ─────────────────────────────────────────────────────────
// EXECUTION: ENTRY & EXIT
// ─────────────────────────────────────────────────────────
async function executeTradeSequence(side, tokenId, askPrice, phaseLevel) {
    const phaseLock = phaseLevel === 1 ? isExecutingP1 : isExecutingP2;
    if (phaseLock) return; // Prevent duplicate execution

    if (phaseLevel === 1) isExecutingP1 = true;
    else isExecutingP2 = true;

    try {
        const shares = (BET_SIZE_USD / askPrice).toFixed(2);
        console.log(`\n[PHASE ${phaseLevel} TRIGGER] ${side} Ask at $${askPrice}. Executing FOK Buy...`);

        // 1. EXECUTE FOK BUY
        const buyOrder = await clobClient.createOrder({
            tokenID: tokenId,
            price: askPrice,
            side: 'BUY',
            size: shares,
            feeRateBps: 150, 
            orderType: OrderType.FOK // Fill-Or-Kill: No partial fills, no slippage
        });
        
        const buyResponse = await clobClient.postOrder(buyOrder);

        if (buyResponse && buyResponse.success) {
            console.log(`[PHASE ${phaseLevel} BUY FILLED] Bought ${shares} ${side} @ $${askPrice}`);

            // 2. IMMEDIATELY PLACE RESTING LIMIT SELL
            const takeProfitPrice = parseFloat((askPrice + TAKE_PROFIT_CENTS).toFixed(2));
            console.log(`[PHASE ${phaseLevel} EXIT SET] Placing Maker Limit Sell @ $${takeProfitPrice}...`);

            const sellOrder = await clobClient.createOrder({
                tokenID: tokenId,
                price: takeProfitPrice,
                side: 'SELL',
                size: shares,
                feeRateBps: 150,
                orderType: OrderType.GTC // Good-Till-Canceled: Rests on the book
            });

            const sellResponse = await clobClient.postOrder(sellOrder);

            if (sellResponse && sellResponse.success) {
                console.log(`[PHASE ${phaseLevel} LIVE] Limit Sell resting on orderbook (ID: ${sellResponse.orderID})`);
                
                // Update State
                const stateObj = phaseLevel === 1 ? phase1 : phase2;
                stateObj.active = true;
                stateObj.side = side;
                stateObj.tokenId = tokenId;
                stateObj.entryPrice = askPrice;
                stateObj.sellOrderId = sellResponse.orderID;
            } else {
                console.error(`[CRITICAL] Failed to place limit sell for Phase ${phaseLevel}! You are holding unprotected shares.`);
            }
        } else {
            console.log(`[PHASE ${phaseLevel} FAILED] FOK order canceled (Liquidity likely pulled).`);
        }
    } catch (err) {
        console.error(`[EXECUTION ERROR] Phase ${phaseLevel}:`, err.message);
    } finally {
        if (phaseLevel === 1) isExecutingP1 = false;
        else isExecutingP2 = false;
    }
}

// ─────────────────────────────────────────────────────────
// WEBSOCKET HANDLERS
// ─────────────────────────────────────────────────────────
function handleOrderbookUpdate(data, side, tokenId) {
    const now = Date.now();
    const secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));

    // Entry Window Closed
    if (secondsLeft < ENTRY_TIME) return;

    if (!data.asks || data.asks.length === 0) return;
    const bestAsk = parseFloat(data.asks[0].price);

    // Phase 1 Trigger
    if (!phase1.active && bestAsk <= ENTRY_PRICE_MAX) {
        executeTradeSequence(side, tokenId, bestAsk, 1);
    }

    // Phase 2 Trigger
    if (phase1.active && !phase2.active && bestAsk <= ENTRY_PRICE_SECOND) {
        // If holding YES, only buy YES. If holding NO, only buy NO.
        if (phase1.side === side) {
            executeTradeSequence(side, tokenId, bestAsk, 2);
        }
    }
}

function handleUserOrderUpdate(data) {
    // Listens to your private wallet stream.
    // When the exchange executes your resting Limit Sell, it notifies you here.
    if (!data || !data.orderID || data.status !== 'FILLED') return;

    // Check if the filled order was our Phase 1 exit
    if (phase1.active && data.orderID === phase1.sellOrderId) {
        console.log(`\n[$$$ PHASE 1 PROFIT] Target Hit! Sold at $${data.price}`);
        // Reset Phase 1 instantly so it can start hunting again
        phase1 = { active: false, side: null, tokenId: null, entryPrice: 0, sellOrderId: null };
    }

    // Check if the filled order was our Phase 2 exit
    if (phase2.active && data.orderID === phase2.sellOrderId) {
        console.log(`\n[$$$ PHASE 2 PROFIT] Target Hit! Sold at $${data.price}`);
        // Reset Phase 2 instantly
        phase2 = { active: false, side: null, tokenId: null, entryPrice: 0, sellOrderId: null };
    }
}

// ─────────────────────────────────────────────────────────
// BOOT SEQUENCE & TIMERS
// ─────────────────────────────────────────────────────────
async function runLiveTrader() {
    console.log("Booting Live WebSockets Engine...");
    
    const wallet = new ethers.Wallet(PRIVATE_KEY);
    clobClient = new ClobClient(HOST, CHAIN_ID, wallet);
    
    // ... (Assume logic to fetch active market and tokens goes here)
    const YES_TOKEN_ID = '...'; 
    const NO_TOKEN_ID = '...';
    marketEndTime = Date.now() + (300 * 1000); // Example 5 min future

    // 1. Initialize WebSocket
    wsClient = clobClient.createWsClient();
    await wsClient.connect();

    // 2. Subscribe to Market Data (Real-time orderbook)
    wsClient.subscribe(`market:${YES_TOKEN_ID}`);
    wsClient.subscribe(`market:${NO_TOKEN_ID}`);
    
    // 3. Subscribe to Private User Data (Real-time order fills)
    wsClient.subscribe(`user`); 

    // 4. WebSocket Event Listeners
    wsClient.on('message', (msg) => {
        const data = JSON.parse(msg);
        
        if (data.channel.startsWith('market:')) {
            const side = data.channel.includes(YES_TOKEN_ID) ? 'YES' : 'NO';
            const tokenId = side === 'YES' ? YES_TOKEN_ID : NO_TOKEN_ID;
            handleOrderbookUpdate(data, side, tokenId);
        }
        
        if (data.channel === 'user') {
            handleUserOrderUpdate(data);
        }
    });

    // 5. The Clock Loop (Only handles Grace Period force-exits now)
    setInterval(async () => {
        const now = Date.now();
        const secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));
        let graceNotified = false;

        if (secondsLeft <= GRACE_PERIOD_END) {
            // GRACE PERIOD LOGIC:
            // If active phases exist, cancel their resting sell orders 
            // and execute market sell orders immediately.
            
            for (const p of [phase1, phase2]) {
                if (p.active) {
                    console.log(`\n[GRACE PERIOD] Force closing resting order: ${p.sellOrderId}`);
                    await clobClient.cancelOrder({ orderID: p.sellOrderId });
                    
                    // Code to place an immediate market sell order goes here...
                    
                    p.active = false; // clear state
                }
            }
        } else {
            // Heartbeat log
            process.stdout.write(`\r[LIVE] Time: ${secondsLeft}s | Phase 1: ${phase1.active ? 'HOLD' : 'HUNT'} | Phase 2: ${phase2.active ? 'HOLD' : 'HUNT'}   `);
        }
    }, 1000);
}

runLiveTrader().catch(console.error);
