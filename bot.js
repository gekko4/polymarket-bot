require('dotenv').config();
const { ClobClient } = require('@polymarket/clob-client');
const { createWalletClient, http: viemHttp } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { polygon } = require('viem/chains');
const WebSocket = require('ws');
const fs = require('fs');
const http = require('http');
const path = require('path');

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

let rawKey = process.env.PRIVATE_KEY;
if (!rawKey) throw new Error(`${colors.red}CRITICAL: PRIVATE_KEY is missing from .env file!${colors.reset}`);
if (!rawKey.startsWith('0x')) rawKey = '0x' + rawKey;

const CHAIN_ID = 137;
const HOST = 'https://clob.polymarket.com';

// --- FAST-HEDGE STRATEGY CONFIGURATION ---
const ENTRY_PRICE = 0.48;                  
const HEDGE_DELAY_SECONDS = 2.0;           
const MAX_HEDGE_PRICE = 0.60;              
const BET_SIZE_USD = 5.00;                 
const ARB_SHARES = parseFloat((BET_SIZE_USD / ENTRY_PRICE).toFixed(2));
const ONE_TRADE_ATTEMPT_PER_MARKET = true; 

const TAKER_FEE_BPS = 180;                 
const PAPER_LIMIT_FEE_BPS = 0;             

const STATES = {
    WAITING_FOR_MARKET: 'WAITING_FOR_MARKET',
    ORDERS_LIVE: 'ORDERS_LIVE',
    ONE_SIDE_FILLED: 'ONE_SIDE_FILLED',
    PAIR_COMPLETED_AT_48: 'PAIR_COMPLETED_AT_48',
    PAIR_COMPLETED_BY_HEDGE: 'PAIR_COMPLETED_BY_HEDGE',
    ABORTED_OR_CANCELLED: 'ABORTED_OR_CANCELLED'
};

let arb = createEmptyArbState();
let stats = { totalTrades: 0, wins: 0, losses: 0, startingBalance: 100.00, currentBalance: 100.00 };

let currentPrices = { YES: 0, NO: 0 }; 
let currentBooks = { YES: { ask: 0, askSize: 0, bid: 0, bidSize: 0 }, NO:  { ask: 0, askSize: 0, bid: 0, bidSize: 0 } };

let recentTrades = [];
let isSearchingNextMarket = false;
let searchCooldownTimer = 0;
let currentYesToken = null;
let currentNoToken = null;
let marketEndTime = 0;
let clobClient;

const tradeStream = fs.createWriteStream('paper_trades_log.csv', { flags: 'a' });
const priceStream = fs.createWriteStream('price_history.csv', { flags: 'a' });
const terminalStream = fs.createWriteStream('terminal_logs.txt', { flags: 'a' });

const originalLog = console.log;
console.log = function (...args) {
    originalLog.apply(console, args);
    const message = args.map(arg => typeof arg === 'object' ? JSON.stringify(arg) : arg).join(' ');
    const cleanMessage = message.replace(/\x1b\[[0-9;]*m/g, '');
    terminalStream.write(`[${new Date().toISOString()}] ${cleanMessage}\n`);
};

if (!fs.existsSync('paper_trades_log.csv') || fs.statSync('paper_trades_log.csv').size === 0) {
    tradeStream.write("Date,Market,Status,Entry_Cost,Exit_Payout,Shares,Net_PnL_USD,Balance_USD,Win_Rate_Pct\n");
}
if (!fs.existsSync('price_history.csv') || fs.statSync('price_history.csv').size === 0) {
    priceStream.write("Timestamp,YES_Ask,NO_Ask\n");
}

function createEmptyArbState() {
    return {
        status: STATES.WAITING_FOR_MARKET,
        attempted: false,
        startedAt: 0,
        firstFillTime: 0,
        firstFillSide: null,
        orders: { YES: null, NO: null },
        positions: { YES: null, NO: null }
    };
}

function tokenForSide(side) { return side === 'YES' ? currentYesToken : currentNoToken; }
function sideForToken(tokenId) {
    if (tokenId === currentYesToken) return 'YES';
    if (tokenId === currentNoToken) return 'NO';
    return null;
}

function createLimitBuyOrder(side, now) {
    return { side, tokenId: tokenForSide(side), type: 'LIMIT', price: ENTRY_PRICE, shares: ARB_SHARES, status: 'OPEN', placedAt: now };
}

function startArbCycle(now) {
    if (arb.status !== STATES.WAITING_FOR_MARKET) return;
    if (ONE_TRADE_ATTEMPT_PER_MARKET && arb.attempted) return;
    if (!currentYesToken || !currentNoToken) return;
    if (currentBooks.YES.ask <= 0 || currentBooks.NO.ask <= 0) return;

    arb.attempted = true;
    arb.status = STATES.ORDERS_LIVE;
    arb.startedAt = now;
    arb.orders.YES = createLimitBuyOrder('YES', now);
    arb.orders.NO = createLimitBuyOrder('NO', now);

    console.log(`\n${colors.cyan}[${STATES.ORDERS_LIVE}] Placed YES and NO limits @ $${ENTRY_PRICE} | Size: ${ARB_SHARES}${colors.reset}`);
}

function paperLimitBuyWouldFill(order) {
    if (!order || order.status !== 'OPEN') return false;
    const book = currentBooks[order.side];
    return book.ask > 0 && book.ask <= order.price && book.askSize >= order.shares;
}

function checkPaperLimitFills(now) {
    if (arb.status !== STATES.ORDERS_LIVE && arb.status !== STATES.ONE_SIDE_FILLED) return;

    for (const side of ['YES', 'NO']) {
        const order = arb.orders[side];
        if (order && order.status === 'OPEN' && paperLimitBuyWouldFill(order)) {
            order.status = 'FILLED';
            arb.positions[side] = { shares: order.shares, entryPrice: ENTRY_PRICE, isTaker: false };

            const filledSides = Object.keys(arb.positions).filter(k => arb.positions[k] !== null);

            if (filledSides.length === 1) {
                arb.status = STATES.ONE_SIDE_FILLED;
                arb.firstFillSide = side;
                arb.firstFillTime = now;
                console.log(`${colors.brightYellow}[${STATES.ONE_SIDE_FILLED}] ${side} filled at $${ENTRY_PRICE}. Started ${HEDGE_DELAY_SECONDS}s hedge timer!${colors.reset}`);
            } 
            else if (filledSides.length === 2) {
                arb.status = STATES.PAIR_COMPLETED_AT_48;
                console.log(`${colors.green}[${STATES.PAIR_COMPLETED_AT_48}] Both sides filled at $${ENTRY_PRICE}!${colors.reset}`);
                recordCompletedPair(STATES.PAIR_COMPLETED_AT_48);
            }
        }
    }
}

function handleHedgeTimer(now) {
    if (arb.status === STATES.ONE_SIDE_FILLED) {
        const elapsedSeconds = (now - arb.firstFillTime) / 1000;
        
        if (elapsedSeconds >= HEDGE_DELAY_SECONDS) {
            const oppositeSide = arb.firstFillSide === 'YES' ? 'NO' : 'YES';
            const hedgeAsk = currentBooks[oppositeSide].ask;
            arb.orders[oppositeSide].status = 'CANCELLED';

            if (hedgeAsk > 0 && hedgeAsk <= MAX_HEDGE_PRICE) {
                arb.positions[oppositeSide] = { shares: ARB_SHARES, entryPrice: hedgeAsk, isTaker: true };
                arb.status = STATES.PAIR_COMPLETED_BY_HEDGE;
                console.log(`${colors.magenta}[${STATES.PAIR_COMPLETED_BY_HEDGE}] Timer expired. Hedged ${oppositeSide} at $${hedgeAsk.toFixed(3)}${colors.reset}`);
                recordCompletedPair(STATES.PAIR_COMPLETED_BY_HEDGE);
            } else {
                arb.status = STATES.ABORTED_OR_CANCELLED;
                const reason = hedgeAsk <= 0 ? "STALE_DATA" : "HEDGE_PRICE_ABOVE_MAX";
                console.log(`${colors.red}[RISK TRIGGERED] ${reason}. Ask was $${hedgeAsk}. Liquidating naked leg!${colors.reset}`);
                liquidateNakedLeg(reason);
            }
        }
    }
}

function recordCompletedPair(status) {
    const yesPos = arb.positions.YES;
    const noPos = arb.positions.NO;
    const payout = 1.00 * ARB_SHARES;
    const costYES = yesPos.entryPrice * yesPos.shares;
    const costNO = noPos.entryPrice * noPos.shares;
    
    const feeYES = yesPos.isTaker ? costYES * (TAKER_FEE_BPS / 10000) : costYES * (PAPER_LIMIT_FEE_BPS / 10000);
    const feeNO = noPos.isTaker ? costNO * (TAKER_FEE_BPS / 10000) : costNO * (PAPER_LIMIT_FEE_BPS / 10000);

    const totalCost = costYES + costNO;
    const netPnL = payout - totalCost - (feeYES + feeNO);
    finalizeTradeRecord(status, netPnL, totalCost, payout, ARB_SHARES);
}

function liquidateNakedLeg(reason) {
    const filledSide = arb.firstFillSide;
    const pos = arb.positions[filledSide];
    const bid = currentBooks[filledSide].bid > 0 ? currentBooks[filledSide].bid : 0.01; 
    const cost = pos.entryPrice * pos.shares;
    const payout = bid * pos.shares;
    const netPnL = payout - cost - (cost * (PAPER_LIMIT_FEE_BPS / 10000)) - (payout * (TAKER_FEE_BPS / 10000));
    
    finalizeTradeRecord(`ABORTED: ${reason}`, netPnL, cost, payout, pos.shares);
}

function finalizeTradeRecord(status, pnl, costDisplay, payoutDisplay, shares) {
    stats.totalTrades++;
    if (pnl > 0) stats.wins++; else stats.losses++;
    stats.currentBalance += pnl;

    const winRate = stats.totalTrades > 0 ? ((stats.wins / stats.totalTrades) * 100).toFixed(1) : '0.0';
    const roi = (((stats.currentBalance - stats.startingBalance) / stats.startingBalance) * 100).toFixed(2);
    const c = pnl > 0 ? colors.brightYellow : colors.red;

    console.log(`\n${colors.gray}========================================${colors.reset}`);
    console.log(`[TRADE CLOSED] Status: ${status}`);
    console.log(`Total Deployment: $${costDisplay.toFixed(3)} | Payout/Recovery: $${payoutDisplay.toFixed(3)}`);
    console.log(`NET PnL: ${c}$${pnl > 0 ? '+' : ''}${pnl.toFixed(4)}${colors.reset}`);
    console.log(`${colors.gray}---${colors.reset}`);
    console.log(`[STATS] Balance: $${stats.currentBalance.toFixed(2)} (${roi > 0 ? '+' : ''}${roi}% ROI) | Win Rate: ${winRate}%`);
    console.log(`${colors.gray}========================================\n${colors.reset}`);

    tradeStream.write(`${new Date().toISOString()},BTC-5M,${status},${costDisplay.toFixed(4)},${payoutDisplay.toFixed(4)},${shares},${pnl.toFixed(4)},${stats.currentBalance.toFixed(2)},${winRate}%\n`);

    recentTrades.unshift({ time: new Date().toLocaleTimeString(), reason: status, entry: costDisplay.toFixed(3), exit: payoutDisplay.toFixed(3), pnl });
    if (recentTrades.length > 10) recentTrades.pop();
}

function handleMarketUpdate(data) {
    if (!data || isNaN(data.bestAsk) || isNaN(data.bestBid)) return;
    const side = sideForToken(data.asset_id);
    if (!side) return;

    const now = Date.now();
    currentPrices[side] = data.bestAsk; 
    currentBooks[side] = { ask: data.bestAsk, askSize: data.bestAskSize || 0, bid: data.bestBid, bidSize: data.bestBidSize || 0 };

    startArbCycle(now);
    checkPaperLimitFills(now);
    handleHedgeTimer(now);
}

function connectWebsocket() {
    if (global.wsMarket) {
        try { global.wsMarket.terminate(); } catch (e) {}
    }

    const wsMarket = new WebSocket('wss://ws-subscriptions-clob.polymarket.com/ws/market');
    global.wsMarket = wsMarket;

    wsMarket.on('open', () => {
        console.log(`${colors.yellow}[WS] Connected to Polymarket stream.${colors.reset}`);
        if (currentYesToken && currentNoToken) {
            wsMarket.send(JSON.stringify({ type: "market", assets_ids: [currentYesToken, currentNoToken] }));
        }
    });

    wsMarket.on('message', (msg) => {
        const textMsg = msg.toString();
        // Respond to explicit heartbeats to stop PM from dropping the connection
        if (textMsg === "PING") { wsMarket.send("PONG"); return; }
        if (textMsg === "PONG") return;

        try {
            const data = JSON.parse(textMsg);
            if (data.event_type === 'book' && data.asks && data.asks.length > 0 && data.bids && data.bids.length > 0) {
                handleMarketUpdate({ asset_id: data.asset_id, bestAsk: parseFloat(data.asks[0].price), bestAskSize: parseFloat(data.asks[0].size), bestBid: parseFloat(data.bids[0].price), bestBidSize: parseFloat(data.bids[0].size) });
            } else if (data.event_type === 'price_change' && data.price_changes && data.price_changes.length > 0) {
                for (const pc of data.price_changes) {
                    handleMarketUpdate({ asset_id: pc.asset_id, bestAsk: parseFloat(pc.best_ask), bestAskSize: 9999, bestBid: parseFloat(pc.best_bid), bestBidSize: 9999 });
                }
            }
        } catch (err) {}
    });

    wsMarket.on('error', (err) => {
        console.log(`${colors.red}[WS ERROR] ${err.message}${colors.reset}`);
    });

    wsMarket.on('close', () => {
        console.log(`${colors.gray}[WS] Connection dropped. Reconnecting...${colors.reset}`);
        setTimeout(connectWebsocket, 2000);
    });
}

async function loadNextMarket() {
    if (isSearchingNextMarket) return;
    isSearchingNextMarket = true;

    console.log(`\n${colors.yellow}[SCANNER] Checking for next active 5-Min BTC Market...${colors.reset}`);

    try {
        const nowSec = Math.floor(Date.now() / 1000);
        const remainder = nowSec % 300;
        const currentIntervalStartSec = nowSec - remainder;
        const currentIntervalEndSec = currentIntervalStartSec + 300;
        const eventSlug = `btc-updown-5m-${currentIntervalStartSec}`;

        const response = await fetch(`https://gamma-api.polymarket.com/events?slug=${eventSlug}`);
        const events = await response.json();

        if (!events || events.length === 0 || !events[0].markets || events[0].markets.length === 0) {
            console.log(`${colors.gray}[SCANNER] API not ready yet. Retrying in 5s...${colors.reset}`);
            searchCooldownTimer = Date.now() + 5000;
            return;
        }

        const validMarket = events[0].markets[0];
        const parsedTokens = typeof validMarket.clobTokenIds === 'string' ? JSON.parse(validMarket.clobTokenIds) : validMarket.clobTokenIds;

        if (parsedTokens[0] && parsedTokens[1] && (parsedTokens[0] !== currentYesToken)) {
            currentYesToken = parsedTokens[0];
            currentNoToken = parsedTokens[1];
            marketEndTime = currentIntervalEndSec * 1000;

            currentPrices = { YES: 0, NO: 0 };
            currentBooks = { YES: { ask: 0, askSize: 0, bid: 0, bidSize: 0 }, NO: { ask: 0, askSize: 0, bid: 0, bidSize: 0 } };
            arb = createEmptyArbState();

            console.log(`${colors.brightYellow}[MARKET LOADED] Subscribed to: ${events[0].title}${colors.reset}`);
            connectWebsocket();
        } else {
            // Found market, but it's the exact same old tokens. API is lagging.
            searchCooldownTimer = Date.now() + 5000;
        }
    } catch (err) {
        console.log(`${colors.red}[SCANNER ERROR] ${err.message}${colors.reset}`);
        searchCooldownTimer = Date.now() + 5000;
    } finally {
        isSearchingNextMarket = false;
    }
}

// --- LOCAL WEBSERVER / DASHBOARD ---
http.createServer((req, res) => {
    if (req.url === '/api/live') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ stats, arb, currentPrices, currentBooks, recentTrades }));
        return;
    }
    fs.readFile(path.join(__dirname, 'dashboard.html'), 'utf8', (err, data) => {
        if (err) return res.writeHead(500), res.end('Dashboard UI missing.');
        res.writeHead(200, { 'Content-Type': 'text/html' }); res.end(data);
    });
}).listen(3000, '0.0.0.0', () => {
    console.log(`${colors.cyan}[DASHBOARD] Web UI running on port 3000${colors.reset}`);
});

async function runLiveTrader() {
    console.log(`${colors.magenta}Booting 48-Centre Fast-Hedge Protocol...${colors.reset}`);
    const account = privateKeyToAccount(rawKey);
    const walletClient = createWalletClient({ account, chain: polygon, transport: viemHttp() });
    clobClient = new ClobClient(HOST, CHAIN_ID, walletClient);

    try { await clobClient.deriveApiKey(); } catch (e) { await clobClient.createApiKey(); }

    await loadNextMarket();

    setInterval(async () => {
        const now = Date.now();
        handleHedgeTimer(now); 

        // If market is not found yet, or has expired, try fetching the next one (respecting the 5-sec cooldown)
        if ((marketEndTime === 0 || now >= marketEndTime) && !isSearchingNextMarket) {
            if (now > searchCooldownTimer) await loadNextMarket();
        }

        if (currentPrices.YES > 0 && currentPrices.NO > 0) {
            priceStream.write(`${new Date().toISOString()},${currentPrices.YES.toFixed(3)},${currentPrices.NO.toFixed(3)}\n`);
        }

        if (Math.floor(now / 1000) % 10 === 0) {
            let displayState = arb.status;
            
            // This explicitly prints the countdown timer so you know WHY it isn't trading again
            if (now < marketEndTime && (arb.status.includes('COMPLETED') || arb.status.includes('ABORTED'))) {
                const secLeft = Math.floor((marketEndTime - now) / 1000);
                displayState = `WAITING_FOR_EXPIRY (${secLeft}s left)`;
            }

            const statusColor = (arb.status === STATES.WAITING_FOR_MARKET || arb.status.includes('COMPLETED')) ? colors.gray : colors.cyan;
            console.log(`${statusColor}[LIVE] State: ${displayState} | Balance: $${stats.currentBalance.toFixed(2)} | YES Ask: $${currentPrices.YES.toFixed(3)} | NO Ask: $${currentPrices.NO.toFixed(3)}${colors.reset}`);
        }
    }, 1000);
}

runLiveTrader().catch(console.error);
