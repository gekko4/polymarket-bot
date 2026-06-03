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

// --- SECURITY & AUTH ---
let rawKey = process.env.PRIVATE_KEY;
if (!rawKey) throw new Error(`${colors.red}CRITICAL: PRIVATE_KEY is missing from .env file!${colors.reset}`);
if (!rawKey.startsWith('0x')) rawKey = '0x' + rawKey;

const CHAIN_ID = 137;
const HOST = 'https://clob.polymarket.com';

// --- PAPER ARB CONFIG ---
const BET_SIZE_USD = 1.00;                 // Per-leg notional
const TAKER_FEE_BPS = 180;                 // Used only for simulated bailout sells
const PAPER_LIMIT_FEE_BPS = 0;             // Simulated passive limit-buy fee assumption
const ARB_LIMIT_PRICE = 0.48;              // User-requested YES and NO paper limit price
const ARB_UNFILLED_TIMEOUT_MS = 2 * 60 * 1000;
const ARB_MIN_SECONDS_LEFT_TO_START = 130; // Timeout + small expiry buffer
const ARB_SHARES = parseFloat((BET_SIZE_USD / ARB_LIMIT_PRICE).toFixed(2));

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
let currentPrices = { YES: 0, NO: 0 }; // Kept for dashboard compatibility; stores best ask
let currentBooks = {
    YES: { ask: 0, askSize: 0, bid: 0, bidSize: 0 },
    NO:  { ask: 0, askSize: 0, bid: 0, bidSize: 0 }
};

let recentTrades = [];
let isExecuting = false;
let isExiting = false;
let isSearchingNextMarket = false;
let searchCooldownTimer = 0;
let postTradeCooldown = 0;
let currentYesToken = null;
let currentNoToken = null;
let marketEndTime = 0;
let clobClient;

function createEmptyArbCycle() {
    return {
        active: false,
        status: 'IDLE', // IDLE | WORKING | ONE_LEG_FILLED | LOCKED | CLOSING
        startedAt: 0,
        hedgeDeadline: 0,
        orders: {
            YES: null,
            NO: null
        },
        positions: {
            YES: null,
            NO: null
        }
    };
}
let arb = createEmptyArbCycle();

// --- ASYNC LOGGING STREAMS ---
const tradeLogFile = 'paper_trades_log.csv';
const priceLogFile = 'price_history.csv';
const terminalLogFile = 'terminal_logs.txt';

const tradeStream = fs.createWriteStream(tradeLogFile, { flags: 'a' });
const priceStream = fs.createWriteStream(priceLogFile, { flags: 'a' });
const terminalStream = fs.createWriteStream(terminalLogFile, { flags: 'a' });

// --- ZERO-LAG TERMINAL LOGGING ---
const originalLog = console.log;
console.log = function (...args) {
    originalLog.apply(console, args);
    const message = args.map(arg => typeof arg === 'object' ? JSON.stringify(arg) : arg).join(' ');
    const cleanMessage = message.replace(/\x1b\[[0-9;]*m/g, '');
    terminalStream.write(`[${new Date().toISOString()}] ${cleanMessage}\n`);
};

if (!fs.existsSync(tradeLogFile) || fs.statSync(tradeLogFile).size === 0) {
    tradeStream.write("Date,Market,Action,Entry_Price,Exit_Price,Shares,PnL_USD,Balance_USD,Win_Rate_Pct\n");
}
if (!fs.existsSync(priceLogFile) || fs.statSync(priceLogFile).size === 0) {
    priceStream.write("Timestamp,YES_Ask,NO_Ask\n");
}

function tokenForSide(side) {
    return side === 'YES' ? currentYesToken : currentNoToken;
}

function sideForToken(tokenId) {
    if (tokenId === currentYesToken) return 'YES';
    if (tokenId === currentNoToken) return 'NO';
    return null;
}

function getFilledSides() {
    return ['YES', 'NO'].filter(side => arb.positions[side] !== null);
}

function resetPaperTradeMirror() {
    trade = { active: false, side: null, tokenId: null, entryPrice: 0, shares: 0, entryTime: 0 };
}

function resetArbCycle() {
    arb = createEmptyArbCycle();
    resetPaperTradeMirror();
    isExecuting = false;
    isExiting = false;
}

function createLimitBuyOrder(side, now) {
    return {
        side,
        tokenId: tokenForSide(side),
        action: 'BUY',
        type: 'LIMIT',
        price: ARB_LIMIT_PRICE,
        shares: ARB_SHARES,
        status: 'OPEN',
        placedAt: now,
        filledAt: 0,
        fillPrice: 0,
        cancelReason: null
    };
}

function startArbCycle(now, secondsLeft) {
    if (arb.active) return;
    if (isExecuting || isExiting) return;
    if (Date.now() < postTradeCooldown) return;
    if (!currentYesToken || !currentNoToken) return;
    if (secondsLeft <= ARB_MIN_SECONDS_LEFT_TO_START) return;

    // Require both books to be populated before placing the paired paper orders.
    if (currentBooks.YES.ask <= 0 || currentBooks.YES.bid <= 0) return;
    if (currentBooks.NO.ask <= 0 || currentBooks.NO.bid <= 0) return;

    arb.active = true;
    arb.status = 'WORKING';
    arb.startedAt = now;
    arb.orders.YES = createLimitBuyOrder('YES', now);
    arb.orders.NO = createLimitBuyOrder('NO', now);

    trade = {
        active: true,
        side: 'YES+NO LIMIT ARB',
        tokenId: 'PAIR',
        entryPrice: ARB_LIMIT_PRICE,
        shares: ARB_SHARES,
        entryTime: now
    };

    console.log(`\n${colors.cyan}[ARB START] PAPER limit BUY YES @ $${ARB_LIMIT_PRICE.toFixed(2)} and NO @ $${ARB_LIMIT_PRICE.toFixed(2)} | Shares/leg: ${ARB_SHARES}${colors.reset}`);
}

function paperLimitBuyWouldFill(order) {
    if (!order || order.status !== 'OPEN') return false;
    const book = currentBooks[order.side];

    // Paper fill model: a resting buy limit is considered filled when visible ask <= limit
    // with enough visible size. This avoids pretending to know maker queue position.
    return book.ask > 0 && book.ask <= order.price && book.askSize >= order.shares;
}

function fillPaperLimitBuy(side, now) {
    const order = arb.orders[side];
    if (!order || order.status !== 'OPEN') return;

    order.status = 'FILLED';
    order.filledAt = now;
    order.fillPrice = order.price;

    arb.positions[side] = {
        side,
        tokenId: order.tokenId,
        entryPrice: order.fillPrice,
        shares: order.shares,
        entryTime: now
    };

    const filledSides = getFilledSides();

    if (filledSides.length === 1) {
        arb.status = 'ONE_LEG_FILLED';
        arb.hedgeDeadline = now + ARB_UNFILLED_TIMEOUT_MS;
        const filledSide = filledSides[0];

        trade = {
            active: true,
            side: `${filledSide} FILLED / WAITING OTHER LEG`,
            tokenId: arb.positions[filledSide].tokenId,
            entryPrice: arb.positions[filledSide].entryPrice,
            shares: arb.positions[filledSide].shares,
            entryTime: now
        };

        console.log(`${colors.brightYellow}[ARB LEG FILLED] ${filledSide} filled @ $${order.fillPrice.toFixed(2)}. Waiting for the other side until ${new Date(arb.hedgeDeadline).toLocaleTimeString()}.${colors.reset}`);
    }

    if (filledSides.length === 2) {
        arb.status = 'LOCKED';
        arb.hedgeDeadline = 0;

        trade = {
            active: true,
            side: 'LOCKED YES+NO ARB',
            tokenId: 'PAIR',
            entryPrice: ARB_LIMIT_PRICE,
            shares: ARB_SHARES,
            entryTime: arb.startedAt
        };

        console.log(`${colors.green}[ARB LOCKED] YES and NO both filled @ $${ARB_LIMIT_PRICE.toFixed(2)}. Pair will be settled at expiry in paper mode.${colors.reset}`);
    }
}

function checkPaperLimitFills(now) {
    if (!arb.active) return;
    if (arb.status !== 'WORKING' && arb.status !== 'ONE_LEG_FILLED') return;

    for (const side of ['YES', 'NO']) {
        const order = arb.orders[side];
        if (paperLimitBuyWouldFill(order)) {
            fillPaperLimitBuy(side, now);
        }
    }
}

function cancelOpenLegs(reason) {
    for (const side of ['YES', 'NO']) {
        const order = arb.orders[side];
        if (order && order.status === 'OPEN') {
            order.status = 'CANCELLED';
            order.cancelReason = reason;
            console.log(`${colors.gray}[ARB CANCEL] ${side} unfilled paper order cancelled. Reason: ${reason}${colors.reset}`);
        }
    }
}

function recordPaperArbResult(reason, pnl, entryDisplay, exitDisplay, sharesDisplay = ARB_SHARES) {
    stats.totalTrades++;
    if (pnl > 0) stats.wins++;
    else stats.losses++;
    stats.currentBalance += pnl;

    const winRate = stats.totalTrades > 0 ? ((stats.wins / stats.totalTrades) * 100).toFixed(1) : '0.0';
    const roi = (((stats.currentBalance - stats.startingBalance) / stats.startingBalance) * 100).toFixed(2);
    const c = pnl > 0 ? colors.brightYellow : colors.red;

    console.log(`\n${colors.gray}========================================${colors.reset}`);
    console.log(`[ARB CLOSED] Reason: ${c}${reason}${colors.reset}`);
    console.log(`Display Entry: $${entryDisplay.toFixed(3)} | Display Exit: $${exitDisplay.toFixed(3)}`);
    console.log(`NET PnL: ${c}$${pnl > 0 ? '+' : ''}${pnl.toFixed(4)}${colors.reset}`);
    console.log(`${colors.gray}---${colors.reset}`);
    console.log(`[STATS] Balance: $${stats.currentBalance.toFixed(2)} (${roi > 0 ? '+' : ''}${roi}% ROI)`);
    console.log(`[STATS] Win Rate: ${winRate}% (${stats.wins}W / ${stats.losses}L)`);
    console.log(`${colors.gray}========================================\n${colors.reset}`);

    const logEntry = `${new Date().toISOString()},BTC-5M,${reason},${entryDisplay},${exitDisplay},${sharesDisplay},${pnl.toFixed(4)},${stats.currentBalance.toFixed(2)},${winRate}%\n`;
    tradeStream.write(logEntry);

    recentTrades.unshift({
        time: new Date().toLocaleTimeString(),
        reason,
        entry: entryDisplay.toFixed(3),
        exit: exitDisplay.toFixed(3),
        pnl
    });
    if (recentTrades.length > 10) recentTrades.pop();

    resetArbCycle();
}

function closeSingleFilledLeg(reason) {
    if (!arb.active || arb.status !== 'ONE_LEG_FILLED') return;

    const filledSides = getFilledSides();
    if (filledSides.length !== 1) return;

    const side = filledSides[0];
    const pos = arb.positions[side];
    if (!pos) return;

    const book = currentBooks[side];
    const exitPrice = book.bid > 0 ? book.bid : 0.01;

    arb.status = 'CLOSING';
    isExiting = true;
    cancelOpenLegs(reason);

    const entryCost = pos.entryPrice * pos.shares;
    const grossReturn = exitPrice * pos.shares;
    const entryFee = entryCost * (PAPER_LIMIT_FEE_BPS / 10000);
    const exitFee = grossReturn * (TAKER_FEE_BPS / 10000);
    const pnl = (grossReturn - entryCost) - entryFee - exitFee;

    console.log(`${colors.red}[ARB TIMEOUT EXIT] ${side} counterpart did not fill. PAPER selling ${side} @ best bid $${exitPrice.toFixed(3)}.${colors.reset}`);
    recordPaperArbResult(reason, pnl, pos.entryPrice, exitPrice, pos.shares);
}

function settleLockedArbAtExpiry() {
    if (!arb.active || arb.status !== 'LOCKED') return;

    const yes = arb.positions.YES;
    const no = arb.positions.NO;
    if (!yes || !no) return;

    const matchedShares = Math.min(yes.shares, no.shares);
    const totalCost = (yes.entryPrice * matchedShares) + (no.entryPrice * matchedShares);
    const paperPayout = matchedShares * 1.00;
    const entryFees = totalCost * (PAPER_LIMIT_FEE_BPS / 10000);
    const pnl = paperPayout - totalCost - entryFees;

    const avgEntry = (yes.entryPrice + no.entryPrice) / 2;
    const displayExit = 0.50;

    recordPaperArbResult('LOCKED ARB SETTLED', pnl, avgEntry, displayExit, matchedShares);
}

function handleArbTimer(now) {
    if (!arb.active) return;

    const secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));

    if (arb.status === 'ONE_LEG_FILLED' && arb.hedgeDeadline > 0 && now >= arb.hedgeDeadline) {
        closeSingleFilledLeg('UNFILLED LEG TIMEOUT');
        return;
    }

    if (arb.status === 'ONE_LEG_FILLED' && secondsLeft <= 8) {
        closeSingleFilledLeg('EXPIRY SINGLE-LEG BAILOUT');
        return;
    }

    if (arb.status === 'LOCKED' && secondsLeft <= 0) {
        settleLockedArbAtExpiry();
    }
}

function handleMarketUpdate(data) {
    if (!data) return;

    const bestAsk = data.bestAsk;
    const bestAskSize = data.bestAskSize;
    const bestBid = data.bestBid;
    const bestBidSize = data.bestBidSize;

    if (isNaN(bestAsk) || isNaN(bestBid)) return;

    const side = sideForToken(data.asset_id);
    if (!side) return;

    const now = Date.now();
    const secondsLeft = Math.max(0, Math.floor((marketEndTime - now) / 1000));

    currentPrices[side] = bestAsk;
    currentBooks[side] = {
        ask: bestAsk,
        askSize: bestAskSize || 0,
        bid: bestBid,
        bidSize: bestBidSize || 0
    };

    const currentMid = (bestAsk + bestBid) / 2;
    if (lastMidpoint[side] !== 0) {
        trend[side] = currentMid - lastMidpoint[side];
    }
    lastMidpoint[side] = currentMid;

    // Match your old bot's behaviour: do not operate in the first 30 seconds of the 5-minute market.
    if (secondsLeft > 270) return;

    startArbCycle(now, secondsLeft);
    checkPaperLimitFills(now);
    handleArbTimer(now);
}

function connectWebsocket() {
    if (global.wsMarket) {
        try { global.wsMarket.terminate(); } catch (e) {}
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

            if (data.event_type === 'book' && data.asks && data.asks.length > 0 && data.bids && data.bids.length > 0) {
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

    wsMarket.on('close', () => {
        console.log(`${colors.yellow}[WS] Market websocket closed. Reconnecting shortly...${colors.reset}`);
        setTimeout(connectWebsocket, 2000);
    });

    wsMarket.on('error', (err) => {
        console.log(`${colors.red}[WS ERROR] ${err.message}${colors.reset}`);
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
            console.log(`${colors.gray}[SCANNER] Market ${eventSlug} not fully indexed yet. Retrying...${colors.reset}`);
            searchCooldownTimer = Date.now() + 5000;
            return;
        }

        const validEvent = events[0];
        const validMarket = validEvent.markets[0];

        const parsedTokens = typeof validMarket.clobTokenIds === 'string'
            ? JSON.parse(validMarket.clobTokenIds)
            : validMarket.clobTokenIds;

        const yesTokenId = parsedTokens[0];
        const noTokenId = parsedTokens[1];

        if (yesTokenId && noTokenId) {
            currentYesToken = yesTokenId;
            currentNoToken = noTokenId;
            marketEndTime = currentIntervalEndSec * 1000;

            lastMidpoint = { YES: 0, NO: 0 };
            trend = { YES: 0, NO: 0 };
            currentPrices = { YES: 0, NO: 0 };
            currentBooks = {
                YES: { ask: 0, askSize: 0, bid: 0, bidSize: 0 },
                NO:  { ask: 0, askSize: 0, bid: 0, bidSize: 0 }
            };
            resetArbCycle();

            console.log(`${colors.brightYellow}[MARKET LOADED] Subscribing to: ${validEvent.title}${colors.reset}`);
            connectWebsocket();
        } else {
            searchCooldownTimer = Date.now() + 5000;
        }
    } catch (err) {
        console.log(`${colors.red}[SCANNER ERROR] ${err.message}${colors.reset}`);
        searchCooldownTimer = Date.now() + 5000;
    } finally {
        isSearchingNextMarket = false;
    }
}

// --- MOBILE WEB DASHBOARD ---
http.createServer((req, res) => {
    if (req.url === '/api/live') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            stats,
            trade,
            currentPrices,
            currentBooks,
            arb,
            recentTrades
        }));
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
    console.log(`${colors.magenta}Booting YES/NO 48c Paper Limit Arb Engine...${colors.reset}`);

    const account = privateKeyToAccount(rawKey);
    const walletClient = createWalletClient({ account, chain: polygon, transport: viemHttp() });
    clobClient = new ClobClient(HOST, CHAIN_ID, walletClient);

    // Kept from your original auth bootstrap so the rest of the project shape stays familiar.
    try { await clobClient.deriveApiKey(); }
    catch (e) { await clobClient.createApiKey(); }

    await loadNextMarket();

    setInterval(async () => {
        if (marketEndTime === 0) {
            if (Date.now() > searchCooldownTimer) loadNextMarket();
            return;
        }

        const now = Date.now();
        handleArbTimer(now);

        if (now >= marketEndTime && !isSearchingNextMarket) {
            await loadNextMarket();
        }

        if (currentPrices.YES > 0 && currentPrices.NO > 0) {
            priceStream.write(`${new Date().toISOString()},${currentPrices.YES.toFixed(3)},${currentPrices.NO.toFixed(3)}\n`);
        }

        if (Math.floor(now / 1000) % 10 === 0) {
            const statusColor = trade.active ? colors.cyan : colors.gray;
            const status = trade.active
                ? `${trade.side} @ $${trade.entryPrice.toFixed(2)}`
                : 'WAITING TO PLACE YES/NO 48c LIMIT PAIR';
            console.log(`${statusColor}[LIVE] Status: ${status} | Balance: $${stats.currentBalance.toFixed(2)} | YES Ask: $${currentPrices.YES.toFixed(3)} | NO Ask: $${currentPrices.NO.toFixed(3)}${colors.reset}`);
        }
    }, 1000);
}

runLiveTrader().catch(console.error);
