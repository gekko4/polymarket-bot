require('dotenv').config();

const { ClobClient } = require('@polymarket/clob-client');
const { createWalletClient, http: viemHttp } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { polygon } = require('viem/chains');
const WebSocket = require('ws');
const fs = require('fs');
const http = require('http');
const path = require('path');

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
if (!rawKey) {
  throw new Error(`${colors.red}CRITICAL: PRIVATE_KEY is missing from .env file!${colors.reset}`);
}
if (!rawKey.startsWith('0x')) rawKey = '0x' + rawKey;

const CHAIN_ID = 137;
const HOST = 'https://clob.polymarket.com';
let clobClient;

// -----------------------------------------------------------------------------
// STRATEGY CONFIG
// -----------------------------------------------------------------------------
// PAPER trading engine.
//
// Corrected strategy:
// 1. Do NOT enter just because one side touches 0.49.
// 2. First require the whole market to be near the 50/50 centre.
// 3. Only then arm simulated resting limit bids at 0.49 on YES and NO.
// 4. If one fills, the opposite side has 15 seconds to fill.
// 5. If not, bailout the unmatched leg at current bid.

const TARGET_BID = 0.49;

// Centre-regime filter.
// The strategy only arms if BOTH YES mid and NO mid are inside this band.
const CENTER_LOW = 0.45;
const CENTER_HIGH = 0.55;

// Execution realism filter.
// A 0.49 buy order is only considered "resting" if the ask is still above 0.49.
const MIN_ASK_ABOVE_TARGET = 0.005;

const SECOND_LEG_TIMEOUT_MS = 15_000;
const BET_SIZE_USD_PER_SIDE = 1.00;
const MIN_SECONDS_LEFT_TO_ARM = 25;
const NO_NEW_TRADES_SECONDS_LEFT = 5;
const TAKER_FEE_BPS = 180;
const ONE_TRADE_PER_MARKET = true;

// -----------------------------------------------------------------------------
// STATE
// -----------------------------------------------------------------------------
function blankState() {
  return {
    active: false,
    status: 'IDLE', // IDLE | WAITING_TO_ARM | WAITING_FIRST_FILL | WAITING_SECOND_FILL | LOCKED

    orderResting: {
      YES: false,
      NO: false
    },

    firstSide: null,
    firstFillTime: 0,
    timeoutAt: 0,

    yesFilled: false,
    noFilled: false,

    yesFillPrice: 0,
    noFillPrice: 0,

    yesShares: 0,
    noShares: 0,

    lastAction: null
  };
}

let arbState = blankState();

let stats = {
  totalTrades: 0,
  wins: 0,
  losses: 0,
  startingBalance: 100.00,
  currentBalance: 100.00
};

let currentAsks = {
  YES: 0,
  NO: 0
};

let currentBids = {
  YES: 0,
  NO: 0
};

let recentTrades = [];
let currentYesToken = null;
let currentNoToken = null;
let marketEndTime = 0;
let currentMarketSlug = null;
let isSearchingNextMarket = false;
let searchCooldownTimer = 0;

const completedMarketSlugs = new Set();

// -----------------------------------------------------------------------------
// LOGGING
// -----------------------------------------------------------------------------
const tradeLogFile = 'paper_trades_log.csv';
const priceLogFile = 'price_history.csv';
const terminalLogFile = 'terminal_logs.txt';

const tradeStream = fs.createWriteStream(tradeLogFile, { flags: 'a' });
const priceStream = fs.createWriteStream(priceLogFile, { flags: 'a' });
const terminalStream = fs.createWriteStream(terminalLogFile, { flags: 'a' });

const originalLog = console.log;

console.log = function (...args) {
  originalLog.apply(console, args);

  const message = args
    .map(a => typeof a === 'object' ? JSON.stringify(a) : String(a))
    .join(' ');

  const cleanMessage = message.replace(/\x1b\[[0-9;]*m/g, '');
  terminalStream.write(`[${new Date().toISOString()}] ${cleanMessage}\n`);
};

if (!fs.existsSync(tradeLogFile) || fs.statSync(tradeLogFile).size === 0) {
  tradeStream.write(
    'Date,Market,Action,PnL_USD,Balance_USD,Win_Rate_Pct,YES_Filled,NO_Filled,YES_Price,NO_Price,YES_Shares,NO_Shares\n'
  );
}

if (!fs.existsSync(priceLogFile) || fs.statSync(priceLogFile).size === 0) {
  priceStream.write('Timestamp,YES_Ask,NO_Ask,YES_Bid,NO_Bid,YES_Mid,NO_Mid\n');
}

// -----------------------------------------------------------------------------
// HELPERS
// -----------------------------------------------------------------------------
function secondsLeftInMarket() {
  return Math.max(0, Math.floor((marketEndTime - Date.now()) / 1000));
}

function sideFromToken(tokenId) {
  if (tokenId === currentYesToken) return 'YES';
  if (tokenId === currentNoToken) return 'NO';
  return null;
}

function opposite(side) {
  return side === 'YES' ? 'NO' : 'YES';
}

function bothPricesKnown() {
  return (
    currentAsks.YES > 0 &&
    currentAsks.NO > 0 &&
    currentBids.YES > 0 &&
    currentBids.NO > 0
  );
}

function getMid(side) {
  if (!bothPricesKnown()) return 0;
  return (currentAsks[side] + currentBids[side]) / 2;
}

function fmtPrice(value) {
  return value > 0 ? value.toFixed(3) : '---';
}

function fmtMid(side) {
  const mid = getMid(side);
  return mid > 0 ? mid.toFixed(3) : '---';
}

function isNearCenter() {
  if (!bothPricesKnown()) return false;

  const yesMid = getMid('YES');
  const noMid = getMid('NO');

  const yesCentered = yesMid >= CENTER_LOW && yesMid <= CENTER_HIGH;
  const noCentered = noMid >= CENTER_LOW && noMid <= CENTER_HIGH;

  return yesCentered && noCentered;
}

function ordersWouldBeResting() {
  if (!bothPricesKnown()) return false;

  return (
    currentAsks.YES > TARGET_BID + MIN_ASK_ABOVE_TARGET &&
    currentAsks.NO > TARGET_BID + MIN_ASK_ABOVE_TARGET
  );
}

function getArmBlockReason() {
  if (!bothPricesKnown()) {
    return 'WAITING_FOR_FULL_PRICE_DATA';
  }

  if (secondsLeftInMarket() < MIN_SECONDS_LEFT_TO_ARM) {
    return 'NOT_ARMED_TOO_CLOSE_TO_MARKET_END';
  }

  if (!isNearCenter()) {
    return 'NOT_ARMED_NOT_NEAR_50_50_CENTER';
  }

  if (!ordersWouldBeResting()) {
    return 'NOT_ARMED_ORDER_WOULD_CROSS_NOT_REST';
  }

  return null;
}

function canArmRestingLimitOrders() {
  return getArmBlockReason() === null;
}

function getReadableStatus() {
  if (arbState.status === 'WAITING_TO_ARM') {
    return 'WAITING FOR 50/50 CENTRE';
  }

  if (arbState.status === 'WAITING_FIRST_FILL') {
    return 'LIMIT ORDERS LIVE';
  }

  if (arbState.status === 'WAITING_SECOND_FILL') {
    return `HOLDING ${arbState.firstSide}, WAITING FOR ${opposite(arbState.firstSide)}`;
  }

  if (arbState.status === 'LOCKED') {
    return 'ARB LOCKED';
  }

  if (arbState.status === 'IDLE') {
    return 'IDLE / DONE THIS MARKET';
  }

  return arbState.status;
}

function tryArmRestingLimitOrders() {
  if (arbState.status !== 'WAITING_TO_ARM') return;
  if (ONE_TRADE_PER_MARKET && completedMarketSlugs.has(currentMarketSlug)) return;

  const blockReason = getArmBlockReason();

  if (blockReason) {
    arbState.lastAction = blockReason;
    return;
  }

  arbState.status = 'WAITING_FIRST_FILL';
  arbState.orderResting = {
    YES: true,
    NO: true
  };
  arbState.lastAction = 'RESTING_LIMIT_BIDS_ARMED_NEAR_CENTER';

  console.log(
    `${colors.magenta}[ORDERS LIVE] Market is near 50/50. ` +
    `Simulated limit buys placed at $${TARGET_BID.toFixed(2)} on YES and NO. ` +
    `YES ask $${currentAsks.YES.toFixed(3)}, bid $${currentBids.YES.toFixed(3)}, mid $${fmtMid('YES')} | ` +
    `NO ask $${currentAsks.NO.toFixed(3)}, bid $${currentBids.NO.toFixed(3)}, mid $${fmtMid('NO')}.${colors.reset}`
  );
}

// -----------------------------------------------------------------------------
// TRADE ACCOUNTING
// -----------------------------------------------------------------------------
function logCompletedTrade(reason, netPnL) {
  stats.totalTrades++;

  if (netPnL > 0) stats.wins++;
  else if (netPnL < 0) stats.losses++;

  stats.currentBalance += netPnL;

  const winRate = stats.totalTrades
    ? ((stats.wins / stats.totalTrades) * 100).toFixed(1)
    : '0.0';

  const roi = (
    ((stats.currentBalance - stats.startingBalance) / stats.startingBalance) *
    100
  ).toFixed(2);

  const c = netPnL > 0
    ? colors.brightYellow
    : netPnL < 0
      ? colors.red
      : colors.gray;

  console.log(`\n${colors.gray}========================================${colors.reset}`);
  console.log(`[ARB RESOLVED] Outcome: ${c}${reason}${colors.reset}`);
  console.log(`NET PnL: ${c}$${netPnL > 0 ? '+' : ''}${netPnL.toFixed(4)}${colors.reset}`);
  console.log(`[STATS] Balance: $${stats.currentBalance.toFixed(2)} (${roi > 0 ? '+' : ''}${roi}% ROI)`);
  console.log(`[STATS] Win Rate: ${winRate}% (${stats.wins}W / ${stats.losses}L)`);
  console.log(`${colors.gray}========================================\n${colors.reset}`);

  tradeStream.write([
    new Date().toISOString(),
    currentMarketSlug || 'BTC-5M',
    reason,
    netPnL.toFixed(4),
    stats.currentBalance.toFixed(2),
    `${winRate}%`,
    arbState.yesFilled,
    arbState.noFilled,
    arbState.yesFillPrice.toFixed(4),
    arbState.noFillPrice.toFixed(4),
    arbState.yesShares.toFixed(6),
    arbState.noShares.toFixed(6)
  ].join(',') + '\n');

  recentTrades.unshift({
    time: new Date().toLocaleTimeString(),
    reason,
    pnl: netPnL.toFixed(4)
  });

  if (recentTrades.length > 10) recentTrades.pop();

  if (currentMarketSlug) completedMarketSlugs.add(currentMarketSlug);

  arbState = blankState();
}

function fillFirstLeg(side) {
  if (!arbState.orderResting[side]) {
    console.log(
      `${colors.gray}[IGNORED] ${side} <= target, but no resting order was armed. ` +
      `This prevents restart-cross fills.${colors.reset}`
    );
    return;
  }

  const now = Date.now();

  arbState.status = 'WAITING_SECOND_FILL';
  arbState.firstSide = side;
  arbState.firstFillTime = now;
  arbState.timeoutAt = now + SECOND_LEG_TIMEOUT_MS;
  arbState.orderResting[side] = false;
  arbState.lastAction = `FIRST_FILL_${side}`;

  if (side === 'YES') {
    arbState.yesFilled = true;
    arbState.yesFillPrice = TARGET_BID;
    arbState.yesShares = BET_SIZE_USD_PER_SIDE / TARGET_BID;
  } else {
    arbState.noFilled = true;
    arbState.noFillPrice = TARGET_BID;
    arbState.noShares = BET_SIZE_USD_PER_SIDE / TARGET_BID;
  }

  console.log(
    `\n${colors.cyan}[FIRST FILL] ${side} resting bid filled at $${TARGET_BID.toFixed(2)}. ` +
    `Waiting ${SECOND_LEG_TIMEOUT_MS / 1000}s for ${opposite(side)}.${colors.reset}`
  );
}

function fillSecondLeg(side) {
  if (!arbState.orderResting[side]) return;

  arbState.orderResting[side] = false;

  if (side === 'YES') {
    arbState.yesFilled = true;
    arbState.yesFillPrice = TARGET_BID;
    arbState.yesShares = BET_SIZE_USD_PER_SIDE / TARGET_BID;
  } else {
    arbState.noFilled = true;
    arbState.noFillPrice = TARGET_BID;
    arbState.noShares = BET_SIZE_USD_PER_SIDE / TARGET_BID;
  }

  arbState.status = 'LOCKED';

  const payoutShares = Math.min(arbState.yesShares, arbState.noShares);

  const totalCost =
    (arbState.yesShares * arbState.yesFillPrice) +
    (arbState.noShares * arbState.noFillPrice);

  const grossReturn = payoutShares;
  const netPnL = grossReturn - totalCost;

  logCompletedTrade('FULL_ARBITRAGE_CAPTURE', netPnL);
}

function bailoutUnmatchedLeg(reason) {
  if (!arbState.active) return;

  let side = null;

  if (arbState.yesFilled && !arbState.noFilled) side = 'YES';
  if (arbState.noFilled && !arbState.yesFilled) side = 'NO';

  if (!side) return;

  const shares = side === 'YES' ? arbState.yesShares : arbState.noShares;

  const entryPrice = side === 'YES'
    ? arbState.yesFillPrice
    : arbState.noFillPrice;

  const exitBid = currentBids[side];

  if (!exitBid || isNaN(exitBid) || exitBid <= 0) {
    console.log(
      `${colors.red}[BAILOUT BLOCKED] No valid ${side} bid. ` +
      `Holding paper state until valid bid/update.${colors.reset}`
    );
    return;
  }

  const entryCost = shares * entryPrice;
  const exitValue = shares * exitBid;
  const takerFee = exitValue * (TAKER_FEE_BPS / 10000);
  const netPnL = exitValue - entryCost - takerFee;

  console.log(
    `${colors.magenta}[BAILOUT] ${reason}. Sold unmatched ${side} at bid $${exitBid.toFixed(3)}.${colors.reset}`
  );

  logCompletedTrade(`BAILOUT_${side}_${reason}`, netPnL);
}

// -----------------------------------------------------------------------------
// CORE STRATEGY
// -----------------------------------------------------------------------------
function handleMarketUpdate(data) {
  if (!data || !arbState.active) return;

  const bestAsk = Number(data.bestAsk);
  const bestBid = Number(data.bestBid);

  if (isNaN(bestAsk) || isNaN(bestBid)) return;

  const side = sideFromToken(data.asset_id);
  if (!side) return;

  currentAsks[side] = bestAsk;
  currentBids[side] = bestBid;

  const now = Date.now();
  const secondsLeft = secondsLeftInMarket();

  if (arbState.status === 'WAITING_TO_ARM') {
    tryArmRestingLimitOrders();
    return;
  }

  if (arbState.status === 'WAITING_SECOND_FILL' && now >= arbState.timeoutAt) {
    bailoutUnmatchedLeg('SECOND_LEG_TIMEOUT');
    return;
  }

  if (secondsLeft <= NO_NEW_TRADES_SECONDS_LEFT) {
    if (arbState.status === 'WAITING_SECOND_FILL') {
      bailoutUnmatchedLeg('MARKET_END_BAILOUT');
    }
    return;
  }

  if (arbState.status === 'WAITING_FIRST_FILL') {
    // Valid only because the order was armed earlier when:
    // 1. market was near 50/50 centre
    // 2. both asks were above the 0.49 target
    //
    // Therefore, a later bestAsk <= target represents price moving down into our resting bid.
    if (arbState.orderResting[side] && bestAsk <= TARGET_BID) {
      fillFirstLeg(side);
    }
    return;
  }

  if (arbState.status === 'WAITING_SECOND_FILL') {
    const needed = opposite(arbState.firstSide);

    if (
      side === needed &&
      arbState.orderResting[side] &&
      bestAsk <= TARGET_BID
    ) {
      fillSecondLeg(side);
    }
  }
}

// -----------------------------------------------------------------------------
// WEBSOCKET
// -----------------------------------------------------------------------------
function connectWebsocket() {
  if (global.wsMarket) {
    try {
      global.wsMarket.terminate();
    } catch (e) {}
  }

  console.log(`${colors.yellow}[WS] Booting fresh market connection...${colors.reset}`);

  const wsMarket = new WebSocket('wss://ws-subscriptions-clob.polymarket.com/ws/market');
  global.wsMarket = wsMarket;

  wsMarket.on('open', () => {
    if (currentYesToken && currentNoToken) {
      wsMarket.send(JSON.stringify({
        type: 'market',
        assets_ids: [currentYesToken, currentNoToken]
      }));
    }
  });

  wsMarket.on('message', (msg) => {
    const txt = msg.toString();
    if (txt === 'PONG') return;

    try {
      const data = JSON.parse(txt);

      if (data.event_type === 'book' && data.asks?.length && data.bids?.length) {
        handleMarketUpdate({
          asset_id: data.asset_id,
          bestAsk: parseFloat(data.asks[0].price),
          bestBid: parseFloat(data.bids[0].price)
        });
      } else if (data.event_type === 'price_change' && data.price_changes?.length) {
        for (const pc of data.price_changes) {
          handleMarketUpdate({
            asset_id: pc.asset_id,
            bestAsk: parseFloat(pc.best_ask),
            bestBid: parseFloat(pc.best_bid)
          });
        }
      }
    } catch (e) {}
  });

  wsMarket.on('error', e => {
    console.log(`${colors.red}[WS ERROR] ${e.message}${colors.reset}`);
  });

  wsMarket.on('close', () => {
    console.log(`${colors.gray}[WS] closed${colors.reset}`);
  });
}

// -----------------------------------------------------------------------------
// MARKET LOADER
// -----------------------------------------------------------------------------
async function loadNextMarket() {
  if (isSearchingNextMarket) return;

  isSearchingNextMarket = true;

  console.log(`\n${colors.yellow}[SCANNER] Calculating current active 5-minute BTC market...${colors.reset}`);

  try {
    const nowSec = Math.floor(Date.now() / 1000);
    const startSec = nowSec - (nowSec % 300);
    const endSec = startSec + 300;
    const eventSlug = `btc-updown-5m-${startSec}`;

    const response = await fetch(`https://gamma-api.polymarket.com/events?slug=${eventSlug}`);
    const events = await response.json();

    if (!events?.length || !events[0].markets?.length) {
      console.log(`${colors.gray}[SCANNER] ${eventSlug} not indexed yet. Retrying...${colors.reset}`);
      searchCooldownTimer = Date.now() + 5000;
      return;
    }

    const event = events[0];
    const market = event.markets[0];

    const tokens = typeof market.clobTokenIds === 'string'
      ? JSON.parse(market.clobTokenIds)
      : market.clobTokenIds;

    if (!tokens?.[0] || !tokens?.[1]) {
      searchCooldownTimer = Date.now() + 5000;
      return;
    }

    currentYesToken = tokens[0];
    currentNoToken = tokens[1];
    currentMarketSlug = eventSlug;
    marketEndTime = endSec * 1000;

    currentAsks = {
      YES: 0,
      NO: 0
    };

    currentBids = {
      YES: 0,
      NO: 0
    };

    arbState = blankState();
    arbState.active = !completedMarketSlugs.has(eventSlug);
    arbState.status = arbState.active ? 'WAITING_TO_ARM' : 'IDLE';

    console.log(`${colors.brightYellow}[MARKET LOADED] ${event.title}${colors.reset}`);

    console.log(
      `${colors.magenta}[PAPER STRATEGY] Waiting for true 50/50 centre. ` +
      `Orders only go live when YES and NO mids are both between ` +
      `${CENTER_LOW.toFixed(2)} and ${CENTER_HIGH.toFixed(2)}, ` +
      `and both asks are above $${TARGET_BID.toFixed(2)}.${colors.reset}`
    );

    connectWebsocket();
  } catch (e) {
    console.log(`${colors.red}[SCANNER ERROR] ${e.message}${colors.reset}`);
    searchCooldownTimer = Date.now() + 5000;
  } finally {
    isSearchingNextMarket = false;
  }
}

// -----------------------------------------------------------------------------
// DASHBOARD
// -----------------------------------------------------------------------------
http.createServer((req, res) => {
  if (req.url === '/api/live') {
    res.writeHead(200, { 'Content-Type': 'application/json' });

    res.end(JSON.stringify({
      stats,
      arbState,
      currentAsks,
      currentBids,
      recentTrades,
      currentMarketSlug,
      marketEndTime
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

// -----------------------------------------------------------------------------
// MAIN
// -----------------------------------------------------------------------------
async function runLiveTrader() {
  console.log(
    `${colors.magenta}Booting BTC 5m 50/50-centre sequential arb PAPER engine...${colors.reset}`
  );

  console.log(
    `${colors.gray}Rule: only arm around true 50/50 centre. ` +
    `YES and NO mids must both be between ${CENTER_LOW.toFixed(2)} and ${CENTER_HIGH.toFixed(2)}. ` +
    `Then place simulated resting bids at ${TARGET_BID}. ` +
    `If one fills, the other has ${SECOND_LEG_TIMEOUT_MS / 1000}s to fill.${colors.reset}`
  );

  const account = privateKeyToAccount(rawKey);

  const walletClient = createWalletClient({
    account,
    chain: polygon,
    transport: viemHttp()
  });

  clobClient = new ClobClient(HOST, CHAIN_ID, walletClient);

  try {
    await clobClient.deriveApiKey();
  } catch (e) {
    await clobClient.createApiKey();
  }

  await loadNextMarket();

  setInterval(async () => {
    const now = Date.now();

    if (marketEndTime === 0) {
      if (now > searchCooldownTimer) await loadNextMarket();
      return;
    }

    if (now >= marketEndTime && !isSearchingNextMarket) {
      await loadNextMarket();
      return;
    }

    // Arm/timeout checks even if only one side is updating.
    if (arbState.active && arbState.status === 'WAITING_TO_ARM') {
      tryArmRestingLimitOrders();
    }

    if (
      arbState.active &&
      arbState.status === 'WAITING_SECOND_FILL' &&
      now >= arbState.timeoutAt
    ) {
      bailoutUnmatchedLeg('SECOND_LEG_TIMEOUT');
    }

    if (bothPricesKnown()) {
      priceStream.write(
        `${new Date().toISOString()},` +
        `${currentAsks.YES.toFixed(3)},` +
        `${currentAsks.NO.toFixed(3)},` +
        `${currentBids.YES.toFixed(3)},` +
        `${currentBids.NO.toFixed(3)},` +
        `${getMid('YES').toFixed(3)},` +
        `${getMid('NO').toFixed(3)}\n`
      );
    }

    if (Math.floor(now / 1000) % 10 === 0) {
      const timeoutRemaining = arbState.timeoutAt
        ? Math.max(0, ((arbState.timeoutAt - now) / 1000)).toFixed(1)
        : '-';

      const readableStatus = getReadableStatus();

      const yesAsk = fmtPrice(currentAsks.YES);
      const yesBid = fmtPrice(currentBids.YES);
      const yesMid = fmtMid('YES');

      const noAsk = fmtPrice(currentAsks.NO);
      const noBid = fmtPrice(currentBids.NO);
      const noMid = fmtMid('NO');

      console.log(
        `${colors.gray}[LIVE] ${readableStatus} | ` +
        `${secondsLeftInMarket()}s left | ` +
        `Timeout ${timeoutRemaining}s | ` +
        `Bal $${stats.currentBalance.toFixed(2)} | ` +
        `YES ask ${yesAsk}, bid ${yesBid}, mid ${yesMid} | ` +
        `NO ask ${noAsk}, bid ${noBid}, mid ${noMid} | ` +
        `Centre ${CENTER_LOW.toFixed(2)}-${CENTER_HIGH.toFixed(2)} | ` +
        `Target $${TARGET_BID.toFixed(2)} | ` +
        `Last ${arbState.lastAction || '-'}${colors.reset}`
      );
    }
  }, 1000);
}

runLiveTrader().catch(console.error);
``