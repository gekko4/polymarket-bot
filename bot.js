require('dotenv').config();

const { ClobClient } = require('@polymarket/clob-client');
const { createWalletClient, http: viemHttp } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { polygon } = require('viem/chains');
const WebSocket = require('ws');
const fs = require('fs');
const http = require('http');
const path = require('path');

// ------------------------------------------------------------
// TERMINAL COLOURS
// ------------------------------------------------------------
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

// ------------------------------------------------------------
// SECURITY & AUTH
// ------------------------------------------------------------
let rawKey = process.env.PRIVATE_KEY;
if (!rawKey) throw new Error(`${colors.red}CRITICAL: PRIVATE_KEY is missing from .env file!${colors.reset}`);
if (!rawKey.startsWith('0x')) rawKey = '0x' + rawKey;

const CHAIN_ID = 137;
const HOST = 'https://clob.polymarket.com';
let clobClient;

// ------------------------------------------------------------
// STRATEGY CONFIGURATION
// ------------------------------------------------------------
// This is now structured around the tested rule from price_history.csv:
// first leg fills <= 0.49, second leg must fill within ~15s, otherwise exit.
const ARB_TARGET_PRICE = 0.49;
const BET_SIZE_USD_PER_SIDE = 1.00;
const SECOND_LEG_TIMEOUT_MS = 15_000;
const MIN_SECONDS_LEFT_TO_ENTER = 25;      // need enough time for 15s wait + bailout buffer
const NO_NEW_FILL_SECONDS_LEFT = 5;        // do not accept new fills inside final 5s
const TAKER_FEE_BPS = 180;                 // used only for simulated bailout market sell
const PAPER_TRADING = true;                // this file still simulates fills; it does not place real orders

// ------------------------------------------------------------
// STATE
// ------------------------------------------------------------
const emptyArbState = () => ({
  active: false,
  status: 'IDLE', // IDLE | WAITING_FIRST_FILL | WAITING_SECOND_FILL | LOCKED

  firstSide: null,
  firstFillTime: 0,

  yesFilled: false,
  noFilled: false,
  yesFillPrice: 0,
  noFillPrice: 0,
  yesShares: 0,
  noShares: 0,

  // For dashboard/debugging
  currentMarketSlug: null,
  lastAction: null,
  timeoutAt: 0
});

let arbState = emptyArbState();

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
let currentMarketSlug = null;

// ------------------------------------------------------------
// LOGGING
// ------------------------------------------------------------
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
  tradeStream.write('Date,Market,Action,PnL_USD,Balance_USD,Win_Rate_Pct,YES_Filled,NO_Filled,YES_Price,NO_Price,YES_Shares,NO_Shares\n');
}
if (!fs.existsSync(priceLogFile) || fs.statSync(priceLogFile).size === 0) {
  priceStream.write('Timestamp,YES_Ask,NO_Ask,YES_Bid,NO_Bid\n');
}

// ------------------------------------------------------------
// HELPERS
// ------------------------------------------------------------
function secondsLeftInMarket() {
  return Math.max(0, Math.floor((marketEndTime - Date.now()) / 1000));
}

function sideTokenToName(tokenId) {
  if (tokenId === currentYesToken) return 'YES';
  if (tokenId === currentNoToken) return 'NO';
  return null;
}

function oppositeSide(side) {
  return side === 'YES' ? 'NO' : 'YES';
}

function fillFirstLeg(side) {
  const now = Date.now();
  arbState.status = 'WAITING_SECOND_FILL';
  arbState.firstSide = side;
  arbState.firstFillTime = now;
  arbState.timeoutAt = now + SECOND_LEG_TIMEOUT_MS;
  arbState.lastAction = `FIRST_FILL_${side}`;

  if (side === 'YES') {
    arbState.yesFilled = true;
    arbState.yesFillPrice = ARB_TARGET_PRICE;
    arbState.yesShares = BET_SIZE_USD_PER_SIDE / ARB_TARGET_PRICE;
  } else {
    arbState.noFilled = true;
    arbState.noFillPrice = ARB_TARGET_PRICE;
    arbState.noShares = BET_SIZE_USD_PER_SIDE / ARB_TARGET_PRICE;
  }

  console.log(`\n${colors.cyan}[FIRST FILL] ${side} filled at $${ARB_TARGET_PRICE.toFixed(2)}. Waiting ${SECOND_LEG_TIMEOUT_MS / 1000}s for ${oppositeSide(side)}.${colors.reset}`);
}

function fillSecondLeg(side) {
  if (side === 'YES') {
    arbState.yesFilled = true;
    arbState.yesFillPrice = ARB_TARGET_PRICE;
    arbState.yesShares = BET_SIZE_USD_PER_SIDE / ARB_TARGET_PRICE;
  } else {
    arbState.noFilled = true;
    arbState.noFillPrice = ARB_TARGET_PRICE;
    arbState.noShares = BET_SIZE_USD_PER_SIDE / ARB_TARGET_PRICE;
  }

  arbState.status = 'LOCKED';
  arbState.lastAction = `SECOND_FILL_${side}`;

  // With equal $ size at equal target price, shares are equal.
  // Guaranteed settlement payout is min(YES shares, NO shares), because exactly one side pays $1/share.
  const payoutShares = Math.min(arbState.yesShares, arbState.noShares);
  const totalCost = (arbState.yesShares * arbState.yesFillPrice) + (arbState.noShares * arbState.noFillPrice);
  const grossReturn = payoutShares * 1.00;
  const netPnL = grossReturn - totalCost;

  console.log(`${colors.green}[ARB LOCKED] Second leg ${side} filled within timeout. Guaranteed settlement profile locked.${colors.reset}`);
  logCompletedArb('FULL ARBITRAGE CAPTURE', netPnL);
}

function bailoutUnmatchedLeg(reason) {
  if (!arbState.active) return;

  let side = null;
  if (arbState.yesFilled && !arbState.noFilled) side = 'YES';
  if (arbState.noFilled && !arbState.yesFilled) side = 'NO';

  if (!side) return;

  const entryPrice = side === 'YES' ? arbState.yesFillPrice : arbState.noFillPrice;
  const shares = side === 'YES' ? arbState.yesShares : arbState.noShares;
  const exitBid = currentBids[side];

  if (!exitBid || isNaN(exitBid) || exitBid <= 0) {
    console.log(`${colors.red}[BAILOUT BLOCKED] No valid ${side} bid available. Cannot simulate exit safely.${colors.reset}`);
    return;
  }

  const entryCost = shares * entryPrice;
  const exitValue = shares * exitBid;
  const takerFee = exitValue * (TAKER_FEE_BPS / 10000);
  const netPnL = exitValue - entryCost - takerFee;

  console.log(`${colors.magenta}[EARLY BAILOUT] ${reason}. Sold unmatched ${side} at bid $${exitBid.toFixed(3)}.${colors.reset}`);
  logCompletedArb(`BAILOUT_${side}_${reason}`, netPnL);
}

function logCompletedArb(exitReason, netPnL) {
  stats.totalTrades++;
  if (netPnL > 0) stats.wins++;
  else if (netPnL < 0) stats.losses++;
  stats.currentBalance += netPnL;

  const winRate = stats.totalTrades > 0 ? ((stats.wins / stats.totalTrades) * 100).toFixed(1) : '0.0';
  const roi = (((stats.currentBalance - stats.startingBalance) / stats.startingBalance) * 100).toFixed(2);
  const c = netPnL > 0 ? colors.brightYellow : (netPnL < 0 ? colors.red : colors.gray);

  console.log(`\n${colors.gray}========================================${colors.reset}`);
  console.log(`[ARB RESOLVED] Outcome: ${c}${exitReason}${colors.reset}`);
  console.log(`NET PnL: ${c}$${netPnL > 0 ? '+' : ''}${netPnL.toFixed(4)}${colors.reset}`);
  console.log(`${colors.gray}---${colors.reset}`);
  console.log(`[STATS] Balance: $${stats.currentBalance.toFixed(2)} (${roi > 0 ? '+' : ''}${roi}% ROI)`);
  console.log(`[STATS] Win Rate: ${winRate}% (${stats.wins}W / ${stats.losses}L)`);
  console.log(`${colors.gray}========================================\n${colors.reset}`);

  const logEntry = [
    new Date().toISOString(),
    currentMarketSlug || 'BTC-5M',
    exitReason,
    netPnL.toFixed(4),
    stats.currentBalance.toFixed(2),
    `${winRate}%`,
    arbState.yesFilled,
    arbState.noFilled,
    arbState.yesFillPrice.toFixed(4),
    arbState.noFillPrice.toFixed(4),
    arbState.yesShares.toFixed(6),
    arbState.noShares.toFixed(6)
  ].join(',') + '\n';

  tradeStream.write(logEntry);

  recentTrades.unshift({
    time: new Date().toLocaleTimeString(),
    reason: exitReason,
    pnl: netPnL.toFixed(4)
  });
  if (recentTrades.length > 10) recentTrades.pop();

  arbState = emptyArbState();
}

// ------------------------------------------------------------
// CORE STRATEGY
// ------------------------------------------------------------
function handleMarketUpdate(data) {
  if (!data || !arbState.active) return;

  const bestAsk = Number(data.bestAsk);
  const bestBid = Number(data.bestBid);
  if (isNaN(bestAsk) || isNaN(bestBid)) return;

  const side = sideTokenToName(data.asset_id);
  if (!side) return;

  currentAsks[side] = bestAsk;
  currentBids[side] = bestBid;

  const now = Date.now();
  const secondsLeft = secondsLeftInMarket();

  // 1) If already holding one leg, enforce the tested 15s timeout immediately.
  if (arbState.status === 'WAITING_SECOND_FILL' && now >= arbState.timeoutAt) {
    bailoutUnmatchedLeg('SECOND_LEG_TIMEOUT');
    return;
  }

  // 2) Do not accept fresh fills too close to market end.
  if (secondsLeft <= NO_NEW_FILL_SECONDS_LEFT) {
    if (arbState.status === 'WAITING_SECOND_FILL') {
      bailoutUnmatchedLeg('MARKET_END_BAILOUT');
    }
    return;
  }

  // 3) First-leg entry: only enter if there is enough time for the timeout + exit buffer.
  if (arbState.status === 'WAITING_FIRST_FILL') {
    if (secondsLeft < MIN_SECONDS_LEFT_TO_ENTER) return;

    if (bestAsk <= ARB_TARGET_PRICE) {
      fillFirstLeg(side);
    }
    return;
  }

  // 4) Second-leg completion: only the opposite side can complete the arb.
  if (arbState.status === 'WAITING_SECOND_FILL') {
    const neededSide = oppositeSide(arbState.firstSide);
    if (side === neededSide && bestAsk <= ARB_TARGET_PRICE) {
      fillSecondLeg(side);
    }
  }
}

// ------------------------------------------------------------
// WEBSOCKET
// ------------------------------------------------------------
function connectWebsocket() {
  if (global.wsMarket) {
    try { global.wsMarket.terminate(); } catch (e) {}
  }

  console.log(`${colors.yellow}[WS] Booting fresh market connection...${colors.reset}`);
  const wsMarket = new WebSocket('wss://ws-subscriptions-clob.polymarket.com/ws/market');
  global.wsMarket = wsMarket;

  wsMarket.on('open', () => {
    if (currentYesToken && currentNoToken) {
      wsMarket.send(JSON.stringify({ type: 'market', assets_ids: [currentYesToken, currentNoToken] }));
    }
  });

  wsMarket.on('message', (msg) => {
    const textMsg = msg.toString();
    if (textMsg === 'PONG') return;

    try {
      const data = JSON.parse(textMsg);

      if (data.event_type === 'book' && data.asks?.length > 0 && data.bids?.length > 0) {
        handleMarketUpdate({
          asset_id: data.asset_id,
          bestAsk: parseFloat(data.asks[0].price),
          bestAskSize: parseFloat(data.asks[0].size),
          bestBid: parseFloat(data.bids[0].price),
          bestBidSize: parseFloat(data.bids[0].size)
        });
      } else if (data.event_type === 'price_change' && data.price_changes?.length > 0) {
        for (const pc of data.price_changes) {
          handleMarketUpdate({
            asset_id: pc.asset_id,
            bestAsk: parseFloat(pc.best_ask),
            bestAskSize: Number(pc.best_ask_size || 0),
            bestBid: parseFloat(pc.best_bid),
            bestBidSize: Number(pc.best_bid_size || 0)
          });
        }
      }
    } catch (err) {
      // Keep websocket resilient; malformed messages should not crash the bot.
    }
  });

  wsMarket.on('close', () => {
    console.log(`${colors.gray}[WS] Market websocket closed.${colors.reset}`);
  });

  wsMarket.on('error', (err) => {
    console.log(`${colors.red}[WS ERROR] ${err.message}${colors.reset}`);
  });
}

// ------------------------------------------------------------
// MARKET SCANNER
// ------------------------------------------------------------
async function loadNextMarket() {
  if (isSearchingNextMarket) return;
  isSearchingNextMarket = true;

  console.log(`\n${colors.yellow}[SCANNER] Calculating the current active 5-minute BTC market...${colors.reset}`);

  try {
    const nowSec = Math.floor(Date.now() / 1000);
    const remainder = nowSec % 300;
    const currentIntervalStartSec = nowSec - remainder;
    const currentIntervalEndSec = currentIntervalStartSec + 300;

    const eventSlug = `btc-updown-5m-${currentIntervalStartSec}`;
    const response = await fetch(`https://gamma-api.polymarket.com/events?slug=${eventSlug}`);
    const events = await response.json();

    if (!events || events.length === 0 || !events[0].markets || events[0].markets.length === 0) {
      console.log(`${colors.gray}[SCANNER] Market ${eventSlug} not fully indexed yet. Retrying shortly...${colors.reset}`);
      searchCooldownTimer = Date.now() + 5000;
      return;
    }

    const validEvent = events[0];
    const validMarket = validEvent.markets[0];

    const parsedTokens = typeof validMarket.clobTokenIds === 'string'
      ? JSON.parse(validMarket.clobTokenIds)
      : validMarket.clobTokenIds;

    const yesTokenId = parsedTokens?.[0];
    const noTokenId = parsedTokens?.[1];

    if (!yesTokenId || !noTokenId) {
      searchCooldownTimer = Date.now() + 5000;
      return;
    }

    currentYesToken = yesTokenId;
    currentNoToken = noTokenId;
    marketEndTime = currentIntervalEndSec * 1000;
    currentMarketSlug = eventSlug;
    currentAsks = { YES: 0, NO: 0 };
    currentBids = { YES: 0, NO: 0 };

    arbState = emptyArbState();
    arbState.active = true;
    arbState.status = 'WAITING_FIRST_FILL';
    arbState.currentMarketSlug = eventSlug;

    console.log(`${colors.brightYellow}[MARKET LOADED] ${validEvent.title}${colors.reset}`);
    console.log(`${colors.magenta}[PAPER STRATEGY] Watching for first leg <= $${ARB_TARGET_PRICE.toFixed(2)}. Second leg must fill within ${SECOND_LEG_TIMEOUT_MS / 1000}s or bailout.${colors.reset}`);

    connectWebsocket();
  } catch (err) {
    console.log(`${colors.red}[SCANNER ERROR] ${err.message}${colors.reset}`);
    searchCooldownTimer = Date.now() + 5000;
  } finally {
    isSearchingNextMarket = false;
  }
}

// ------------------------------------------------------------
// DASHBOARD
// ------------------------------------------------------------
http.createServer((req, res) => {
  if (req.url === '/api/live') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ stats, arbState, currentAsks, currentBids, recentTrades, currentMarketSlug, marketEndTime }));
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

// ------------------------------------------------------------
// MAIN LOOP
// ------------------------------------------------------------
async function runLiveTrader() {
  console.log(`${colors.magenta}Booting BTC 5m Sequential Arbitrage Engine in PAPER TRADING MODE...${colors.reset}`);
  console.log(`${colors.gray}Rule: first fill <= ${ARB_TARGET_PRICE}, second fill timeout ${SECOND_LEG_TIMEOUT_MS / 1000}s, bailout unmatched leg at current bid.${colors.reset}`);

  const account = privateKeyToAccount(rawKey);
  const walletClient = createWalletClient({ account, chain: polygon, transport: viemHttp() });
  clobClient = new ClobClient(HOST, CHAIN_ID, walletClient);

  try { await clobClient.deriveApiKey(); }
  catch (e) { await clobClient.createApiKey(); }

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

    // Enforce timeout even if websocket updates pause.
    if (arbState.active && arbState.status === 'WAITING_SECOND_FILL' && now >= arbState.timeoutAt) {
      bailoutUnmatchedLeg('SECOND_LEG_TIMEOUT');
    }

    if (currentAsks.YES > 0 && currentAsks.NO > 0) {
      priceStream.write(`${new Date().toISOString()},${currentAsks.YES.toFixed(3)},${currentAsks.NO.toFixed(3)},${currentBids.YES.toFixed(3)},${currentBids.NO.toFixed(3)}\n`);
    }

    if (Math.floor(now / 1000) % 10 === 0) {
      const secondsLeft = secondsLeftInMarket();
      const timeoutRemaining = arbState.timeoutAt ? Math.max(0, ((arbState.timeoutAt - now) / 1000).toFixed(1)) : '-';
      const statusColor = arbState.status === 'WAITING_SECOND_FILL' ? colors.cyan : colors.gray;
      console.log(`${statusColor}[LIVE] ${arbState.status} | Market left: ${secondsLeft}s | Timeout: ${timeoutRemaining}s | Bal: $${stats.currentBalance.toFixed(2)} | YES A/B: ${currentAsks.YES.toFixed(3)}/${currentBids.YES.toFixed(3)} | NO A/B: ${currentAsks.NO.toFixed(3)}/${currentBids.NO.toFixed(3)}${colors.reset}`);
    }
  }, 1000);
}

runLiveTrader().catch(console.error);
