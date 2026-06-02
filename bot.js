require('dotenv').config();

const { ClobClient, OrderType, Side } = require('@polymarket/clob-client');
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

// ============================================================================
// ENV / MODE
// ============================================================================

let rawKey = process.env.PRIVATE_KEY;
if (!rawKey) throw new Error('PRIVATE_KEY missing from .env');
if (!rawKey.startsWith('0x')) rawKey = '0x' + rawKey;

const TRADING_MODE = (process.env.TRADING_MODE || 'paper').toLowerCase();

if (!['paper', 'live'].includes(TRADING_MODE)) {
  throw new Error(`Invalid TRADING_MODE=${TRADING_MODE}. Use paper or live.`);
}

const IS_PAPER = TRADING_MODE === 'paper';

const HOST = 'https://clob.polymarket.com';
const CHAIN_ID = 137;

const SIGNATURE_TYPE = process.env.SIGNATURE_TYPE
  ? Number(process.env.SIGNATURE_TYPE)
  : undefined;

const FUNDER = process.env.FUNDER || undefined;

let clobClient;
let execution;

// ============================================================================
// STRATEGY CONFIG
// ============================================================================

const TARGET_BID = 0.49;
const BET_SIZE_USD_PER_SIDE = 1.00;

const CENTER_ASK_LOW = 0.47;
const CENTER_ASK_HIGH = 0.56;

const CENTER_BID_LOW = 0.44;
const CENTER_BID_HIGH = 0.53;

const MAX_SIDE_SPREAD = 0.08;
const MAX_COMBINED_ASKS = 1.12;
const MIN_COMBINED_BIDS = 0.88;

const MIN_ASK_ABOVE_TARGET = 0.005;
const SECOND_LEG_TIMEOUT_MS = 15_000;
const MIN_SECONDS_LEFT_TO_PLACE_ORDERS = 35;
const NO_NEW_ENTRIES_SECONDS_LEFT = 20;
const ONE_TRADE_PER_MARKET = true;

const PAPER_TAKER_FEE_BPS = 180;
const EXIT_MIN_ACCEPTABLE_PRICE = 0.01;

// ============================================================================
// STATE
// ============================================================================

function blankState() {
  return {
    active: false,

    status: 'IDLE',
    // IDLE
    // WAITING_FOR_CENTER
    // PLACING_ORDERS
    // ORDERS_WORKING
    // ONE_LEG_FILLED
    // ARB_LOCKED
    // EXITING
    // DONE

    yesOrderId: null,
    noOrderId: null,

    yesOrderLive: false,
    noOrderLive: false,

    yesFilled: false,
    noFilled: false,

    yesAvgFillPrice: 0,
    noAvgFillPrice: 0,

    yesFilledShares: 0,
    noFilledShares: 0,

    firstFilledSide: null,
    firstFillTime: 0,
    timeoutAt: 0,

    lastAction: null,
    lastBlockReason: null
  };
}

let state = blankState();

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

let currentYesToken = null;
let currentNoToken = null;
let currentMarketSlug = null;
let marketEndTime = 0;

let currentMarketMeta = {
  tickSize: '0.001',
  negRisk: false
};

let isSearchingNextMarket = false;
let searchCooldownTimer = 0;

const completedMarketSlugs = new Set();
let recentTrades = [];

// ============================================================================
// FILE LOGGING
// ============================================================================

const tradeLogFile = IS_PAPER ? 'paper_trades_log.csv' : 'live_trades_log.csv';
const priceLogFile = 'price_history.csv';
const terminalLogFile = 'terminal_logs.txt';

const tradeStream = fs.createWriteStream(tradeLogFile, { flags: 'a' });
const priceStream = fs.createWriteStream(priceLogFile, { flags: 'a' });
const terminalStream = fs.createWriteStream(terminalLogFile, { flags: 'a' });

function stripAnsi(value) {
  return String(value).replace(/\x1b\[[0-9;]*m/g, '');
}

const originalLog = console.log;

console.log = function (...args) {
  originalLog.apply(console, args);

  const message = args
    .map(a => typeof a === 'object' ? JSON.stringify(a) : String(a))
    .join(' ');

  terminalStream.write(`[${new Date().toISOString()}] ${stripAnsi(message)}\n`);
};

if (!fs.existsSync(tradeLogFile) || fs.statSync(tradeLogFile).size === 0) {
  tradeStream.write(
    'Date,Mode,Market,Action,PnL_USD,Balance_USD,Win_Rate_Pct,YES_Filled,NO_Filled,YES_Avg,NO_Avg,YES_Shares,NO_Shares\n'
  );
}

if (!fs.existsSync(priceLogFile) || fs.statSync(priceLogFile).size === 0) {
  priceStream.write(
    'Timestamp,YES_Ask,YES_Bid,YES_Spread,NO_Ask,NO_Bid,NO_Spread,AskSum,BidSum,Status,LastAction\n'
  );
}

// ============================================================================
// TERMINAL LOGGER
// ============================================================================

const LOG_STATUS_EVERY_MS = 10_000;

let lastStatusLogAt = 0;
let lastStatusKey = '';

function terminalPrice(value) {
  if (!value || Number(value) <= 0 || Number.isNaN(Number(value))) return '---';
  return Number(value).toFixed(3);
}

function money(value) {
  const n = Number(value || 0);
  return `$${n.toFixed(2)}`;
}

function pnlMoney(value) {
  const n = Number(value || 0);
  return `${n >= 0 ? '+' : ''}$${n.toFixed(4)}`;
}

function roiPct(value) {
  const n = Number(value || 0);
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`;
}

function compactReason(reason) {
  return String(reason || '-')
    .replace(/_/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function line(text = '') {
  console.log(text);
}

const logger = {
  boot(message) {
    line(`${colors.magenta}▶ BOOT${colors.reset} | ${message}`);
  },

  scanner(message) {
    line(`${colors.yellow}◆ SCANNER${colors.reset} | ${message}`);
  },

  ws(message) {
    line(`${colors.cyan}◇ WS${colors.reset} | ${message}`);
  },

  event(title, fields = {}) {
    const parts = Object.entries(fields)
      .filter(([, value]) => value !== undefined && value !== null && value !== '')
      .map(([key, value]) => `${key}=${value}`);

    line(
      `${colors.green}✓ ${title}${colors.reset}` +
      (parts.length ? ` | ${parts.join(' | ')}` : '')
    );
  },

  warn(title, fields = {}) {
    const parts = Object.entries(fields)
      .filter(([, value]) => value !== undefined && value !== null && value !== '')
      .map(([key, value]) => `${key}=${value}`);

    line(
      `${colors.yellow}⚠ ${title}${colors.reset}` +
      (parts.length ? ` | ${parts.join(' | ')}` : '')
    );
  },

  error(title, errorOrFields = {}) {
    let fields = {};

    if (errorOrFields instanceof Error) {
      fields.message = errorOrFields.message;
    } else if (typeof errorOrFields === 'string') {
      fields.message = errorOrFields;
    } else {
      fields = errorOrFields;
    }

    const parts = Object.entries(fields)
      .filter(([, value]) => value !== undefined && value !== null && value !== '')
      .map(([key, value]) => `${key}=${value}`);

    line(
      `${colors.red}✕ ${title}${colors.reset}` +
      (parts.length ? ` | ${parts.join(' | ')}` : '')
    );
  },

  trade({ reason, netPnL, balance, roi, winRate, wins, losses }) {
    const isWin = Number(netPnL) > 0;
    const isLoss = Number(netPnL) < 0;
    const c = isWin ? colors.green : isLoss ? colors.red : colors.gray;

    line('');
    line(`${colors.gray}┌──────────────── TRADE RESOLVED ────────────────┐${colors.reset}`);
    line(`${colors.gray}│${colors.reset} Result   : ${c}${compactReason(reason)}${colors.reset}`);
    line(`${colors.gray}│${colors.reset} PnL      : ${c}${pnlMoney(netPnL)}${colors.reset}`);
    line(`${colors.gray}│${colors.reset} Balance  : ${money(balance)} (${roiPct(roi)} ROI)`);
    line(`${colors.gray}│${colors.reset} WinRate  : ${winRate}% (${wins}W / ${losses}L)`);
    line(`${colors.gray}└────────────────────────────────────────────────┘${colors.reset}`);
    line('');
  },

  status(snapshot) {
    const now = Date.now();

    const key = [
      snapshot.mode,
      snapshot.status,
      snapshot.left,
      snapshot.timeout,
      snapshot.yesAsk,
      snapshot.yesBid,
      snapshot.noAsk,
      snapshot.noBid,
      snapshot.last
    ].join('|');

    const shouldPrint =
      key !== lastStatusKey ||
      now - lastStatusLogAt >= LOG_STATUS_EVERY_MS;

    if (!shouldPrint) return;

    lastStatusKey = key;
    lastStatusLogAt = now;

    const statusColour =
      snapshot.status.includes('HOLDING') ||
      snapshot.status.includes('ORDERS WORKING')
        ? colors.cyan
        : snapshot.status.includes('WAITING')
          ? colors.yellow
          : snapshot.status.includes('DONE')
            ? colors.gray
            : colors.green;

    line(
      `${colors.gray}[${snapshot.mode}]${colors.reset} ` +
      `${statusColour}${snapshot.status}${colors.reset} | ` +
      `${snapshot.left}s left | ` +
      `timeout ${snapshot.timeout}s | ` +
      `bal ${snapshot.balance} | ` +
      `YES ask ${snapshot.yesAsk} bid ${snapshot.yesBid} spr ${snapshot.yesSpread} | ` +
      `NO ask ${snapshot.noAsk} bid ${snapshot.noBid} spr ${snapshot.noSpread} | ` +
      `sum ask ${snapshot.askSum} bid ${snapshot.bidSum} | ` +
      `last ${snapshot.last}`
    );
  }
};

// ============================================================================
// HELPERS
// ============================================================================

function secondsLeftInMarket() {
  return Math.max(0, Math.floor((marketEndTime - Date.now()) / 1000));
}

function sideFromToken(tokenId) {
  if (tokenId === currentYesToken) return 'YES';
  if (tokenId === currentNoToken) return 'NO';
  return null;
}

function tokenForSide(side) {
  return side === 'YES' ? currentYesToken : currentNoToken;
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

function getSpread(side) {
  if (!bothPricesKnown()) return 999;
  return currentAsks[side] - currentBids[side];
}

function askSum() {
  if (!bothPricesKnown()) return 0;
  return currentAsks.YES + currentAsks.NO;
}

function bidSum() {
  if (!bothPricesKnown()) return 0;
  return currentBids.YES + currentBids.NO;
}

function fmt(value) {
  return value > 0 ? Number(value).toFixed(3) : '---';
}

function readableStatus() {
  switch (state.status) {
    case 'WAITING_FOR_CENTER':
      return 'WAITING FOR REAL 50/50 BOOK';
    case 'PLACING_ORDERS':
      return 'PLACING LIMIT ORDERS';
    case 'ORDERS_WORKING':
      return 'LIMIT ORDERS WORKING';
    case 'ONE_LEG_FILLED':
      return `HOLDING ${state.firstFilledSide}, WAITING FOR ${opposite(state.firstFilledSide)}`;
    case 'ARB_LOCKED':
      return 'ARB LOCKED';
    case 'EXITING':
      return 'EXITING UNMATCHED LEG';
    case 'DONE':
      return 'DONE THIS MARKET';
    case 'IDLE':
      return 'IDLE';
    default:
      return state.status;
  }
}

// ============================================================================
// ENTRY FILTER
// ============================================================================

function hasTightSpreads() {
  if (!bothPricesKnown()) return false;

  return (
    getSpread('YES') > 0 &&
    getSpread('NO') > 0 &&
    getSpread('YES') <= MAX_SIDE_SPREAD &&
    getSpread('NO') <= MAX_SIDE_SPREAD
  );
}

function hasSaneCombinedBook() {
  if (!bothPricesKnown()) return false;
  return askSum() <= MAX_COMBINED_ASKS && bidSum() >= MIN_COMBINED_BIDS;
}

function isRealCenterBook() {
  if (!bothPricesKnown()) return false;

  const yesAskOk =
    currentAsks.YES >= CENTER_ASK_LOW &&
    currentAsks.YES <= CENTER_ASK_HIGH;

  const noAskOk =
    currentAsks.NO >= CENTER_ASK_LOW &&
    currentAsks.NO <= CENTER_ASK_HIGH;

  const yesBidOk =
    currentBids.YES >= CENTER_BID_LOW &&
    currentBids.YES <= CENTER_BID_HIGH;

  const noBidOk =
    currentBids.NO >= CENTER_BID_LOW &&
    currentBids.NO <= CENTER_BID_HIGH;

  return yesAskOk && noAskOk && yesBidOk && noBidOk;
}

function ordersWouldRest() {
  if (!bothPricesKnown()) return false;

  return (
    currentAsks.YES > TARGET_BID + MIN_ASK_ABOVE_TARGET &&
    currentAsks.NO > TARGET_BID + MIN_ASK_ABOVE_TARGET
  );
}

function getEntryBlockReason() {
  if (!bothPricesKnown()) return 'WAITING_FOR_FULL_BOOK';

  if (secondsLeftInMarket() < MIN_SECONDS_LEFT_TO_PLACE_ORDERS) {
    return 'TOO_CLOSE_TO_MARKET_END';
  }

  if (!hasTightSpreads()) {
    return 'SPREAD_TOO_WIDE';
  }

  if (!hasSaneCombinedBook()) {
    return 'COMBINED_BOOK_BAD';
  }

  if (!isRealCenterBook()) {
    return 'NOT_REAL_50_50_BOOK';
  }

  if (!ordersWouldRest()) {
    return 'ORDER_WOULD_CROSS_NOT_REST';
  }

  return null;
}

// ============================================================================
// EXECUTION ADAPTERS
// ============================================================================

class PaperExecutionAdapter {
  constructor() {
    this.orders = new Map();
    this.nextId = 1;
  }

  async placeLimitBuy({ tokenId, sideName, price, usdSize }) {
    const currentAsk = currentAsks[sideName];

    if (!currentAsk || currentAsk <= price + MIN_ASK_ABOVE_TARGET) {
      throw new Error(`PAPER_REJECT_${sideName}_ORDER_WOULD_CROSS ask=${fmt(currentAsk)} price=${price}`);
    }

    const orderId = `paper-${Date.now()}-${this.nextId++}`;
    const shares = usdSize / price;

    const order = {
      orderId,
      tokenId,
      sideName,
      type: 'BUY',
      price,
      usdSize,
      shares,
      filledShares: 0,
      avgFillPrice: 0,
      isFullyFilled: false,
      isCancelled: false,
      createdAt: Date.now()
    };

    this.orders.set(orderId, order);

    logger.event('PAPER ORDER PLACED', {
      side: sideName,
      type: 'BUY',
      shares: shares.toFixed(6),
      price: price.toFixed(2),
      orderId
    });

    return { orderId, raw: order };
  }

  async cancelOrder(orderId) {
    const order = this.orders.get(orderId);

    if (!order) {
      return { ok: false, raw: null };
    }

    order.isCancelled = true;

    logger.warn('PAPER ORDER CANCELLED', { orderId });

    return { ok: true, raw: order };
  }

  async getOrderStatus(orderId) {
    const order = this.orders.get(orderId);

    if (!order) {
      throw new Error(`PAPER_ORDER_NOT_FOUND ${orderId}`);
    }

    if (!order.isCancelled && !order.isFullyFilled) {
      const ask = currentAsks[order.sideName];

      if (ask > 0 && ask <= order.price) {
        order.filledShares = order.shares;
        order.avgFillPrice = order.price;
        order.isFullyFilled = true;

        logger.event('PAPER FILL', {
          side: order.sideName,
          orderId,
          price: order.price.toFixed(2),
          ask: ask.toFixed(3)
        });
      }
    }

    return {
      orderId: order.orderId,
      filledShares: order.filledShares,
      avgFillPrice: order.avgFillPrice,
      isFullyFilled: order.isFullyFilled,
      isCancelled: order.isCancelled,
      raw: order
    };
  }

  async exitPosition({ tokenId, sideName, shares, minAcceptablePrice }) {
    const bid = currentBids[sideName];

    if (!bid || bid <= 0) {
      throw new Error(`PAPER_EXIT_NO_VALID_BID_${sideName}`);
    }

    if (bid < minAcceptablePrice) {
      throw new Error(`PAPER_EXIT_BID_BELOW_MIN_${sideName} bid=${bid}`);
    }

    const gross = shares * bid;
    const fee = gross * (PAPER_TAKER_FEE_BPS / 10000);
    const netValue = gross - fee;
    const effectiveExit = netValue / shares;

    logger.event('PAPER EXIT', {
      side: sideName,
      shares: shares.toFixed(6),
      bid: bid.toFixed(3),
      effective: effectiveExit.toFixed(4)
    });

    return {
      soldShares: shares,
      avgExitPrice: effectiveExit,
      raw: { tokenId, sideName, shares, bid, fee, netValue }
    };
  }
}

class LiveExecutionAdapter {
  constructor(client) {
    this.client = client;
  }

  async placeLimitBuy({ tokenId, sideName, price, usdSize }) {
    const shares = usdSize / price;

    const response = await this.client.createAndPostOrder(
      {
        tokenID: tokenId,
        price,
        side: Side.BUY,
        size: shares
      },
      {
        tickSize: currentMarketMeta.tickSize,
        negRisk: currentMarketMeta.negRisk
      },
      OrderType.GTC
    );

    const orderId = response?.orderID || response?.orderId || response?.id;

    if (!orderId) {
      throw new Error(`LIVE_ORDER_NO_ID ${JSON.stringify(response)}`);
    }

    logger.event('LIVE ORDER PLACED', {
      side: sideName,
      type: 'BUY',
      shares: shares.toFixed(6),
      price: price.toFixed(2),
      orderId
    });

    return { orderId, raw: response };
  }

  async cancelOrder(orderId) {
    if (typeof this.client.cancelOrder === 'function') {
      const response = await this.client.cancelOrder(orderId);
      return { ok: true, raw: response };
    }

    if (typeof this.client.cancel === 'function') {
      const response = await this.client.cancel(orderId);
      return { ok: true, raw: response };
    }

    throw new Error('LIVE_CANCEL_METHOD_NOT_AVAILABLE');
  }

  async getOrderStatus(orderId) {
    let response = null;

    if (typeof this.client.getOrder === 'function') {
      response = await this.client.getOrder(orderId);
    } else if (typeof this.client.getOrders === 'function') {
      const orders = await this.client.getOrders();
      response = Array.isArray(orders)
        ? orders.find(o => o.id === orderId || o.orderId === orderId || o.orderID === orderId)
        : null;
    } else {
      throw new Error('LIVE_ORDER_STATUS_METHOD_NOT_AVAILABLE');
    }

    if (!response) {
      return {
        orderId,
        filledShares: 0,
        avgFillPrice: 0,
        isFullyFilled: false,
        isCancelled: false,
        raw: null
      };
    }

    const filledShares = Number(
      response.filled_size ??
      response.filledSize ??
      response.size_matched ??
      response.matched_size ??
      response.filled ??
      0
    );

    const avgFillPrice = Number(
      response.avg_price ??
      response.avgPrice ??
      response.price ??
      0
    );

    const status = String(response.status || '').toLowerCase();

    return {
      orderId,
      filledShares,
      avgFillPrice,
      isFullyFilled:
        status.includes('matched') ||
        status.includes('filled') ||
        filledShares > 0,
      isCancelled: status.includes('cancel'),
      raw: response
    };
  }

  async exitPosition({ tokenId, sideName, shares, minAcceptablePrice }) {
    const bid = currentBids[sideName];

    if (!bid || bid < minAcceptablePrice) {
      throw new Error(`LIVE_EXIT_BLOCKED_${sideName} bid=${fmt(bid)} min=${minAcceptablePrice}`);
    }

    const response = await this.client.createAndPostOrder(
      {
        tokenID: tokenId,
        price: bid,
        side: Side.SELL,
        size: shares
      },
      {
        tickSize: currentMarketMeta.tickSize,
        negRisk: currentMarketMeta.negRisk
      },
      OrderType.FAK
    );

    return {
      soldShares: shares,
      avgExitPrice: bid,
      raw: response
    };
  }
}

// ============================================================================
// STRATEGY
// ============================================================================

async function maybePlaceEntryOrders() {
  if (state.status !== 'WAITING_FOR_CENTER') return;

  if (ONE_TRADE_PER_MARKET && completedMarketSlugs.has(currentMarketSlug)) {
    state.lastAction = 'MARKET_ALREADY_TRADED';
    return;
  }

  const blockReason = getEntryBlockReason();

  if (blockReason) {
    state.lastBlockReason = blockReason;
    state.lastAction = blockReason;
    return;
  }

  state.status = 'PLACING_ORDERS';
  state.lastAction = 'REAL_CENTER_CONFIRMED_PLACING_ORDERS';

  logger.event('ENTRY APPROVED', {
    yes: `${currentBids.YES.toFixed(3)}-${currentAsks.YES.toFixed(3)}`,
    yesSpread: getSpread('YES').toFixed(3),
    no: `${currentBids.NO.toFixed(3)}-${currentAsks.NO.toFixed(3)}`,
    noSpread: getSpread('NO').toFixed(3),
    askSum: askSum().toFixed(3),
    bidSum: bidSum().toFixed(3)
  });

  try {
    const yesOrder = await execution.placeLimitBuy({
      tokenId: currentYesToken,
      sideName: 'YES',
      price: TARGET_BID,
      usdSize: BET_SIZE_USD_PER_SIDE
    });

    const noOrder = await execution.placeLimitBuy({
      tokenId: currentNoToken,
      sideName: 'NO',
      price: TARGET_BID,
      usdSize: BET_SIZE_USD_PER_SIDE
    });

    state.yesOrderId = yesOrder.orderId;
    state.noOrderId = noOrder.orderId;
    state.yesOrderLive = true;
    state.noOrderLive = true;
    state.status = 'ORDERS_WORKING';
    state.lastAction = 'LIMIT_ORDERS_WORKING';

    logger.event('ORDERS WORKING', {
      yesOrder: state.yesOrderId,
      noOrder: state.noOrderId
    });
  } catch (err) {
    state.status = 'WAITING_FOR_CENTER';
    state.lastAction = `ORDER_PLACEMENT_FAILED ${err.message}`;
    logger.error('ORDER ERROR', err);
  }
}

async function checkOrderFills() {
  if (state.status !== 'ORDERS_WORKING' && state.status !== 'ONE_LEG_FILLED') return;

  try {
    if (state.yesOrderLive && state.yesOrderId && !state.yesFilled) {
      const s = await execution.getOrderStatus(state.yesOrderId);

      if (Number(s.filledShares) > 0) {
        state.yesFilled = true;
        state.yesFilledShares = Number(s.filledShares);
        state.yesAvgFillPrice = Number(s.avgFillPrice);
        state.yesOrderLive = !s.isFullyFilled;
        await handleLegFilled('YES');
      }
    }

    if (state.noOrderLive && state.noOrderId && !state.noFilled) {
      const s = await execution.getOrderStatus(state.noOrderId);

      if (Number(s.filledShares) > 0) {
        state.noFilled = true;
        state.noFilledShares = Number(s.filledShares);
        state.noAvgFillPrice = Number(s.avgFillPrice);
        state.noOrderLive = !s.isFullyFilled;
        await handleLegFilled('NO');
      }
    }

    if (state.yesFilled && state.noFilled) {
      lockArb();
    }
  } catch (err) {
    state.lastAction = `ORDER_STATUS_ERROR ${err.message}`;
    logger.error('STATUS ERROR', err);
  }
}

async function handleLegFilled(side) {
  if (!state.firstFilledSide) {
    state.firstFilledSide = side;
    state.firstFillTime = Date.now();
    state.timeoutAt = state.firstFillTime + SECOND_LEG_TIMEOUT_MS;
    state.status = 'ONE_LEG_FILLED';
    state.lastAction = `FIRST_FILL_${side}`;

    logger.event('FIRST FILL', {
      side,
      waitingFor: opposite(side),
      timeout: `${SECOND_LEG_TIMEOUT_MS / 1000}s`
    });
  }
}

function lockArb() {
  state.status = 'ARB_LOCKED';
  state.lastAction = 'BOTH_LEGS_FILLED';

  const payoutShares = Math.min(state.yesFilledShares, state.noFilledShares);

  const totalCost =
    (state.yesFilledShares * state.yesAvgFillPrice) +
    (state.noFilledShares * state.noAvgFillPrice);

  const netPnL = payoutShares - totalCost;

  logger.event('ARB LOCKED', {
    yesShares: state.yesFilledShares.toFixed(6),
    noShares: state.noFilledShares.toFixed(6)
  });

  logCompletedTrade('FULL_ARBITRAGE_CAPTURE', netPnL, 0);
}

async function handleSecondLegTimeout() {
  if (state.status !== 'ONE_LEG_FILLED') return;
  if (Date.now() < state.timeoutAt) return;

  const filledSide = state.firstFilledSide;
  const unfilledSide = opposite(filledSide);

  state.status = 'EXITING';
  state.lastAction = `SECOND_LEG_TIMEOUT_EXITING_${filledSide}`;

  logger.warn('SECOND LEG TIMEOUT', {
    filledSide,
    cancelling: unfilledSide,
    action: `exit ${filledSide}`
  });

  try {
    if (unfilledSide === 'YES' && state.yesOrderLive && state.yesOrderId) {
      await execution.cancelOrder(state.yesOrderId);
      state.yesOrderLive = false;
    }

    if (unfilledSide === 'NO' && state.noOrderLive && state.noOrderId) {
      await execution.cancelOrder(state.noOrderId);
      state.noOrderLive = false;
    }

    const shares = filledSide === 'YES' ? state.yesFilledShares : state.noFilledShares;
    const entryAvg = filledSide === 'YES' ? state.yesAvgFillPrice : state.noAvgFillPrice;

    const exit = await execution.exitPosition({
      tokenId: tokenForSide(filledSide),
      sideName: filledSide,
      shares,
      minAcceptablePrice: EXIT_MIN_ACCEPTABLE_PRICE
    });

    const soldShares = Number(exit.soldShares);
    const avgExitPrice = Number(exit.avgExitPrice);
    const netPnL = (soldShares * avgExitPrice) - (soldShares * entryAvg);

    logCompletedTrade(`BAILOUT_${filledSide}_SECOND_LEG_TIMEOUT`, netPnL, avgExitPrice);
  } catch (err) {
    state.status = 'EXITING';
    state.lastAction = `EXIT_FAILED ${err.message}`;
    logger.error('EXIT ERROR', err);
  }
}

function logCompletedTrade(reason, netPnL, exitPrice) {
  stats.totalTrades++;

  if (netPnL > 0) stats.wins++;
  else if (netPnL < 0) stats.losses++;

  stats.currentBalance += netPnL;

  const winRate = stats.totalTrades
    ? ((stats.wins / stats.totalTrades) * 100).toFixed(1)
    : '0.0';

  const roi =
    (((stats.currentBalance - stats.startingBalance) / stats.startingBalance) * 100).toFixed(2);

  logger.trade({
    reason,
    netPnL,
    balance: stats.currentBalance,
    roi,
    winRate,
    wins: stats.wins,
    losses: stats.losses
  });

  tradeStream.write([
    new Date().toISOString(),
    TRADING_MODE,
    currentMarketSlug || 'BTC-5M',
    reason,
    netPnL.toFixed(4),
    stats.currentBalance.toFixed(2),
    `${winRate}%`,
    state.yesFilled,
    state.noFilled,
    state.yesAvgFillPrice.toFixed(4),
    state.noAvgFillPrice.toFixed(4),
    state.yesFilledShares.toFixed(6),
    state.noFilledShares.toFixed(6)
  ].join(',') + '\n');

  const entryPrice = state.firstFilledSide === 'YES'
    ? state.yesAvgFillPrice
    : state.firstFilledSide === 'NO'
      ? state.noAvgFillPrice
      : 0;

  recentTrades.unshift({
    time: new Date().toLocaleTimeString(),
    reason,
    pnl: Number(netPnL.toFixed(4)),
    entry: entryPrice,
    exit: exitPrice || 0
  });

  if (recentTrades.length > 10) recentTrades.pop();

  if (currentMarketSlug) {
    completedMarketSlugs.add(currentMarketSlug);
  }

  state = blankState();
  state.status = 'DONE';
}

// ============================================================================
// MARKET DATA
// ============================================================================

function handleMarketUpdate(data) {
  const bestAsk = Number(data.bestAsk);
  const bestBid = Number(data.bestBid);

  if (isNaN(bestAsk) || isNaN(bestBid)) return;

  const side = sideFromToken(data.asset_id);
  if (!side) return;

  currentAsks[side] = bestAsk;
  currentBids[side] = bestBid;
}

function connectWebsocket() {
  if (global.wsMarket) {
    try {
      global.wsMarket.terminate();
    } catch (e) {}
  }

  logger.ws('booting market websocket');

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

  wsMarket.on('error', e => logger.error('WS ERROR', e));
  wsMarket.on('close', () => logger.warn('WS CLOSED'));
}

// ============================================================================
// MARKET LOADER
// ============================================================================

async function loadNextMarket() {
  if (isSearchingNextMarket) return;

  isSearchingNextMarket = true;

  logger.scanner('calculating current active BTC 5-minute market');

  try {
    const nowSec = Math.floor(Date.now() / 1000);
    const startSec = nowSec - (nowSec % 300);
    const endSec = startSec + 300;
    const eventSlug = `btc-updown-5m-${startSec}`;

    const response = await fetch(`https://gamma-api.polymarket.com/events?slug=${eventSlug}`);
    const events = await response.json();

    if (!events?.length || !events[0].markets?.length) {
      logger.warn('MARKET NOT INDEXED', { market: eventSlug });
      searchCooldownTimer = Date.now() + 5000;
      return;
    }

    const event = events[0];
    const market = event.markets[0];

    const tokens = typeof market.clobTokenIds === 'string'
      ? JSON.parse(market.clobTokenIds)
      : market.clobTokenIds;

    if (!tokens?.[0] || !tokens?.[1]) {
      logger.warn('MARKET TOKENS MISSING', { market: eventSlug });
      searchCooldownTimer = Date.now() + 5000;
      return;
    }

    currentYesToken = tokens[0];
    currentNoToken = tokens[1];
    currentMarketSlug = eventSlug;
    marketEndTime = endSec * 1000;

    currentMarketMeta = {
      tickSize: String(market.minimumTickSize || market.tickSize || '0.001'),
      negRisk: Boolean(market.negRisk || market.neg_risk || false)
    };

    currentAsks = { YES: 0, NO: 0 };
    currentBids = { YES: 0, NO: 0 };

    state = blankState();
    state.active = !completedMarketSlugs.has(eventSlug);
    state.status = state.active ? 'WAITING_FOR_CENTER' : 'DONE';

    logger.event('MARKET LOADED', {
      title: event.title,
      slug: eventSlug,
      endsIn: `${secondsLeftInMarket()}s`
    });

    logger.event('STRATEGY READY', {
      mode: TRADING_MODE.toUpperCase(),
      target: TARGET_BID.toFixed(2),
      maxSpread: MAX_SIDE_SPREAD.toFixed(2),
      askSumMax: MAX_COMBINED_ASKS.toFixed(2),
      bidSumMin: MIN_COMBINED_BIDS.toFixed(2)
    });

    connectWebsocket();
  } catch (e) {
    logger.error('SCANNER ERROR', e);
    searchCooldownTimer = Date.now() + 5000;
  } finally {
    isSearchingNextMarket = false;
  }
}

// ============================================================================
// DASHBOARD API
// ============================================================================

http.createServer((req, res) => {
  if (req.url === '/api/live') {
    res.writeHead(200, { 'Content-Type': 'application/json' });

    res.end(JSON.stringify({
      mode: TRADING_MODE,

      stats,

      trade: {
        active:
          state.status === 'PLACING_ORDERS' ||
          state.status === 'ORDERS_WORKING' ||
          state.status === 'ONE_LEG_FILLED' ||
          state.status === 'EXITING',

        side: state.firstFilledSide || '',

        entryPrice:
          state.firstFilledSide === 'YES'
            ? state.yesAvgFillPrice
            : state.firstFilledSide === 'NO'
              ? state.noAvgFillPrice
              : 0,

        status: state.status,
        readableStatus: readableStatus(),
        lastAction: state.lastAction
      },

      currentPrices: {
        YES: currentAsks.YES || 0,
        NO: currentAsks.NO || 0
      },

      currentBook: {
        YES: {
          ask: currentAsks.YES,
          bid: currentBids.YES,
          spread: bothPricesKnown() ? getSpread('YES') : 0
        },
        NO: {
          ask: currentAsks.NO,
          bid: currentBids.NO,
          spread: bothPricesKnown() ? getSpread('NO') : 0
        },
        askSum: bothPricesKnown() ? askSum() : 0,
        bidSum: bothPricesKnown() ? bidSum() : 0
      },

      state,
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
  logger.event('DASHBOARD READY', { port: 3000 });
});

// ============================================================================
// STATUS LOG
// ============================================================================

function logLiveStatus() {
  const timeoutRemaining = state.timeoutAt
    ? Math.max(0, ((state.timeoutAt - Date.now()) / 1000)).toFixed(1)
    : '-';

  logger.status({
    mode: TRADING_MODE.toUpperCase(),
    status: readableStatus(),
    left: secondsLeftInMarket(),
    timeout: timeoutRemaining,
    balance: money(stats.currentBalance),

    yesAsk: terminalPrice(currentAsks.YES),
    yesBid: terminalPrice(currentBids.YES),
    yesSpread: bothPricesKnown() ? getSpread('YES').toFixed(3) : '---',

    noAsk: terminalPrice(currentAsks.NO),
    noBid: terminalPrice(currentBids.NO),
    noSpread: bothPricesKnown() ? getSpread('NO').toFixed(3) : '---',

    askSum: bothPricesKnown() ? askSum().toFixed(3) : '---',
    bidSum: bothPricesKnown() ? bidSum().toFixed(3) : '---',

    last: compactReason(state.lastAction || '-')
  });
}

// ============================================================================
// MAIN
// ============================================================================

async function initClobClient() {
  const account = privateKeyToAccount(rawKey);

  const walletClient = createWalletClient({
    account,
    chain: polygon,
    transport: viemHttp()
  });

  const base = new ClobClient(HOST, CHAIN_ID, walletClient);

  let creds = null;

  if (typeof base.createOrDeriveApiKey === 'function') {
    creds = await base.createOrDeriveApiKey();
  } else if (typeof base.deriveApiKey === 'function') {
    try {
      creds = await base.deriveApiKey();
    } catch (e) {
      if (typeof base.createApiKey === 'function') {
        creds = await base.createApiKey();
      }
    }
  }

  if (creds) {
    return new ClobClient(
      HOST,
      CHAIN_ID,
      walletClient,
      creds,
      SIGNATURE_TYPE,
      FUNDER
    );
  }

  return base;
}

async function runTrader() {
  logger.boot('BTC 5m real-behaviour arb engine');

  logger.event('MODE READY', {
    mode: TRADING_MODE.toUpperCase(),
    lifecycle: 'single strategy / paper-live adapter'
  });

  clobClient = await initClobClient();

  execution = IS_PAPER
    ? new PaperExecutionAdapter()
    : new LiveExecutionAdapter(clobClient);

  await loadNextMarket();

  setInterval(async () => {
    const now = Date.now();

    if (marketEndTime === 0) {
      if (now > searchCooldownTimer) await loadNextMarket();
      logLiveStatus();
      return;
    }

    if (now >= marketEndTime && !isSearchingNextMarket) {
      await loadNextMarket();
      logLiveStatus();
      return;
    }

    if (bothPricesKnown()) {
      priceStream.write(
        `${new Date().toISOString()},` +
        `${currentAsks.YES.toFixed(3)},${currentBids.YES.toFixed(3)},${getSpread('YES').toFixed(3)},` +
        `${currentAsks.NO.toFixed(3)},${currentBids.NO.toFixed(3)},${getSpread('NO').toFixed(3)},` +
        `${askSum().toFixed(3)},${bidSum().toFixed(3)},${state.status},${state.lastAction || ''}\n`
      );
    }

    if (
      state.active &&
      state.status === 'WAITING_FOR_CENTER' &&
      secondsLeftInMarket() > NO_NEW_ENTRIES_SECONDS_LEFT
    ) {
      await maybePlaceEntryOrders();
    }

    if (
      state.active &&
      (
        state.status === 'ORDERS_WORKING' ||
        state.status === 'ONE_LEG_FILLED'
      )
    ) {
      await checkOrderFills();
    }

    if (state.active && state.status === 'ONE_LEG_FILLED') {
      await handleSecondLegTimeout();
    }

    logLiveStatus();
  }, 1000);
}

runTrader().catch(err => {
  logger.error('FATAL', err);
});