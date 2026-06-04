require('dotenv').config();

const WebSocket = require('ws');
const fs = require('fs');
const http = require('http');
const path = require('path');

// =====================================================
// BASIC SAFETY NOTE
// =====================================================
//
// This implementation is PAPER-FIRST.
//
// It does NOT place real orders.
// It validates the 48-cent fast-hedge state machine using live market data.
//
// Do not move this to live trading until:
// 1. paper logs are positive after fees/slippage,
// 2. stale-data behaviour is clean,
// 3. hedge failures are rare,
// 4. real Polymarket order placement/cancellation methods are verified.
//
// =====================================================


// =====================================================
// TERMINAL COLOURS
// =====================================================

const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  brightYellow: '\x1b[93m',
  cyan: '\x1b[36m',
  magenta: '\x1b[35m',
  gray: '\x1b[90m'
};


// =====================================================
// CONFIG
// =====================================================

function num(name, fallback) {
  const raw = process.env[name];

  if (raw === undefined || raw === '') {
    return fallback;
  }

  const value = Number(raw);

  if (!Number.isFinite(value)) {
    throw new Error(`Invalid number in .env: ${name}=${raw}`);
  }

  return value;
}

function bool(name, fallback) {
  const raw = process.env[name];

  if (raw === undefined || raw === '') {
    return fallback;
  }

  return ['true', '1', 'yes', 'y'].includes(String(raw).toLowerCase());
}

const MODE = process.env.MODE || 'paper';

// Strategy config
const ENTRY_PRICE = num('ENTRY_PRICE', 0.48);
const HEDGE_DELAY_SECONDS = num('HEDGE_DELAY_SECONDS', 2.0);
const MAX_HEDGE_PRICE = num('MAX_HEDGE_PRICE', 0.60);
const SIZE_PER_SIDE = num('SIZE_PER_SIDE', 1);
const ONE_TRADE_ATTEMPT_PER_MARKET = bool('ONE_TRADE_ATTEMPT_PER_MARKET', true);

// Paper assumptions
const MAKER_FEE_BPS = num('MAKER_FEE_BPS', 0);
const TAKER_FEE_BPS = num('TAKER_FEE_BPS', 180);
const SLIPPAGE_BPS = num('SLIPPAGE_BPS', 0);

// Risk controls
const STALE_QUOTE_MS = num('STALE_QUOTE_MS', 1500);
const MAX_SIZE_PER_MARKET = num('MAX_SIZE_PER_MARKET', 1);
const MAX_TOTAL_OPEN_EXPOSURE = num('MAX_TOTAL_OPEN_EXPOSURE', 5);
const MAX_DAILY_REALIZED_LOSS = num('MAX_DAILY_REALIZED_LOSS', 10);
const MAX_DAILY_HEDGE_FAILURES = num('MAX_DAILY_HEDGE_FAILURES', 3);

// Market config
const MARKET_SYMBOL = process.env.MARKET_SYMBOL || 'btc';
const MARKET_INTERVAL_SECONDS = num('MARKET_INTERVAL_SECONDS', 300);

// Server
const PORT = num('PORT', 3000);

if (MODE !== 'paper') {
  throw new Error(
    'This simplified implementation is paper-only. Set MODE=paper in .env.'
  );
}

if (SIZE_PER_SIDE > MAX_SIZE_PER_MARKET) {
  throw new Error(
    `SIZE_PER_SIDE=${SIZE_PER_SIDE} is greater than MAX_SIZE_PER_MARKET=${MAX_SIZE_PER_MARKET}`
  );
}


// =====================================================
// STATES
// =====================================================

const STATES = {
  WAITING_FOR_MARKET: 'WAITING_FOR_MARKET',
  ORDERS_LIVE: 'ORDERS_LIVE',
  ONE_SIDE_FILLED: 'ONE_SIDE_FILLED',
  PAIR_COMPLETED_AT_48: 'PAIR_COMPLETED_AT_48',
  PAIR_COMPLETED_BY_HEDGE: 'PAIR_COMPLETED_BY_HEDGE',
  ABORTED_OR_CANCELLED: 'ABORTED_OR_CANCELLED'
};

function oppositeSide(side) {
  return side === 'YES' ? 'NO' : 'YES';
}

function isTerminalState(status) {
  return (
    status === STATES.PAIR_COMPLETED_AT_48 ||
    status === STATES.PAIR_COMPLETED_BY_HEDGE ||
    status === STATES.ABORTED_OR_CANCELLED
  );
}


// =====================================================
// GLOBAL STATE
// =====================================================

let arb = createEmptyArbState();

let stats = {
  totalTrades: 0,
  wins: 0,
  losses: 0,
  startingBalance: 100.00,
  currentBalance: 100.00,
  dailyRealizedPnl: 0,
  hedgeFailures: 0
};

let currentMarket = null;

let currentYesToken = null;
let currentNoToken = null;

let marketEndTime = 0;

let currentPrices = {
  YES: 0,
  NO: 0
};

let currentBooks = {
  YES: {
    ask: 0,
    askSize: 0,
    bid: 0,
    bidSize: 0,
    ts: 0
  },
  NO: {
    ask: 0,
    askSize: 0,
    bid: 0,
    bidSize: 0,
    ts: 0
  }
};

let recentTrades = [];

let completedMarketIds = new Set();

let isSearchingNextMarket = false;
let searchCooldownUntil = 0;


// =====================================================
// LOGGING
// =====================================================

if (!fs.existsSync('logs')) {
  fs.mkdirSync('logs');
}

const tradeStream = fs.createWriteStream('paper_trades_log.csv', {
  flags: 'a'
});

const auditStream = fs.createWriteStream('logs/audit_events.jsonl', {
  flags: 'a'
});

const priceStream = fs.createWriteStream('price_history.csv', {
  flags: 'a'
});

const terminalStream = fs.createWriteStream('terminal_logs.txt', {
  flags: 'a'
});

const originalLog = console.log;

console.log = function (...args) {
  originalLog.apply(console, args);

  const message = args
    .map(arg => (typeof arg === 'object' ? JSON.stringify(arg) : arg))
    .join(' ');

  const cleanMessage = message.replace(/\x1b\[[0-9;]*m/g, '');

  terminalStream.write(`[${new Date().toISOString()}] ${cleanMessage}\n`);
};

if (
  !fs.existsSync('paper_trades_log.csv') ||
  fs.statSync('paper_trades_log.csv').size === 0
) {
  tradeStream.write(
    [
      'Date',
      'Market_ID',
      'Market_Title',
      'Market_Start',
      'Market_End',
      'YES_Token',
      'NO_Token',
      'Status',
      'First_Fill_Side',
      'First_Fill_Price',
      'First_Fill_Time',
      'Second_Fill_Price',
      'Second_Fill_Time',
      'Hedge_Price',
      'Hedge_Time',
      'Seconds_To_Complete',
      'Shares',
      'Gross_PnL_USD',
      'Fees_USD',
      'Net_PnL_USD',
      'Balance_USD',
      'Win_Rate_Pct',
      'Risk_Reason'
    ].join(',') + '\n'
  );
}

if (
  !fs.existsSync('price_history.csv') ||
  fs.statSync('price_history.csv').size === 0
) {
  priceStream.write(
    'Timestamp,Market_ID,YES_Ask,YES_Ask_Size,YES_Bid,NO_Ask,NO_Ask_Size,NO_Bid\n'
  );
}

function audit(type, payload = {}) {
  auditStream.write(
    JSON.stringify({
      ts: new Date().toISOString(),
      type,
      ...payload
    }) + '\n'
  );
}


// =====================================================
// STATE FACTORY
// =====================================================

function createEmptyArbState() {
  return {
    status: STATES.WAITING_FOR_MARKET,

    attempted: false,

    startedAt: 0,
    completedAt: 0,

    firstFillTime: 0,
    firstFillSide: null,

    riskReason: null,

    orders: {
      YES: null,
      NO: null,
      HEDGE: null
    },

    positions: {
      YES: null,
      NO: null
    },

    fills: {
      YES: null,
      NO: null
    }
  };
}


// =====================================================
// TOKEN HELPERS
// =====================================================

function tokenForSide(side) {
  return side === 'YES' ? currentYesToken : currentNoToken;
}

function sideForToken(tokenId) {
  if (tokenId === currentYesToken) {
    return 'YES';
  }

  if (tokenId === currentNoToken) {
    return 'NO';
  }

  return null;
}


// =====================================================
// RISK CONTROLS
// =====================================================

function emergencyStopActive() {
  return fs.existsSync('EMERGENCY_STOP');
}

function validateCanStartMarket() {
  if (emergencyStopActive()) {
    return {
      ok: false,
      reason: 'EMERGENCY_STOP'
    };
  }

  if (!currentMarket || !currentMarket.id) {
    return {
      ok: false,
      reason: 'NO_CURRENT_MARKET'
    };
  }

  if (
    ONE_TRADE_ATTEMPT_PER_MARKET &&
    completedMarketIds.has(currentMarket.id)
  ) {
    return {
      ok: false,
      reason: 'MARKET_ALREADY_ATTEMPTED'
    };
  }

  if (stats.dailyRealizedPnl <= -Math.abs(MAX_DAILY_REALIZED_LOSS)) {
    return {
      ok: false,
      reason: 'MAX_DAILY_REALIZED_LOSS'
    };
  }

  if (stats.hedgeFailures >= MAX_DAILY_HEDGE_FAILURES) {
    return {
      ok: false,
      reason: 'MAX_DAILY_HEDGE_FAILURES'
    };
  }

  const expectedExposure = ENTRY_PRICE * SIZE_PER_SIDE * 2;

  if (expectedExposure > MAX_TOTAL_OPEN_EXPOSURE) {
    return {
      ok: false,
      reason: 'MAX_TOTAL_OPEN_EXPOSURE'
    };
  }

  return {
    ok: true
  };
}

function validateFreshQuotes(sides = ['YES', 'NO']) {
  const now = Date.now();

  for (const side of sides) {
    const book = currentBooks[side];

    if (!book || book.ask <= 0 || book.bid <= 0) {
      return {
        ok: false,
        reason: `MISSING_${side}_QUOTE`
      };
    }

    if (!book.ts || now - book.ts > STALE_QUOTE_MS) {
      return {
        ok: false,
        reason: `STALE_${side}_QUOTE`
      };
    }
  }

  return {
    ok: true
  };
}

function validateHedge(side) {
  const book = currentBooks[side];

  if (!book || book.ask <= 0) {
    return {
      ok: false,
      reason: 'INVALID_HEDGE_PRICE'
    };
  }

  if (book.ask > MAX_HEDGE_PRICE) {
    return {
      ok: false,
      reason: 'HEDGE_PRICE_ABOVE_MAX'
    };
  }

  if (book.askSize < SIZE_PER_SIDE) {
    return {
      ok: false,
      reason: 'INSUFFICIENT_HEDGE_SIZE'
    };
  }

  return {
    ok: true
  };
}


// =====================================================
// PAPER ORDERS
// =====================================================

let paperOrderSeq = 1;

function createLimitBuyOrder(side) {
  return {
    id: `paper-${paperOrderSeq++}`,
    marketId: currentMarket.id,
    side,
    tokenId: tokenForSide(side),
    type: 'LIMIT_BUY',
    price: ENTRY_PRICE,
    shares: SIZE_PER_SIDE,
    status: 'OPEN',
    placedAt: Date.now()
  };
}

function createHedgeOrder(side, hedgePrice) {
  return {
    id: `paper-${paperOrderSeq++}`,
    marketId: currentMarket.id,
    side,
    tokenId: tokenForSide(side),
    type: 'MARKETABLE_BUY',
    price: hedgePrice,
    shares: SIZE_PER_SIDE,
    status: 'FILLED',
    placedAt: Date.now(),
    filledAt: Date.now()
  };
}

function paperLimitBuyWouldFill(order) {
  if (!order || order.status !== 'OPEN') {
    return false;
  }

  const book = currentBooks[order.side];

  if (!book) {
    return false;
  }

  return (
    book.ask > 0 &&
    book.ask <= order.price &&
    book.askSize >= order.shares
  );
}


// =====================================================
// STRATEGY
// =====================================================

function startArbCycle() {
  if (arb.status !== STATES.WAITING_FOR_MARKET) {
    return;
  }

  if (ONE_TRADE_ATTEMPT_PER_MARKET && arb.attempted) {
    return;
  }

  const riskGate = validateCanStartMarket();

  if (!riskGate.ok) {
    audit('START_BLOCKED', {
      reason: riskGate.reason,
      market: currentMarket
    });

    return;
  }

  const fresh = validateFreshQuotes(['YES', 'NO']);

  if (!fresh.ok) {
    return;
  }

  arb.attempted = true;
  arb.status = STATES.ORDERS_LIVE;
  arb.startedAt = Date.now();

  arb.orders.YES = createLimitBuyOrder('YES');
  arb.orders.NO = createLimitBuyOrder('NO');

  audit('ORDERS_LIVE', {
    market: currentMarket,
    yesOrder: arb.orders.YES,
    noOrder: arb.orders.NO
  });

  console.log(
    `\n${colors.cyan}[${STATES.ORDERS_LIVE}] YES and NO paper limits placed @ $${ENTRY_PRICE} | Size: ${SIZE_PER_SIDE}${colors.reset}`
  );
}

function checkPaperLimitFills() {
  if (
    arb.status !== STATES.ORDERS_LIVE &&
    arb.status !== STATES.ONE_SIDE_FILLED
  ) {
    return;
  }

  for (const side of ['YES', 'NO']) {
    const order = arb.orders[side];

    if (paperLimitBuyWouldFill(order)) {
      order.status = 'FILLED';
      order.filledAt = Date.now();

      const fill = {
        orderId: order.id,
        side,
        price: order.price,
        shares: order.shares,
        time: Date.now(),
        isTaker: false
      };

      arb.fills[side] = fill;

      arb.positions[side] = {
        side,
        shares: order.shares,
        entryPrice: order.price,
        isTaker: false,
        filledAt: fill.time
      };

      audit('LIMIT_FILLED', {
        market: currentMarket,
        fill
      });

      const filledSides = Object.keys(arb.positions).filter(
        k => arb.positions[k] !== null
      );

      if (filledSides.length === 1 && !arb.firstFillSide) {
        arb.status = STATES.ONE_SIDE_FILLED;
        arb.firstFillSide = side;
        arb.firstFillTime = Date.now();

        console.log(
          `${colors.brightYellow}[${STATES.ONE_SIDE_FILLED}] ${side} filled at $${ENTRY_PRICE}. Hedge timer started: ${HEDGE_DELAY_SECONDS}s${colors.reset}`
        );

        audit('FIRST_FILL', {
          market: currentMarket,
          side,
          time: arb.firstFillTime
        });
      }

      if (filledSides.length === 2) {
        completePair(STATES.PAIR_COMPLETED_AT_48);
        return;
      }
    }
  }
}

function handleHedgeTimer() {
  if (arb.status !== STATES.ONE_SIDE_FILLED) {
    return;
  }

  const elapsedMs = Date.now() - arb.firstFillTime;

  if (elapsedMs < HEDGE_DELAY_SECONDS * 1000) {
    return;
  }

  const hedgeSide = oppositeSide(arb.firstFillSide);

  const fresh = validateFreshQuotes([hedgeSide]);

  if (!fresh.ok) {
    abortAttempt(fresh.reason);
    return;
  }

  const hedgeRisk = validateHedge(hedgeSide);

  if (!hedgeRisk.ok) {
    stats.hedgeFailures++;
    abortAttempt(hedgeRisk.reason);
    return;
  }

  if (arb.orders[hedgeSide] && arb.orders[hedgeSide].status === 'OPEN') {
    arb.orders[hedgeSide].status = 'CANCELLED';
    arb.orders[hedgeSide].cancelReason = 'HEDGE_TIMER_EXPIRED';
    arb.orders[hedgeSide].cancelledAt = Date.now();
  }

  const rawAsk = currentBooks[hedgeSide].ask;
  const slippageMultiplier = 1 + SLIPPAGE_BPS / 10000;
  const hedgePrice = Number((rawAsk * slippageMultiplier).toFixed(4));

  const hedgeOrder = createHedgeOrder(hedgeSide, hedgePrice);

  arb.orders.HEDGE = hedgeOrder;

  arb.fills[hedgeSide] = {
    orderId: hedgeOrder.id,
    side: hedgeSide,
    price: hedgePrice,
    shares: SIZE_PER_SIDE,
    time: Date.now(),
    isTaker: true
  };

  arb.positions[hedgeSide] = {
    side: hedgeSide,
    shares: SIZE_PER_SIDE,
    entryPrice: hedgePrice,
    isTaker: true,
    filledAt: Date.now()
  };

  audit('HEDGE_FILLED', {
    market: currentMarket,
    hedgeSide,
    rawAsk,
    hedgePrice,
    order: hedgeOrder
  });

  console.log(
    `${colors.magenta}[${STATES.PAIR_COMPLETED_BY_HEDGE}] Hedged ${hedgeSide} at $${hedgePrice.toFixed(4)}${colors.reset}`
  );

  completePair(STATES.PAIR_COMPLETED_BY_HEDGE);
}


// =====================================================
// PNL
// =====================================================

function fee(amount, bps) {
  return amount * (bps / 10000);
}

function calculatePairPnl() {
  const yesPos = arb.positions.YES;
  const noPos = arb.positions.NO;

  const yesCost = yesPos.entryPrice * yesPos.shares;
  const noCost = noPos.entryPrice * noPos.shares;

  const payout = 1.00 * SIZE_PER_SIDE;

  const yesFee = fee(
    yesCost,
    yesPos.isTaker ? TAKER_FEE_BPS : MAKER_FEE_BPS
  );

  const noFee = fee(
    noCost,
    noPos.isTaker ? TAKER_FEE_BPS : MAKER_FEE_BPS
  );

  const fees = yesFee + noFee;

  const grossPnl = payout - yesCost - noCost;
  const netPnl = grossPnl - fees;

  return {
    yesCost,
    noCost,
    deployedCost: yesCost + noCost,
    payout,
    fees,
    grossPnl,
    netPnl
  };
}

function completePair(status) {
  arb.status = status;
  arb.completedAt = Date.now();

  const pnl = calculatePairPnl();

  stats.totalTrades++;

  if (pnl.netPnl > 0) {
    stats.wins++;
  } else {
    stats.losses++;
  }

  stats.currentBalance += pnl.netPnl;
  stats.dailyRealizedPnl += pnl.netPnl;

  completedMarketIds.add(currentMarket.id);

  const secondsToComplete = arb.firstFillTime
    ? (arb.completedAt - arb.firstFillTime) / 1000
    : 0;

  const winRate =
    stats.totalTrades > 0
      ? ((stats.wins / stats.totalTrades) * 100).toFixed(1)
      : '0.0';

  const roi = (
    ((stats.currentBalance - stats.startingBalance) /
      stats.startingBalance) *
    100
  ).toFixed(2);

  const pnlColor = pnl.netPnl >= 0 ? colors.brightYellow : colors.red;

  console.log(`\n${colors.gray}========================================${colors.reset}`);
  console.log(`[TRADE CLOSED] Status: ${status}`);
  console.log(`Market: ${currentMarket.title}`);
  console.log(
    `Cost: $${pnl.deployedCost.toFixed(4)} | Payout: $${pnl.payout.toFixed(4)} | Fees: $${pnl.fees.toFixed(4)}`
  );
  console.log(
    `NET PnL: ${pnlColor}${pnl.netPnl >= 0 ? '+' : ''}$${pnl.netPnl.toFixed(4)}${colors.reset}`
  );
  console.log(
    `[STATS] Balance: $${stats.currentBalance.toFixed(2)} (${roi >= 0 ? '+' : ''}${roi}% ROI) | Win Rate: ${winRate}%`
  );
  console.log(`${colors.gray}========================================\n${colors.reset}`);

  writeTradeCsv({
    status,
    pnl,
    secondsToComplete,
    winRate,
    riskReason: ''
  });

  recentTrades.unshift({
    time: new Date().toLocaleTimeString(),
    reason: status,
    entry: pnl.deployedCost.toFixed(4),
    exit: pnl.payout.toFixed(4),
    pnl: pnl.netPnl
  });

  if (recentTrades.length > 10) {
    recentTrades.pop();
  }

  audit('PAIR_COMPLETED', {
    market: currentMarket,
    status,
    pnl,
    secondsToComplete,
    arb
  });
}

function abortAttempt(reason) {
  arb.status = STATES.ABORTED_OR_CANCELLED;
  arb.completedAt = Date.now();
  arb.riskReason = reason;

  if (arb.orders.YES && arb.orders.YES.status === 'OPEN') {
    arb.orders.YES.status = 'CANCELLED';
    arb.orders.YES.cancelReason = reason;
    arb.orders.YES.cancelledAt = Date.now();
  }

  if (arb.orders.NO && arb.orders.NO.status === 'OPEN') {
    arb.orders.NO.status = 'CANCELLED';
    arb.orders.NO.cancelReason = reason;
    arb.orders.NO.cancelledAt = Date.now();
  }

  completedMarketIds.add(currentMarket.id);

  const secondsToComplete = arb.firstFillTime
    ? (Date.now() - arb.firstFillTime) / 1000
    : 0;

  console.log(
    `${colors.red}[${STATES.ABORTED_OR_CANCELLED}] ${reason}${colors.reset}`
  );

  const pnl = {
    deployedCost: 0,
    payout: 0,
    fees: 0,
    grossPnl: 0,
    netPnl: 0
  };

  writeTradeCsv({
    status: STATES.ABORTED_OR_CANCELLED,
    pnl,
    secondsToComplete,
    winRate:
      stats.totalTrades > 0
        ? ((stats.wins / stats.totalTrades) * 100).toFixed(1)
        : '0.0',
    riskReason: reason
  });

  audit('ATTEMPT_ABORTED', {
    market: currentMarket,
    reason,
    arb
  });
}

function writeTradeCsv({
  status,
  pnl,
  secondsToComplete,
  winRate,
  riskReason
}) {
  const firstFill =
    arb.firstFillSide && arb.fills[arb.firstFillSide]
      ? arb.fills[arb.firstFillSide]
      : null;

  const secondSide = arb.firstFillSide
    ? oppositeSide(arb.firstFillSide)
    : null;

  const secondFill =
    secondSide && arb.fills[secondSide] ? arb.fills[secondSide] : null;

  const hedgeOrder = arb.orders.HEDGE;

  const row = [
    new Date().toISOString(),

    currentMarket?.id || '',
    currentMarket?.title || '',
    currentMarket?.startTime || '',
    currentMarket?.endTime || '',

    currentYesToken || '',
    currentNoToken || '',

    status,

    arb.firstFillSide || '',
    firstFill?.price || '',
    firstFill?.time ? new Date(firstFill.time).toISOString() : '',

    secondFill?.price || '',
    secondFill?.time ? new Date(secondFill.time).toISOString() : '',

    hedgeOrder?.price || '',
    hedgeOrder?.filledAt ? new Date(hedgeOrder.filledAt).toISOString() : '',

    secondsToComplete,

    SIZE_PER_SIDE,

    pnl.grossPnl.toFixed(6),
    pnl.fees.toFixed(6),
    pnl.netPnl.toFixed(6),
    stats.currentBalance.toFixed(2),

    `${winRate}%`,

    riskReason || ''
  ];

  tradeStream.write(row.map(v => JSON.stringify(v)).join(',') + '\n');
}


// =====================================================
// MARKET DATA
// =====================================================

function handleMarketUpdate(data) {
  if (!data || !data.asset_id) {
    return;
  }

  const side = sideForToken(data.asset_id);

  if (!side) {
    return;
  }

  const ask = Number(data.bestAsk);
  const bid = Number(data.bestBid);
  const askSize = Number(data.bestAskSize || 0);
  const bidSize = Number(data.bestBidSize || 0);

  if (!Number.isFinite(ask) || !Number.isFinite(bid)) {
    return;
  }

  currentPrices[side] = ask;

  currentBooks[side] = {
    ask,
    askSize,
    bid,
    bidSize,
    ts: Date.now()
  };

  audit('QUOTE_UPDATE', {
    marketId: currentMarket?.id,
    side,
    book: currentBooks[side]
  });

  startArbCycle();
  checkPaperLimitFills();
  handleHedgeTimer();
}

function connectWebsocket() {
  if (global.wsMarket) {
    try {
      global.wsMarket.terminate();
    } catch {}
  }

  const wsMarket = new WebSocket(
    'wss://ws-subscriptions-clob.polymarket.com/ws/market'
  );

  global.wsMarket = wsMarket;

  wsMarket.on('open', () => {
    console.log(
      `${colors.yellow}[WS] Connected to Polymarket market stream.${colors.reset}`
    );

    if (currentYesToken && currentNoToken) {
      wsMarket.send(
        JSON.stringify({
          type: 'market',
          assets_ids: [currentYesToken, currentNoToken]
        })
      );
    }
  });

  wsMarket.on('message', msg => {
    const textMsg = msg.toString();

    if (textMsg === 'PING') {
      wsMarket.send('PONG');
      return;
    }

    if (textMsg === 'PONG') {
      return;
    }

    let data;

    try {
      data = JSON.parse(textMsg);
    } catch {
      return;
    }

    if (
      data.event_type === 'book' &&
      data.asks &&
      data.asks.length > 0 &&
      data.bids &&
      data.bids.length > 0
    ) {
      handleMarketUpdate({
        asset_id: data.asset_id,
        bestAsk: data.asks[0].price,
        bestAskSize: data.asks[0].size,
        bestBid: data.bids[0].price,
        bestBidSize: data.bids[0].size
      });

      return;
    }

    if (
      data.event_type === 'price_change' &&
      data.price_changes &&
      data.price_changes.length > 0
    ) {
      for (const pc of data.price_changes) {
        handleMarketUpdate({
          asset_id: pc.asset_id,
          bestAsk: pc.best_ask,
          bestAskSize: pc.best_ask_size || 0,
          bestBid: pc.best_bid,
          bestBidSize: pc.best_bid_size || 0
        });
      }
    }
  });

  wsMarket.on('error', err => {
    console.log(`${colors.red}[WS ERROR] ${err.message}${colors.reset}`);

    audit('WS_ERROR', {
      message: err.message
    });
  });

  wsMarket.on('close', () => {
    console.log(
      `${colors.gray}[WS] Connection dropped. Reconnecting...${colors.reset}`
    );

    setTimeout(() => {
      if (currentYesToken && currentNoToken && Date.now() < marketEndTime) {
        connectWebsocket();
      }
    }, 2000);
  });
}


// =====================================================
// MARKET SCANNER
// =====================================================

async function loadNextMarket() {
  if (isSearchingNextMarket) {
    return;
  }

  isSearchingNextMarket = true;

  console.log(
    `\n${colors.yellow}[SCANNER] Checking for active 5-minute BTC market...${colors.reset}`
  );

  try {
    const nowSec = Math.floor(Date.now() / 1000);

    const remainder = nowSec % MARKET_INTERVAL_SECONDS;

    const currentIntervalStartSec = nowSec - remainder;

    const currentIntervalEndSec =
      currentIntervalStartSec + MARKET_INTERVAL_SECONDS;

    const eventSlug = `${MARKET_SYMBOL}-updown-5m-${currentIntervalStartSec}`;

    const response = await fetch(
      `https://gamma-api.polymarket.com/events?slug=${eventSlug}`
    );

    const events = await response.json();

    if (
      !events ||
      events.length === 0 ||
      !events[0].markets ||
      events[0].markets.length === 0
    ) {
      console.log(
        `${colors.gray}[SCANNER] Market API not ready. Retrying shortly.${colors.reset}`
      );

      searchCooldownUntil = Date.now() + 5000;
      return;
    }

    const market = events[0].markets[0];

    const parsedTokens =
      typeof market.clobTokenIds === 'string'
        ? JSON.parse(market.clobTokenIds)
        : market.clobTokenIds;

    if (!parsedTokens || !parsedTokens[0] || !parsedTokens[1]) {
      console.log(
        `${colors.red}[SCANNER] Could not parse YES/NO token IDs.${colors.reset}`
      );

      searchCooldownUntil = Date.now() + 5000;
      return;
    }

    if (parsedTokens[0] === currentYesToken && Date.now() < marketEndTime) {
      searchCooldownUntil = Date.now() + 5000;
      return;
    }

    currentYesToken = parsedTokens[0];
    currentNoToken = parsedTokens[1];

    marketEndTime = currentIntervalEndSec * 1000;

    currentMarket = {
      id: market.conditionId || market.id || eventSlug,
      slug: eventSlug,
      title: events[0].title || eventSlug,
      startTime: new Date(currentIntervalStartSec * 1000).toISOString(),
      endTime: new Date(currentIntervalEndSec * 1000).toISOString()
    };

    currentPrices = {
      YES: 0,
      NO: 0
    };

    currentBooks = {
      YES: {
        ask: 0,
        askSize: 0,
        bid: 0,
        bidSize: 0,
        ts: 0
      },
      NO: {
        ask: 0,
        askSize: 0,
        bid: 0,
        bidSize: 0,
        ts: 0
      }
    };

    arb = createEmptyArbState();

    console.log(
      `${colors.brightYellow}[MARKET LOADED] ${currentMarket.title}${colors.reset}`
    );

    console.log(
      `${colors.gray}YES token: ${currentYesToken}${colors.reset}`
    );

    console.log(
      `${colors.gray}NO token:  ${currentNoToken}${colors.reset}`
    );

    audit('MARKET_LOADED', {
      market: currentMarket,
      yesToken: currentYesToken,
      noToken: currentNoToken
    });

    connectWebsocket();
  } catch (err) {
    console.log(`${colors.red}[SCANNER ERROR] ${err.message}${colors.reset}`);

    audit('SCANNER_ERROR', {
      message: err.message,
      stack: err.stack
    });

    searchCooldownUntil = Date.now() + 5000;
  } finally {
    isSearchingNextMarket = false;
  }
}


// =====================================================
// DASHBOARD SERVER
// =====================================================

http
  .createServer((req, res) => {
    if (req.url === '/api/live') {
      res.writeHead(200, {
        'Content-Type': 'application/json'
      });

      res.end(
        JSON.stringify({
          mode: MODE,
          stats,
          arb,
          currentMarket,
          currentPrices,
          currentBooks,
          recentTrades
        })
      );

      return;
    }

    fs.readFile(
      path.join(__dirname, 'dashboard.html'),
      'utf8',
      (err, data) => {
        if (err) {
          res.writeHead(500);
          res.end('Dashboard UI missing.');
          return;
        }

        res.writeHead(200, {
          'Content-Type': 'text/html'
        });

        res.end(data);
      }
    );
  })
  .listen(PORT, '0.0.0.0', () => {
    console.log(
      `${colors.cyan}[DASHBOARD] Web UI running on port ${PORT}${colors.reset}`
    );
  });


// =====================================================
// MAIN LOOP
// =====================================================

async function runPaperTrader() {
  console.log(
    `${colors.magenta}Booting 48-Centre Fast-Hedge PAPER validator...${colors.reset}`
  );

  console.log(
    `${colors.gray}Entry: ${ENTRY_PRICE} | Hedge delay: ${HEDGE_DELAY_SECONDS}s | Max hedge: ${MAX_HEDGE_PRICE} | Size: ${SIZE_PER_SIDE}${colors.reset}`
  );

  await loadNextMarket();

  setInterval(async () => {
    const now = Date.now();

    try {
      handleHedgeTimer();

      if (
        (!currentMarket || now >= marketEndTime) &&
        !isSearchingNextMarket &&
        now > searchCooldownUntil
      ) {
        await loadNextMarket();
      }

      if (
        currentMarket &&
        currentPrices.YES > 0 &&
        currentPrices.NO > 0
      ) {
        priceStream.write(
          [
            new Date().toISOString(),
            currentMarket.id,
            currentBooks.YES.ask.toFixed(4),
            currentBooks.YES.askSize,
            currentBooks.YES.bid.toFixed(4),
            currentBooks.NO.ask.toFixed(4),
            currentBooks.NO.askSize,
            currentBooks.NO.bid.toFixed(4)
          ].join(',') + '\n'
        );
      }

      if (Math.floor(now / 1000) % 10 === 0) {
        let displayState = arb.status;

        if (
          now < marketEndTime &&
          isTerminalState(arb.status)
        ) {
          const secLeft = Math.floor((marketEndTime - now) / 1000);
          displayState = `WAITING_FOR_EXPIRY (${secLeft}s left)`;
        }

        console.log(
          `${colors.gray}[LIVE PAPER] State: ${displayState} | Balance: $${stats.currentBalance.toFixed(2)} | YES Ask: $${currentPrices.YES.toFixed(3)} | NO Ask: $${currentPrices.NO.toFixed(3)}${colors.reset}`
        );
      }
    } catch (err) {
      console.log(`${colors.red}[LOOP ERROR] ${err.message}${colors.reset}`);

      audit('LOOP_ERROR', {
        message: err.message,
        stack: err.stack
      });
    }
  }, 1000);
}

runPaperTrader().catch(err => {
  console.error(err);
  process.exit(1);
});