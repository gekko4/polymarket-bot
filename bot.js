require('dotenv').config();

const WebSocket = require('ws');
const fs = require('fs');
const http = require('http');
const path = require('path');

/*
  48-Centre Fast-Hedge Strategy
  PAPER validator using live Polymarket quote data.

  This file follows the documented strategy:
  - Detect eligible 5-minute BTC binary market.
  - Identify YES and NO token IDs.
  - Immediately arm/rest paper YES and NO buy limits at ENTRY_PRICE.
  - Do NOT wait for both prices to be near 0.48.
  - A single fill is NOT arbitrage; it starts the hedge timer.
  - If the opposite 0.48 order fills before timer expiry, pair completes at 48/48.
  - If timer expires, buy opposite side at current ask if <= MAX_HEDGE_PRICE.
  - Record market, tokens, orders, fills, hedge, PnL, fees, quote history and risk triggers.
  - PAPER ONLY. This does not place live exchange orders.
*/

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
// CONFIG HELPERS
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

// =====================================================
// CONFIG
// =====================================================

const MODE = process.env.MODE || 'paper';

const ENTRY_PRICE = num('ENTRY_PRICE', 0.48);
const HEDGE_DELAY_SECONDS = num('HEDGE_DELAY_SECONDS', 2.0);
const MAX_HEDGE_PRICE = num('MAX_HEDGE_PRICE', 0.60);
const SIZE_PER_SIDE = num('SIZE_PER_SIDE', 1);
const ONE_TRADE_ATTEMPT_PER_MARKET = bool('ONE_TRADE_ATTEMPT_PER_MARKET', true);

const MAKER_FEE_BPS = num('MAKER_FEE_BPS', 0);
const TAKER_FEE_BPS = num('TAKER_FEE_BPS', 180);
const SLIPPAGE_BPS = num('SLIPPAGE_BPS', 0);

const STALE_QUOTE_MS = num('STALE_QUOTE_MS', 1500);
const MAX_SIZE_PER_MARKET = num('MAX_SIZE_PER_MARKET', 1);
const MAX_TOTAL_OPEN_EXPOSURE = num('MAX_TOTAL_OPEN_EXPOSURE', 5);
const MAX_DAILY_REALIZED_LOSS = num('MAX_DAILY_REALIZED_LOSS', 10);
const MAX_DAILY_HEDGE_FAILURES = num('MAX_DAILY_HEDGE_FAILURES', 3);

const MARKET_SYMBOL = process.env.MARKET_SYMBOL || 'btc';
const MARKET_INTERVAL_SECONDS = num('MARKET_INTERVAL_SECONDS', 300);

const PORT = num('PORT', 3000);

if (MODE !== 'paper') {
  throw new Error('This implementation is paper-only. Set MODE=paper.');
}

if (SIZE_PER_SIDE > MAX_SIZE_PER_MARKET) {
  throw new Error(
    `SIZE_PER_SIDE=${SIZE_PER_SIDE} exceeds MAX_SIZE_PER_MARKET=${MAX_SIZE_PER_MARKET}`
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
  startingBalance: 100.0,
  currentBalance: 100.0,
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

let currentBooks = createEmptyBooks();

let recentTrades = [];
let completedMarketIds = new Set();

let isSearchingNextMarket = false;
let searchCooldownUntil = 0;
let lastStatusPrintSecond = 0;

// =====================================================
// LOGGING SETUP
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
    .map(arg => (typeof arg === 'object' ? JSON.stringify(arg) : String(arg)))
    .join(' ');

  const cleanMessage = message.replace(/\x1b\[[0-9;]*m/g, '');

  terminalStream.write(`[${new Date().toISOString()}] ${cleanMessage}\n`);
};

ensureCsvHeaders();

function ensureCsvHeaders() {
  if (
    !fs.existsSync('paper_trades_log.csv') ||
    fs.statSync('paper_trades_log.csv').size === 0
  ) {
    tradeStream.write(
      [
        'Date',
        'Market_ID',
        'Market_Slug',
        'Market_Title',
        'Market_Start',
        'Market_End',
        'YES_Token',
        'NO_Token',
        'YES_Order_ID',
        'NO_Order_ID',
        'Hedge_Order_ID',
        'Status',
        'First_Fill_Side',
        'First_Fill_Price',
        'First_Fill_Time',
        'First_Fill_Size',
        'First_Fill_Observed_Ask',
        'First_Fill_Observed_Ask_Size',
        'Second_Fill_Side',
        'Second_Fill_Price',
        'Second_Fill_Time',
        'Second_Fill_Size',
        'Second_Fill_Observed_Ask',
        'Second_Fill_Observed_Ask_Size',
        'Hedge_Side',
        'Hedge_Price',
        'Hedge_Time',
        'Hedge_Observed_Ask',
        'Hedge_Observed_Ask_Size',
        'Seconds_To_Complete',
        'Shares',
        'Gross_PnL_USD',
        'Fees_USD',
        'Net_PnL_USD',
        'Deployed_Cost_USD',
        'Payout_USD',
        'Balance_USD',
        'Win_Rate_Pct',
        'Risk_Reason',
        'Queue_Model'
      ].join(',') + '\n'
    );
  }

  if (
    !fs.existsSync('price_history.csv') ||
    fs.statSync('price_history.csv').size === 0
  ) {
    priceStream.write(
      [
        'Timestamp',
        'Market_ID',
        'Market_Slug',
        'YES_Ask',
        'YES_Ask_Size',
        'YES_Bid',
        'YES_Bid_Size',
        'YES_Source',
        'YES_Can_Fill_From_Book',
        'YES_Quote_Age_Ms',
        'NO_Ask',
        'NO_Ask_Size',
        'NO_Bid',
        'NO_Bid_Size',
        'NO_Source',
        'NO_Can_Fill_From_Book',
        'NO_Quote_Age_Ms',
        'Arb_State'
      ].join(',') + '\n'
    );
  }
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
// STATE FACTORIES
// =====================================================

function createEmptyBooks() {
  return {
    YES: {
      ask: 0,
      askSize: 0,
      bid: 0,
      bidSize: 0,
      ts: 0,
      source: null,
      canFillFromBook: false
    },
    NO: {
      ask: 0,
      askSize: 0,
      bid: 0,
      bidSize: 0,
      ts: 0,
      source: null,
      canFillFromBook: false
    }
  };
}

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
    },
    quoteAtArm: null
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
        reason: `STALE_${side}_QUOTE`,
        quoteAgeMs: book.ts ? now - book.ts : null
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

  if (!book.ts || Date.now() - book.ts > STALE_QUOTE_MS) {
    return {
      ok: false,
      reason: `STALE_${side}_QUOTE`,
      quoteAgeMs: book.ts ? Date.now() - book.ts : null
    };
  }

  if (!book.canFillFromBook || book.source !== 'book') {
    return {
      ok: false,
      reason: 'NO_FULL_BOOK_SNAPSHOT_FOR_HEDGE',
      source: book.source,
      ask: book.ask,
      askSize: book.askSize
    };
  }

  if (book.ask > MAX_HEDGE_PRICE) {
    return {
      ok: false,
      reason: 'HEDGE_PRICE_ABOVE_MAX',
      observedAsk: book.ask,
      maxHedgePrice: MAX_HEDGE_PRICE
    };
  }

  if (book.askSize < SIZE_PER_SIDE) {
    return {
      ok: false,
      reason: 'INSUFFICIENT_HEDGE_SIZE',
      askSize: book.askSize,
      needed: SIZE_PER_SIDE
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

function paperLimitBuyFillCheck(order) {
  if (!order || order.status !== 'OPEN') {
    return {
      fill: false,
      reason: 'ORDER_NOT_OPEN'
    };
  }

  const book = currentBooks[order.side];

  if (!book) {
    return {
      fill: false,
      reason: 'NO_BOOK'
    };
  }

  if (!book.ts || Date.now() - book.ts > STALE_QUOTE_MS) {
    return {
      fill: false,
      reason: 'STALE_QUOTE',
      quoteAgeMs: book.ts ? Date.now() - book.ts : null
    };
  }

  if (!book.canFillFromBook || book.source !== 'book') {
    return {
      fill: false,
      reason: 'NO_FULL_BOOK_SNAPSHOT_FOR_FILL',
      source: book.source,
      ask: book.ask,
      askSize: book.askSize
    };
  }

  if (book.ask <= 0) {
    return {
      fill: false,
      reason: 'INVALID_ASK'
    };
  }

  if (book.ask > order.price) {
    return {
      fill: false,
      reason: 'ASK_ABOVE_LIMIT',
      ask: book.ask,
      limit: order.price,
      askSize: book.askSize
    };
  }

  if (book.askSize < order.shares) {
    return {
      fill: false,
      reason: 'INSUFFICIENT_DISPLAYED_SIZE',
      ask: book.ask,
      askSize: book.askSize,
      needed: order.shares
    };
  }

  return {
    fill: true,
    reason: 'FULL_BOOK_ASK_CROSSED_LIMIT_WITH_DISPLAYED_SIZE',
    ask: book.ask,
    askSize: book.askSize,
    bid: book.bid,
    bidSize: book.bidSize,
    limit: order.price,
    requestedShares: order.shares,
    quoteTs: book.ts,
    queueModel: 'optimistic_displayed_size_no_queue_position'
  };
}

// =====================================================
// STRATEGY: START / ARM RESTING LIMITS
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

  arb.quoteAtArm = {
    YES: { ...currentBooks.YES },
    NO: { ...currentBooks.NO },
    timestamp: Date.now()
  };

  audit('RESTING_LIMITS_ARMED', {
    market: currentMarket,
    yesToken: currentYesToken,
    noToken: currentNoToken,
    yesOrder: arb.orders.YES,
    noOrder: arb.orders.NO,
    quoteAtArm: arb.quoteAtArm,
    note:
      'Following documentation: YES and NO 0.48 paper buy limits armed immediately for eligible market.'
  });

  console.log(
    `\n${colors.cyan}[${STATES.ORDERS_LIVE}] Resting paper limits armed: YES @ $${ENTRY_PRICE}, NO @ $${ENTRY_PRICE} | Size: ${SIZE_PER_SIDE} | Current YES Ask: $${currentBooks.YES.ask.toFixed(3)} | Current NO Ask: $${currentBooks.NO.ask.toFixed(3)}${colors.reset}`
  );
}

// =====================================================
// STRATEGY: LIMIT FILLS
// =====================================================

function checkPaperLimitFills() {
  if (
    arb.status !== STATES.ORDERS_LIVE &&
    arb.status !== STATES.ONE_SIDE_FILLED
  ) {
    return;
  }

  for (const side of ['YES', 'NO']) {
    const order = arb.orders[side];

    const fillCheck = paperLimitBuyFillCheck(order);

    if (!fillCheck.fill) {
      continue;
    }

    order.status = 'FILLED';
    order.filledAt = Date.now();

    const fill = {
      orderId: order.id,
      side,
      price: order.price,
      shares: order.shares,
      time: Date.now(),
      isTaker: false,
      observedAskAtFill: fillCheck.ask,
      observedAskSizeAtFill: fillCheck.askSize,
      observedBidAtFill: fillCheck.bid,
      observedBidSizeAtFill: fillCheck.bidSize,
      fillReason: fillCheck.reason,
      queueModel: fillCheck.queueModel
    };

    arb.fills[side] = fill;

    arb.positions[side] = {
      side,
      shares: order.shares,
      entryPrice: order.price,
      isTaker: false,
      filledAt: fill.time
    };

    audit('PAPER_LIMIT_FILLED', {
      market: currentMarket,
      fill,
      quoteAtFill: { ...currentBooks[side] },
      warning:
        'Paper fill uses full book displayed ask crossing the limit. Queue position is not guaranteed.'
    });

    console.log(
      `${colors.brightYellow}[PAPER FILL] ${side} limit @ $${ENTRY_PRICE} filled | Observed ask: $${fillCheck.ask.toFixed(3)} | Ask size: ${fillCheck.askSize}${colors.reset}`
    );

    const filledSides = Object.keys(arb.positions).filter(
      key => arb.positions[key] !== null
    );

    if (filledSides.length === 1 && !arb.firstFillSide) {
      arb.status = STATES.ONE_SIDE_FILLED;
      arb.firstFillSide = side;
      arb.firstFillTime = Date.now();

      console.log(
        `${colors.brightYellow}[${STATES.ONE_SIDE_FILLED}] ${side} filled at $${ENTRY_PRICE}. Hedge timer started: ${HEDGE_DELAY_SECONDS}s${colors.reset}`
      );

      audit('FIRST_FILL_TIMER_STARTED', {
        market: currentMarket,
        side,
        firstFillTime: arb.firstFillTime,
        hedgeDelaySeconds: HEDGE_DELAY_SECONDS
      });
    }

    if (filledSides.length === 2) {
      completePair(STATES.PAIR_COMPLETED_AT_48);
      return;
    }
  }
}

// =====================================================
// STRATEGY: HEDGE TIMER
// =====================================================

function handleHedgeTimer() {
  if (arb.status !== STATES.ONE_SIDE_FILLED) {
    return;
  }

  const elapsedMs = Date.now() - arb.firstFillTime;

  if (elapsedMs < HEDGE_DELAY_SECONDS * 1000) {
    return;
  }

  const hedgeSide = oppositeSide(arb.firstFillSide);

  if (arb.positions[hedgeSide]) {
    return;
  }

  const fresh = validateFreshQuotes([hedgeSide]);

  if (!fresh.ok) {
    abortAttempt(fresh.reason, fresh);
    return;
  }

  const hedgeRisk = validateHedge(hedgeSide);

  if (!hedgeRisk.ok) {
    stats.hedgeFailures++;
    abortAttempt(hedgeRisk.reason, hedgeRisk);
    return;
  }

  if (arb.orders[hedgeSide] && arb.orders[hedgeSide].status === 'OPEN') {
    arb.orders[hedgeSide].status = 'CANCELLED';
    arb.orders[hedgeSide].cancelReason = 'HEDGE_TIMER_EXPIRED';
    arb.orders[hedgeSide].cancelledAt = Date.now();

    audit('RESTING_OPPOSITE_CANCELLED_FOR_HEDGE', {
      market: currentMarket,
      cancelledOrder: arb.orders[hedgeSide],
      hedgeSide
    });
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
    isTaker: true,
    observedAskAtFill: rawAsk,
    observedAskSizeAtFill: currentBooks[hedgeSide].askSize,
    observedBidAtFill: currentBooks[hedgeSide].bid,
    observedBidSizeAtFill: currentBooks[hedgeSide].bidSize,
    fillReason: 'HEDGE_TIMER_EXPIRED_MARKETABLE_BUY_FROM_FULL_BOOK',
    queueModel: 'marketable_buy_assumes_displayed_ask_available'
  };

  arb.positions[hedgeSide] = {
    side: hedgeSide,
    shares: SIZE_PER_SIDE,
    entryPrice: hedgePrice,
    isTaker: true,
    filledAt: Date.now()
  };

  audit('PAPER_HEDGE_FILLED', {
    market: currentMarket,
    firstFillSide: arb.firstFillSide,
    hedgeSide,
    rawAsk,
    hedgePrice,
    askSize: currentBooks[hedgeSide].askSize,
    elapsedSeconds: elapsedMs / 1000,
    order: hedgeOrder
  });

  console.log(
    `${colors.magenta}[${STATES.PAIR_COMPLETED_BY_HEDGE}] Timer expired. Hedged ${hedgeSide} at $${hedgePrice.toFixed(4)} | Raw ask: $${rawAsk.toFixed(4)}${colors.reset}`
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

  if (!yesPos || !noPos) {
    return {
      yesCost: 0,
      noCost: 0,
      deployedCost: 0,
      payout: 0,
      fees: 0,
      grossPnl: 0,
      netPnl: 0
    };
  }

  const yesCost = yesPos.entryPrice * yesPos.shares;
  const noCost = noPos.entryPrice * noPos.shares;

  const payout = 1.0 * SIZE_PER_SIDE;

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
  if (isTerminalState(arb.status) && arb.completedAt) {
    return;
  }

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

  if (currentMarket && currentMarket.id) {
    completedMarketIds.add(currentMarket.id);
  }

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
  console.log(`Market: ${currentMarket ? currentMarket.title : 'UNKNOWN'}`);
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

  closeMarketWebsocket('PAIR_COMPLETED_STOP_TRADING_THIS_MARKET');
}

function abortAttempt(reason, detail = {}) {
  if (isTerminalState(arb.status) && arb.completedAt) {
    return;
  }

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

  if (currentMarket && currentMarket.id) {
    completedMarketIds.add(currentMarket.id);
  }

  const secondsToComplete = arb.firstFillTime
    ? (Date.now() - arb.firstFillTime) / 1000
    : 0;

  console.log(
    `${colors.red}[${STATES.ABORTED_OR_CANCELLED}] ${reason}${colors.reset}`
  );

  const yesCost = arb.positions.YES
    ? arb.positions.YES.entryPrice * arb.positions.YES.shares
    : 0;

  const noCost = arb.positions.NO
    ? arb.positions.NO.entryPrice * arb.positions.NO.shares
    : 0;

  const pnl = {
    yesCost,
    noCost,
    deployedCost: yesCost + noCost,
    payout: 0,
    fees: 0,
    grossPnl: 0,
    netPnl: 0
  };

  const winRate =
    stats.totalTrades > 0
      ? ((stats.wins / stats.totalTrades) * 100).toFixed(1)
      : '0.0';

  writeTradeCsv({
    status: STATES.ABORTED_OR_CANCELLED,
    pnl,
    secondsToComplete,
    winRate,
    riskReason: reason
  });

  audit('ATTEMPT_ABORTED', {
    market: currentMarket,
    reason,
    detail,
    arb
  });

  closeMarketWebsocket('ATTEMPT_ABORTED_STOP_TRADING_THIS_MARKET');
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

  const hedgeFill =
    hedgeOrder && hedgeOrder.side ? arb.fills[hedgeOrder.side] : null;

  const queueModel =
    firstFill?.queueModel ||
    secondFill?.queueModel ||
    hedgeFill?.queueModel ||
    '';

  const row = [
    new Date().toISOString(),

    currentMarket?.id || '',
    currentMarket?.slug || '',
    currentMarket?.title || '',
    currentMarket?.startTime || '',
    currentMarket?.endTime || '',

    currentYesToken || '',
    currentNoToken || '',

    arb.orders.YES?.id || '',
    arb.orders.NO?.id || '',
    arb.orders.HEDGE?.id || '',

    status,

    arb.firstFillSide || '',
    firstFill?.price || '',
    firstFill?.time ? new Date(firstFill.time).toISOString() : '',
    firstFill?.shares || '',
    firstFill?.observedAskAtFill || '',
    firstFill?.observedAskSizeAtFill || '',

    secondFill?.side || '',
    secondFill?.price || '',
    secondFill?.time ? new Date(secondFill.time).toISOString() : '',
    secondFill?.shares || '',
    secondFill?.observedAskAtFill || '',
    secondFill?.observedAskSizeAtFill || '',

    hedgeOrder?.side || '',
    hedgeOrder?.price || '',
    hedgeOrder?.filledAt ? new Date(hedgeOrder.filledAt).toISOString() : '',
    hedgeFill?.observedAskAtFill || '',
    hedgeFill?.observedAskSizeAtFill || '',

    secondsToComplete,
    SIZE_PER_SIDE,

    Number(pnl.grossPnl || 0).toFixed(6),
    Number(pnl.fees || 0).toFixed(6),
    Number(pnl.netPnl || 0).toFixed(6),
    Number(pnl.deployedCost || 0).toFixed(6),
    Number(pnl.payout || 0).toFixed(6),
    stats.currentBalance.toFixed(2),

    `${winRate}%`,

    riskReason || '',
    queueModel
  ];

  tradeStream.write(row.map(v => JSON.stringify(v)).join(',') + '\n');
}

// =====================================================
// MARKET DATA HELPERS
// =====================================================

function bestAskFromArray(asks) {
  if (!Array.isArray(asks) || asks.length === 0) {
    return null;
  }

  let best = null;

  for (const level of asks) {
    const price = Number(level.price);
    const size = Number(level.size || 0);

    if (!Number.isFinite(price)) {
      continue;
    }

    if (!best || price < best.price) {
      best = {
        price,
        size
      };
    }
  }

  return best;
}

function bestBidFromArray(bids) {
  if (!Array.isArray(bids) || bids.length === 0) {
    return null;
  }

  let best = null;

  for (const level of bids) {
    const price = Number(level.price);
    const size = Number(level.size || 0);

    if (!Number.isFinite(price)) {
      continue;
    }

    if (!best || price > best.price) {
      best = {
        price,
        size
      };
    }
  }

  return best;
}

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

  if (!Number.isFinite(ask) || !Number.isFinite(bid)) {
    return;
  }

  const isFullBook = data.source === 'book';

  let askSize = 0;
  let bidSize = 0;

  if (isFullBook) {
    askSize = Number(data.bestAskSize || 0);
    bidSize = Number(data.bestBidSize || 0);
  } else {
    // Important: do not reuse old sizes for price_change.
    // price_change can update displayed price, but cannot prove fillable size.
    askSize = 0;
    bidSize = 0;
  }

  currentPrices[side] = ask;

  currentBooks[side] = {
    ask,
    askSize: Number.isFinite(askSize) ? askSize : 0,
    bid,
    bidSize: Number.isFinite(bidSize) ? bidSize : 0,
    ts: Date.now(),
    source: data.source || 'unknown',
    canFillFromBook: isFullBook
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

// =====================================================
// WEBSOCKET
// =====================================================

function shouldKeepMarketWebsocketAlive() {
  if (!currentMarket) {
    return false;
  }

  if (!currentYesToken || !currentNoToken) {
    return false;
  }

  if (Date.now() >= marketEndTime) {
    return false;
  }

  if (isTerminalState(arb.status)) {
    return false;
  }

  return true;
}

function closeMarketWebsocket(reason) {
  if (!global.wsMarket) {
    return;
  }

  try {
    audit('WS_CLOSED_BY_BOT', {
      reason,
      market: currentMarket,
      state: arb.status
    });

    global.wsMarket.removeAllListeners('close');
    global.wsMarket.removeAllListeners('error');
    global.wsMarket.removeAllListeners('message');
    global.wsMarket.removeAllListeners('open');

    global.wsMarket.terminate();
  } catch {}

  global.wsMarket = null;
}

function connectWebsocket() {
  if (global.wsMarket) {
    try {
      global.wsMarket.terminate();
    } catch {}
  }

  if (!currentYesToken || !currentNoToken || !currentMarket) {
    return;
  }

  if (isTerminalState(arb.status)) {
    audit('WS_CONNECT_SKIPPED_TERMINAL_STATE', {
      market: currentMarket,
      state: arb.status
    });

    return;
  }

  const wsMarket = new WebSocket(
    'wss://ws-subscriptions-clob.polymarket.com/ws/market'
  );

  global.wsMarket = wsMarket;

  wsMarket.on('open', () => {
    console.log(
      `${colors.yellow}[WS] Connected to Polymarket market stream.${colors.reset}`
    );

    wsMarket.send(
      JSON.stringify({
        type: 'market',
        assets_ids: [currentYesToken, currentNoToken]
      })
    );
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

    if (data.event_type === 'book') {
      const ask = bestAskFromArray(data.asks);
      const bid = bestBidFromArray(data.bids);

      if (ask && bid) {
        handleMarketUpdate({
          asset_id: data.asset_id,
          bestAsk: ask.price,
          bestAskSize: ask.size,
          bestBid: bid.price,
          bestBidSize: bid.size,
          source: 'book'
        });
      }

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
          bestAskSize: pc.best_ask_size,
          bestBid: pc.best_bid,
          bestBidSize: pc.best_bid_size,
          source: 'price_change'
        });
      }
    }
  });

  wsMarket.on('error', err => {
    console.log(`${colors.red}[WS ERROR] ${err.message}${colors.reset}`);

    audit('WS_ERROR', {
      message: err.message,
      market: currentMarket,
      state: arb.status
    });
  });

  wsMarket.on('close', () => {
    global.wsMarket = null;

    if (!shouldKeepMarketWebsocketAlive()) {
      audit('WS_CLOSE_NO_RECONNECT', {
        market: currentMarket,
        state: arb.status,
        marketEndTime,
        now: Date.now()
      });

      return;
    }

    console.log(
      `${colors.gray}[WS] Connection dropped. Reconnecting...${colors.reset}`
    );

    setTimeout(() => {
      if (shouldKeepMarketWebsocketAlive()) {
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
      endTime: new Date(currentIntervalEndSec * 1000).toISOString(),
      endMs: marketEndTime
    };

    currentPrices = {
      YES: 0,
      NO: 0
    };

    currentBooks = createEmptyBooks();
    arb = createEmptyArbState();

    console.log(
      `${colors.brightYellow}[MARKET LOADED] ${currentMarket.title}${colors.reset}`
    );

    console.log(`${colors.gray}YES token: ${currentYesToken}${colors.reset}`);
    console.log(`${colors.gray}NO token:  ${currentNoToken}${colors.reset}`);

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
// PRICE HISTORY
// =====================================================

function writePriceHistory() {
  if (!currentMarket) {
    return;
  }

  if (currentPrices.YES <= 0 || currentPrices.NO <= 0) {
    return;
  }

  const now = Date.now();

  const yesAge = currentBooks.YES.ts ? now - currentBooks.YES.ts : '';
  const noAge = currentBooks.NO.ts ? now - currentBooks.NO.ts : '';

  priceStream.write(
    [
      new Date().toISOString(),
      currentMarket.id,
      currentMarket.slug,

      currentBooks.YES.ask.toFixed(4),
      currentBooks.YES.askSize,
      currentBooks.YES.bid.toFixed(4),
      currentBooks.YES.bidSize,
      currentBooks.YES.source || '',
      currentBooks.YES.canFillFromBook,
      yesAge,

      currentBooks.NO.ask.toFixed(4),
      currentBooks.NO.askSize,
      currentBooks.NO.bid.toFixed(4),
      currentBooks.NO.bidSize,
      currentBooks.NO.source || '',
      currentBooks.NO.canFillFromBook,
      noAge,

      arb.status
    ].join(',') + '\n'
  );
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

      writePriceHistory();

      const currentSecond = Math.floor(now / 1000);

      if (currentSecond % 10 === 0 && currentSecond !== lastStatusPrintSecond) {
        lastStatusPrintSecond = currentSecond;

        let displayState = arb.status;

        if (now < marketEndTime && isTerminalState(arb.status)) {
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
