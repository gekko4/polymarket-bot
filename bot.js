require('dotenv').config();

const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const http = require('http');

// ============================================================
// BTC 5m Polymarket PAPER BOT — Latent State Paper Engine V2
//
// Purpose:
// - collect paper data that can actually support live-transition decisions
// - realistic paper accounting:
//   cashBalance = spendable cash
//   currentBalance = equity = cash + open position marked to bid
// - entry buys at ask and deducts stake immediately
// - exit/settlement credits proceeds, not PnL
// - PnL is reporting only
// - no negative balance
// - unresolved settlements are logged and refunded, not counted
// ============================================================

const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  brightYellow: '\x1b[93m',
  cyan: '\x1b[36m',
  magenta: '\x1b[35m',
  gray: '\x1b[90m',
};

// ----------------------------
// CONFIG
// ----------------------------
const CONFIG = {
  MARKET: {
    intervalSec: 300,
    slugPrefix: 'btc-updown-5m',
    gammaEventsUrl: 'https://gamma-api.polymarket.com/events',
    clobWsUrl: 'wss://ws-subscriptions-clob.polymarket.com/ws/market',
  },

  STRATEGY: {
    name: 'LATENT_STATE_ENGINE_V2',

    maxAllowedSpread: Number(process.env.MAX_ALLOWED_SPREAD || 0.03),
    requireBid: String(process.env.REQUIRE_BID || 'true').toLowerCase() === 'true',
    oneTradePerMarket: true,
    minTicksBeforeSignal: Number(process.env.MIN_TICKS_BEFORE_SIGNAL || 25),
    conservativeOnly: String(process.env.CONSERVATIVE_ONLY || 'false').toLowerCase() === 'true',

    states: [
      {
        id: 'YES_CENTRAL_REVERSAL',
        enabled: envBool('ENABLE_YES_CENTRAL_REVERSAL', true),
        side: 'YES',
        quality: 'A_LATENT_YES_REVERSAL',
        sharp: false,
      },
      {
        id: 'YES_LATE_DECISIVE',
        enabled: envBool('ENABLE_YES_LATE_DECISIVE', true),
        side: 'YES',
        quality: 'A_LATENT_LATE_DOMINANCE',
        sharp: false,
      },
      {
        id: 'YES_CONTROLLED_CONTINUATION',
        enabled: envBool('ENABLE_YES_CONTROLLED_CONTINUATION', true),
        side: 'YES',
        quality: 'B_LATENT_CONTROLLED_CONTINUATION',
        sharp: false,
      },
      {
        id: 'NO_LOW_STICKY',
        enabled: envBool('ENABLE_NO_LOW_STICKY', true),
        side: 'NO',
        quality: 'B_LATENT_LOW_STICKY',
        sharp: false,
      },
      {
        id: 'NO_EARLY_WEAK_BOUNCE_TRAP',
        enabled: envBool('ENABLE_NO_EARLY_WEAK_BOUNCE_TRAP', true),
        side: 'NO',
        quality: 'A_LATENT_WEAK_BOUNCE_TRAP',
        sharp: true,
      },
      {
        id: 'NO_DAMAGED_BOUNCE',
        enabled: envBool('ENABLE_NO_DAMAGED_BOUNCE', true),
        side: 'NO',
        quality: 'A_LATENT_DAMAGED_BOUNCE',
        sharp: true,
      },
      {
        id: 'NO_MID_COLLAPSE_STATE',
        enabled: envBool('ENABLE_NO_MID_COLLAPSE_STATE', true),
        side: 'NO',
        quality: 'A_LATENT_COLLAPSE_STATE',
        sharp: true,
      },
    ],
  },

  EXIT: {
    enabled: envBool('EXIT_ENABLED', false),

    killAt180IfBidBelowEntry: envBool('KILL_180_BID_BELOW_ENTRY', true),
    killAt180IfBidBelow: Number(process.env.KILL_180_BID_BELOW || 0),
    killAt240IfBidBelow: Number(process.env.KILL_240_BID_BELOW || 0.34),
    killAt270IfBidBelow: Number(process.env.KILL_270_BID_BELOW || 0.34),

    confirmAt180Bid: Number(process.env.CONFIRM_180_BID || 0.85),
    confirmAt210Bid: Number(process.env.CONFIRM_210_BID || 0.90),
    confirmAt240Bid: Number(process.env.CONFIRM_240_BID || 0.65),
    confirmAt240MaxMomentum30: Number(process.env.CONFIRM_240_MAX_MOM30 || 0.05),
  },

  PAPER: {
    startingBalance: Number(process.env.STARTING_BALANCE || 10),
    stakeUsd: Number(process.env.STAKE_USD || 1),
    minStakeUsd: Number(process.env.MIN_STAKE_USD || 1),
    feeBps: Number(process.env.FEE_BPS || 0),

    // Strict final quote settlement avoids polluting paper data.
    strictSettlement: envBool('STRICT_SETTLEMENT', true),
    settlementWinAsk: Number(process.env.SETTLEMENT_WIN_ASK || 0.90),
    settlementLoseAsk: Number(process.env.SETTLEMENT_LOSE_ASK || 0.10),
  },

  SERVER: {
    port: Number(process.env.PORT || 3000),
    host: process.env.HOST || '0.0.0.0',
  },

  FILES: {
    trades: process.env.TRADE_LOG_FILE || 'paper_trades_log.csv',
    prices: process.env.PRICE_LOG_FILE || 'price_history.csv',
    terminal: process.env.TERMINAL_LOG_FILE || 'terminal_logs.txt',
    dashboard: process.env.DASHBOARD_FILE || 'dashboard.html',
  },
};

function envBool(name, fallback) {
  const v = process.env[name];
  if (v == null) return fallback;
  return String(v).toLowerCase() === 'true';
}

// ----------------------------
// GLOBAL STATE
// ----------------------------
let wsMarket = null;
let isSearchingNextMarket = false;
let searchCooldownUntil = 0;
let lastStatusPrintSec = 0;
let lastPriceLogSec = 0;

let currentMarket = emptyMarket();
let currentPrices = { YES: 0, NO: 0 };
let recentTrades = [];
let trade = emptyTrade();

let stats = {
  totalTrades: 0,
  wins: 0,
  losses: 0,
  unresolved: 0,

  startingBalance: CONFIG.PAPER.startingBalance,

  // Spendable paper cash.
  cashBalance: CONFIG.PAPER.startingBalance,

  // Equity = cash + current bid value of open position.
  currentBalance: CONFIG.PAPER.startingBalance,

  openPositionValue: 0,
  realisedPnl: 0,
};

function emptyMarket() {
  return {
    slug: null,
    title: null,
    startTimeMs: 0,
    endTimeMs: 0,
    yesToken: null,
    noToken: null,
    hasTraded: false,
    settled: false,
    lastQuoteTime: 0,

    quote: {
      YES: { ask: null, bid: null, askSize: null, bidSize: null, spread: null },
      NO: { ask: null, bid: null, askSize: null, bidSize: null, spread: null },
    },

    priceTicks: [],
    holdConfirmed: false,
    holdConfirmReason: null,
  };
}

function emptyTrade() {
  return {
    active: false,
    marketSlug: null,
    marketTitle: null,
    side: null,
    tokenId: null,

    entryPrice: 0,
    shares: 0,
    stakeUsd: 0,
    entryCost: 0,
    entryFee: 0,

    entryTime: 0,
    entryElapsedSec: 0,

    quality: null,
    reason: null,
    state: null,
    pathSnapshot: null,

    exitMode: 'HOLD_TO_SETTLEMENT',
    confirmedHold: false,
    confirmReason: null,
  };
}

// ----------------------------
// FILE LOGGING
// ----------------------------
const tradeStream = fs.createWriteStream(CONFIG.FILES.trades, { flags: 'a' });
const priceStream = fs.createWriteStream(CONFIG.FILES.prices, { flags: 'a' });
const terminalStream = fs.createWriteStream(CONFIG.FILES.terminal, { flags: 'a' });

const originalLog = console.log;
console.log = function (...args) {
  originalLog.apply(console, args);
  const message = args.map((arg) => (typeof arg === 'object' ? JSON.stringify(arg) : String(arg))).join(' ');
  const clean = message.replace(/\x1b\[[0-9;]*m/g, '');
  terminalStream.write(`[${new Date().toISOString()}] ${clean}\n`);
};

if (!fs.existsSync(CONFIG.FILES.trades) || fs.statSync(CONFIG.FILES.trades).size === 0) {
  tradeStream.write([
    'Timestamp',
    'MarketSlug',
    'Event',
    'Side',
    'State',
    'EntryPrice',
    'ExitValue',
    'StakeUSD',
    'Shares',
    'EntryElapsedSec',
    'ExitElapsedSec',
    'Quality',
    'Reason',
    'PnL_USD',
    'CashBalance_USD',
    'Equity_USD',
    'OpenPositionValue_USD',
    'RealisedPnL_USD',
    'WinRate_Pct',
    'ExitReason',
    'HeldBid',
    'Momentum30',
    'MID',
    'D10',
    'D30',
    'RANGE',
    'CROSSES',
    'DAMAGE',
  ].join(',') + '\n');
}

if (!fs.existsSync(CONFIG.FILES.prices) || fs.statSync(CONFIG.FILES.prices).size === 0) {
  priceStream.write('Timestamp,MarketSlug,ElapsedSec,YES_Ask,YES_Bid,NO_Ask,NO_Bid,YES_Mid,SpreadYES,SpreadNO\n');
}

function fmt(x) {
  if (x == null || Number.isNaN(Number(x))) return '';
  return Number(x).toFixed(3);
}

function money(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n.toFixed(2) : '0.00';
}

// ----------------------------
// PAPER ACCOUNTING
// ----------------------------
function getOpenPositionValue() {
  if (!trade.active) return 0;

  const heldBid = getHeldBid(trade.side);
  if (heldBid == null || !Number.isFinite(Number(heldBid))) return 0;

  return Number(heldBid) * Number(trade.shares || 0);
}

function syncPaperEquity() {
  stats.openPositionValue = getOpenPositionValue();
  stats.currentBalance = stats.cashBalance + stats.openPositionValue;

  if (stats.cashBalance < 0 && stats.cashBalance > -1e-9) stats.cashBalance = 0;
  if (stats.currentBalance < 0 && stats.currentBalance > -1e-9) stats.currentBalance = 0;

  return stats;
}

function winRateText() {
  return stats.totalTrades > 0 ? `${((stats.wins / stats.totalTrades) * 100).toFixed(1)}%` : '0.0%';
}

function writeTradeLog({
  event,
  side = '',
  state = '',
  entryPrice = '',
  exitValue = '',
  stakeUsd = '',
  shares = '',
  entryElapsedSec = '',
  exitElapsedSec = '',
  quality = '',
  reason = '',
  pnl = '',
  exitReason = '',
  heldBid = '',
  momentum30 = '',
  pathSnapshot = null,
}) {
  syncPaperEquity();

  tradeStream.write([
    new Date().toISOString(),
    currentMarket.slug || trade.marketSlug || '',
    event,
    side,
    state,
    entryPrice === '' ? '' : fmt(entryPrice),
    exitValue === '' ? '' : fmt(exitValue),
    stakeUsd === '' ? '' : Number(stakeUsd).toFixed(2),
    shares === '' ? '' : Number(shares).toFixed(4),
    entryElapsedSec,
    exitElapsedSec,
    quality,
    String(reason || '').replaceAll(',', ';'),
    pnl === '' ? '' : Number(pnl).toFixed(4),
    stats.cashBalance.toFixed(2),
    stats.currentBalance.toFixed(2),
    stats.openPositionValue.toFixed(2),
    stats.realisedPnl.toFixed(4),
    winRateText(),
    String(exitReason || '').replaceAll(',', ';'),
    heldBid === '' ? '' : fmt(heldBid),
    momentum30 == null || momentum30 === '' ? '' : fmt(momentum30),
    pathSnapshot ? fmt(pathSnapshot.yesMid) : '',
    pathSnapshot ? fmt(pathSnapshot.d10) : '',
    pathSnapshot ? fmt(pathSnapshot.d30) : '',
    pathSnapshot ? fmt(pathSnapshot.range) : '',
    pathSnapshot ? pathSnapshot.crosses : '',
    pathSnapshot ? fmt(pathSnapshot.damage) : '',
  ].join(',') + '\n');
}

function logPriceTick() {
  if (!currentMarket.slug) return;

  const nowSec = Math.floor(Date.now() / 1000);
  if (nowSec === lastPriceLogSec) return;
  lastPriceLogSec = nowSec;

  const y = currentMarket.quote.YES;
  const n = currentMarket.quote.NO;

  if (y.ask == null || y.bid == null || n.ask == null || n.bid == null) return;

  const yesMid = (Number(y.ask) + Number(y.bid)) / 2;
  const spreadYES = y.ask - y.bid;
  const spreadNO = n.ask - n.bid;

  const tick = {
    elapsedSec: getElapsedSec(),
    YES: { ask: y.ask, bid: y.bid },
    NO: { ask: n.ask, bid: n.bid },
  };

  currentMarket.priceTicks.push(tick);
  if (currentMarket.priceTicks.length > 600) currentMarket.priceTicks.shift();

  priceStream.write([
    new Date().toISOString(),
    currentMarket.slug,
    getElapsedSec(),
    fmt(y.ask),
    fmt(y.bid),
    fmt(n.ask),
    fmt(n.bid),
    fmt(yesMid),
    fmt(spreadYES),
    fmt(spreadNO),
  ].join(',') + '\n');
}

// ----------------------------
// MARKET TIME HELPERS
// ----------------------------
function getElapsedSec(now = Date.now()) {
  if (!currentMarket.startTimeMs) return 0;
  return Math.max(0, Math.floor((now - currentMarket.startTimeMs) / 1000));
}

function getSecondsLeft(now = Date.now()) {
  if (!currentMarket.endTimeMs) return 0;
  return Math.max(0, Math.floor((currentMarket.endTimeMs - now) / 1000));
}

function getCurrentBtc5mInterval(nowMs = Date.now()) {
  const nowSec = Math.floor(nowMs / 1000);
  const startSec = nowSec - (nowSec % CONFIG.MARKET.intervalSec);
  const endSec = startSec + CONFIG.MARKET.intervalSec;

  return {
    startSec,
    endSec,
    slug: `${CONFIG.MARKET.slugPrefix}-${startSec}`,
  };
}

// ----------------------------
// MARKET SCANNER
// ----------------------------
async function loadCurrentMarket() {
  if (isSearchingNextMarket) return;

  isSearchingNextMarket = true;

  const { startSec, endSec, slug } = getCurrentBtc5mInterval();

  console.log(`\n${colors.yellow}[SCANNER] Looking for active BTC 5m market: ${slug}${colors.reset}`);

  try {
    const url = `${CONFIG.MARKET.gammaEventsUrl}?slug=${slug}`;
    const response = await fetch(url);
    const events = await response.json();

    if (!events || events.length === 0 || !events[0].markets || events[0].markets.length === 0) {
      console.log(`${colors.gray}[SCANNER] Market not indexed yet. Retrying.${colors.reset}`);
      searchCooldownUntil = Date.now() + 5000;
      return;
    }

    const event = events[0];
    const market = event.markets[0];

    const parsedTokens =
      typeof market.clobTokenIds === 'string'
        ? JSON.parse(market.clobTokenIds)
        : market.clobTokenIds;

    const yesToken = parsedTokens && parsedTokens[0];
    const noToken = parsedTokens && parsedTokens[1];

    if (!yesToken || !noToken) {
      console.log(`${colors.red}[SCANNER] Could not parse YES/NO token IDs.${colors.reset}`);
      searchCooldownUntil = Date.now() + 5000;
      return;
    }

    currentMarket = emptyMarket();
    currentMarket.slug = slug;
    currentMarket.title = event.title || slug;
    currentMarket.startTimeMs = startSec * 1000;
    currentMarket.endTimeMs = endSec * 1000;
    currentMarket.yesToken = String(yesToken);
    currentMarket.noToken = String(noToken);

    currentPrices = { YES: 0, NO: 0 };
    trade = emptyTrade();

    console.log(`${colors.brightYellow}[MARKET LOADED] ${currentMarket.title}${colors.reset}`);
    console.log(`${colors.gray}YES=${currentMarket.yesToken}${colors.reset}`);
    console.log(`${colors.gray}NO =${currentMarket.noToken}${colors.reset}`);

    connectWebsocket();
  } catch (err) {
    console.log(`${colors.red}[SCANNER ERROR] ${err.message}${colors.reset}`);
    searchCooldownUntil = Date.now() + 5000;
  } finally {
    isSearchingNextMarket = false;
  }
}

// ----------------------------
// WEBSOCKET
// ----------------------------
function connectWebsocket() {
  if (wsMarket) {
    try {
      wsMarket.terminate();
    } catch (_) {}
  }

  console.log(`${colors.yellow}[WS] Connecting market stream...${colors.reset}`);

  wsMarket = new WebSocket(CONFIG.MARKET.clobWsUrl);

  wsMarket.on('open', () => {
    if (!currentMarket.yesToken || !currentMarket.noToken) return;

    wsMarket.send(JSON.stringify({
      type: 'market',
      assets_ids: [currentMarket.yesToken, currentMarket.noToken],
    }));

    console.log(`${colors.green}[WS] Subscribed to YES/NO books.${colors.reset}`);
  });

  wsMarket.on('message', (msg) => {
    const text = msg.toString();
    if (text === 'PONG') return;

    try {
      const data = JSON.parse(text);
      handleWsMessage(data);
    } catch (_) {}
  });

  wsMarket.on('close', () => {
    console.log(`${colors.yellow}[WS] Closed.${colors.reset}`);
  });

  wsMarket.on('error', (err) => {
    console.log(`${colors.red}[WS ERROR] ${err.message}${colors.reset}`);
  });
}

function handleWsMessage(data) {
  if (!data || !currentMarket.slug) return;

  if (data.event_type === 'book') {
    const quote = normaliseBookMessage(data);
    if (quote) handleMarketUpdate(quote);
    return;
  }

  if (data.event_type === 'price_change' && Array.isArray(data.price_changes)) {
    for (const pc of data.price_changes) {
      const quote = normalisePriceChange(pc);
      if (quote) handleMarketUpdate(quote);
    }
  }
}

function normaliseBookMessage(data) {
  const side = tokenToSide(data.asset_id);
  if (!side) return null;

  const asks = Array.isArray(data.asks) ? data.asks : [];
  const bids = Array.isArray(data.bids) ? data.bids : [];

  if (asks.length === 0 && bids.length === 0) return null;

  const bestAskObj = asks.length
    ? asks.reduce((best, x) => Number(x.price) < Number(best.price) ? x : best, asks[0])
    : null;

  const bestBidObj = bids.length
    ? bids.reduce((best, x) => Number(x.price) > Number(best.price) ? x : best, bids[0])
    : null;

  return {
    side,
    ask: bestAskObj ? Number(bestAskObj.price) : null,
    askSize: bestAskObj ? Number(bestAskObj.size) : null,
    bid: bestBidObj ? Number(bestBidObj.price) : null,
    bidSize: bestBidObj ? Number(bestBidObj.size) : null,
  };
}

function normalisePriceChange(pc) {
  const side = tokenToSide(pc.asset_id);
  if (!side) return null;

  return {
    side,
    ask: pc.best_ask != null ? Number(pc.best_ask) : null,
    askSize: null,
    bid: pc.best_bid != null ? Number(pc.best_bid) : null,
    bidSize: null,
  };
}

function tokenToSide(tokenId) {
  const id = String(tokenId);

  if (id === currentMarket.yesToken) return 'YES';
  if (id === currentMarket.noToken) return 'NO';

  return null;
}

// ----------------------------
// QUOTE HELPERS
// ----------------------------
function handleMarketUpdate(update) {
  if (!update || !update.side) return;

  const q = currentMarket.quote[update.side];

  if (update.ask != null && !Number.isNaN(update.ask)) q.ask = update.ask;
  if (update.bid != null && !Number.isNaN(update.bid)) q.bid = update.bid;
  if (update.askSize != null && !Number.isNaN(update.askSize)) q.askSize = update.askSize;
  if (update.bidSize != null && !Number.isNaN(update.bidSize)) q.bidSize = update.bidSize;

  q.spread = q.ask != null && q.bid != null ? q.ask - q.bid : null;

  currentMarket.lastQuoteTime = Date.now();

  currentPrices.YES = currentMarket.quote.YES.ask || 0;
  currentPrices.NO = currentMarket.quote.NO.ask || 0;

  logPriceTick();
  syncPaperEquity();

  const signal = evaluateLatentStateStrategy();
  if (signal) executePaperEntry(signal);
}

function getSideQuote(side) {
  return currentMarket.quote[side] || {};
}

function getSideAsk(side) {
  const q = getSideQuote(side);
  return q.ask == null || Number.isNaN(Number(q.ask)) ? null : Number(q.ask);
}

function getSideBid(side) {
  const q = getSideQuote(side);
  return q.bid == null || Number.isNaN(Number(q.bid)) ? null : Number(q.bid);
}

function getYesMidNow() {
  const y = currentMarket.quote.YES;
  if (!y || y.ask == null || y.bid == null) return null;
  return (Number(y.ask) + Number(y.bid)) / 2;
}

function getYesMidAtOrBefore(targetElapsedSec) {
  if (!Array.isArray(currentMarket.priceTicks)) return null;

  for (let i = currentMarket.priceTicks.length - 1; i >= 0; i--) {
    const t = currentMarket.priceTicks[i];

    if (t.elapsedSec <= targetElapsedSec && t.YES && t.YES.ask != null && t.YES.bid != null) {
      return (Number(t.YES.ask) + Number(t.YES.bid)) / 2;
    }
  }

  return null;
}

function getYesTrend(secondsBack, nowElapsedSec = getElapsedSec()) {
  const nowMid = getYesMidNow();
  const priorMid = getYesMidAtOrBefore(nowElapsedSec - secondsBack);

  if (nowMid == null || priorMid == null) return null;

  return nowMid - priorMid;
}

function spreadOk(side) {
  const q = getSideQuote(side);

  if (q.spread == null || Number.isNaN(Number(q.spread))) return true;

  return Number(q.spread) <= CONFIG.STRATEGY.maxAllowedSpread;
}

function between(x, lo, hi) {
  return x != null && Number.isFinite(Number(x)) && Number(x) >= lo && Number(x) <= hi;
}

function askBidOk(side, ask, bid) {
  if (ask == null || !Number.isFinite(Number(ask))) return false;
  if (!spreadOk(side)) return false;

  if (CONFIG.STRATEGY.requireBid && (bid == null || bid <= 0)) return false;

  return true;
}

// ----------------------------
// LATENT STATE ENGINE
// ----------------------------
function getPathState(elapsed = getElapsedSec()) {
  const yesMid = getYesMidNow();

  if (yesMid == null || !Array.isArray(currentMarket.priceTicks)) return null;

  const mids = [];
  let crosses = 0;
  let prevAbove = null;

  for (const t of currentMarket.priceTicks) {
    if (!t || t.elapsedSec > elapsed || !t.YES || t.YES.ask == null || t.YES.bid == null) continue;

    const m = (Number(t.YES.ask) + Number(t.YES.bid)) / 2;
    if (!Number.isFinite(m)) continue;

    mids.push(m);

    const above = m >= 0.5;
    if (prevAbove != null && above !== prevAbove) crosses += 1;
    prevAbove = above;
  }

  if (mids.length < CONFIG.STRATEGY.minTicksBeforeSignal) return null;

  const maxMid = Math.max(...mids);
  const minMid = Math.min(...mids);

  const range = maxMid - minMid;
  const drawdown = yesMid - maxMid;
  const bounce = yesMid - minMid;

  const d5 = getYesTrend(5, elapsed);
  const d10 = getYesTrend(10, elapsed);
  const d15 = getYesTrend(15, elapsed);
  const d30 = getYesTrend(30, elapsed);
  const d60 = getYesTrend(60, elapsed);

  const uncertainty = 4 * yesMid * (1 - yesMid);
  const timePressure = elapsed / CONFIG.MARKET.intervalSec;
  const signedSide = (2 * yesMid) - 1;

  const v10 = d10 == null ? null : d10 / 10;
  const v30 = d30 == null ? null : d30 / 30;
  const curve = v10 == null || v30 == null ? null : v10 - v30;

  const normalisedImpulse = d10 == null ? null : d10 / (uncertainty + 0.05);
  const clockImpulse = normalisedImpulse == null ? null : normalisedImpulse * timePressure;

  const damage =
    range +
    crosses * 0.03 +
    Math.max(-drawdown, 0) * 0.5;

  return {
    elapsed,
    yesMid,
    d5,
    d10,
    d15,
    d30,
    d60,
    range,
    crosses,
    drawdown,
    bounce,
    uncertainty,
    timePressure,
    signedSide,
    curve,
    normalisedImpulse,
    clockImpulse,
    damage,
    tickCount: mids.length,
  };
}

function stateMatches(state, ctx) {
  const s = ctx.path;

  if (!state.enabled) return false;
  if (CONFIG.STRATEGY.conservativeOnly && state.sharp) return false;

  switch (state.id) {
    case 'YES_CENTRAL_REVERSAL':
      return between(s.elapsed, 30, 120)
        && between(s.yesMid, 0.48, 0.65)
        && s.d10 > 0.005
        && s.d30 > -0.08
        && s.d30 < 0.02
        && between(s.range, 0.08, 0.30)
        && ctx.yesAsk <= Number(process.env.YES_CENTRAL_REVERSAL_MAX_ASK || 0.67);

    case 'YES_LATE_DECISIVE':
      return between(s.elapsed, 180, 270)
        && between(s.yesMid, 0.78, 0.94)
        && s.d10 > -0.03
        && s.d30 > -0.05
        && ctx.yesAsk <= Number(process.env.YES_LATE_DECISIVE_MAX_ASK || 0.95);

    case 'YES_CONTROLLED_CONTINUATION':
      return between(s.elapsed, 60, 210)
        && between(s.yesMid, 0.55, 0.80)
        && between(s.d10, 0.005, 0.08)
        && s.d30 > -0.03
        && between(s.range, 0.10, 0.40)
        && s.crosses <= 5
        && ctx.yesAsk <= Number(process.env.YES_CONTROLLED_MAX_ASK || 0.82);

    case 'NO_LOW_STICKY':
      return between(s.elapsed, 45, 240)
        && between(s.yesMid, 0.08, 0.25)
        && s.d10 < 0.03
        && s.d30 < 0.03
        && ctx.noAsk <= Number(process.env.NO_LOW_STICKY_MAX_ASK || 0.93);

    case 'NO_EARLY_WEAK_BOUNCE_TRAP':
      return between(s.elapsed, 31, 60)
        && between(s.yesMid, 0.30, 0.40)
        && between(s.d10, 0.02, 0.05)
        && between(s.d30, -0.08, -0.05)
        && ctx.noAsk <= Number(process.env.NO_EARLY_BOUNCE_MAX_ASK || 0.70);

    case 'NO_DAMAGED_BOUNCE':
      return between(s.elapsed, 181, 210)
        && between(s.yesMid, 0.30, 0.40)
        && between(s.d10, 0.05, 0.08)
        && between(s.range, 0.35, 0.55)
        && s.crosses >= 3
        && ctx.noAsk <= Number(process.env.NO_DAMAGED_BOUNCE_MAX_ASK || 0.72);

    case 'NO_MID_COLLAPSE_STATE':
      return between(s.elapsed, 121, 150)
        && between(s.yesMid, 0.30, 0.40)
        && between(s.d10, -0.20, -0.12)
        && between(s.d30, -0.30, -0.20)
        && ctx.noAsk <= Number(process.env.NO_MID_COLLAPSE_MAX_ASK || 0.70);

    default:
      return false;
  }
}

function evaluateLatentStateStrategy() {
  if (!currentMarket.slug || currentMarket.hasTraded || trade.active) return null;

  const elapsed = getElapsedSec();

  const yesAsk = getSideAsk('YES');
  const yesBid = getSideBid('YES');
  const noAsk = getSideAsk('NO');
  const noBid = getSideBid('NO');

  const pathState = getPathState(elapsed);

  if (yesAsk == null || yesBid == null || noAsk == null || noBid == null || !pathState) {
    return null;
  }

  const ctx = {
    elapsed,
    yesAsk,
    yesBid,
    noAsk,
    noBid,
    path: pathState,
  };

  const candidates = [];

  for (const state of CONFIG.STRATEGY.states) {
    if (!stateMatches(state, ctx)) continue;

    const side = state.side;
    const ask = side === 'YES' ? yesAsk : noAsk;
    const bid = side === 'YES' ? yesBid : noBid;

    if (!askBidOk(side, ask, bid)) continue;

    let priority = 0;
    if (state.quality.startsWith('A_')) priority += 100;
    if (state.sharp) priority += 25;
    priority += Math.max(0, 1 - ask) * 10;

    candidates.push({ state, side, ask, priority });
  }

  if (!candidates.length) return null;

  candidates.sort((a, b) => b.priority - a.priority);

  const chosen = candidates[0];
  const side = chosen.side;
  const price = chosen.ask;
  const tokenId = side === 'YES' ? currentMarket.yesToken : currentMarket.noToken;
  const s = pathState;

  const reason = [
    chosen.state.id,
    `MID_${fmt(s.yesMid)}`,
    `D10_${fmt(s.d10)}`,
    `D30_${fmt(s.d30)}`,
    `RNG_${fmt(s.range)}`,
    `X_${s.crosses}`,
    `DMG_${fmt(s.damage)}`,
  ].join('_');

  return {
    type: 'BUY',
    marketSlug: currentMarket.slug,
    marketTitle: currentMarket.title,
    side,
    tokenId,
    price,
    elapsedSec: elapsed,
    quality: chosen.state.quality,
    reason,
    timestamp: Date.now(),
    state: chosen.state.id,
    path: pathState,
  };
}

// ----------------------------
// PAPER BROKER
// ----------------------------
function executePaperEntry(signal) {
  if (!signal || currentMarket.hasTraded || trade.active) return;

  syncPaperEquity();

  const requestedStake = Number(CONFIG.PAPER.stakeUsd);
  const availableCash = Number(stats.cashBalance);

  if (!Number.isFinite(availableCash) || availableCash <= 0) {
    console.log(`${colors.red}[BANKROLL] No paper cash left. Skipping entry.${colors.reset}`);
    return;
  }

  const stakeUsd = Math.min(requestedStake, availableCash);

  if (stakeUsd < CONFIG.PAPER.minStakeUsd) {
    console.log(`${colors.yellow}[BANKROLL] Cash $${availableCash.toFixed(2)} below min stake $${CONFIG.PAPER.minStakeUsd.toFixed(2)}. Skipping entry.${colors.reset}`);
    return;
  }

  const shares = stakeUsd / signal.price;
  const entryCost = signal.price * shares;
  const entryFee = entryCost * (CONFIG.PAPER.feeBps / 10000);
  const totalDebit = entryCost + entryFee;

  if (stats.cashBalance < totalDebit) {
    console.log(`${colors.yellow}[BANKROLL] Insufficient cash. Need $${totalDebit.toFixed(2)}, have $${stats.cashBalance.toFixed(2)}.${colors.reset}`);
    return;
  }

  // Cash leaves account at entry.
  stats.cashBalance -= totalDebit;

  trade = {
    active: true,
    marketSlug: signal.marketSlug,
    marketTitle: signal.marketTitle,
    side: signal.side,
    tokenId: signal.tokenId,
    entryPrice: signal.price,
    shares,
    stakeUsd,
    entryCost,
    entryFee,
    entryTime: signal.timestamp,
    entryElapsedSec: signal.elapsedSec,
    quality: signal.quality,
    reason: signal.reason,
    state: signal.state,
    pathSnapshot: signal.path,
    exitMode: 'HOLD_TO_SETTLEMENT',
    confirmedHold: false,
    confirmReason: null,
  };

  currentMarket.hasTraded = true;

  syncPaperEquity();

  console.log(`\n${colors.cyan}[PAPER ENTRY] BUY ${trade.side} @ ${fmt(trade.entryPrice)} | stake=$${money(entryCost)} | cash=$${money(stats.cashBalance)} | equity=$${money(stats.currentBalance)} | ${trade.reason}${colors.reset}`);

  writeTradeLog({
    event: 'ENTRY',
    side: trade.side,
    state: trade.state,
    entryPrice: trade.entryPrice,
    stakeUsd: trade.stakeUsd,
    shares: trade.shares,
    entryElapsedSec: trade.entryElapsedSec,
    quality: trade.quality,
    reason: trade.reason,
    pathSnapshot: trade.pathSnapshot,
  });
}

function getHeldBid(side = trade.side) {
  if (!side || !currentMarket.quote[side]) return null;

  const b = currentMarket.quote[side].bid;

  return b == null || Number.isNaN(Number(b)) ? null : Number(b);
}

function getHeldBidAtOrBefore(side, targetElapsedSec) {
  if (!side || !Array.isArray(currentMarket.priceTicks)) return null;

  for (let i = currentMarket.priceTicks.length - 1; i >= 0; i--) {
    const t = currentMarket.priceTicks[i];

    if (t.elapsedSec <= targetElapsedSec && t[side] && t[side].bid != null) {
      return Number(t[side].bid);
    }
  }

  return null;
}

function getMomentum30(side, nowElapsedSec = getElapsedSec()) {
  const nowBid = getHeldBid(side);
  const priorBid = getHeldBidAtOrBefore(side, nowElapsedSec - 30);

  if (nowBid == null || priorBid == null) return null;

  return nowBid - priorBid;
}

function markHoldConfirmed(reason) {
  if (!trade.active || trade.confirmedHold) return;

  trade.confirmedHold = true;
  trade.confirmReason = reason;
  currentMarket.holdConfirmed = true;
  currentMarket.holdConfirmReason = reason;

  console.log(`${colors.green}[HOLD CONFIRMED] ${reason} | ${trade.side} bid=${fmt(getHeldBid())} | elapsed=${getElapsedSec()}s${colors.reset}`);
}

function executePaperExit(reason, exitBid, momentum30 = null) {
  if (!trade.active) return;
  if (exitBid == null || Number.isNaN(Number(exitBid))) return;

  const elapsed = getElapsedSec();

  const grossReturn = Number(exitBid) * trade.shares;
  const exitFee = grossReturn * (CONFIG.PAPER.feeBps / 10000);

  // PnL is reporting only.
  const pnl = grossReturn - trade.entryCost - trade.entryFee - exitFee;

  // Cash receives sale proceeds. Do NOT add pnl; stake was already debited.
  stats.cashBalance += grossReturn - exitFee;
  stats.realisedPnl += pnl;

  stats.totalTrades += 1;
  if (pnl > 0) stats.wins += 1;
  else stats.losses += 1;

  const closed = { ...trade };

  trade = emptyTrade();
  syncPaperEquity();

  const c = pnl > 0 ? colors.green : colors.red;
  console.log(`\n${colors.yellow}[PAPER EXIT] ${reason} | Sell ${closed.side} bid=${fmt(exitBid)} | elapsed=${elapsed}s | PnL=${c}${pnl >= 0 ? '+' : ''}$${pnl.toFixed(4)}${colors.reset}`);

  writeTradeLog({
    event: 'EXIT',
    side: closed.side,
    state: closed.state,
    entryPrice: closed.entryPrice,
    exitValue: exitBid,
    stakeUsd: closed.stakeUsd,
    shares: closed.shares,
    entryElapsedSec: closed.entryElapsedSec,
    exitElapsedSec: elapsed,
    quality: closed.quality,
    reason: closed.reason,
    pnl,
    exitReason: reason,
    heldBid: exitBid,
    momentum30,
    pathSnapshot: closed.pathSnapshot,
  });

  recentTrades.unshift({
    time: new Date().toLocaleTimeString(),
    reason: `${reason} ${closed.side}`,
    entry: closed.entryPrice.toFixed(3),
    exit: Number(exitBid).toFixed(3),
    pnl,
  });

  if (recentTrades.length > 10) recentTrades.pop();
}

function manageOpenTrade() {
  if (!CONFIG.EXIT.enabled || !trade.active || !currentMarket.slug) return;

  const elapsed = getElapsedSec();
  const heldBid = getHeldBid();

  if (heldBid == null) return;

  const momentum30 = getMomentum30(trade.side, elapsed);

  if (elapsed >= 180 && heldBid >= CONFIG.EXIT.confirmAt180Bid) {
    markHoldConfirmed(`CONFIRM_180_BID_GE_${Math.round(CONFIG.EXIT.confirmAt180Bid * 100)}C`);
  }

  if (elapsed >= 210 && heldBid >= CONFIG.EXIT.confirmAt210Bid) {
    markHoldConfirmed(`CONFIRM_210_BID_GE_${Math.round(CONFIG.EXIT.confirmAt210Bid * 100)}C`);
  }

  if (
    elapsed >= 240 &&
    heldBid >= CONFIG.EXIT.confirmAt240Bid &&
    momentum30 != null &&
    momentum30 < CONFIG.EXIT.confirmAt240MaxMomentum30
  ) {
    markHoldConfirmed(`CONFIRM_240_BID_GE_${Math.round(CONFIG.EXIT.confirmAt240Bid * 100)}C_MOM30_LT_${Math.round(CONFIG.EXIT.confirmAt240MaxMomentum30 * 100)}C`);
  }

  if (trade.confirmedHold) return;

  if (elapsed >= 180 && CONFIG.EXIT.killAt180IfBidBelowEntry && heldBid < trade.entryPrice) {
    return executePaperExit('TIME_KILL_180_BID_BELOW_ENTRY', heldBid, momentum30);
  }

  if (elapsed >= 180 && CONFIG.EXIT.killAt180IfBidBelow > 0 && heldBid < CONFIG.EXIT.killAt180IfBidBelow) {
    return executePaperExit(`TIME_KILL_180_BID_LT_${Math.round(CONFIG.EXIT.killAt180IfBidBelow * 100)}C`, heldBid, momentum30);
  }

  if (elapsed >= 240 && heldBid < CONFIG.EXIT.killAt240IfBidBelow) {
    return executePaperExit(`TIME_KILL_240_BID_LT_${Math.round(CONFIG.EXIT.killAt240IfBidBelow * 100)}C`, heldBid, momentum30);
  }

  if (elapsed >= 270 && heldBid < CONFIG.EXIT.killAt270IfBidBelow) {
    return executePaperExit(`TIME_KILL_270_BID_LT_${Math.round(CONFIG.EXIT.killAt270IfBidBelow * 100)}C`, heldBid, momentum30);
  }
}

function settleIfNeeded() {
  if (!currentMarket.slug || currentMarket.settled) return;
  if (Date.now() < currentMarket.endTimeMs) return;

  currentMarket.settled = true;

  if (!trade.active) {
    console.log(`${colors.gray}[SETTLEMENT] No trade for ${currentMarket.slug}.${colors.reset}`);
    syncPaperEquity();
    return;
  }

  const winningSide = inferWinnerFromFinalQuotes();

  if (!winningSide) {
    return handleUnresolvedSettlement('UNRESOLVED_FINAL_QUOTES');
  }

  const closed = { ...trade };
  const exitValue = closed.side === winningSide ? 1 : 0;

  const grossReturn = exitValue * closed.shares;
  const exitFee = grossReturn * (CONFIG.PAPER.feeBps / 10000);
  const pnl = grossReturn - closed.entryCost - closed.entryFee - exitFee;

  stats.cashBalance += grossReturn - exitFee;
  stats.realisedPnl += pnl;

  stats.totalTrades += 1;
  if (pnl > 0) stats.wins += 1;
  else stats.losses += 1;

  trade = emptyTrade();
  syncPaperEquity();

  const c = pnl > 0 ? colors.green : colors.red;

  console.log(`\n${colors.gray}========================================${colors.reset}`);
  console.log(`[PAPER SETTLEMENT] Winner: ${winningSide} | Held: ${closed.side}`);
  console.log(`Entry: $${fmt(closed.entryPrice)} | Exit value: $${exitValue.toFixed(2)}`);
  console.log(`PnL: ${c}${pnl >= 0 ? '+' : ''}$${pnl.toFixed(4)}${colors.reset}`);
  console.log(`[STATS] Cash: $${money(stats.cashBalance)} | Equity: $${money(stats.currentBalance)} | Win rate: ${winRateText()} (${stats.wins}W / ${stats.losses}L / ${stats.unresolved}U)`);
  console.log(`${colors.gray}========================================\n${colors.reset}`);

  writeTradeLog({
    event: 'SETTLEMENT',
    side: closed.side,
    state: closed.state,
    entryPrice: closed.entryPrice,
    exitValue,
    stakeUsd: closed.stakeUsd,
    shares: closed.shares,
    entryElapsedSec: closed.entryElapsedSec,
    exitElapsedSec: getElapsedSec(),
    quality: closed.quality,
    reason: closed.reason,
    pnl,
    exitReason: `WINNER_${winningSide}`,
    heldBid: '',
    momentum30: '',
    pathSnapshot: closed.pathSnapshot,
  });

  recentTrades.unshift({
    time: new Date().toLocaleTimeString(),
    reason: `${closed.reason} ${closed.side} ${closed.quality}`,
    entry: closed.entryPrice.toFixed(3),
    exit: exitValue.toFixed(3),
    pnl,
  });

  if (recentTrades.length > 10) recentTrades.pop();
}

function handleUnresolvedSettlement(reason) {
  const closed = { ...trade };

  // Refund entry cost. Do not count this as win/loss. This prevents bad settlement inference from corrupting paper data.
  stats.cashBalance += closed.entryCost;
  stats.unresolved += 1;

  trade = emptyTrade();
  syncPaperEquity();

  console.log(`${colors.yellow}[SETTLEMENT] ${reason}. Refunding stake and excluding from W/L.${colors.reset}`);

  writeTradeLog({
    event: 'UNRESOLVED',
    side: closed.side,
    state: closed.state,
    entryPrice: closed.entryPrice,
    exitValue: '',
    stakeUsd: closed.stakeUsd,
    shares: closed.shares,
    entryElapsedSec: closed.entryElapsedSec,
    exitElapsedSec: getElapsedSec(),
    quality: closed.quality,
    reason: closed.reason,
    pnl: 0,
    exitReason: reason,
    pathSnapshot: closed.pathSnapshot,
  });
}

function inferWinnerFromFinalQuotes() {
  const yesAsk = currentMarket.quote.YES.ask;
  const noAsk = currentMarket.quote.NO.ask;

  if (yesAsk == null || noAsk == null) return null;

  const y = Number(yesAsk);
  const n = Number(noAsk);

  if (!Number.isFinite(y) || !Number.isFinite(n)) return null;

  if (CONFIG.PAPER.strictSettlement) {
    if (y >= CONFIG.PAPER.settlementWinAsk && n <= CONFIG.PAPER.settlementLoseAsk) return 'YES';
    if (n >= CONFIG.PAPER.settlementWinAsk && y <= CONFIG.PAPER.settlementLoseAsk) return 'NO';
    return null;
  }

  if (y === n) return null;
  return y > n ? 'YES' : 'NO';
}

// ----------------------------
// DASHBOARD SERVER
// ----------------------------
http.createServer((req, res) => {
  if (req.url === '/api/live') {
    res.writeHead(200, { 'Content-Type': 'application/json' });

    syncPaperEquity();

    const pathState = getPathState(getElapsedSec());

    res.end(JSON.stringify({
      stats,
      trade,
      currentPrices,
      recentTrades,
      pathState,
      market: {
        slug: currentMarket.slug,
        title: currentMarket.title,
        elapsedSec: getElapsedSec(),
        secondsLeft: getSecondsLeft(),
        strategy: CONFIG.STRATEGY,
        exit: CONFIG.EXIT,
      },
    }));

    return;
  }

  fs.readFile(path.join(__dirname, CONFIG.FILES.dashboard), 'utf8', (err, data) => {
    if (err) {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Error loading dashboard UI. Make sure dashboard.html exists.');
      return;
    }

    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(data);
  });
}).listen(CONFIG.SERVER.port, CONFIG.SERVER.host, () => {
  console.log(`${colors.cyan}[DASHBOARD] Web UI running on port ${CONFIG.SERVER.port}${colors.reset}`);
});

// ----------------------------
// MAIN LOOP
// ----------------------------
async function run() {
  console.log(`${colors.magenta}Booting BTC 5m Latent State Paper Engine V2...${colors.reset}`);
  console.log(`${colors.gray}No PRIVATE_KEY required. This file does paper trading only.${colors.reset}`);

  console.log(
    `${colors.gray}Strategy: ${CONFIG.STRATEGY.name} | maxSpread<=${CONFIG.STRATEGY.maxAllowedSpread} | requireBid=${CONFIG.STRATEGY.requireBid} | conservativeOnly=${CONFIG.STRATEGY.conservativeOnly} | states=${CONFIG.STRATEGY.states.filter(r => r.enabled).map(r => r.id).join(',')}.${colors.reset}`
  );

  console.log(
    `${colors.gray}Paper bankroll: start=$${CONFIG.PAPER.startingBalance} | stake=$${CONFIG.PAPER.stakeUsd} | minStake=$${CONFIG.PAPER.minStakeUsd} | cash/equity accounting=ON | strictSettlement=${CONFIG.PAPER.strictSettlement}.${colors.reset}`
  );

  console.log(
    `${colors.gray}Exit layer: ${CONFIG.EXIT.enabled ? 'ON' : 'OFF'} | kill180BelowEntry=${CONFIG.EXIT.killAt180IfBidBelowEntry} | kill240Bid<${CONFIG.EXIT.killAt240IfBidBelow}.${colors.reset}`
  );

  await loadCurrentMarket();

  setInterval(async () => {
    const now = Date.now();

    if (!currentMarket.slug) {
      if (now > searchCooldownUntil) await loadCurrentMarket();
      return;
    }

    syncPaperEquity();
    manageOpenTrade();
    settleIfNeeded();

    if (now >= currentMarket.endTimeMs + 1000 && !isSearchingNextMarket) {
      await loadCurrentMarket();
      return;
    }

    const nowSec = Math.floor(now / 1000);

    if (nowSec % 10 === 0 && nowSec !== lastStatusPrintSec) {
      lastStatusPrintSec = nowSec;

      syncPaperEquity();

      const status = trade.active
        ? `HOLDING ${trade.side} @ $${fmt(trade.entryPrice)} (${trade.quality})`
        : 'AWAITING_LATENT_STATE';

      const statusColor = trade.active ? colors.cyan : colors.gray;
      const yesMid = getYesMidNow();
      const pathState = getPathState(getElapsedSec());

      console.log(
        `${statusColor}[LIVE] ${status} | ${currentMarket.slug} | elapsed=${getElapsedSec()}s | left=${getSecondsLeft()}s | YES=${fmt(currentMarket.quote.YES.ask)} | NO=${fmt(currentMarket.quote.NO.ask)} | MID=${fmt(yesMid)} | D10=${fmt(pathState && pathState.d10)} | D30=${fmt(pathState && pathState.d30)} | RNG=${fmt(pathState && pathState.range)} | X=${pathState ? pathState.crosses : ''} | Cash=$${money(stats.cashBalance)} | Equity=$${money(stats.currentBalance)} | Open=$${money(stats.openPositionValue)}${colors.reset}`
      );
    }
  }, 1000);
}

process.on('SIGINT', () => {
  console.log(`\n${colors.yellow}[SHUTDOWN] Closing streams...${colors.reset}`);

  try {
    if (wsMarket) wsMarket.terminate();
  } catch (_) {}

  syncPaperEquity();

  tradeStream.end();
  priceStream.end();
  terminalStream.end();

  process.exit(0);
});

run().catch((err) => {
  console.error(`${colors.red}[FATAL]${colors.reset}`, err);
  process.exit(1);
});