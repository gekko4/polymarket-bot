require('dotenv').config();

const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const http = require('http');

// ============================================================
// BTC 5m Polymarket PAPER BOT — Empirical Pattern Strategy
// Single-file version to keep your repo simple/private.
//
// Strategy from the price-log analysis:
// - BTC 5m binary markets only
// - One trade per market
// - Old FIRST_TOUCH_34 logic removed
// - Uses time × YES mid × recent trend interaction rules
// - Default test mode holds to inferred settlement
// - Old exit layer is OFF by default
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
    // Confirmed Direction V3 — based on the full 492k price-log interaction analysis.
    maxAllowedSpread: Number(process.env.MAX_ALLOWED_SPREAD || 0.03),
    requireBid: String(process.env.REQUIRE_BID || 'true').toLowerCase() === 'true',
    oneTradePerMarket: true,

    rules: [
      {
        id: 'YES_EARLY_FAV_70_80',
        enabled: String(process.env.ENABLE_YES_EARLY_FAV_70_80 || 'true').toLowerCase() === 'true',
        side: 'YES',
        elapsedMin: 31,
        elapsedMax: 60,
        yesMidMin: 0.70,
        yesMidMax: 0.80,
        maxAsk: Number(process.env.YES_EARLY_FAV_MAX_ASK || 0.82),
        quality: 'A_YES_EARLY_FAVOURITE_CONTINUATION',
      },
      {
        id: 'YES_EARLY_PULLBACK_60_70',
        enabled: String(process.env.ENABLE_YES_EARLY_PULLBACK_60_70 || 'true').toLowerCase() === 'true',
        side: 'YES',
        elapsedMin: 31,
        elapsedMax: 60,
        yesMidMin: 0.60,
        yesMidMax: 0.70,
        trend5Min: -0.05,
        trend5Max: -0.02,
        maxAsk: Number(process.env.YES_EARLY_PULLBACK_MAX_ASK || 0.67),
        quality: 'A_YES_EARLY_PULLBACK_CHEAP',
      },
      {
        id: 'NO_90_110_CONFIRMED_DRIFT',
        enabled: String(process.env.ENABLE_NO_90_110_CONFIRMED_DRIFT || 'true').toLowerCase() === 'true',
        side: 'NO',
        elapsedMin: 96,
        elapsedMax: 110,
        yesMidMin: 0.30,
        yesMidMax: 0.40,
        trend5Min: -0.02,
        trend5Max: -0.005,
        trend15Min: -0.12,
        trend15Max: -0.05,
        maxAsk: Number(process.env.NO_90_110_CONFIRMED_MAX_ASK || 0.68),
        quality: 'A_NO_90_110_CONFIRMED_DRIFT',
      },
      {
        id: 'NO_EARLY_CRUSH_PERSIST_50_60',
        enabled: String(process.env.ENABLE_NO_EARLY_CRUSH_PERSIST_50_60 || 'true').toLowerCase() === 'true',
        side: 'NO',
        elapsedMin: 50,
        elapsedMax: 60,
        yesMidMin: 0.10,
        yesMidMax: 0.20,
        maxAsk: Number(process.env.NO_EARLY_CRUSH_MAX_ASK || 0.85),
        quality: 'A_NO_EARLY_CRUSH_PERSISTENCE',
      },
      {
        id: 'NO_MIDGAME_WEAK_YES_DOWNTREND',
        enabled: String(process.env.ENABLE_NO_MIDGAME_WEAK_YES_DOWNTREND || 'true').toLowerCase() === 'true',
        side: 'NO',
        elapsedMin: 181,
        elapsedMax: 210,
        yesMidMin: 0.20,
        yesMidMax: 0.30,
        trend15Min: -0.08,
        trend15Max: -0.03,
        maxAsk: Number(process.env.NO_MIDGAME_WEAK_MAX_ASK || 0.78),
        quality: 'A_NO_MIDGAME_TREND_CONFIRMATION',
      },
      {
        id: 'YES_LATE_SOFT_FAV_55_60',
        enabled: String(process.env.ENABLE_YES_LATE_SOFT_FAV_55_60 || 'true').toLowerCase() === 'true',
        side: 'YES',
        elapsedMin: 211,
        elapsedMax: 240,
        yesMidMin: 0.55,
        yesMidMax: 0.60,
        maxAsk: Number(process.env.YES_LATE_SOFT_FAV_MAX_ASK || 0.62),
        quality: 'B_YES_LATE_SOFT_FAVOURITE',
      },
    ],
  },
  
  EXIT: {
    // Off by default because the tested patterns were entry-to-settlement.
    enabled: String(process.env.EXIT_ENABLED || 'false').toLowerCase() === 'true',
    killAt180IfBidBelowEntry: String(process.env.KILL_180_BID_BELOW_ENTRY || 'true').toLowerCase() === 'true',
    killAt180IfBidBelow: Number(process.env.KILL_180_BID_BELOW || 0),
    killAt240IfBidBelow: Number(process.env.KILL_240_BID_BELOW || 0.34),
    killAt270IfBidBelow: Number(process.env.KILL_270_BID_BELOW || 0.34),
    confirmAt180Bid: Number(process.env.CONFIRM_180_BID || 0.85),
    confirmAt210Bid: Number(process.env.CONFIRM_210_BID || 0.90),
    confirmAt240Bid: Number(process.env.CONFIRM_240_BID || 0.65),
    confirmAt240MaxMomentum30: Number(process.env.CONFIRM_240_MAX_MOM30 || 0.05),
    requireBidForExit: String(process.env.REQUIRE_BID_FOR_EXIT || 'true').toLowerCase() === 'true',
  },

  PAPER: {
    startingBalance: Number(process.env.STARTING_BALANCE || 100),
    stakeUsd: Number(process.env.STAKE_USD || 1),
    feeBps: Number(process.env.FEE_BPS || 0),
    assumeFillOnAskTouch: true,
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
  startingBalance: CONFIG.PAPER.startingBalance,
  currentBalance: CONFIG.PAPER.startingBalance,
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
    entryCost: 0,
    entryTime: 0,
    entryElapsedSec: 0,
    quality: null,
    reason: null,
    exitMode: 'HOLD_TO_SETTLEMENT',
    confirmedHold: false,
    confirmReason: null,
  };
}

// ----------------------------
// LOGGING
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
  tradeStream.write(
    'Timestamp,MarketSlug,Event,Side,EntryPrice,ExitValue,Shares,EntryElapsedSec,Quality,Reason,PnL_USD,Balance_USD,WinRate_Pct,ExitElapsedSec,ExitReason,HeldBid,Momentum30\n'
  );
}

if (!fs.existsSync(CONFIG.FILES.prices) || fs.statSync(CONFIG.FILES.prices).size === 0) {
  priceStream.write('Timestamp,MarketSlug,ElapsedSec,YES_Ask,YES_Bid,NO_Ask,NO_Bid\n');
}

function logPriceTick() {
  if (!currentMarket.slug) return;

  const nowSec = Math.floor(Date.now() / 1000);
  if (nowSec === lastPriceLogSec) return;
  lastPriceLogSec = nowSec;

  const y = currentMarket.quote.YES;
  const n = currentMarket.quote.NO;
  if (y.ask == null || y.bid == null || n.ask == null || n.bid == null) return;

  const tick = {
    elapsedSec: getElapsedSec(),
    YES: { ask: y.ask, bid: y.bid },
    NO: { ask: n.ask, bid: n.bid },
  };

  currentMarket.priceTicks.push(tick);
  if (currentMarket.priceTicks.length > 420) currentMarket.priceTicks.shift();

  priceStream.write([
    new Date().toISOString(),
    currentMarket.slug,
    getElapsedSec(),
    fmt(y.ask),
    fmt(y.bid),
    fmt(n.ask),
    fmt(n.bid),
  ].join(',') + '\n');
}

function fmt(x) {
  if (x == null || Number.isNaN(Number(x))) return '';
  return Number(x).toFixed(3);
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
  return { startSec, endSec, slug: `${CONFIG.MARKET.slugPrefix}-${startSec}` };
}

// ----------------------------
// POLYMARKET MARKET SCANNER
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
      console.log(`${colors.gray}[SCANNER] Market not indexed yet. Will retry.${colors.reset}`);
      searchCooldownUntil = Date.now() + 5000;
      return;
    }

    const event = events[0];
    const market = event.markets[0];

    const parsedTokens = typeof market.clobTokenIds === 'string'
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
    try { wsMarket.terminate(); } catch (_) {}
  }

  console.log(`${colors.yellow}[WS] Connecting market stream...${colors.reset}`);
  wsMarket = new WebSocket(CONFIG.MARKET.clobWsUrl);

  wsMarket.on('open', () => {
    if (!currentMarket.yesToken || !currentMarket.noToken) return;

    const payload = {
      type: 'market',
      assets_ids: [currentMarket.yesToken, currentMarket.noToken],
    };

    wsMarket.send(JSON.stringify(payload));
    console.log(`${colors.green}[WS] Subscribed to YES/NO books.${colors.reset}`);
  });

  wsMarket.on('message', (msg) => {
    const text = msg.toString();
    if (text === 'PONG') return;

    try {
      const data = JSON.parse(text);
      handleWsMessage(data);
    } catch (_) {
      // Ignore malformed heartbeat-ish messages
    }
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
// STRATEGY + PAPER BROKER
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

  const signal = evaluatePatternStrategy();
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

function inRangeInclusive(value, min, max) {
  if (value == null || Number.isNaN(Number(value))) return false;
  return Number(value) >= min && Number(value) <= max;
}

function ruleMatches(rule, ctx) {
  if (!rule.enabled) return false;

  if (!inRangeInclusive(ctx.elapsed, rule.elapsedMin, rule.elapsedMax)) return false;
  if (!inRangeInclusive(ctx.yesMid, rule.yesMidMin, rule.yesMidMax)) return false;

  const ask = rule.side === 'YES' ? ctx.yesAsk : ctx.noAsk;
  const bid = rule.side === 'YES' ? ctx.yesBid : ctx.noBid;

  if (ask == null || ask > rule.maxAsk) return false;
  if (!spreadOk(rule.side)) return false;

  if (CONFIG.STRATEGY.requireBid && (bid == null || bid <= 0)) return false;

  if (rule.trend5Min != null && !inRangeInclusive(ctx.trend5, rule.trend5Min, rule.trend5Max)) {
    return false;
  }

  if (rule.trend15Min != null && !inRangeInclusive(ctx.trend15, rule.trend15Min, rule.trend15Max)) {
    return false;
  }

  return true;
}

function evaluatePatternStrategy() {
  if (!currentMarket.slug || currentMarket.hasTraded || trade.active) return null;

  const elapsed = getElapsedSec();

  const yesAsk = getSideAsk('YES');
  const yesBid = getSideBid('YES');
  const noAsk = getSideAsk('NO');
  const noBid = getSideBid('NO');
  const yesMid = getYesMidNow();

  if (yesAsk == null || yesBid == null || noAsk == null || noBid == null || yesMid == null) {
    return null;
  }

  const ctx = {
    elapsed,
    yesAsk,
    yesBid,
    noAsk,
    noBid,
    yesMid,
    trend5: getYesTrend(5, elapsed),
    trend15: getYesTrend(15, elapsed),
  };

  const matches = CONFIG.STRATEGY.rules.filter((rule) => ruleMatches(rule, ctx));

  if (!matches.length) return null;

  // If more than one setup is live, prefer the earlier rule in CONFIG.STRATEGY.rules.
  // The order is deliberately the stricter/highest-confidence shortlist first.
  const rule = matches[0];

  const price = rule.side === 'YES' ? yesAsk : noAsk;
  const tokenId = rule.side === 'YES' ? currentMarket.yesToken : currentMarket.noToken;

  const trendBits = [];
  if (ctx.trend5 != null) trendBits.push(`T5=${fmt(ctx.trend5)}`);
  if (ctx.trend15 != null) trendBits.push(`T15=${fmt(ctx.trend15)}`);

  return {
    type: 'BUY',
    marketSlug: currentMarket.slug,
    marketTitle: currentMarket.title,
    side: rule.side,
    tokenId,
    price,
    elapsedSec: elapsed,
    quality: rule.quality,
    reason: `${rule.id}_MID_${fmt(yesMid)}_${trendBits.join('_')}`,
    timestamp: Date.now(),
  };
}

function executePaperEntry(signal) {
  if (!signal || currentMarket.hasTraded || trade.active) return;

  const shares = CONFIG.PAPER.stakeUsd / signal.price;
  const entryCost = signal.price * shares;

  trade = {
    active: true,
    marketSlug: signal.marketSlug,
    marketTitle: signal.marketTitle,
    side: signal.side,
    tokenId: signal.tokenId,
    entryPrice: signal.price,
    shares,
    entryCost,
    entryTime: signal.timestamp,
    entryElapsedSec: signal.elapsedSec,
    quality: signal.quality,
    reason: signal.reason,
    exitMode: 'HOLD_TO_SETTLEMENT',
    confirmedHold: false,
    confirmReason: null,
  };

  currentMarket.hasTraded = true;

  console.log(
    `\n${colors.cyan}[PAPER ENTRY] BUY ${trade.side} @ ${fmt(trade.entryPrice)} | ${trade.reason} | elapsed=${trade.entryElapsedSec}s | ${trade.quality}${colors.reset}`
  );

  const winRate = stats.totalTrades > 0 ? ((stats.wins / stats.totalTrades) * 100).toFixed(1) : '0.0';

  tradeStream.write([
    new Date().toISOString(),
    trade.marketSlug,
    'ENTRY',
    trade.side,
    fmt(trade.entryPrice),
    '',
    trade.shares.toFixed(4),
    trade.entryElapsedSec,
    trade.quality,
    trade.reason,
    '',
    stats.currentBalance.toFixed(2),
    `${winRate}%`,
    '',
    '',
    '',
    '',
  ].join(',') + '\n');
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

  console.log(
    `${colors.green}[HOLD CONFIRMED] ${reason} | ${trade.side} bid=${fmt(getHeldBid())} | elapsed=${getElapsedSec()}s${colors.reset}`
  );
}

function executePaperExit(reason, exitBid, momentum30 = null) {
  if (!trade.active) return;
  if (exitBid == null || Number.isNaN(Number(exitBid))) return;

  const elapsed = getElapsedSec();

  const grossReturn = Number(exitBid) * trade.shares;
  const entryFee = trade.entryCost * (CONFIG.PAPER.feeBps / 10000);
  const exitFee = grossReturn * (CONFIG.PAPER.feeBps / 10000);
  const pnl = grossReturn - trade.entryCost - entryFee - exitFee;

  stats.totalTrades += 1;
  if (pnl > 0) stats.wins += 1;
  else stats.losses += 1;

  stats.currentBalance += pnl;

  const winRate = ((stats.wins / stats.totalTrades) * 100).toFixed(1);
  const c = pnl > 0 ? colors.green : colors.red;

  console.log(
    `\n${colors.yellow}[PAPER EXIT] ${reason} | Sell ${trade.side} bid=${fmt(exitBid)} | elapsed=${elapsed}s | PnL=${c}${pnl >= 0 ? '+' : ''}$${pnl.toFixed(4)}${colors.reset}`
  );

  tradeStream.write([
    new Date().toISOString(),
    trade.marketSlug,
    'EXIT',
    trade.side,
    fmt(trade.entryPrice),
    fmt(exitBid),
    trade.shares.toFixed(4),
    trade.entryElapsedSec,
    trade.quality,
    trade.reason,
    pnl.toFixed(4),
    stats.currentBalance.toFixed(2),
    `${winRate}%`,
    elapsed,
    reason,
    fmt(exitBid),
    momentum30 == null ? '' : fmt(momentum30),
  ].join(',') + '\n');

  recentTrades.unshift({
    time: new Date().toLocaleTimeString(),
    reason: `${reason} ${trade.side}`,
    entry: trade.entryPrice.toFixed(3),
    exit: Number(exitBid).toFixed(3),
    pnl,
  });

  if (recentTrades.length > 10) recentTrades.pop();

  trade = emptyTrade();
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
    markHoldConfirmed(
      `CONFIRM_240_BID_GE_${Math.round(CONFIG.EXIT.confirmAt240Bid * 100)}C_MOM30_LT_${Math.round(CONFIG.EXIT.confirmAt240MaxMomentum30 * 100)}C`
    );
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
    return;
  }

  const winningSide = inferWinnerFromFinalQuotes();

  if (!winningSide) {
    console.log(`${colors.yellow}[SETTLEMENT] Could not infer winner. Leaving trade unsettled in logs.${colors.reset}`);
    return;
  }

  const exitValue = trade.side === winningSide ? 1 : 0;
  const grossReturn = exitValue * trade.shares;
  const entryFee = trade.entryCost * (CONFIG.PAPER.feeBps / 10000);
  const exitFee = grossReturn * (CONFIG.PAPER.feeBps / 10000);
  const pnl = grossReturn - trade.entryCost - entryFee - exitFee;

  stats.totalTrades += 1;
  if (pnl > 0) stats.wins += 1;
  else stats.losses += 1;

  stats.currentBalance += pnl;

  const winRate = ((stats.wins / stats.totalTrades) * 100).toFixed(1);
  const c = pnl > 0 ? colors.green : colors.red;

  console.log(`\n${colors.gray}========================================${colors.reset}`);
  console.log(`[PAPER SETTLEMENT] Winner: ${winningSide} | Held: ${trade.side}`);
  console.log(`Entry: $${fmt(trade.entryPrice)} | Exit value: $${exitValue.toFixed(2)}`);
  console.log(`PnL: ${c}${pnl >= 0 ? '+' : ''}$${pnl.toFixed(4)}${colors.reset}`);
  console.log(`[STATS] Balance: $${stats.currentBalance.toFixed(2)} | Win rate: ${winRate}% (${stats.wins}W / ${stats.losses}L)`);
  console.log(`${colors.gray}========================================\n${colors.reset}`);

  tradeStream.write([
    new Date().toISOString(),
    trade.marketSlug,
    'SETTLEMENT',
    trade.side,
    fmt(trade.entryPrice),
    exitValue.toFixed(2),
    trade.shares.toFixed(4),
    trade.entryElapsedSec,
    trade.quality,
    trade.reason,
    pnl.toFixed(4),
    stats.currentBalance.toFixed(2),
    `${winRate}%`,
    '',
    '',
    '',
    '',
  ].join(',') + '\n');

  recentTrades.unshift({
    time: new Date().toLocaleTimeString(),
    reason: `${trade.reason} ${trade.side} ${trade.quality}`,
    entry: trade.entryPrice.toFixed(3),
    exit: exitValue.toFixed(3),
    pnl,
  });

  if (recentTrades.length > 10) recentTrades.pop();

  trade = emptyTrade();
}

function inferWinnerFromFinalQuotes() {
  const yesAsk = currentMarket.quote.YES.ask;
  const noAsk = currentMarket.quote.NO.ask;

  if (yesAsk == null || noAsk == null) return null;
  if (yesAsk === noAsk) return null;

  // Near settlement, the winning side usually trades near 1 and the losing side near 0.
  return yesAsk > noAsk ? 'YES' : 'NO';
}

// ----------------------------
// DASHBOARD SERVER
// ----------------------------
http.createServer((req, res) => {
  if (req.url === '/api/live') {
    res.writeHead(200, { 'Content-Type': 'application/json' });

    res.end(JSON.stringify({
      stats,
      trade,
      currentPrices,
      recentTrades,
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
  console.log(`${colors.magenta}Booting BTC 5m empirical-pattern PAPER BOT...${colors.reset}`);
  console.log(`${colors.gray}No PRIVATE_KEY required. This file does paper trading only.${colors.reset}`);

  console.log(
    `${colors.gray}Strategy: empirical pattern rules ON | maxSpread<=${CONFIG.STRATEGY.maxAllowedSpread} | requireBid=${CONFIG.STRATEGY.requireBid} | rules=${CONFIG.STRATEGY.rules.filter(r => r.enabled).map(r => r.id).join(',')}.${colors.reset}`
  );

  console.log(
    `${colors.gray}Exit layer: ${CONFIG.EXIT.enabled ? 'ON' : 'OFF'} | kill180BelowEntry=${CONFIG.EXIT.killAt180IfBidBelowEntry} | kill240Bid<${CONFIG.EXIT.killAt240IfBidBelow} | confirmations 180>=${CONFIG.EXIT.confirmAt180Bid}, 210>=${CONFIG.EXIT.confirmAt210Bid}, 240>=${CONFIG.EXIT.confirmAt240Bid}+flat30.${colors.reset}`
  );

  await loadCurrentMarket();

  setInterval(async () => {
    const now = Date.now();

    if (!currentMarket.slug) {
      if (now > searchCooldownUntil) await loadCurrentMarket();
      return;
    }

    manageOpenTrade();
    settleIfNeeded();

    // Move to next market after expiry.
    if (now >= currentMarket.endTimeMs + 1000 && !isSearchingNextMarket) {
      await loadCurrentMarket();
      return;
    }

    const nowSec = Math.floor(now / 1000);

    if (nowSec % 10 === 0 && nowSec !== lastStatusPrintSec) {
      lastStatusPrintSec = nowSec;

      const status = trade.active
        ? `HOLDING ${trade.side} @ $${fmt(trade.entryPrice)} (${trade.quality})`
        : 'AWAITING_PATTERN_SETUP';

      const statusColor = trade.active ? colors.cyan : colors.gray;

      const yesMid = getYesMidNow();
      const trend5 = getYesTrend(5);
      const trend15 = getYesTrend(15);

      console.log(
        `${statusColor}[LIVE] ${status} | ${currentMarket.slug} | elapsed=${getElapsedSec()}s | left=${getSecondsLeft()}s | YES=${fmt(currentMarket.quote.YES.ask)} | NO=${fmt(currentMarket.quote.NO.ask)} | YES_mid=${fmt(yesMid)} | T5=${fmt(trend5)} | T15=${fmt(trend15)} | Bal=$${stats.currentBalance.toFixed(2)}${colors.reset}`
      );
    }
  }, 1000);
}

process.on('SIGINT', () => {
  console.log(`\n${colors.yellow}[SHUTDOWN] Closing streams...${colors.reset}`);

  try { if (wsMarket) wsMarket.terminate(); } catch (_) {}

  tradeStream.end();
  priceStream.end();
  terminalStream.end();

  process.exit(0);
});

run().catch((err) => {
  console.error(`${colors.red}[FATAL]${colors.reset}`, err);
  process.exit(1);
});