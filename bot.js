require('dotenv').config();

const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const http = require('http');

// ============================================================
// BTC 5m Polymarket PAPER BOT — Surface Edge V4
//
// Scraps old YES-mid-only rules.
// Implements price-history findings as side-normalized surfaces:
//
// 1. Pullback with intact structure
// 2. Loose continuation with sufficient confirmation
// 3. Expensive collapse persistence
// 4. Stable dominance, flat derivative
// 5. Midgame underpriced stable side
// 6. Late soft favorite non-takeover
//
// Still paper-only.
// Still one trade per market.
// Dashboard API remains compatible.
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

const CONFIG = {
  MARKET: {
    intervalSec: 300,
    slugPrefix: 'btc-updown-5m',
    gammaEventsUrl: 'https://gamma-api.polymarket.com/events',
    clobWsUrl: 'wss://ws-subscriptions-clob.polymarket.com/ws/market',
  },

  STRATEGY: {
    name: 'Surface Edge V4',
    oneTradePerMarket: true,

    // Global liquidity guard. Individual surfaces can be stricter.
    maxAllowedSpread: Number(process.env.MAX_ALLOWED_SPREAD || 0.05),
    requireBid: String(process.env.REQUIRE_BID || 'true').toLowerCase() === 'true',

    // If true, prints why the best near-miss failed every 10s status cycle.
    debugSurface: String(process.env.DEBUG_SURFACE || 'false').toLowerCase() === 'true',

    // Order matters. Earlier surfaces get priority if multiple match.
    surfaces: [
      {
        id: 'PULLBACK_INTACT_TREND',
        enabled: envBool('ENABLE_PULLBACK_INTACT_TREND', true),
        priority: 10,
        quality: 'A_PULLBACK_INTACT_STRUCTURE',
        description: 'v5 pullback, v15 intact, lead intact, tight spread',
        elapsedMin: envNum('PULLBACK_ELAPSED_MIN', 30),
        elapsedMax: envNum('PULLBACK_ELAPSED_MAX', 210),
        askMin: envNum('PULLBACK_ASK_MIN', 0.60),
        askMax: envNum('PULLBACK_ASK_MAX', 0.75),
        spreadMax: envNum('PULLBACK_SPREAD_MAX', 0.03),
        v5Min: envNum('PULLBACK_V5_MIN', -0.06),
        v5Max: envNum('PULLBACK_V5_MAX', -0.01),
        v15Min: envNum('PULLBACK_V15_MIN', 0.00),
        leadMin: envNum('PULLBACK_LEAD_MIN', 0.15),
        maxOppBidMinusBid: envNum('PULLBACK_MAX_OPP_BID_MINUS_BID', 0.05),
      },

      {
        id: 'STABLE_DOMINANCE_70_80',
        enabled: envBool('ENABLE_STABLE_DOMINANCE_70_80', true),
        priority: 20,
        quality: 'A_STABLE_DOMINANCE',
        description: '0.70-0.80 side, tight spread, positive lead, flat/non-reversing derivative',
        elapsedMin: envNum('STABLE_DOM_ELAPSED_MIN', 90),
        elapsedMax: envNum('STABLE_DOM_ELAPSED_MAX', 180),
        askMin: envNum('STABLE_DOM_ASK_MIN', 0.70),
        askMax: envNum('STABLE_DOM_ASK_MAX', 0.80),
        spreadMin: envNum('STABLE_DOM_SPREAD_MIN', 0.01),
        spreadMax: envNum('STABLE_DOM_SPREAD_MAX', 0.02),
        v5Min: envNum('STABLE_DOM_V5_MIN', -0.015),
        v5Max: envNum('STABLE_DOM_V5_MAX', 0.015),
        v15Min: envNum('STABLE_DOM_V15_MIN', -0.03),
        v15Max: envNum('STABLE_DOM_V15_MAX', 0.03),
        leadMin: envNum('STABLE_DOM_LEAD_MIN', 0.25),
        leadMax: envNum('STABLE_DOM_LEAD_MAX', 0.50),
      },

      {
        id: 'MIDGAME_UNDERPRICED_STABLE',
        enabled: envBool('ENABLE_MIDGAME_UNDERPRICED_STABLE', true),
        priority: 30,
        quality: 'A_MIDGAME_TIME_PRICE_MISPRICING',
        description: 'midgame 0.50-0.60 side, slight lead, flat derivative, tight spread',
        elapsedMin: envNum('MID_UNDER_ELAPSED_MIN', 180),
        elapsedMax: envNum('MID_UNDER_ELAPSED_MAX', 210),
        askMin: envNum('MID_UNDER_ASK_MIN', 0.50),
        askMax: envNum('MID_UNDER_ASK_MAX', 0.60),
        spreadMin: envNum('MID_UNDER_SPREAD_MIN', 0.01),
        spreadMax: envNum('MID_UNDER_SPREAD_MAX', 0.02),
        v5Min: envNum('MID_UNDER_V5_MIN', -0.015),
        v5Max: envNum('MID_UNDER_V5_MAX', 0.015),
        v15Min: envNum('MID_UNDER_V15_MIN', -0.03),
        v15Max: envNum('MID_UNDER_V15_MAX', 0.03),
        leadMin: envNum('MID_UNDER_LEAD_MIN', 0.10),
        leadMax: envNum('MID_UNDER_LEAD_MAX', 0.25),
      },

      {
        id: 'LOOSE_CONTINUATION',
        enabled: envBool('ENABLE_LOOSE_CONTINUATION', true),
        priority: 40,
        quality: 'A_LOOSE_CONTINUATION',
        description: 'side already moving, enough lead, not over-confirmed',
        elapsedMin: envNum('CONT_ELAPSED_MIN', 30),
        elapsedMax: envNum('CONT_ELAPSED_MAX', 210),
        askMin: envNum('CONT_ASK_MIN', 0.60),
        askMax: envNum('CONT_ASK_MAX', 0.82),
        spreadMax: envNum('CONT_SPREAD_MAX', 0.05),
        v5Min: envNum('CONT_V5_MIN', 0.01),
        v15Min: envNum('CONT_V15_MIN', 0.02),
        leadMin: envNum('CONT_LEAD_MIN', 0.15),
      },

      {
        id: 'EXPENSIVE_COLLAPSE_PERSISTENCE',
        enabled: envBool('ENABLE_EXPENSIVE_COLLAPSE_PERSISTENCE', true),
        priority: 50,
        quality: 'B_EXPENSIVE_COLLAPSE_PERSISTENCE',
        description: 'opposite side crushed, side expensive but still positive in history',
        elapsedMin: envNum('EXP_COLLAPSE_ELAPSED_MIN', 50),
        elapsedMax: envNum('EXP_COLLAPSE_ELAPSED_MAX', 210),
        askMin: envNum('EXP_COLLAPSE_ASK_MIN', 0.80),
        askMax: envNum('EXP_COLLAPSE_ASK_MAX', 0.90),
        spreadMax: envNum('EXP_COLLAPSE_SPREAD_MAX', 0.03),
        oppMidMax: envNum('EXP_COLLAPSE_OPP_MID_MAX', 0.25),
        leadMin: envNum('EXP_COLLAPSE_LEAD_MIN', 0.45),
      },

      {
        id: 'LATE_SOFT_FAV_NON_TAKEOVER',
        enabled: envBool('ENABLE_LATE_SOFT_FAV_NON_TAKEOVER', true),
        priority: 60,
        quality: 'B_LATE_SOFT_FAV_NON_TAKEOVER',
        description: 'late 0.55-0.65 side where opposite side has not taken over',
        elapsedMin: envNum('LATE_SOFT_ELAPSED_MIN', 210),
        elapsedMax: envNum('LATE_SOFT_ELAPSED_MAX', 240),
        askMin: envNum('LATE_SOFT_ASK_MIN', 0.55),
        askMax: envNum('LATE_SOFT_ASK_MAX', 0.65),
        spreadMax: envNum('LATE_SOFT_SPREAD_MAX', 0.03),
        leadMin: envNum('LATE_SOFT_LEAD_MIN', 0.05),
        oppBidMax: envNum('LATE_SOFT_OPP_BID_MAX', 0.48),
      },
    ],

    // Explicit no-trade filters from the analysis.
    guards: {
      avoidTightMidCompression: envBool('AVOID_TIGHT_MID_COMPRESSION', true),
      avoidBadPullbackBreakdown: envBool('AVOID_BAD_PULLBACK_BREAKDOWN', true),
      avoidExhaustedFavorite: envBool('AVOID_EXHAUSTED_FAVORITE', true),
      avoidReversalDanger: envBool('AVOID_REVERSAL_DANGER', true),
    },
  },

  EXIT: {
    // Kept OFF. The tested edge is entry-to-settlement.
    // Existing dashboard compatibility preserved.
    enabled: String(process.env.EXIT_ENABLED || 'false').toLowerCase() === 'true',

    // Optional safety-only kill. Default false because old exit logic hurt expectancy.
    structuralKillEnabled: envBool('STRUCTURAL_KILL_ENABLED', false),
    minHoldSec: envNum('STRUCTURAL_KILL_MIN_HOLD_SEC', 25),
    adverseFromEntry: envNum('STRUCTURAL_KILL_ADVERSE_FROM_ENTRY', 0.14),
    maxMfeBeforeKill: envNum('STRUCTURAL_KILL_MAX_MFE_BEFORE_KILL', 0.10),
    v15Max: envNum('STRUCTURAL_KILL_V15_MAX', -0.08),
    oppBidLeadMin: envNum('STRUCTURAL_KILL_OPP_BID_LEAD_MIN', 0.20),
    confirmTicks: envNum('STRUCTURAL_KILL_CONFIRM_TICKS', 2),
  },

  PAPER: {
    startingBalance: Number(process.env.STARTING_BALANCE || 100),
    stakeUsd: Number(process.env.STAKE_USD || 1),
    feeBps: Number(process.env.FEE_BPS || 0),
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
  return String(process.env[name] ?? String(fallback)).toLowerCase() === 'true';
}

function envNum(name, fallback) {
  const n = Number(process.env[name]);
  return Number.isFinite(n) ? n : fallback;
}

// ----------------------------
// GLOBAL STATE
// ----------------------------

let wsMarket = null;
let isSearchingNextMarket = false;
let searchCooldownUntil = 0;
let lastStatusPrintSec = 0;
let lastPriceLogSec = 0;
let lastDebugSurfaceSec = 0;

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
    killConfirmCount: 0,
    peakHeldBid: null,
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
    surfaceId: null,
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
    'Timestamp,MarketSlug,Event,Side,EntryPrice,ExitValue,Shares,EntryElapsedSec,Quality,Reason,PnL_USD,Balance_USD,WinRate_Pct,ExitElapsedSec,ExitReason,HeldBid,V15,Lead\n'
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
  if (currentMarket.priceTicks.length > 500) currentMarket.priceTicks.shift();

  priceStream.write([
    new Date().toISOString(),
    currentMarket.slug,
    tick.elapsedSec,
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
// TIME HELPERS
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
    } catch (_) {
      // Ignore malformed heartbeat-ish payloads.
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
// QUOTE / SURFACE HELPERS
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

  const signal = evaluateSurfaceStrategy();
  if (signal) executePaperEntry(signal);
}

function otherSide(side) {
  return side === 'YES' ? 'NO' : 'YES';
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

function getSideSpread(side) {
  const q = getSideQuote(side);
  if (q.ask == null || q.bid == null) return null;
  return Number(q.ask) - Number(q.bid);
}

function getSideMid(side) {
  const ask = getSideAsk(side);
  const bid = getSideBid(side);
  if (ask == null || bid == null) return null;
  return (ask + bid) / 2;
}

function getSideMidAtOrBefore(side, targetElapsedSec) {
  if (!Array.isArray(currentMarket.priceTicks)) return null;

  for (let i = currentMarket.priceTicks.length - 1; i >= 0; i--) {
    const t = currentMarket.priceTicks[i];
    if (t.elapsedSec <= targetElapsedSec && t[side] && t[side].ask != null && t[side].bid != null) {
      return (Number(t[side].ask) + Number(t[side].bid)) / 2;
    }
  }

  return null;
}

function getSideTrend(side, secondsBack, nowElapsedSec = getElapsedSec()) {
  const nowMid = getSideMid(side);
  const priorMid = getSideMidAtOrBefore(side, nowElapsedSec - secondsBack);
  if (nowMid == null || priorMid == null) return null;
  return nowMid - priorMid;
}

function getSideBidAtOrBefore(side, targetElapsedSec) {
  if (!Array.isArray(currentMarket.priceTicks)) return null;

  for (let i = currentMarket.priceTicks.length - 1; i >= 0; i--) {
    const t = currentMarket.priceTicks[i];
    if (t.elapsedSec <= targetElapsedSec && t[side] && t[side].bid != null) {
      return Number(t[side].bid);
    }
  }

  return null;
}

function getSideContext(side) {
  const opp = otherSide(side);
  const elapsed = getElapsedSec();

  const ask = getSideAsk(side);
  const bid = getSideBid(side);
  const oppAsk = getSideAsk(opp);
  const oppBid = getSideBid(opp);

  const heldMid = getSideMid(side);
  const oppMid = getSideMid(opp);

  if ([ask, bid, oppAsk, oppBid, heldMid, oppMid].some((x) => x == null)) return null;

  const spread = ask - bid;
  const oppSpread = oppAsk - oppBid;
  const maxSpread = Math.max(spread, oppSpread);

  const v5 = getSideTrend(side, 5, elapsed);
  const v15 = getSideTrend(side, 15, elapsed);
  const v30 = getSideTrend(side, 30, elapsed);

  return {
    side,
    opp,
    elapsed,
    ask,
    bid,
    oppAsk,
    oppBid,
    heldMid,
    oppMid,
    spread,
    oppSpread,
    maxSpread,
    lead: heldMid - oppMid,
    bidLead: bid - oppBid,
    v5,
    v15,
    v30,
    yesMid: getSideMid('YES'),
    noMid: getSideMid('NO'),
  };
}

function inRange(value, min, max) {
  if (value == null || Number.isNaN(Number(value))) return false;
  if (min != null && Number(value) < min) return false;
  if (max != null && Number(value) > max) return false;
  return true;
}

function passesRange(ctx, rule, field, minKey, maxKey) {
  const min = rule[minKey];
  const max = rule[maxKey];
  if (min == null && max == null) return true;
  return inRange(ctx[field], min, max);
}

function surfaceMatches(rule, ctx) {
  if (!rule.enabled) return false;

  if (!inRange(ctx.elapsed, rule.elapsedMin, rule.elapsedMax)) return false;
  if (!inRange(ctx.ask, rule.askMin, rule.askMax)) return false;

  if (CONFIG.STRATEGY.requireBid && (!ctx.bid || ctx.bid <= 0)) return false;

  if (ctx.maxSpread > CONFIG.STRATEGY.maxAllowedSpread) return false;

  if (!passesRange(ctx, rule, 'spread', 'spreadMin', 'spreadMax')) return false;
  if (!passesRange(ctx, rule, 'v5', 'v5Min', 'v5Max')) return false;
  if (!passesRange(ctx, rule, 'v15', 'v15Min', 'v15Max')) return false;
  if (!passesRange(ctx, rule, 'v30', 'v30Min', 'v30Max')) return false;
  if (!passesRange(ctx, rule, 'lead', 'leadMin', 'leadMax')) return false;
  if (!passesRange(ctx, rule, 'oppMid', 'oppMidMin', 'oppMidMax')) return false;
  if (!passesRange(ctx, rule, 'oppBid', 'oppBidMin', 'oppBidMax')) return false;

  if (rule.maxOppBidMinusBid != null && (ctx.oppBid - ctx.bid) > rule.maxOppBidMinusBid) {
    return false;
  }

  return true;
}

function blockedByNoTradeGuard(ctx) {
  const g = CONFIG.STRATEGY.guards;

  // Balanced tight compression was negative in state-level testing.
  if (
    g.avoidTightMidCompression &&
    inRange(ctx.ask, 0.48, 0.58) &&
    Math.abs(ctx.lead) <= 0.10 &&
    ctx.spread <= 0.02 &&
    ctx.v5 != null &&
    Math.abs(ctx.v5) >= 0.015 &&
    inRange(ctx.elapsed, 20, 180)
  ) {
    return 'GUARD_TIGHT_MID_COMPRESSION';
  }

  // Bad pullback: both short and medium derivatives broken.
  if (
    g.avoidBadPullbackBreakdown &&
    inRange(ctx.ask, 0.60, 0.75) &&
    ctx.v5 != null &&
    ctx.v15 != null &&
    ctx.v5 <= -0.02 &&
    ctx.v15 <= -0.03 &&
    (ctx.spread > 0.03 || ctx.oppBid > ctx.bid)
  ) {
    return 'GUARD_BAD_PULLBACK_BREAKDOWN';
  }

  // High favorite but current derivative is stalling.
  if (
    g.avoidExhaustedFavorite &&
    inRange(ctx.elapsed, 45, 240) &&
    inRange(ctx.ask, 0.80, 0.95) &&
    ctx.v15 != null &&
    ctx.v5 != null &&
    ctx.v15 >= 0.04 &&
    ctx.v5 <= 0 &&
    (ctx.spread > 0.02 || ctx.oppBid >= ctx.bid - 0.20)
  ) {
    return 'GUARD_EXHAUSTED_FAVORITE';
  }

  // Favorite with negative v5/v15 and opposite bid alive.
  if (
    g.avoidReversalDanger &&
    inRange(ctx.elapsed, 60, 260) &&
    inRange(ctx.ask, 0.70, 0.95) &&
    ctx.v5 != null &&
    ctx.v15 != null &&
    ctx.v5 <= -0.04 &&
    ctx.v15 <= -0.02 &&
    ctx.oppBid >= 0.25 &&
    ctx.spread >= 0.02
  ) {
    return 'GUARD_REVERSAL_DANGER';
  }

  return null;
}

// ----------------------------
// STRATEGY
// ----------------------------

function evaluateSurfaceStrategy() {
  if (!currentMarket.slug || currentMarket.hasTraded || trade.active) return null;

  const yesCtx = getSideContext('YES');
  const noCtx = getSideContext('NO');

  if (!yesCtx || !noCtx) return null;

  const candidates = [];

  for (const ctx of [yesCtx, noCtx]) {
    const guard = blockedByNoTradeGuard(ctx);
    if (guard) {
      maybeDebugSurface(ctx, guard);
      continue;
    }

    for (const surface of CONFIG.STRATEGY.surfaces) {
      if (surfaceMatches(surface, ctx)) {
        candidates.push({
          surface,
          ctx,
          score: scoreCandidate(surface, ctx),
        });
      }
    }
  }

  if (!candidates.length) return null;

  candidates.sort((a, b) => {
    if (a.surface.priority !== b.surface.priority) return a.surface.priority - b.surface.priority;
    return b.score - a.score;
  });

  const best = candidates[0];
  const { surface, ctx } = best;

  const tokenId = ctx.side === 'YES' ? currentMarket.yesToken : currentMarket.noToken;

  return {
    type: 'BUY',
    marketSlug: currentMarket.slug,
    marketTitle: currentMarket.title,
    side: ctx.side,
    tokenId,
    price: ctx.ask,
    elapsedSec: ctx.elapsed,
    quality: surface.quality,
    surfaceId: surface.id,
    reason: buildReason(surface, ctx),
    timestamp: Date.now(),
  };
}

function scoreCandidate(surface, ctx) {
  // Not an ML score. Just deterministic priority within tied surfaces.
  const payout = 1 / ctx.ask - 1;
  const structure = Math.max(0, ctx.lead) + Math.max(0, ctx.v15 || 0) + Math.max(0, ctx.v5 || 0);
  const spreadPenalty = ctx.spread * 2;
  return payout + structure - spreadPenalty - surface.priority / 1000;
}

function buildReason(surface, ctx) {
  return [
    surface.id,
    `SIDE_${ctx.side}`,
    `ASK_${fmt(ctx.ask)}`,
    `EL_${ctx.elapsed}`,
    `SP_${fmt(ctx.spread)}`,
    `V5_${fmt(ctx.v5)}`,
    `V15_${fmt(ctx.v15)}`,
    `LEAD_${fmt(ctx.lead)}`,
  ].join('_');
}

function maybeDebugSurface(ctx, guard) {
  if (!CONFIG.STRATEGY.debugSurface) return;
  const nowSec = Math.floor(Date.now() / 1000);
  if (nowSec === lastDebugSurfaceSec) return;
  lastDebugSurfaceSec = nowSec;

  console.log(
    `${colors.gray}[SURFACE GUARD] ${guard} ${ctx.side} ask=${fmt(ctx.ask)} spread=${fmt(ctx.spread)} v5=${fmt(ctx.v5)} v15=${fmt(ctx.v15)} lead=${fmt(ctx.lead)} elapsed=${ctx.elapsed}${colors.reset}`
  );
}

// ----------------------------
// PAPER BROKER
// ----------------------------

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
    surfaceId: signal.surfaceId,
    exitMode: 'HOLD_TO_SETTLEMENT',
    confirmedHold: false,
    confirmReason: null,
  };

  currentMarket.hasTraded = true;
  currentMarket.peakHeldBid = getSideBid(trade.side);

  console.log(
    `\n${colors.cyan}[PAPER ENTRY] BUY ${trade.side} @ ${fmt(trade.entryPrice)} | ${trade.surfaceId} | elapsed=${trade.entryElapsedSec}s | ${trade.quality}${colors.reset}`
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
    '',
  ].join(',') + '\n');
}

function executePaperExit(reason, exitBid, v15 = null, lead = null) {
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
    v15 == null ? '' : fmt(v15),
    lead == null ? '' : fmt(lead),
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
  if (!CONFIG.EXIT.structuralKillEnabled) return;

  const elapsed = getElapsedSec();
  if (elapsed < trade.entryElapsedSec + CONFIG.EXIT.minHoldSec) return;

  const ctx = getSideContext(trade.side);
  if (!ctx) return;

  if (currentMarket.peakHeldBid == null) currentMarket.peakHeldBid = ctx.bid;
  currentMarket.peakHeldBid = Math.max(currentMarket.peakHeldBid, ctx.bid);

  const mfe = currentMarket.peakHeldBid - trade.entryPrice;
  const adverse = ctx.bid <= Math.max(0.01, trade.entryPrice - CONFIG.EXIT.adverseFromEntry);
  const neverDeveloped = mfe <= CONFIG.EXIT.maxMfeBeforeKill;
  const derivativeBroken = ctx.v15 != null && ctx.v15 <= CONFIG.EXIT.v15Max;
  const oppTakingOver = (ctx.oppBid - ctx.bid) >= CONFIG.EXIT.oppBidLeadMin;

  if (adverse && neverDeveloped && derivativeBroken && oppTakingOver) {
    currentMarket.killConfirmCount += 1;
  } else {
    currentMarket.killConfirmCount = 0;
  }

  if (currentMarket.killConfirmCount >= CONFIG.EXIT.confirmTicks) {
    executePaperExit('STRUCTURAL_INVALIDATION', ctx.bid, ctx.v15, ctx.lead);
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
    '',
  ].join(',') + '\n');

  recentTrades.unshift({
    time: new Date().toLocaleTimeString(),
    reason: `${trade.surfaceId} ${trade.side} ${trade.quality}`,
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
  console.log(`${colors.magenta}Booting BTC 5m Surface Edge V4 PAPER BOT...${colors.reset}`);
  console.log(`${colors.gray}Paper trading only. No PRIVATE_KEY required.${colors.reset}`);
  console.log(
    `${colors.gray}Strategy: ${CONFIG.STRATEGY.name} | maxSpread<=${CONFIG.STRATEGY.maxAllowedSpread} | requireBid=${CONFIG.STRATEGY.requireBid}${colors.reset}`
  );
  console.log(
    `${colors.gray}Surfaces ON: ${CONFIG.STRATEGY.surfaces.filter(s => s.enabled).map(s => s.id).join(', ')}${colors.reset}`
  );
  console.log(
    `${colors.gray}Exit: ${CONFIG.EXIT.enabled ? 'ON' : 'OFF'} | structuralKill=${CONFIG.EXIT.structuralKillEnabled}${colors.reset}`
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

    if (now >= currentMarket.endTimeMs + 1000 && !isSearchingNextMarket) {
      await loadCurrentMarket();
      return;
    }

    const nowSec = Math.floor(now / 1000);

    if (nowSec % 10 === 0 && nowSec !== lastStatusPrintSec) {
      lastStatusPrintSec = nowSec;

      const status = trade.active
        ? `HOLDING ${trade.side} @ $${fmt(trade.entryPrice)} (${trade.surfaceId})`
        : 'AWAITING_SURFACE_SETUP';

      const statusColor = trade.active ? colors.cyan : colors.gray;
      const yesCtx = getSideContext('YES');
      const noCtx = getSideContext('NO');

      console.log(
        `${statusColor}[LIVE] ${status} | ${currentMarket.slug} | elapsed=${getElapsedSec()}s | left=${getSecondsLeft()}s | YES=${fmt(currentMarket.quote.YES.ask)} | NO=${fmt(currentMarket.quote.NO.ask)} | YESv5=${fmt(yesCtx && yesCtx.v5)} | YESv15=${fmt(yesCtx && yesCtx.v15)} | NOv5=${fmt(noCtx && noCtx.v5)} | NOv15=${fmt(noCtx && noCtx.v15)} | Bal=$${stats.currentBalance.toFixed(2)}${colors.reset}`
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
``