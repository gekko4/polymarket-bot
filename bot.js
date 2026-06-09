require('dotenv').config();

const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const http = require('http');

// ============================================================
// BTC 5m Polymarket PAPER BOT — First Touch 34 Strategy
// Single-file version to keep your repo simple/private.
//
// Strategy agreed from backtest:
// - BTC 5m binary markets only
// - Ignore first 10 seconds after market open
// - Buy the FIRST side, YES or NO, whose ask touches <= 0.34
// - One trade per market
// - Do NOT buy both sides
// - Prefer 30s–60s touches as higher quality, but allow 10s–120s by default
// - Hold to inferred settlement in paper mode
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
    entryAsk: Number(process.env.ENTRY_ASK || 0.34),
    ignoreBeforeSec: Number(process.env.IGNORE_BEFORE_SEC || 10),
    maxEntrySec: Number(process.env.MAX_ENTRY_SEC || 120),
    preferredStartSec: Number(process.env.PREFERRED_START_SEC || 30),
    preferredEndSec: Number(process.env.PREFERRED_END_SEC || 60),
    maxAllowedSpread: Number(process.env.MAX_ALLOWED_SPREAD || 0.08),
    requireBid: String(process.env.REQUIRE_BID || 'false').toLowerCase() === 'true',
    oneTradePerMarket: true,
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
// GLOBAL STATE — intentionally simple single-file structure
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
  tradeStream.write('Timestamp,MarketSlug,Event,Side,EntryPrice,ExitValue,Shares,EntryElapsedSec,Quality,Reason,PnL_USD,Balance_USD,WinRate_Pct\n');
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
  if (y.ask == null || n.ask == null) return;

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

  const bestAskObj = asks.length ? asks.reduce((best, x) => Number(x.price) < Number(best.price) ? x : best, asks[0]) : null;
  const bestBidObj = bids.length ? bids.reduce((best, x) => Number(x.price) > Number(best.price) ? x : best, bids[0]) : null;

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

  const signal = evaluateFirstTouch34();
  if (signal) executePaperEntry(signal);
}

function evaluateFirstTouch34() {
  if (!currentMarket.slug || currentMarket.hasTraded || trade.active) return null;

  const elapsed = getElapsedSec();
  const s = CONFIG.STRATEGY;

  if (elapsed < s.ignoreBeforeSec) return null;
  if (elapsed > s.maxEntrySec) return null;

  const yes = currentMarket.quote.YES;
  const no = currentMarket.quote.NO;
  if (yes.ask == null || no.ask == null) return null;

  const yesSpreadOk = yes.spread == null || yes.spread <= s.maxAllowedSpread;
  const noSpreadOk = no.spread == null || no.spread <= s.maxAllowedSpread;

  const yesTouched = yes.ask <= s.entryAsk && yesSpreadOk && (!s.requireBid || yes.bid > 0);
  const noTouched = no.ask <= s.entryAsk && noSpreadOk && (!s.requireBid || no.bid > 0);

  if (!yesTouched && !noTouched) return null;

  let side;
  let price;
  let tokenId;

  // If both are already touched on the same evaluation, take the cheaper side.
  // If tied, skip to avoid ambiguous simultaneous fills.
  if (yesTouched && noTouched) {
    if (yes.ask < no.ask) {
      side = 'YES'; price = yes.ask; tokenId = currentMarket.yesToken;
    } else if (no.ask < yes.ask) {
      side = 'NO'; price = no.ask; tokenId = currentMarket.noToken;
    } else {
      console.log(`${colors.gray}[SKIP] Both sides touched equally at ${yes.ask}. Ambiguous.${colors.reset}`);
      currentMarket.hasTraded = true;
      return null;
    }
  } else if (yesTouched) {
    side = 'YES'; price = yes.ask; tokenId = currentMarket.yesToken;
  } else {
    side = 'NO'; price = no.ask; tokenId = currentMarket.noToken;
  }

  const quality = elapsed >= s.preferredStartSec && elapsed <= s.preferredEndSec
    ? 'PREFERRED_30_60S'
    : 'ALLOWED_10_120S';

  return {
    type: 'BUY',
    marketSlug: currentMarket.slug,
    marketTitle: currentMarket.title,
    side,
    tokenId,
    price,
    elapsedSec: elapsed,
    quality,
    reason: `FIRST_TOUCH_${Math.round(s.entryAsk * 100)}C`,
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
  };

  currentMarket.hasTraded = true;

  console.log(`\n${colors.cyan}[PAPER ENTRY] BUY ${trade.side} @ ${fmt(trade.entryPrice)} | ${trade.reason} | elapsed=${trade.entryElapsedSec}s | ${trade.quality}${colors.reset}`);

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
  ].join(',') + '\n');
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
  return yesAsk > noAsk ? 'YES' : 'NO';
}

// ----------------------------
// DASHBOARD SERVER — keeps your existing dashboard.html contract
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
  console.log(`${colors.magenta}Booting BTC 5m First-Touch-34 PAPER BOT...${colors.reset}`);
  console.log(`${colors.gray}No PRIVATE_KEY required. This file does paper trading only.${colors.reset}`);
  console.log(`${colors.gray}Strategy: ignore <${CONFIG.STRATEGY.ignoreBeforeSec}s, buy first side <= ${CONFIG.STRATEGY.entryAsk}, max entry ${CONFIG.STRATEGY.maxEntrySec}s, prefer ${CONFIG.STRATEGY.preferredStartSec}-${CONFIG.STRATEGY.preferredEndSec}s.${colors.reset}`);

  await loadCurrentMarket();

  setInterval(async () => {
    const now = Date.now();

    if (!currentMarket.slug) {
      if (now > searchCooldownUntil) await loadCurrentMarket();
      return;
    }

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
        : 'AWAITING FIRST_TOUCH_34';
      const statusColor = trade.active ? colors.cyan : colors.gray;
      console.log(`${statusColor}[LIVE] ${status} | ${currentMarket.slug} | elapsed=${getElapsedSec()}s | left=${getSecondsLeft()}s | YES=${fmt(currentMarket.quote.YES.ask)} | NO=${fmt(currentMarket.quote.NO.ask)} | Bal=$${stats.currentBalance.toFixed(2)}${colors.reset}`);
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
