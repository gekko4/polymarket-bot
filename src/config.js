require('dotenv').config();

function num(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || raw === '') {
    return fallback;
  }

  const value = Number(raw);
  if (Number.isNaN(value)) {
    throw new Error(`Invalid numeric env ${name}=${raw}`);
  }

  return value;
}

function bool(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || raw === '') {
    return fallback;
  }

  return ['1', 'true', 'yes', 'on'].includes(String(raw).toLowerCase());
}

function list(name) {
  const raw = process.env[name];
  if (!raw) {
    return [];
  }

  return raw
    .split(',')
    .map(v => v.trim())
    .filter(Boolean);
}

const config = {
  mode: (process.env.MODE || 'live').toLowerCase(),

  entryPrice: num('ENTRY_PRICE', 0.48),
  hedgeDelaySeconds: num('HEDGE_DELAY_SECONDS', 2.0),
  maxHedgePrice: num('MAX_HEDGE_PRICE', 0.60),
  sizePerSide: num('SIZE_PER_SIDE', 1),

  oneTradeAttemptPerMarket: bool('ONE_TRADE_ATTEMPT_PER_MARKET', true),
  allowDuplicateMarketAttempts: bool('ALLOW_DUPLICATE_MARKET_ATTEMPTS', false),

  staleQuoteMs: num('STALE_QUOTE_MS', 1500),
  quotePollMs: num('QUOTE_POLL_MS', 400),
  marketScanMs: num('MARKET_SCAN_MS', 1000),
  hedgeOrderTimeoutMs: num('HEDGE_ORDER_TIMEOUT_MS', 2500),

  maxSizePerMarket: num('MAX_SIZE_PER_MARKET', 1),
  maxTotalOpenExposure: num('MAX_TOTAL_OPEN_EXPOSURE', 5),
  maxActiveMarkets: num('MAX_ACTIVE_MARKETS', 1),
  maxDailyRealizedLoss: num('MAX_DAILY_REALIZED_LOSS', 10),
  maxDailyHedgeFailures: num('MAX_DAILY_HEDGE_FAILURES', 3),

  maxRuntimeMarkets: num('MAX_RUNTIME_MARKETS', 0),

  makerFeeBps: num('MAKER_FEE_BPS', 0),
  takerFeeBps: num('TAKER_FEE_BPS', 180),

  marketSymbol: (process.env.MARKET_SYMBOL || 'btc').toLowerCase(),
  marketIntervalSeconds: num('MARKET_INTERVAL_SECONDS', 300),
  marketWhitelist: list('MARKET_WHITELIST'),

  logsDir: process.env.LOGS_DIR || 'logs',
  tradesCsvPath: process.env.TRADES_CSV_PATH || 'trades_log.csv',
  auditJsonlPath: process.env.AUDIT_JSONL_PATH || 'logs/audit_events.jsonl',

  emergencyCancelAllOnStart: bool('EMERGENCY_CANCEL_ALL_ON_START', false),

  clobHost: process.env.CLOB_HOST || 'https://clob.polymarket.com',
  gammaHost: process.env.GAMMA_HOST || 'https://gamma-api.polymarket.com',
  chainId: num('CHAIN_ID', 137),

  funderAddress: process.env.FUNDER_ADDRESS,
  privateKey: process.env.PRIVATE_KEY,
  signatureType: num('SIGNATURE_TYPE', 1),
  clobApiKey: process.env.CLOB_API_KEY,
  clobApiSecret: process.env.CLOB_API_SECRET,
  clobApiPassphrase: process.env.CLOB_API_PASSPHRASE,

  port: num('PORT', 3000),
  dryRun: bool('DRY_RUN', false)
};

if (!['live', 'paper'].includes(config.mode)) {
  throw new Error(`MODE must be live or paper, got: ${config.mode}`);
}

if (config.sizePerSide > config.maxSizePerMarket) {
  throw new Error(
    `SIZE_PER_SIDE=${config.sizePerSide} exceeds MAX_SIZE_PER_MARKET=${config.maxSizePerMarket}`
  );
}

if (config.mode === 'live') {
  if (!config.privateKey) {
    throw new Error('PRIVATE_KEY is required in live mode');
  }

  if (!config.funderAddress) {
    throw new Error('FUNDER_ADDRESS is required in live mode');
  }
}

module.exports = {
  config
};
