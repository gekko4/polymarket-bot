const http = require('http');
const { config } = require('./config');
const { MarketDiscovery } = require('./market-discovery');
const { QuoteCache } = require('./quote-cache');
const { RiskManager } = require('./risk-manager');
const { AuditLogger } = require('./audit-logger');
const { createExecutionAdapter } = require('./execution');
const { StrategyRunner } = require('./strategy-runner');

async function main() {
  const logger = new AuditLogger(config);
  const discovery = new MarketDiscovery({ config });
  const quoteCache = new QuoteCache();
  const riskManager = new RiskManager(config, logger);
  const adapter = createExecutionAdapter({ config, logger });

  const runner = new StrategyRunner({
    config,
    marketDiscovery: discovery,
    quoteCache,
    riskManager,
    logger,
    adapter
  });

  installSignalHandlers(runner, logger);
  startHealthServer(config.port, runner);

  logger.audit('BOOT', {
    mode: config.mode,
    marketSymbol: config.marketSymbol,
    livePrimary: true
  });

  await runner.start();
}

function startHealthServer(port, runner) {
  http
    .createServer((req, res) => {
      if (req.url === '/api/live') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(
          JSON.stringify({
            ok: true,
            marketId: runner.currentAttempt?.market?.id || null,
            state: runner.currentAttempt?.status || null,
            mode: runner.config.mode
          })
        );
        return;
      }

      res.writeHead(404);
      res.end('Not found');
    })
    .listen(port, () => {
      console.log(`Health endpoint listening on :${port}`);
    });
}

function installSignalHandlers(runner, logger) {
  const stop = async signal => {
    logger.audit('SHUTDOWN_SIGNAL', { signal });
    await runner.stop(`signal_${signal}`);
    process.exit(0);
  };

  process.on('SIGINT', () => {
    stop('SIGINT').catch(err => {
      logger.audit('SHUTDOWN_ERROR', { error: err.message });
      process.exit(1);
    });
  });

  process.on('SIGTERM', () => {
    stop('SIGTERM').catch(err => {
      logger.audit('SHUTDOWN_ERROR', { error: err.message });
      process.exit(1);
    });
  });
}

main().catch(err => {
  console.error('Fatal startup error:', err.message);
  process.exit(1);
});
