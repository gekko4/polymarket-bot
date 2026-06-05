const test = require('node:test');
const assert = require('node:assert/strict');

const { RiskManager } = require('../src/risk-manager');

function makeConfig() {
  return {
    oneTradeAttemptPerMarket: true,
    allowDuplicateMarketAttempts: false,
    maxActiveMarkets: 1,
    maxDailyRealizedLoss: 10,
    maxDailyHedgeFailures: 2,
    maxTotalOpenExposure: 5
  };
}

test('blocks duplicate market attempts by default', () => {
  const risk = new RiskManager(makeConfig(), null);

  const first = risk.canStartMarket({ marketId: 'm1', projectedExposure: 1 });
  assert.equal(first.ok, true);

  risk.registerMarketStart('m1', 1);
  risk.registerMarketEnd('m1', 1);

  const second = risk.canStartMarket({ marketId: 'm1', projectedExposure: 1 });
  assert.equal(second.ok, false);
  assert.equal(second.reason, 'duplicate_market_attempt_blocked');
});

test('blocks when open exposure would exceed cap', () => {
  const risk = new RiskManager(makeConfig(), null);
  risk.config.maxActiveMarkets = 2;
  risk.registerMarketStart('m1', 5);

  const gate = risk.canStartMarket({ marketId: 'm2', projectedExposure: 1 });
  assert.equal(gate.ok, false);
  assert.equal(gate.reason, 'max_total_open_exposure_reached');
});

test('tracks hedge failures and blocks after max', () => {
  const risk = new RiskManager(makeConfig(), null);

  risk.registerHedgeFailure();
  risk.registerHedgeFailure();

  const gate = risk.canStartMarket({ marketId: 'm2', projectedExposure: 1 });
  assert.equal(gate.ok, false);
  assert.equal(gate.reason, 'max_daily_hedge_failures_reached');
});
