class RiskManager {
  constructor(config, logger) {
    this.config = config;
    this.logger = logger;

    this.attemptedMarkets = new Set();
    this.activeMarkets = new Set();
    this.dailyRealizedLoss = 0;
    this.dailyHedgeFailures = 0;
    this.lastDay = currentDay();
    this.openExposure = 0;
  }

  rotateDayIfNeeded() {
    const day = currentDay();
    if (day !== this.lastDay) {
      this.dailyRealizedLoss = 0;
      this.dailyHedgeFailures = 0;
      this.lastDay = day;
    }
  }

  canStartMarket({ marketId, projectedExposure }) {
    this.rotateDayIfNeeded();

    if (this.config.oneTradeAttemptPerMarket && this.attemptedMarkets.has(marketId) && !this.config.allowDuplicateMarketAttempts) {
      return { ok: false, reason: 'duplicate_market_attempt_blocked' };
    }

    if (this.activeMarkets.size >= this.config.maxActiveMarkets) {
      return { ok: false, reason: 'max_active_markets_reached' };
    }

    if (this.dailyRealizedLoss >= this.config.maxDailyRealizedLoss) {
      return { ok: false, reason: 'max_daily_realized_loss_reached' };
    }

    if (this.dailyHedgeFailures >= this.config.maxDailyHedgeFailures) {
      return { ok: false, reason: 'max_daily_hedge_failures_reached' };
    }

    if (this.openExposure + projectedExposure > this.config.maxTotalOpenExposure) {
      return { ok: false, reason: 'max_total_open_exposure_reached' };
    }

    return { ok: true };
  }

  registerMarketStart(marketId, projectedExposure) {
    this.attemptedMarkets.add(marketId);
    this.activeMarkets.add(marketId);
    this.openExposure += projectedExposure;
  }

  registerMarketEnd(marketId, releasedExposure) {
    this.activeMarkets.delete(marketId);
    this.openExposure = Math.max(0, this.openExposure - releasedExposure);
  }

  registerAttemptPnl(netPnl) {
    this.rotateDayIfNeeded();

    if (netPnl < 0) {
      this.dailyRealizedLoss += Math.abs(netPnl);
    }
  }

  registerHedgeFailure() {
    this.rotateDayIfNeeded();
    this.dailyHedgeFailures += 1;
  }
}

function currentDay() {
  return new Date().toISOString().slice(0, 10);
}

module.exports = {
  RiskManager
};
