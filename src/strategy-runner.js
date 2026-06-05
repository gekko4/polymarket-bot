const { STATES, oppositeSide, isTerminalState } = require('./lib/states');

class StrategyRunner {
  constructor({ config, marketDiscovery, quoteCache, riskManager, logger, adapter }) {
    this.config = config;
    this.marketDiscovery = marketDiscovery;
    this.quoteCache = quoteCache;
    this.riskManager = riskManager;
    this.logger = logger;
    this.adapter = adapter;

    this.currentAttempt = null;
    this.loopHandle = null;
    this.running = false;
    this.completedMarkets = 0;
  }

  async start() {
    this.running = true;
    await this.adapter.init();

    if (this.config.emergencyCancelAllOnStart) {
      this.logger.audit('EMERGENCY_CANCEL_ALL_START', { reason: 'startup_flag' });
      await this.adapter.cancelAll();
      return;
    }

    this.logger.audit('RUNNER_START', {
      mode: this.config.mode,
      entryPrice: this.config.entryPrice,
      hedgeDelaySeconds: this.config.hedgeDelaySeconds,
      maxHedgePrice: this.config.maxHedgePrice,
      sizePerSide: this.config.sizePerSide
    });

    this.loopHandle = setInterval(() => {
      this.tick().catch(err => {
        this.logger.audit('TICK_ERROR', { error: err.message });
        this.handleTickFailure(err).catch(inner => {
          this.logger.audit('TICK_FAILURE_HANDLER_ERROR', { error: inner.message });
        });
      });
    }, this.config.quotePollMs);

    await this.tick();
  }

  async stop(reason = 'stop_requested') {
    this.running = false;

    if (this.loopHandle) {
      clearInterval(this.loopHandle);
      this.loopHandle = null;
    }

    if (this.currentAttempt && !isTerminalState(this.currentAttempt.status)) {
      await this.abortAttempt(this.currentAttempt, reason);
    }

    try {
      await this.adapter.cancelAll();
    } catch (err) {
      this.logger.audit('CANCEL_ALL_ERROR', { reason, error: err.message });
    }
  }

  async tick() {
    if (!this.running) {
      return;
    }

    if (!this.currentAttempt || isTerminalState(this.currentAttempt.status)) {
      if (this.config.maxRuntimeMarkets > 0 && this.completedMarkets >= this.config.maxRuntimeMarkets) {
        await this.stop('max_runtime_markets_reached');
        return;
      }

      try {
        await this.tryStartNewAttempt();
      } catch (err) {
        this.logger.audit('MARKET_DISCOVERY_ERROR', { error: err.message });
      }
      return;
    }

    await this.refreshQuotes(this.currentAttempt);
    await this.updateOrderStates(this.currentAttempt);
    await this.advanceState(this.currentAttempt);
  }

  async handleTickFailure(err) {
    if (this.currentAttempt && !isTerminalState(this.currentAttempt.status)) {
      await this.abortAttempt(this.currentAttempt, 'runtime_error', { error: err.message });
      return;
    }

    try {
      await this.adapter.cancelAll();
    } catch (cancelErr) {
      this.logger.audit('CANCEL_ALL_ERROR', { reason: 'tick_failure', error: cancelErr.message });
    }
  }

  async tryStartNewAttempt() {
    const market = await this.marketDiscovery.discoverCurrentMarket();
    const marketValidation = this.marketDiscovery.validateMarket(market);

    if (!marketValidation.ok) {
      this.logger.audit('MARKET_SKIPPED', {
        reason: marketValidation.reason,
        marketId: market?.id || null
      });
      return;
    }

    const projectedExposure = 2 * this.config.entryPrice * this.config.sizePerSide;
    const riskGate = this.riskManager.canStartMarket({
      marketId: market.id,
      projectedExposure
    });

    if (!riskGate.ok) {
      this.logger.audit('START_BLOCKED', {
        marketId: market.id,
        reason: riskGate.reason
      });
      return;
    }

    const attempt = createAttempt(market, this.config);
    this.currentAttempt = attempt;

    this.riskManager.registerMarketStart(market.id, projectedExposure);

    this.logger.audit('ATTEMPT_STARTED', {
      marketId: market.id,
      startTime: market.startTime,
      yesToken: market.yesToken,
      noToken: market.noToken
    });

    await this.refreshQuotes(attempt);

    const freshness = this.quoteCache.validateFresh(['YES', 'NO'], this.config.staleQuoteMs);
    if (!freshness.ok) {
      await this.abortAttempt(attempt, freshness.reason);
      return;
    }

    await this.placeEntryOrders(attempt);
  }

  async refreshQuotes(attempt) {
    const yesBook = await this.adapter.getOrderBook(attempt.market.yesToken);
    const noBook = await this.adapter.getOrderBook(attempt.market.noToken);

    this.quoteCache.update('YES', attempt.market.yesToken, yesBook);
    this.quoteCache.update('NO', attempt.market.noToken, noBook);
  }

  async placeEntryOrders(attempt) {
    const yesQuote = this.quoteCache.get('YES');

    const yesOrder = await this.adapter.placeLimitBuy({
      tokenId: attempt.market.yesToken,
      price: this.config.entryPrice,
      size: this.config.sizePerSide,
      options: {
        tickSize: yesQuote?.tickSize || '0.01',
        negRisk: yesQuote?.negRisk || false
      }
    });

    const noQuote = this.quoteCache.get('NO');

    const noOrder = await this.adapter.placeLimitBuy({
      tokenId: attempt.market.noToken,
      price: this.config.entryPrice,
      size: this.config.sizePerSide,
      options: {
        tickSize: noQuote?.tickSize || '0.01',
        negRisk: noQuote?.negRisk || false
      }
    });

    attempt.orders.YES = normalizeAttemptOrder('YES', yesOrder);
    attempt.orders.NO = normalizeAttemptOrder('NO', noOrder);
    attempt.status = STATES.ORDERS_LIVE;

    this.logger.audit('ENTRY_ORDERS_LIVE', {
      marketId: attempt.market.id,
      yesOrderId: attempt.orders.YES.id,
      noOrderId: attempt.orders.NO.id,
      size: this.config.sizePerSide,
      price: this.config.entryPrice
    });
  }

  async updateOrderStates(attempt) {
    for (const side of ['YES', 'NO']) {
      const attemptOrder = attempt.orders[side];
      if (!attemptOrder) {
        continue;
      }

      if (attemptOrder.status === 'FILLED' || attemptOrder.status === 'CANCELLED') {
        continue;
      }

      if (this.adapter.mode === 'paper') {
        const quote = this.quoteCache.get(side);
        if (quote?.bestAsk && quote.bestAsk.price <= attemptOrder.price) {
          this.adapter.simulateFill(attemptOrder.id, quote.bestAsk.price);
        }
      }

      const latest = await this.adapter.getOrder(attemptOrder.id);
      if (!latest) {
        continue;
      }

      applyOrderSnapshot(attemptOrder, latest);
      this.captureFillFromOrder(attempt, side, attemptOrder);
    }

    if (attempt.orders.HEDGE) {
      const hedge = attempt.orders.HEDGE;

      if (hedge.status !== 'FILLED' && hedge.status !== 'CANCELLED') {
        const latest = await this.adapter.getOrder(hedge.id);
        if (latest) {
          applyOrderSnapshot(hedge, latest);
        }
      }

      const opposite = oppositeSide(attempt.firstFillSide);
      this.captureFillFromOrder(attempt, opposite, attempt.orders.HEDGE);
    }
  }

  captureFillFromOrder(attempt, side, order) {
    if (!order || order.filledSize <= 0) {
      return;
    }

    const existing = attempt.fills[side] || {
      side,
      orderId: order.id,
      filledSize: 0,
      avgPrice: 0,
      fillTime: null
    };

    existing.orderId = order.id;
    existing.filledSize = Math.max(existing.filledSize, order.filledSize);
    existing.avgPrice = order.avgPrice || order.price;
    existing.fillTime = existing.fillTime || new Date().toISOString();

    attempt.fills[side] = existing;
  }

  async advanceState(attempt) {
    if (Date.now() >= attempt.market.endMs && !isTerminalState(attempt.status)) {
      await this.abortAttempt(attempt, 'market_expired_before_completion');
      return;
    }

    const yesFilled = attempt.fills.YES?.filledSize || 0;
    const noFilled = attempt.fills.NO?.filledSize || 0;

    if (attempt.status === STATES.ORDERS_LIVE) {
      if (yesFilled >= this.config.sizePerSide && noFilled >= this.config.sizePerSide) {
        await this.completeAttempt(attempt, STATES.PAIR_COMPLETED_AT_48);
        return;
      }

      const yesDone = yesFilled >= this.config.sizePerSide;
      const noDone = noFilled >= this.config.sizePerSide;

      if (yesDone || noDone) {
        attempt.status = STATES.ONE_SIDE_FILLED;
        attempt.firstFillSide = yesDone ? 'YES' : 'NO';
        attempt.firstFillTime = Date.now();

        this.logger.audit('FIRST_SIDE_FILLED', {
          marketId: attempt.market.id,
          side: attempt.firstFillSide,
          fillPrice: attempt.fills[attempt.firstFillSide]?.avgPrice,
          fillSize: attempt.fills[attempt.firstFillSide]?.filledSize
        });
      }

      return;
    }

    if (attempt.status !== STATES.ONE_SIDE_FILLED) {
      return;
    }

    const opposite = oppositeSide(attempt.firstFillSide);
    const oppositeFilled = attempt.fills[opposite]?.filledSize || 0;

    if (oppositeFilled >= this.config.sizePerSide) {
      await this.completeAttempt(attempt, STATES.PAIR_COMPLETED_AT_48);
      return;
    }

    if (!attempt.hedgePlacedAt) {
      const elapsed = Date.now() - attempt.firstFillTime;
      if (elapsed < this.config.hedgeDelaySeconds * 1000) {
        return;
      }

      const freshness = this.quoteCache.validateFresh([opposite], this.config.staleQuoteMs);
      if (!freshness.ok) {
        await this.abortAttempt(attempt, freshness.reason);
        return;
      }

      const oppositeQuote = this.quoteCache.get(opposite);
      const ask = oppositeQuote?.bestAsk?.price;

      if (!ask) {
        await this.abortAttempt(attempt, 'missing_opposite_ask_for_hedge');
        return;
      }

      if (ask > this.config.maxHedgePrice) {
        this.riskManager.registerHedgeFailure();
        await this.abortAttempt(attempt, 'max_hedge_price_exceeded', { ask, cap: this.config.maxHedgePrice });
        return;
      }

      const remaining = Math.max(0, this.config.sizePerSide - oppositeFilled);
      if (remaining === 0) {
        await this.completeAttempt(attempt, STATES.PAIR_COMPLETED_AT_48);
        return;
      }

      const order = await this.adapter.placeAggressiveBuy({
        tokenId: opposite === 'YES' ? attempt.market.yesToken : attempt.market.noToken,
        maxPrice: this.config.maxHedgePrice,
        size: remaining,
        fillPrice: ask,
        options: {
          tickSize: oppositeQuote.tickSize || '0.01',
          negRisk: oppositeQuote.negRisk || false
        }
      });

      attempt.orders.HEDGE = normalizeAttemptOrder('HEDGE', order);
      attempt.hedgePlacedAt = Date.now();

      this.logger.audit('HEDGE_ORDER_SENT', {
        marketId: attempt.market.id,
        side: opposite,
        hedgeOrderId: order.id,
        hedgePriceCap: this.config.maxHedgePrice,
        intendedAsk: ask,
        remaining
      });

      return;
    }

    const finalOppositeFilled = attempt.fills[opposite]?.filledSize || 0;
    if (finalOppositeFilled >= this.config.sizePerSide) {
      await this.completeAttempt(attempt, STATES.PAIR_COMPLETED_BY_HEDGE);
      return;
    }

    if (Date.now() - attempt.hedgePlacedAt > this.config.hedgeOrderTimeoutMs) {
      this.riskManager.registerHedgeFailure();
      await this.abortAttempt(attempt, 'hedge_order_not_filled_in_time');
    }
  }

  async completeAttempt(attempt, status) {
    attempt.status = status;
    attempt.completedAt = Date.now();

    await this.cancelOpenOrders(attempt);

    const metrics = calculatePnl(attempt, this.config);
    this.riskManager.registerAttemptPnl(metrics.netPnl);
    this.riskManager.registerMarketEnd(attempt.market.id, 2 * this.config.entryPrice * this.config.sizePerSide);

    this.logger.audit('ATTEMPT_COMPLETED', {
      marketId: attempt.market.id,
      status,
      firstFillSide: attempt.firstFillSide,
      grossPnl: metrics.grossPnl,
      fees: metrics.fees,
      netPnl: metrics.netPnl,
      deployedCost: metrics.deployedCost,
      secondsToCompletion: (attempt.completedAt - attempt.createdAt) / 1000
    });

    this.logger.writeAttemptSummary(buildSummary(attempt, metrics));

    this.completedMarkets += 1;
  }

  async abortAttempt(attempt, reason, detail = {}) {
    attempt.status = STATES.ABORTED_OR_CANCELLED;
    attempt.completedAt = Date.now();
    attempt.riskReason = reason;

    await this.cancelOpenOrders(attempt);

    const metrics = calculatePnl(attempt, this.config, true);
    this.riskManager.registerAttemptPnl(metrics.netPnl);
    this.riskManager.registerMarketEnd(attempt.market.id, 2 * this.config.entryPrice * this.config.sizePerSide);

    this.logger.audit('ATTEMPT_ABORTED', {
      marketId: attempt.market.id,
      reason,
      detail,
      firstFillSide: attempt.firstFillSide,
      grossPnl: metrics.grossPnl,
      netPnl: metrics.netPnl
    });

    this.logger.writeAttemptSummary(buildSummary(attempt, metrics));

    this.completedMarkets += 1;
  }

  async cancelOpenOrders(attempt) {
    for (const key of ['YES', 'NO', 'HEDGE']) {
      const order = attempt.orders[key];
      if (!order) {
        continue;
      }

      if (order.status === 'FILLED' || order.status === 'CANCELLED') {
        continue;
      }

      try {
        await this.adapter.cancelOrder(order.id);
        order.status = 'CANCELLED';
      } catch (err) {
        this.logger.audit('CANCEL_ORDER_ERROR', {
          marketId: attempt.market.id,
          orderId: order.id,
          error: err.message
        });
      }
    }
  }
}

function createAttempt(market, config) {
  return {
    market,
    status: STATES.WAITING_FOR_MARKET,
    createdAt: Date.now(),
    completedAt: null,
    firstFillSide: null,
    firstFillTime: null,
    hedgePlacedAt: null,
    riskReason: null,
    orders: {
      YES: null,
      NO: null,
      HEDGE: null
    },
    fills: {
      YES: null,
      NO: null
    },
    configSnapshot: {
      entryPrice: config.entryPrice,
      maxHedgePrice: config.maxHedgePrice,
      hedgeDelaySeconds: config.hedgeDelaySeconds,
      sizePerSide: config.sizePerSide
    }
  };
}

function normalizeAttemptOrder(type, order) {
  return {
    id: order.id,
    type,
    tokenId: order.tokenId,
    price: Number(order.price),
    size: Number(order.size),
    filledSize: Number(order.filledSize || 0),
    avgPrice: Number(order.avgPrice || order.price || 0),
    status: order.status || 'LIVE'
  };
}

function applyOrderSnapshot(target, snapshot) {
  target.status = snapshot.status || target.status;
  target.filledSize = Number(snapshot.filledSize || 0);
  target.avgPrice = Number(snapshot.avgPrice || target.avgPrice || target.price);
}

function calculatePnl(attempt, config, isAbort = false) {
  const yesFill = attempt.fills.YES;
  const noFill = attempt.fills.NO;

  const yesSize = yesFill?.filledSize || 0;
  const noSize = noFill?.filledSize || 0;

  const yesCost = yesSize * (yesFill?.avgPrice || 0);
  const noCost = noSize * (noFill?.avgPrice || 0);

  const deployedCost = yesCost + noCost;

  const pairedSize = Math.min(yesSize, noSize);
  const pairedCost = pairedSize * (yesFill?.avgPrice || 0) + pairedSize * (noFill?.avgPrice || 0);
  const pairedPayout = pairedSize;

  const unmatchedYesCost = Math.max(0, yesSize - pairedSize) * (yesFill?.avgPrice || 0);
  const unmatchedNoCost = Math.max(0, noSize - pairedSize) * (noFill?.avgPrice || 0);

  let grossPnl = pairedPayout - pairedCost;

  if (isAbort) {
    grossPnl -= unmatchedYesCost + unmatchedNoCost;
  }

  const makerFees = (yesCost + noCost) * (config.makerFeeBps / 10000);
  const hedgeCost = attempt.orders.HEDGE?.filledSize
    ? attempt.orders.HEDGE.filledSize * (attempt.orders.HEDGE.avgPrice || attempt.orders.HEDGE.price)
    : 0;
  const takerFees = hedgeCost * (config.takerFeeBps / 10000);

  const fees = makerFees + takerFees;
  const netPnl = grossPnl - fees;

  return {
    grossPnl: round(grossPnl),
    fees: round(fees),
    netPnl: round(netPnl),
    deployedCost: round(deployedCost)
  };
}

function buildSummary(attempt, metrics) {
  const firstSide = attempt.firstFillSide;
  const secondSide = firstSide ? oppositeSide(firstSide) : null;

  const firstFill = firstSide ? attempt.fills[firstSide] : null;
  const secondFill = secondSide ? attempt.fills[secondSide] : null;

  return {
    marketId: attempt.market.id,
    marketStart: attempt.market.startTime,
    marketEnd: attempt.market.endTime,
    yesToken: attempt.market.yesToken,
    noToken: attempt.market.noToken,
    yesOrderId: attempt.orders.YES?.id,
    noOrderId: attempt.orders.NO?.id,
    hedgeOrderId: attempt.orders.HEDGE?.id,
    firstFillSide: firstSide,
    firstFillPrice: firstFill?.avgPrice,
    firstFillTime: firstFill?.fillTime,
    firstFillSize: firstFill?.filledSize,
    secondFillPrice: secondFill?.avgPrice,
    secondFillTime: secondFill?.fillTime,
    secondFillSize: secondFill?.filledSize,
    hedgePrice: attempt.orders.HEDGE?.avgPrice || null,
    hedgeTime: attempt.hedgePlacedAt ? new Date(attempt.hedgePlacedAt).toISOString() : null,
    secondsToCompletion:
      attempt.completedAt && attempt.createdAt
        ? round((attempt.completedAt - attempt.createdAt) / 1000)
        : null,
    status: attempt.status,
    grossPnl: metrics.grossPnl,
    fees: metrics.fees,
    netPnl: metrics.netPnl,
    deployedCost: metrics.deployedCost,
    riskReason: attempt.riskReason
  };
}

function round(value) {
  return Number(Number(value).toFixed(6));
}

module.exports = {
  StrategyRunner
};
