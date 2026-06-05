class QuoteCache {
  constructor() {
    this.quotes = new Map();
  }

  update(side, tokenId, book) {
    this.quotes.set(side, {
      side,
      tokenId,
      bestAsk: toOrder(book?.asks?.[0]),
      bestBid: toOrder(book?.bids?.[0]),
      timestamp: Date.now(),
      rawTimestamp: book?.timestamp || null,
      tickSize: book?.tick_size || '0.01',
      negRisk: Boolean(book?.neg_risk)
    });
  }

  get(side) {
    return this.quotes.get(side) || null;
  }

  isFresh(side, staleMs) {
    const quote = this.get(side);
    if (!quote) {
      return false;
    }

    return Date.now() - quote.timestamp <= staleMs;
  }

  validateFresh(sides, staleMs) {
    for (const side of sides) {
      if (!this.isFresh(side, staleMs)) {
        return {
          ok: false,
          reason: `stale_quote_${side.toLowerCase()}`
        };
      }
    }

    return { ok: true };
  }
}

function toOrder(level) {
  if (!level) {
    return null;
  }

  return {
    price: Number(level.price),
    size: Number(level.size)
  };
}

module.exports = {
  QuoteCache
};
