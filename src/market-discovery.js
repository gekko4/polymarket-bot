function floorToInterval(nowSec, intervalSec) {
  const remainder = nowSec % intervalSec;
  return nowSec - remainder;
}

class MarketDiscovery {
  constructor({ config, fetchImpl = fetch }) {
    this.config = config;
    this.fetchImpl = fetchImpl;
  }

  async discoverCurrentMarket() {
    const nowSec = Math.floor(Date.now() / 1000);
    const intervalStart = floorToInterval(nowSec, this.config.marketIntervalSeconds);
    const intervalEnd = intervalStart + this.config.marketIntervalSeconds;

    const eventSlug = `${this.config.marketSymbol}-updown-5m-${intervalStart}`;
    const url = `${this.config.gammaHost}/events?slug=${encodeURIComponent(eventSlug)}`;

    const response = await this.fetchImpl(url);
    if (!response.ok) {
      throw new Error(`Gamma response status ${response.status}`);
    }

    const events = await response.json();
    if (!events?.length || !events[0]?.markets?.length) {
      return null;
    }

    const market = events[0].markets[0];
    const tokenIds = parseTokenIds(market.clobTokenIds);

    if (!tokenIds) {
      return null;
    }

    const conditionId = market.conditionId || market.id;

    return {
      id: conditionId || eventSlug,
      slug: eventSlug,
      title: events[0].title || eventSlug,
      startTime: new Date(intervalStart * 1000).toISOString(),
      endTime: new Date(intervalEnd * 1000).toISOString(),
      startMs: intervalStart * 1000,
      endMs: intervalEnd * 1000,
      yesToken: tokenIds[0],
      noToken: tokenIds[1],
      conditionId: conditionId || null,
      whitelistKey: conditionId || eventSlug
    };
  }

  validateMarket(market) {
    if (!market) {
      return { ok: false, reason: 'market_not_found' };
    }

    const expectedPrefix = `${this.config.marketSymbol}-updown-5m-`;
    if (!market.slug.startsWith(expectedPrefix)) {
      return { ok: false, reason: 'invalid_symbol_or_interval' };
    }

    if (this.config.marketWhitelist.length > 0) {
      const allowed = this.config.marketWhitelist.includes(market.whitelistKey);
      if (!allowed) {
        return { ok: false, reason: 'market_not_whitelisted' };
      }
    }

    return { ok: true };
  }
}

function parseTokenIds(rawTokenIds) {
  if (!rawTokenIds) {
    return null;
  }

  let parsed = rawTokenIds;
  if (typeof rawTokenIds === 'string') {
    try {
      parsed = JSON.parse(rawTokenIds);
    } catch {
      return null;
    }
  }

  if (!Array.isArray(parsed) || parsed.length < 2) {
    return null;
  }

  return [parsed[0], parsed[1]];
}

module.exports = {
  MarketDiscovery
};
