const { ClobClient } = require('@polymarket/clob-client');

class PaperAdapter {
  constructor({ logger, config }) {
    this.logger = logger;
    this.config = config;
    this.orders = new Map();
    this.seq = 1;
    this.client = null;
  }

  get mode() {
    return 'paper';
  }

  async init() {
    this.client = new ClobClient(this.config.clobHost, this.config.chainId);
  }

  async getOrderBook(tokenId) {
    return this.client.getOrderBook(tokenId);
  }

  async placeLimitBuy({ tokenId, price, size, options: _options }) {
    const id = `paper-${this.seq++}`;
    const order = {
      id,
      tokenId,
      price,
      size,
      filledSize: 0,
      avgPrice: 0,
      status: 'LIVE'
    };

    this.orders.set(id, order);
    return order;
  }

  async placeAggressiveBuy({ tokenId, maxPrice, size, fillPrice, options: _options }) {
    const id = `paper-${this.seq++}`;
    const price = fillPrice || maxPrice;
    const order = {
      id,
      tokenId,
      price,
      size,
      filledSize: size,
      avgPrice: price,
      status: 'FILLED',
      filledAt: Date.now()
    };

    this.orders.set(id, order);
    return order;
  }

  async getOrder(orderId) {
    return this.orders.get(orderId) || null;
  }

  simulateFill(orderId, fillPrice, availableSize = Infinity) {
    const order = this.orders.get(orderId);
    if (!order || order.status === 'FILLED' || order.status === 'CANCELLED') {
      return order;
    }

    if (fillPrice > order.price) {
      return order;
    }

    if (availableSize < order.size) {
      return order;
    }

    order.filledSize = order.size;
    order.avgPrice = fillPrice;
    order.status = 'FILLED';
    order.filledAt = Date.now();
    return order;
  }

  async cancelOrder(orderId) {
    const order = this.orders.get(orderId);
    if (!order) {
      return;
    }

    if (order.status !== 'FILLED') {
      order.status = 'CANCELLED';
    }
  }

  async cancelAll() {
    for (const order of this.orders.values()) {
      if (order.status !== 'FILLED') {
        order.status = 'CANCELLED';
      }
    }
  }
}

module.exports = {
  PaperAdapter
};
