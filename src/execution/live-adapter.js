const { ClobClient, Side, OrderType } = require('@polymarket/clob-client');
const { createWalletClient, http } = require('viem');
const { polygon, polygonAmoy } = require('viem/chains');
const { privateKeyToAccount } = require('viem/accounts');

class LiveAdapter {
  constructor({ config, logger }) {
    this.config = config;
    this.logger = logger;
    this.client = null;
  }

  get mode() {
    return 'live';
  }

  async init() {
    const account = privateKeyToAccount(ensureHex(this.config.privateKey));
    const chain = this.config.chainId === 80002 ? polygonAmoy : polygon;

    const walletClient = createWalletClient({
      account,
      chain,
      transport: http()
    });

    let creds;
    if (this.config.clobApiKey && this.config.clobApiSecret && this.config.clobApiPassphrase) {
      creds = {
        key: this.config.clobApiKey,
        secret: this.config.clobApiSecret,
        passphrase: this.config.clobApiPassphrase
      };
    }

    this.client = new ClobClient(
      this.config.clobHost,
      this.config.chainId,
      walletClient,
      creds,
      this.config.signatureType,
      this.config.funderAddress,
      undefined,
      undefined,
      undefined,
      undefined,
      true,
      undefined,
      true
    );

    if (!creds) {
      const derived = await this.client.createOrDeriveApiKey();
      this.client = new ClobClient(
        this.config.clobHost,
        this.config.chainId,
        walletClient,
        derived,
        this.config.signatureType,
        this.config.funderAddress,
        undefined,
        undefined,
        undefined,
        undefined,
        true,
        undefined,
        true
      );
    }
  }

  async getOrderBook(tokenId) {
    return this.client.getOrderBook(tokenId);
  }

  async placeLimitBuy({ tokenId, price, size, options }) {
    const result = await this.client.createAndPostOrder(
      {
        tokenID: tokenId,
        price,
        side: Side.BUY,
        size
      },
      options,
      OrderType.GTC
    );

    return {
      id: extractOrderId(result),
      tokenId,
      price,
      size,
      status: 'LIVE'
    };
  }

  async placeAggressiveBuy({ tokenId, maxPrice, size, options }) {
    const result = await this.client.createAndPostOrder(
      {
        tokenID: tokenId,
        price: maxPrice,
        side: Side.BUY,
        size
      },
      options,
      OrderType.FAK
    );

    return {
      id: extractOrderId(result),
      tokenId,
      price: maxPrice,
      size,
      status: 'LIVE'
    };
  }

  async getOrder(orderId) {
    const order = await this.client.getOrder(orderId);

    const originalSize = Number(order.original_size || 0);
    const matchedSize = Number(order.size_matched || 0);

    return {
      id: order.id,
      tokenId: order.asset_id,
      price: Number(order.price),
      size: originalSize,
      filledSize: matchedSize,
      avgPrice: Number(order.price),
      status: normalizeStatus(order.status),
      filledAt: normalizeTimestamp(order.last_update || order.created_at),
      raw: order
    };
  }

  async cancelOrder(orderId) {
    await this.client.cancelOrder({ orderID: orderId });
  }

  async cancelAll() {
    await this.client.cancelAll();
  }
}

function extractOrderId(result) {
  return (
    result?.orderID ||
    result?.id ||
    result?.order_id ||
    result?.orderId ||
    result?.data?.orderID ||
    result?.data?.id
  );
}

function normalizeStatus(raw) {
  const value = String(raw || '').toLowerCase();
  if (value.includes('cancel')) {
    return 'CANCELLED';
  }

  if (value === 'filled' || value === 'matched') {
    return 'FILLED';
  }

  return 'LIVE';
}

function ensureHex(value) {
  return value.startsWith('0x') ? value : `0x${value}`;
}

function normalizeTimestamp(value) {
  if (!value) {
    return null;
  }

  const numeric = Number(value);
  if (!Number.isNaN(numeric)) {
    return numeric > 1e12 ? numeric : numeric * 1000;
  }

  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? null : parsed;
}

module.exports = {
  LiveAdapter
};
