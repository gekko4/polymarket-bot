# polymarket-bot

Live-first implementation of the 48-Centre Fast-Hedge strategy for Polymarket BTC 5-minute markets.

## Strategy states

- `WAITING_FOR_MARKET`
- `ORDERS_LIVE`
- `ONE_SIDE_FILLED`
- `PAIR_COMPLETED_AT_48`
- `PAIR_COMPLETED_BY_HEDGE`
- `ABORTED_OR_CANCELLED`

## Live setup

1. Install dependencies:
   - `npm ci`
2. Set required environment variables:
   - `MODE=live`
   - `PRIVATE_KEY`
   - `FUNDER_ADDRESS`
   - Optional API key set: `CLOB_API_KEY`, `CLOB_API_SECRET`, `CLOB_API_PASSPHRASE`
3. Start live runner:
   - `npm run live`

Health endpoint:
- `GET /api/live` (default port `3000`)

## Paper mode

Paper mode uses the same state machine and risk controls while simulating fills.

- `npm run paper`

## Risk controls

Implemented risk gates:

- max size per market
- max total open exposure
- max active markets
- max daily realized loss
- max daily hedge failures
- max hedge price cap
- emergency cancel-all (`EMERGENCY_CANCEL_ALL_ON_START=true`)
- market whitelist (`MARKET_WHITELIST=id1,id2,...`)
- stale quote protection (`STALE_QUOTE_MS`)
- duplicate order protection (default one attempt per market)

## Important disclaimer

This code can place real orders in `MODE=live`.
Use conservative limits first and verify all credentials, balances, and environment variables before running with real funds.
