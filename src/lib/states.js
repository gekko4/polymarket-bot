const STATES = {
  WAITING_FOR_MARKET: 'WAITING_FOR_MARKET',
  ORDERS_LIVE: 'ORDERS_LIVE',
  ONE_SIDE_FILLED: 'ONE_SIDE_FILLED',
  PAIR_COMPLETED_AT_48: 'PAIR_COMPLETED_AT_48',
  PAIR_COMPLETED_BY_HEDGE: 'PAIR_COMPLETED_BY_HEDGE',
  ABORTED_OR_CANCELLED: 'ABORTED_OR_CANCELLED'
};

function oppositeSide(side) {
  return side === 'YES' ? 'NO' : 'YES';
}

function isTerminalState(state) {
  return (
    state === STATES.PAIR_COMPLETED_AT_48 ||
    state === STATES.PAIR_COMPLETED_BY_HEDGE ||
    state === STATES.ABORTED_OR_CANCELLED
  );
}

module.exports = {
  STATES,
  oppositeSide,
  isTerminalState
};
