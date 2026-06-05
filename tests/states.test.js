const test = require('node:test');
const assert = require('node:assert/strict');

const { STATES, oppositeSide, isTerminalState } = require('../src/lib/states');

test('oppositeSide flips YES/NO', () => {
  assert.equal(oppositeSide('YES'), 'NO');
  assert.equal(oppositeSide('NO'), 'YES');
});

test('terminal states are detected', () => {
  assert.equal(isTerminalState(STATES.PAIR_COMPLETED_AT_48), true);
  assert.equal(isTerminalState(STATES.PAIR_COMPLETED_BY_HEDGE), true);
  assert.equal(isTerminalState(STATES.ABORTED_OR_CANCELLED), true);
  assert.equal(isTerminalState(STATES.ORDERS_LIVE), false);
});
