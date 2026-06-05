const { PaperAdapter } = require('./paper-adapter');
const { LiveAdapter } = require('./live-adapter');

function createExecutionAdapter({ config, logger }) {
  if (config.mode === 'paper') {
    return new PaperAdapter({ logger, config });
  }

  return new LiveAdapter({ config, logger });
}

module.exports = {
  createExecutionAdapter
};
