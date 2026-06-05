const fs = require('fs');
const path = require('path');

class AuditLogger {
  constructor(config) {
    this.config = config;
    this.ensureLogDirs();

    this.auditStream = fs.createWriteStream(config.auditJsonlPath, { flags: 'a' });
    this.tradeStream = fs.createWriteStream(config.tradesCsvPath, { flags: 'a' });

    this.ensureCsvHeaders();
  }

  ensureLogDirs() {
    if (!fs.existsSync(this.config.logsDir)) {
      fs.mkdirSync(this.config.logsDir, { recursive: true });
    }

    const auditDir = path.dirname(this.config.auditJsonlPath);
    if (!fs.existsSync(auditDir)) {
      fs.mkdirSync(auditDir, { recursive: true });
    }
  }

  ensureCsvHeaders() {
    if (!fs.existsSync(this.config.tradesCsvPath) || fs.statSync(this.config.tradesCsvPath).size === 0) {
      this.tradeStream.write(
        [
          'Date',
          'Market_ID',
          'Market_Start',
          'Market_End',
          'YES_Token',
          'NO_Token',
          'YES_Order_ID',
          'NO_Order_ID',
          'Hedge_Order_ID',
          'First_Fill_Side',
          'First_Fill_Price',
          'First_Fill_Time',
          'First_Fill_Size',
          'Opposite_Fill_Price',
          'Opposite_Fill_Time',
          'Opposite_Fill_Size',
          'Hedge_Price',
          'Hedge_Time',
          'Seconds_To_Completion',
          'Status',
          'Gross_PnL',
          'Fees',
          'Net_PnL',
          'Deployed_Cost',
          'Risk_Reason'
        ].join(',') + '\n'
      );
    }
  }

  audit(type, payload = {}) {
    this.auditStream.write(
      JSON.stringify({
        ts: new Date().toISOString(),
        type,
        ...payload
      }) + '\n'
    );
  }

  writeAttemptSummary(summary) {
    const row = [
      new Date().toISOString(),
      summary.marketId,
      summary.marketStart,
      summary.marketEnd,
      summary.yesToken,
      summary.noToken,
      summary.yesOrderId,
      summary.noOrderId,
      summary.hedgeOrderId,
      summary.firstFillSide,
      summary.firstFillPrice,
      summary.firstFillTime,
      summary.firstFillSize,
      summary.secondFillPrice,
      summary.secondFillTime,
      summary.secondFillSize,
      summary.hedgePrice,
      summary.hedgeTime,
      summary.secondsToCompletion,
      summary.status,
      summary.grossPnl,
      summary.fees,
      summary.netPnl,
      summary.deployedCost,
      summary.riskReason
    ]
      .map(v => (v === undefined || v === null ? '' : String(v)))
      .join(',');

    this.tradeStream.write(`${row}\n`);
  }
}

module.exports = {
  AuditLogger
};
