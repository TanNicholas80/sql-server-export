require('dotenv').config();
const SqlServerService = require('./services/sqlserver.service');
const CsvService = require('./services/csv.service');
const UploaderService = require('./services/uploader.service');
const StateService = require('./services/state.service');
const logger = require('./utils/logger');

let isRunning = true;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function main() {
  logger.info('================================================================');
  logger.info('Starting SQL Server 2014 Continuous Incremental & Delta Crawler');
  logger.info('================================================================');

  let dbPool = null;
  const pollIntervalMs = parseInt(process.env.POLL_INTERVAL_MS || '5000', 10);
  
  const batchSizeMatch = (process.env.SQL_QUERY || '').match(/TOP\s+(\d+)/i);
  const targetBatchSize = batchSizeMatch ? parseInt(batchSizeMatch[1], 10) : 1000;

  // Handle graceful shutdown signals (PM2 stop, SIGINT, SIGTERM)
  const shutdown = async (signal) => {
    logger.info({ signal }, 'Shutdown signal received. Exiting continuous loop gracefully...');
    isRunning = false;
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('message', (msg) => {
    if (msg === 'shutdown') shutdown('PM2 shutdown');
  });

  while (isRunning) {
    let generatedFilePath = null;
    const batchStartTime = Date.now();
    let totalProcessedInIteration = 0;

    try {
      // Ensure DB Connection Pool is active
      dbPool = await SqlServerService.ensureConnection(dbPool);
      let state = StateService.loadState();

      // ==========================================
      // PHASE 1: Incremental Pull for New Records
      // ==========================================
      const baseQuery = process.env.SQL_QUERY || 'SELECT TOP 1000 * FROM [TICKET].[dbo].[TICKET_DETAIL]';
      const { query: incQuery, incrementalColumn: incCol } = SqlServerService.buildIncrementalQuery(baseQuery, state);

      logger.info(
        { executedQuery: incQuery, incrementalColumn: incCol, lastValue: state.lastValue },
        'Executing Incremental Pull Stream Query...'
      );

      const incStreamRequest = SqlServerService.createStreamRequest(dbPool, incQuery);
      const incResult = await CsvService.streamSqlToCsv(incStreamRequest, null, incCol);
      generatedFilePath = incResult.filePath;

      if (incResult.recordCount > 0) {
        logger.info({ recordCount: incResult.recordCount }, 'Uploading Incremental CSV batch to Laravel VPS...');
        await UploaderService.uploadCsvToLaravel(generatedFilePath);

        if (incResult.maxIncrementalValue !== null && incResult.maxIncrementalValue !== undefined) {
          StateService.saveState({
            lastValue: incResult.maxIncrementalValue,
            lastRunAt: new Date().toISOString(),
            lastRecordCount: incResult.recordCount
          });
        }
        totalProcessedInIteration += incResult.recordCount;
      }

      // Cleanup temporary incremental CSV file
      if (generatedFilePath) {
        CsvService.cleanupFile(generatedFilePath);
        generatedFilePath = null;
      }

      // Refresh state after incremental phase
      state = StateService.loadState();

      // ==========================================
      // PHASE 2: Delta Load for Updated ACTUAL_WT Rows
      // ==========================================
      const deltaEnabled = process.env.DELTA_ENABLE === 'true';
      if (deltaEnabled) {
        const baseDeltaQuery = process.env.DELTA_QUERY || `SELECT TOP 1000 * FROM [TICKET].[dbo].[TICKET_DETAIL] WHERE (ACTUAL_WT IS NOT NULL AND ACTUAL_WT > 0) AND DyeWeightTime > :last_delta_time ORDER BY DyeWeightTime ASC`;
        const { query: deltaQuery, incrementalColumn: deltaCol } = SqlServerService.buildDeltaQuery(baseDeltaQuery, state);

        if (deltaQuery) {
          logger.info(
            { executedQuery: deltaQuery, deltaColumn: deltaCol, lastDeltaTimestamp: state.lastDeltaTimestamp },
            'Executing Delta Load Stream Query (ACTUAL_WT Updates)...'
          );

          const deltaStreamRequest = SqlServerService.createStreamRequest(dbPool, deltaQuery);
          const deltaResult = await CsvService.streamSqlToCsv(deltaStreamRequest, `delta_ticket_export_${Date.now()}.csv`, deltaCol);
          generatedFilePath = deltaResult.filePath;

          if (deltaResult.recordCount > 0) {
            logger.info({ recordCount: deltaResult.recordCount }, 'Uploading Delta CSV batch to Laravel VPS...');
            await UploaderService.uploadCsvToLaravel(generatedFilePath);

            if (deltaResult.maxIncrementalValue !== null && deltaResult.maxIncrementalValue !== undefined) {
              StateService.saveState({
                lastDeltaTimestamp: deltaResult.maxIncrementalValue,
                lastRunAt: new Date().toISOString(),
                lastDeltaRecordCount: deltaResult.recordCount
              });
            }
            totalProcessedInIteration += deltaResult.recordCount;
          }

          if (generatedFilePath) {
            CsvService.cleanupFile(generatedFilePath);
            generatedFilePath = null;
          }
        }
      }

      const durationSeconds = ((Date.now() - batchStartTime) / 1000).toFixed(2);

      // Check if we should loop immediately or idle sleep
      if (totalProcessedInIteration >= targetBatchSize && isRunning) {
        logger.info(
          { totalProcessedInIteration, targetBatchSize },
          'Catch-up mode active: waiting 2s buffer before next batch iteration...'
        );
        await sleep(2000); // Buffer agar tidak membanjiri CPU VPS & koneksi web
      } else if (isRunning) {
        logger.info(
          { totalProcessedInIteration, pollIntervalMs },
          `Iteration complete (${totalProcessedInIteration} records processed). Sleeping for ${pollIntervalMs / 1000}s...`
        );
        await sleep(pollIntervalMs);
      }

    } catch (err) {
      logger.error({ err: err.message, stack: err.stack }, 'Error in crawler loop. Retrying in 10s...');
      await sleep(10000);
    } finally {
      if (generatedFilePath) {
        CsvService.cleanupFile(generatedFilePath);
      }
    }
  }

  // Final cleanup on loop termination
  if (dbPool) {
    await SqlServerService.close(dbPool);
  }
  logger.info('Continuous Crawler Pipeline stopped cleanly.');
}

if (require.main === module) {
  main();
}
