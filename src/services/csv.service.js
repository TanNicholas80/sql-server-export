const fs = require('fs');
const path = require('path');
const fastCsv = require('fast-csv');
const logger = require('../utils/logger');

class CsvService {
  /**
   * Ensure target directory exists
   * @param {string} dirPath 
   */
  static ensureDirectoryExists(dirPath) {
    if (!fs.existsSync(dirPath)) {
      fs.mkdirSync(dirPath, { recursive: true });
    }
  }

  /**
   * Convert SQL Stream Request into a CSV File on Disk and track max incremental value
   * 
   * @param {import('mssql').Request} sqlStreamRequest 
   * @param {string} [outputFileName] 
   * @param {string} [incrementalColumn]
   * @returns {Promise<{ filePath: string, recordCount: number, maxIncrementalValue: any }>}
   */
  static streamSqlToCsv(sqlStreamRequest, outputFileName, incrementalColumn) {
    return new Promise((resolve, reject) => {
      const rawTempDir = process.env.TEMP_DIR || './tmp';
      const tempDir = path.isAbsolute(rawTempDir) ? rawTempDir : path.resolve(__dirname, '../../', rawTempDir);
      CsvService.ensureDirectoryExists(tempDir);

      const fileName = outputFileName || `ticket_export_${Date.now()}.csv`;
      const filePath = path.join(tempDir, fileName);

      logger.info({ filePath }, 'Starting CSV generation from SQL stream...');

      const writeStream = fs.createWriteStream(filePath, { encoding: 'utf8' });
      const csvStream = fastCsv.format({ headers: true, quoteColumns: true });

      let recordCount = 0;
      let maxIncrementalValue = null;
      let hasError = false;

      csvStream.pipe(writeStream);

      // Handle individual rows from SQL stream
      sqlStreamRequest.on('row', (row) => {
        // Skip row if any of unique identifier values (ID_NO, PRODUCT_CODE, STEP_NO) is missing/null/empty
        const idNoKey = Object.keys(row).find((k) => k.toLowerCase() === 'id_no');
        const stepNoKey = Object.keys(row).find((k) => k.toLowerCase() === 'step_no');
        const productCodeKey = Object.keys(row).find((k) => k.toLowerCase() === 'product_code');

        const idNoVal = idNoKey ? row[idNoKey] : null;
        const stepNoVal = stepNoKey ? row[stepNoKey] : null;
        const productCodeVal = productCodeKey ? row[productCodeKey] : null;

        if (
          idNoVal === null || idNoVal === undefined || String(idNoVal).trim() === '' ||
          stepNoVal === null || stepNoVal === undefined || String(stepNoVal).trim() === '' ||
          productCodeVal === null || productCodeVal === undefined || String(productCodeVal).trim() === ''
        ) {
          logger.warn(
            { id_no: idNoVal, step_no: stepNoVal, product_code: productCodeVal },
            'Skipping row from CSV generation: missing required unique identifier (ID_NO, STEP_NO, or PRODUCT_CODE)'
          );
          return;
        }

        recordCount++;
        csvStream.write(row);

        if (incrementalColumn) {
          const foundKey = Object.keys(row).find(
            (key) => key.toLowerCase() === incrementalColumn.toLowerCase()
          );

          if (foundKey && row[foundKey] !== null && row[foundKey] !== undefined) {
            const rawVal = row[foundKey];
            if (typeof rawVal === 'number') {
              if (maxIncrementalValue === null || rawVal > maxIncrementalValue) {
                maxIncrementalValue = rawVal;
              }
            } else if (rawVal instanceof Date) {
              const timeStr = rawVal.toISOString().slice(0, 19).replace('T', ' ');
              if (maxIncrementalValue === null || timeStr > maxIncrementalValue) {
                maxIncrementalValue = timeStr;
              }
            } else {
              const num = Number(rawVal);
              if (!isNaN(num) && String(rawVal).trim() !== '') {
                if (maxIncrementalValue === null || num > maxIncrementalValue) {
                  maxIncrementalValue = num;
                }
              } else {
                const strVal = String(rawVal);
                if (maxIncrementalValue === null || strVal > String(maxIncrementalValue)) {
                  maxIncrementalValue = strVal;
                }
              }
            }
          }
        }
      });

      // Handle SQL Stream errors
      sqlStreamRequest.on('error', (err) => {
        logger.error({ err }, 'SQL Stream Error encountered');
        hasError = true;
        csvStream.end();
        reject(err);
      });

      // Handle SQL Stream completion
      sqlStreamRequest.on('done', (result) => {
        logger.info({ rowsAffected: result?.rowsAffected, recordCount, maxIncrementalValue }, 'SQL stream completed successfully');
        csvStream.end();
      });

      // Handle CSV write completion
      writeStream.on('finish', () => {
        if (!hasError) {
          logger.info({ filePath, recordCount, maxIncrementalValue }, 'CSV file generated successfully.');
          resolve({ filePath, recordCount, maxIncrementalValue });
        }
      });

      // Handle WriteStream errors
      writeStream.on('error', (err) => {
        logger.error({ err }, 'File WriteStream Error encountered');
        hasError = true;
        reject(err);
      });
    });
  }

  /**
   * Delete temporary CSV file after upload
   * @param {string} filePath 
   */
  static cleanupFile(filePath) {
    try {
      if (fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
        logger.info({ filePath }, 'Temporary CSV file cleaned up.');
      }
    } catch (err) {
      logger.warn({ err, filePath }, 'Failed to delete temporary CSV file.');
    }
  }
}

module.exports = CsvService;
