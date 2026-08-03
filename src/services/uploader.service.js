const fs = require('fs');
const path = require('path');
const axios = require('axios');
const FormData = require('form-data');
const apiConfig = require('../config/api');
const logger = require('../utils/logger');

class UploaderService {
  /**
   * Upload CSV file to Laravel API Endpoint on Docker VPS
   * 
   * @param {string} filePath Absolute path to local CSV file
   * @returns {Promise<object>} API Response data
   */
  static async uploadCsvToLaravel(filePath) {
    if (!fs.existsSync(filePath)) {
      throw new Error(`CSV file does not exist at path: ${filePath}`);
    }

    const fileName = path.basename(filePath);
    logger.info({ url: apiConfig.url, fileName }, 'Preparing to upload CSV to Laravel VPS...');

    const form = new FormData();
    form.append('csv_file', fs.createReadStream(filePath), {
      filename: fileName,
      contentType: 'text/csv',
    });

    const headers = {
      ...form.getHeaders(),
    };

    try {
      const response = await axios.post(apiConfig.url, form, {
        headers,
        timeout: apiConfig.timeout,
        maxBodyLength: Infinity, // Support large CSV files
        maxContentLength: Infinity,
      });

      logger.info(
        { status: response.status, data: response.data },
        'CSV successfully uploaded to Laravel VPS.'
      );

      return response.data;
    } catch (err) {
      if (err.response) {
        logger.error(
          {
            status: err.response.status,
            data: err.response.data,
            url: apiConfig.url,
          },
          'Laravel VPS rejected the CSV upload request.'
        );
      } else if (err.request) {
        logger.error(
          { url: apiConfig.url },
          'No response received from Laravel VPS (Network error/Timeout).'
        );
      } else {
        logger.error({ message: err.message }, 'Error preparing HTTP upload request.');
      }
      throw err;
    }
  }
}

module.exports = UploaderService;
