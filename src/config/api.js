require('dotenv').config();

const apiConfig = {
  url: process.env.LARAVEL_API_URL || 'http://127.0.0.1:8000/api/ticket-details/import-csv',
  timeout: 600000, // 10 minutes timeout for file uploads
};

module.exports = apiConfig;
