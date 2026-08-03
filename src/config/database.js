require('dotenv').config();

let server = process.env.DB_SERVER || '127.0.0.1';
let instanceName = process.env.DB_INSTANCE || null;

// Auto-detect named instance if server is specified as SERVER_NAME\INSTANCE (e.g. MSI\SQLEXPRESS or localhost\SQLEXPRESS)
if (server.includes('\\')) {
  const parts = server.split('\\');
  server = parts[0];
  instanceName = parts[1];
}

// Convert "MSI" or "localhost" to "127.0.0.1" if running locally with DB_PORT
if (process.env.DB_PORT && (server === 'MSI' || server.toLowerCase() === 'localhost')) {
  server = '127.0.0.1';
}

const dbConfig = {
  user: process.env.DB_USER || 'sa',
  password: process.env.DB_PASSWORD || '1234',
  server: server,
  database: process.env.DB_NAME || 'TICKET',
  options: {
    encrypt: process.env.DB_ENCRYPT === 'true',
    trustServerCertificate: process.env.DB_TRUST_SERVER_CERT !== 'false', // Default true for legacy SQL Server 2014
    cryptoCredentialsDetails: {
      minVersion: 'TLSv1', // Allows compatibility with older SQL Server 2014 TLS
    },
    requestTimeout: 300000, // 5 minutes timeout for long queries
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000,
  },
};

// Prioritize direct TCP port connection if DB_PORT is set (bypasses SQL Browser Service lookup)
if (process.env.DB_PORT) {
  dbConfig.port = parseInt(process.env.DB_PORT, 10);
} else if (instanceName) {
  dbConfig.options.instanceName = instanceName;
}

module.exports = dbConfig;
