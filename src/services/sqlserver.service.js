const sql = require('mssql');
const dbConfig = require('../config/database');
const logger = require('../utils/logger');

/**
 * Service to manage SQL Server 2014 connection and stream queries.
 */
class SqlServerService {
  /**
   * Connect to SQL Server pool
   * @returns {Promise<sql.ConnectionPool>}
   */
  static async connect() {
    try {
      logger.info({ server: dbConfig.server, port: dbConfig.port, database: dbConfig.database }, 'Connecting to SQL Server 2014...');
      const pool = await sql.connect(dbConfig);
      logger.info('Connected to SQL Server 2014 successfully.');
      return pool;
    } catch (err) {
      logger.error({ err }, 'Failed to connect to SQL Server 2014.');
      throw err;
    }
  }

  /**
   * Ensure ConnectionPool is connected and alive.
   * Reconnects automatically if disconnected.
   *
   * @param {sql.ConnectionPool|null} pool 
   * @returns {Promise<sql.ConnectionPool>}
   */
  static async ensureConnection(pool) {
    if (pool && pool.connected) {
      return pool;
    }
    if (pool) {
      await SqlServerService.close(pool);
    }
    return await SqlServerService.connect();
  }

  /**
   * Build incremental query based on current state and environment configuration.
   * Supports placeholder replacement (:last_id, :last_value, :last_timestamp)
   * or automatic WHERE clause injection.
   *
   * @param {string} baseQuery 
   * @param {object} state 
   * @returns {{ query: string, incrementalColumn: string|null }}
   */
  static buildIncrementalQuery(baseQuery, state) {
    const enabled = process.env.INCREMENTAL_ENABLE !== 'false';
    const column = process.env.INCREMENTAL_COLUMN || 'RECORD_ORDER';
    
    let defaultQuery = baseQuery;
    if (!defaultQuery) {
      defaultQuery = 'SELECT TOP 1000 t.*, p.ProductName AS PRODUCT_NAME FROM [TICKET].[dbo].[TICKET_DETAIL] t LEFT JOIN [TICKET].[dbo].[PRODUCT] p ON t.PRODUCT_CODE = p.ProductCode';
    }

    if (!enabled) {
      return { query: defaultQuery, incrementalColumn: null };
    }

    const lastValue = state?.lastValue;
    const hasPlaceholder = /:last_id|:last_value|:last_timestamp/i.test(defaultQuery);

    if (hasPlaceholder) {
      const type = (process.env.INCREMENTAL_TYPE || 'numeric').toLowerCase();
      let replacement = '0';
      if (lastValue !== null && lastValue !== undefined) {
        replacement = typeof lastValue === 'string' ? `'${lastValue}'` : lastValue;
      } else if (type === 'timestamp') {
        replacement = "'1970-01-01 00:00:00'";
      } else if (type === 'string') {
        replacement = "''";
      }

      const query = defaultQuery.replace(/:last_id|:last_value|:last_timestamp/gi, replacement);
      return { query, incrementalColumn: column };
    }

    const qualifiedColumn = column.includes('.') ? column : `t.${column}`;

    if (lastValue !== null && lastValue !== undefined) {
      const isStringType = (process.env.INCREMENTAL_TYPE || 'numeric').toLowerCase() === 'string' || typeof lastValue === 'string';
      const formattedVal = isStringType ? `'${lastValue}'` : lastValue;
      const condition = `${qualifiedColumn} > ${formattedVal}`;

      let query = defaultQuery.trim();
      const orderByMatch = query.match(/\s+ORDER\s+BY\s+/i);

      if (/\s+WHERE\s+/i.test(query)) {
        if (orderByMatch) {
          const orderByIndex = orderByMatch.index;
          query = query.slice(0, orderByIndex) + ` AND ${condition}` + query.slice(orderByIndex);
        } else {
          query += ` AND ${condition}`;
        }
      } else {
        if (orderByMatch) {
          const orderByIndex = orderByMatch.index;
          query = query.slice(0, orderByIndex) + ` WHERE ${condition}` + query.slice(orderByIndex);
        } else {
          query += ` WHERE ${condition} ORDER BY ${qualifiedColumn} ASC`;
        }
      }
      return { query, incrementalColumn: column };
    }

    // First run without placeholder or prior state: ensure ordering for consistent pulls
    let query = defaultQuery.trim();
    if (!/\s+ORDER\s+BY\s+/i.test(query)) {
      query += ` ORDER BY ${qualifiedColumn} ASC`;
    }

    return { query, incrementalColumn: column };
  }

  /**
   * Build delta load query for updated rows (e.g. updated ACTUAL_WT / COMP_DATE).
   *
   * @param {string} baseDeltaQuery 
   * @param {object} state 
   * @returns {{ query: string|null, incrementalColumn: string|null }}
   */
  static buildDeltaQuery(baseDeltaQuery, state) {
    const enabled = process.env.DELTA_ENABLE === 'true';
    if (!enabled) {
      return { query: null, incrementalColumn: null };
    }

    const column = process.env.DELTA_COLUMN || 'COMP_DATE';
    const lastDeltaTimestamp = state?.lastDeltaTimestamp;
    
    let defaultQuery = baseDeltaQuery;
    if (!defaultQuery) {
      defaultQuery = `SELECT TOP 1000 t.*, p.ProductName AS PRODUCT_NAME FROM [TICKET].[dbo].[TICKET_DETAIL] t LEFT JOIN [TICKET].[dbo].[PRODUCT] p ON t.PRODUCT_CODE = p.ProductCode WHERE (t.ACTUAL_WT IS NOT NULL AND t.ACTUAL_WT > 0) AND (t.COMP_DATE >= :last_delta_time OR :last_delta_time IS NULL) ORDER BY t.COMP_DATE ASC, t.COMP_TIME ASC`;
    }

    const hasPlaceholder = /:last_delta_time|:last_timestamp/i.test(defaultQuery);

    if (hasPlaceholder) {
      const replacement = lastDeltaTimestamp ? `'${lastDeltaTimestamp}'` : "''";
      const query = defaultQuery.replace(/:last_delta_time|:last_timestamp/gi, replacement);
      return { query, incrementalColumn: column };
    }

    const qualifiedColumn = column.includes('.') ? column : `t.${column}`;

    if (lastDeltaTimestamp) {
      const formattedVal = `'${lastDeltaTimestamp}'`;
      const condition = `${qualifiedColumn} >= ${formattedVal}`;

      let query = defaultQuery.trim();
      const orderByMatch = query.match(/\s+ORDER\s+BY\s+/i);

      if (/\s+WHERE\s+/i.test(query)) {
        if (orderByMatch) {
          const orderByIndex = orderByMatch.index;
          query = query.slice(0, orderByIndex) + ` AND ${condition}` + query.slice(orderByIndex);
        } else {
          query += ` AND ${condition}`;
        }
      } else {
        if (orderByMatch) {
          const orderByIndex = orderByMatch.index;
          query = query.slice(0, orderByIndex) + ` WHERE ${condition}` + query.slice(orderByIndex);
        } else {
          query += ` WHERE ${condition} ORDER BY t.COMP_DATE ASC, t.COMP_TIME ASC`;
        }
      }
      return { query, incrementalColumn: column };
    }

    // Default first run: fallback with empty string replacement
    const query = defaultQuery.replace(/:last_delta_time|:last_timestamp/gi, "''");
    return { query, incrementalColumn: column };
  }

  /**
   * Execute a query and return a readable stream request.
   * Useful for handling large datasets with zero memory bloat.
   * 
   * @param {sql.ConnectionPool} pool 
   * @param {string} query 
   * @returns {sql.Request} Streamable SQL request object
   */
  static createStreamRequest(pool, query) {
    const request = pool.request();
    request.stream = true;
    
    // Execute query asynchronously; events 'row', 'error', 'done' will be emitted on request
    process.nextTick(() => {
      request.query(query).catch((err) => {
        logger.error({ err }, 'Error during SQL stream query execution');
        request.emit('error', err);
      });
    });

    return request;
  }

  /**
   * Close database connection pool
   * @param {sql.ConnectionPool} pool 
   */
  static async close(pool) {
    if (pool) {
      try {
        await pool.close();
        logger.info('SQL Server connection pool closed.');
      } catch (err) {
        logger.error({ err }, 'Error closing SQL Server connection pool.');
      }
    }
  }
}

module.exports = SqlServerService;
