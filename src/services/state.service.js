const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');

class StateService {
  /**
   * Get resolved path of the state file
   * @returns {string}
   */
  static getFilePath() {
    const stateFile = process.env.STATE_FILE || './state.json';
    return path.resolve(stateFile);
  }

  /**
   * Load state from JSON file
   * @returns {{ lastValue: any, lastDeltaTimestamp: any, lastRunAt: string|null }}
   */
  static loadState() {
    const filePath = StateService.getFilePath();
    try {
      if (fs.existsSync(filePath)) {
        const raw = fs.readFileSync(filePath, 'utf8');
        const data = JSON.parse(raw);
        logger.info(
          { filePath, lastValue: data.lastValue, lastDeltaTimestamp: data.lastDeltaTimestamp, lastRunAt: data.lastRunAt },
          'Loaded incremental & delta state.'
        );
        return data;
      }
    } catch (err) {
      logger.warn({ err: err.message }, 'Failed to read state file, starting with default state.');
    }
    return { lastValue: null, lastDeltaTimestamp: null, lastRunAt: null };
  }

  /**
   * Save state to JSON file
   * @param {object} newState 
   */
  static saveState(newState) {
    const filePath = StateService.getFilePath();
    try {
      const dir = path.dirname(filePath);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }

      const current = StateService.loadState();
      const merged = {
        ...current,
        ...newState,
        updatedAt: new Date().toISOString()
      };

      fs.writeFileSync(filePath, JSON.stringify(merged, null, 2), 'utf8');
      logger.info({ filePath, lastValue: merged.lastValue, lastDeltaTimestamp: merged.lastDeltaTimestamp }, 'Saved updated state.');
    } catch (err) {
      logger.error({ err: err.message }, 'Failed to save state file.');
    }
  }
}

module.exports = StateService;
