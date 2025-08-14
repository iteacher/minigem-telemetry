export const CONFIG = {
    PORT: parseInt(process.env.PORT || '8088', 10),
    LOG_DIR: process.env.LOG_DIR || '/opt/jwc-telemetry/logs/events-transformed',
    MAX_BODY: 64 * 1024,
    YEARLY_SALT: process.env.YEARLY_SALT || 'jwc-2025-salt',
    GEO_DB: process.env.GEO_DB || '/opt/jwc-telemetry/geo/GeoLite2-City.mmdb',
    RATE_LIMIT_MAX: parseInt(process.env.RATE_LIMIT_MAX || '2000', 10),
    RATE_LIMIT_TIME_WINDOW: process.env.RATE_LIMIT_TIME_WINDOW || '1 hour',
    STATS_SECRET: process.env.STATS_SECRET || '',
    STATS_WINDOW_DAYS: parseInt(process.env.STATS_WINDOW_DAYS || '7', 10),
    PG_URL: process.env.PG_URL || '',
    PG_SSL: (process.env.PG_SSL || 'false').toLowerCase() === 'true',
    // Debug/trace flags
    DEBUG_DB: ['1', 'true', 'yes'].includes(String(process.env.DEBUG_DB || '').toLowerCase()),
    DEBUG_STATS_TRACE: ['1', 'true', 'yes'].includes(String(process.env.DEBUG_STATS_TRACE || '').toLowerCase())
};
