import path from 'path';

export const CONFIG = {
  // HTTP server port: always prefer platform-injected PORT; never default to DB port
  PORT: Number.isFinite(parseInt(process.env.PORT || '', 10)) ? parseInt(process.env.PORT as string, 10) : 3000,
  LOG_DIR: process.env.LOG_DIR || '/opt/jwc-telemetry/logs/events-transformed',
  MAX_BODY: 64 * 1024,
  YEARLY_SALT: process.env.YEARLY_SALT || 'jwc-2025-salt',
  GEO_DB: process.env.GEO_DB || '/opt/jwc-telemetry/geo/GeoLite2-City.mmdb',
  RATE_LIMIT_MAX: parseInt(process.env.RATE_LIMIT_MAX || '2000', 10),
  RATE_LIMIT_TIME_WINDOW: process.env.RATE_LIMIT_TIME_WINDOW || '1 hour',
  STATS_SECRET: process.env.STATS_SECRET || '',
  // monthly stats view (no DB)
  STATS_WINDOW_DAYS: parseInt(process.env.STATS_WINDOW_DAYS || '7', 10),
  // Debug/trace flags (DB removed, but keep stats trace flag)
  DEBUG_STATS_TRACE: ['1','true','yes'].includes(String(process.env.DEBUG_STATS_TRACE||'').toLowerCase()),
  // Launch window: when set, keep per-day tallies starting at LAUNCH_DATE for LAUNCH_DURATION days
  LAUNCH_DATE: process.env.LAUNCH_DATE || new Date().toISOString().slice(0, 10), // YYYY-MM-DD
  LAUNCH_DURATION: Number.isFinite(parseInt(process.env.LAUNCH_DURATION || '', 10)) ? parseInt(process.env.LAUNCH_DURATION as string, 10) : 90,
  // File store path for installs counters
  STORE_FILE: process.env.STATS_JSON_FILE || path.join(process.env.LOG_DIR || '/opt/jwc-telemetry/logs', 'installs.json')
};