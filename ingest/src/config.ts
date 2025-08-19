import path from 'path';

// Resolve port from several common env var names used by hosts (A2, Render, Heroku, etc.).
// Falls back to 3000 when not provided so the app still runs in local/dev or misconfigured envs.
function resolvePort(): number {
  const candidates = [
    process.env.PORT,
    process.env.APP_PORT as string | undefined,
    process.env.PORT0 as string | undefined,
    process.env.WEB_PORT as string | undefined,
    process.env.HTTP_PORT as string | undefined,
    // Passenger-specific port variables
    process.env.PASSENGER_PORT as string | undefined,
    process.env.PHUSION_PASSENGER_PORT as string | undefined,
    process.env.SERVER_PORT as string | undefined
  ].filter(Boolean) as string[];
  for (const raw of candidates) {
    if (/^\d+$/.test(raw)) {
      const n = parseInt(raw, 10);
      if (n > 0 && n < 65536) return n;
    }
  }
  return 3000;
}

export const CONFIG = {
  // HTTP server port
  PORT: resolvePort(),
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