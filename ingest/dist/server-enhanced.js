const http = require('http');
const url = require('url');
const fs = require('fs');
const path = require('path');

// Configuration - using relative paths for hosting compatibility
const CONFIG = {
  PORT: Number.isFinite(parseInt(process.env.PORT || '', 10)) ? parseInt(process.env.PORT, 10) : 3000,
  LOG_DIR: process.env.LOG_DIR || './logs',
  MAX_BODY: 64 * 1024,
  YEARLY_SALT: process.env.YEARLY_SALT || 'jwc-2025-salt',
  GEO_DB: process.env.GEO_DB || process.env.GEO_MMDB || './geo/GeoLite2-City.mmdb',
  RATE_LIMIT_MAX: parseInt(process.env.RATE_LIMIT_MAX || '2000', 10),
  RATE_LIMIT_TIME_WINDOW: process.env.RATE_LIMIT_TIME_WINDOW || '1 hour',
  STATS_SECRET: process.env.STATS_SECRET || '',
  STATS_WINDOW_DAYS: parseInt(process.env.STATS_WINDOW_DAYS || '7', 10),
  DEBUG_STATS_TRACE: ['1','true','yes'].includes(String(process.env.DEBUG_STATS_TRACE||'').toLowerCase()),
  LAUNCH_DATE: process.env.LAUNCH_DATE || new Date().toISOString().slice(0, 10),
  LAUNCH_DURATION: Number.isFinite(parseInt(process.env.LAUNCH_DURATION || '', 10)) ? parseInt(process.env.LAUNCH_DURATION, 10) : 90,
  STORE_FILE: process.env.STATS_JSON_FILE || path.join(process.env.LOG_DIR || './data', 'installs.json')
};

// Simple logging
const LOG_FILE = process.env.LOG_FILE || './logs/jwc-telemetry.log';
const LOG_LEVEL = ((process.env.LOG_LEVEL || 'info').toString().toLowerCase());

function ensureDir(filePath) {
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) {
    try {
      fs.mkdirSync(dir, { recursive: true });
    } catch (e) {
      console.warn('Could not create directory:', dir, e.message);
    }
  }
}

function writeLog(level, msg, data) {
  const ts = new Date().toISOString();
  const line = `[${ts}] [${level}] ${msg}${data !== undefined ? ' ' + JSON.stringify(data) : ''}`;
  console.log(line);
  
  try {
    ensureDir(LOG_FILE);
    fs.appendFileSync(LOG_FILE, line + '\n');
  } catch (e) {
    // Ignore file write errors in hosting environment
  }
}

const log = {
  debug: (m, d) => writeLog('debug', m, d),
  info: (m, d) => writeLog('info', m, d),
  warn: (m, d) => writeLog('warn', m, d),
  error: (m, d) => writeLog('error', m, d)
};

// Geo lookup (simplified, no maxmind dependency for now)
let geoEnabled = false;

function initGeo() {
  const geoPath = CONFIG.GEO_DB;
  if (!geoPath) {
    log.warn('geo.init.no_mmdb', 'GEO_DB not set; headers-only geolocation');
    return;
  }
  
  if (fs.existsSync(geoPath)) {
    geoEnabled = true;
    log.info('geo.init.loaded', { path: geoPath, note: 'Using header-based geo for now' });
  } else {
    log.warn('geo.init.file_not_found', { path: geoPath });
  }
}

function lookup(ip) {
  // Simplified geo lookup using headers only
  return { country: '', region: '' };
}

// Store functions
function defaultStore() {
  const now = new Date().toISOString();
  const currentYear = String(new Date().getUTCFullYear());
  const currentMonth = String(new Date().getUTCMonth() + 1).padStart(2, '0');
  const currentMonthKey = `${currentYear}-${currentMonth}`;
  
  return {
    meta: { createdAt: now, updatedAt: now },
    months: {
      [currentYear]: {
        [currentMonth]: { installs: 0 }
      }
    },
    upgrades: {
      total: 0,
      months: {
        [currentYear]: {
          [currentMonth]: { upgrades: 0 }
        }
      }
    },
    usage: {
      javaRuns: {
        total: 0,
        months: {
          [currentMonthKey]: { runs: 0 }
        }
      },
      themeChanges: {
        total: 0,
        months: {
          [currentMonthKey]: { changes: 0 }
        }
      }
    },
    byExt: { "0.0.0": 0 },
    byOs: {
      "darwin-arm64": 0, "darwin-x64": 0, "win32-x64": 0, "win32-arm64": 0,
      "linux-x64": 0, "linux-arm64": 0, "Unknown": 0
    },
    byCountry: {
      "US": 0, "GB": 0, "CA": 0, "DE": 0, "FR": 0, "AU": 0, "JP": 0, "Unknown": 0
    },
    versionsByMonth: { [currentMonthKey]: { "0.0.0": 0 } },
    osByMonth: { [currentMonthKey]: {
      "darwin-arm64": 0, "darwin-x64": 0, "win32-x64": 0, "win32-arm64": 0,
      "linux-x64": 0, "linux-arm64": 0, "Unknown": 0
    }},
    geoByMonth: { [currentMonthKey]: {
      "US": 0, "GB": 0, "CA": 0, "DE": 0, "FR": 0, "AU": 0, "JP": 0, "Unknown": 0
    }},
    launchDays: {},
    launchVersionsByDay: {},
    launchOsByDay: {},
    launchGeoByDay: {},
    launchRolledUp: false
  };
}

function readStore() {
  try {
    if (!fs.existsSync(CONFIG.STORE_FILE)) return defaultStore();
    const raw = fs.readFileSync(CONFIG.STORE_FILE, 'utf8');
    const obj = JSON.parse(raw);
    if (!obj || typeof obj !== 'object') return defaultStore();
    
    // Ensure all required properties exist
    obj.months = obj.months || {};
    obj.byExt = obj.byExt || {};
    obj.byOs = obj.byOs || {};
    obj.byCountry = obj.byCountry || {};
    obj.versionsByMonth = obj.versionsByMonth || {};
    obj.osByMonth = obj.osByMonth || {};
    obj.geoByMonth = obj.geoByMonth || {};
    obj.launchDays = obj.launchDays || {};
    obj.launchVersionsByDay = obj.launchVersionsByDay || {};
    obj.launchOsByDay = obj.launchOsByDay || {};
    obj.launchGeoByDay = obj.launchGeoByDay || {};
    obj.launchRolledUp = obj.launchRolledUp || false;
    obj.meta = obj.meta || { createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
    
    obj.upgrades = obj.upgrades || { total: 0, months: {} };
    obj.usage = obj.usage || {
      javaRuns: { total: 0, months: {} },
      themeChanges: { total: 0, months: {} }
    };
    
    return obj;
  } catch (e) {
    log.warn('store.read.failed', { error: e.message });
    return defaultStore();
  }
}

function writeStore(store) {
  try {
    ensureDir(CONFIG.STORE_FILE);
    const tmp = CONFIG.STORE_FILE + '.tmp';
    const content = JSON.stringify(store, null, 2);
    fs.writeFileSync(tmp, content, 'utf8');
    fs.renameSync(tmp, CONFIG.STORE_FILE);
    log.debug('store.write.success', { file: CONFIG.STORE_FILE });
  } catch (e) {
    log.error('store.write.failed', { file: CONFIG.STORE_FILE, error: e.message });
    throw e;
  }
}

let storeLock = Promise.resolve();
function serialize(fn) {
  storeLock = storeLock.then(async () => { await fn(); }).catch(() => {});
  return storeLock;
}

function storeInit() {
  return serialize(() => {
    log.info('store.init', { file: CONFIG.STORE_FILE });
    try {
      const s = readStore();
      writeStore(s);
      log.info('store.init.success', { file: CONFIG.STORE_FILE });
    } catch (e) {
      log.error('store.init.failed', { file: CONFIG.STORE_FILE, error: e.message });
      throw e;
    }
  });
}

function storeUpsertInstall(ev, geo) {
  return serialize(() => {
    const store = readStore();
    const nowIso = new Date().toISOString();

    const evt = String(ev.evt || '');
    if (!['install.created', 'extension.upgraded', 'java.run.started', 'feature.theme.change'].includes(evt)) return;

    const ext = String(ev.ext || '0.0.0');
    const os = String(ev.os || 'unknown');
    const country = String((geo?.country || 'Unknown')).toUpperCase();

    let t = typeof ev.t === 'number' ? ev.t : Date.parse(ev.t);
    if (!Number.isFinite(t)) t = Date.now();
    if (t < 1e12) t = t * 1000;
    const d = new Date(t);
    const y = String(d.getUTCFullYear());
    const m = String(d.getUTCMonth() + 1).padStart(2, '0');
    const monthKey = `${y}-${m}`;
    const dayKey = new Date(t).toISOString().slice(0,10);

    // Determine if in launch window
    let inLaunchWindow = false;
    try {
      const launchStart = new Date(CONFIG.LAUNCH_DATE + 'T00:00:00Z');
      const launchDuration = Number(CONFIG.LAUNCH_DURATION) || 0;
      const diffDays = Math.floor((new Date(t).getTime() - launchStart.getTime()) / 86400000);
      inLaunchWindow = diffDays >= 0 && diffDays < launchDuration;
    } catch {
      inLaunchWindow = false;
    }

    if (evt === 'install.created') {
      if (inLaunchWindow) {
        store.launchDays = store.launchDays || {};
        store.launchDays[dayKey] = (store.launchDays[dayKey] || 0) + 1;
        
        store.launchVersionsByDay = store.launchVersionsByDay || {};
        store.launchVersionsByDay[dayKey] = store.launchVersionsByDay[dayKey] || {};
        store.launchVersionsByDay[dayKey][ext] = (store.launchVersionsByDay[dayKey][ext] || 0) + 1;
        
        store.launchOsByDay = store.launchOsByDay || {};
        store.launchOsByDay[dayKey] = store.launchOsByDay[dayKey] || {};
        store.launchOsByDay[dayKey][os] = (store.launchOsByDay[dayKey][os] || 0) + 1;
        
        store.launchGeoByDay = store.launchGeoByDay || {};
        store.launchGeoByDay[dayKey] = store.launchGeoByDay[dayKey] || {};
        store.launchGeoByDay[dayKey][country] = (store.launchGeoByDay[dayKey][country] || 0) + 1;
      }

      store.byExt[ext] = (store.byExt[ext] || 0) + 1;
      
      if (!store.months[y]) store.months[y] = {};
      if (!store.months[y][m]) store.months[y][m] = { installs: 0 };
      store.months[y][m].installs += 1;
      
      store.byOs[os] = (store.byOs[os] || 0) + 1;
      store.byCountry[country] = (store.byCountry[country] || 0) + 1;
      
      if (!store.versionsByMonth[monthKey]) store.versionsByMonth[monthKey] = {};
      store.versionsByMonth[monthKey][ext] = (store.versionsByMonth[monthKey][ext] || 0) + 1;
      
      if (!store.osByMonth[monthKey]) store.osByMonth[monthKey] = {};
      store.osByMonth[monthKey][os] = (store.osByMonth[monthKey][os] || 0) + 1;
      
      if (!store.geoByMonth[monthKey]) store.geoByMonth[monthKey] = {};
      store.geoByMonth[monthKey][country] = (store.geoByMonth[monthKey][country] || 0) + 1;

    } else if (evt === 'extension.upgraded') {
      store.upgrades.total++;
      if (!store.upgrades.months[y]) store.upgrades.months[y] = {};
      if (!store.upgrades.months[y][m]) store.upgrades.months[y][m] = { upgrades: 0 };
      store.upgrades.months[y][m].upgrades++;
      store.byExt[ext] = (store.byExt[ext] || 0) + 1;

    } else if (evt === 'java.run.started') {
      store.usage.javaRuns.total++;
      if (!store.usage.javaRuns.months[monthKey]) {
        store.usage.javaRuns.months[monthKey] = { runs: 0 };
      }
      store.usage.javaRuns.months[monthKey].runs++;

    } else if (evt === 'feature.theme.change') {
      store.usage.themeChanges.total++;
      if (!store.usage.themeChanges.months[monthKey]) {
        store.usage.themeChanges.months[monthKey] = { changes: 0 };
      }
      store.usage.themeChanges.months[monthKey].changes++;
    }

    store.meta.updatedAt = nowIso;
    writeStore(store);
  });
}

function storeReadInstallStats(windowMonths = 12) {
  const s = readStore();
  
  let installsTotal = 0;
  const years = Object.keys(s.months).sort();
  for (const y of years) {
    const monthsInYear = Object.keys(s.months[y] || {}).sort();
    for (const m of monthsInYear) {
      installsTotal += Number(s.months[y][m]?.installs || 0);
    }
  }
  
  const months = [];
  const counts = [];
  for (const y of years) {
    const monthsInYear = Object.keys(s.months[y] || {}).sort();
    for (const m of monthsInYear) {
      const ym = `${y}-${m}`;
      months.push(ym);
      counts.push(Number(s.months[y][m]?.installs || 0));
    }
  }

  // Check if in launch window
  let inLaunchWindow = false;
  try {
    const launchStart = new Date(CONFIG.LAUNCH_DATE + 'T00:00:00Z');
    const launchDuration = Number(CONFIG.LAUNCH_DURATION) || 0;
    const diffDaysNow = Math.floor((Date.now() - launchStart.getTime()) / 86400000);
    inLaunchWindow = diffDaysNow >= 0 && diffDaysNow < launchDuration;
  } catch {
    inLaunchWindow = false;
  }

  if (inLaunchWindow && s.launchDays && Object.keys(s.launchDays).length > 0) {
    const days = Object.keys(s.launchDays).sort();
    const dayCounts = days.map(d => Number(s.launchDays[d] || 0));

    return {
      from: days[0] || 'N/A',
      to: days[days.length - 1] || 'N/A',
      windowMonths: months.length,
      totalMonths: months.length,
      installsTotal,
      monthlyInstalls: { months, counts },
      dailyInstalls: { dates: days, counts: dayCounts },
      byExt: s.byExt,
      byOs: s.byOs,
      byCountry: s.byCountry,
      versionTimeline: { months: days, versions: Object.keys(s.byExt), data: {} },
      osTimeline: { months: days, osTypes: Object.keys(s.byOs), data: {} },
      geoTimeline: { months: days, countries: Object.keys(s.byCountry), data: {} },
      seasonalPatterns: [],
      geographicGrowth: {},
      osGeoPreferences: {},
      versionMigration: {},
      growthTrajectory: null,
      platformTrends: {},
      upgradeAnalytics: { total: s.upgrades?.total || 0, months: [], timeline: [] },
      usageAnalytics: { 
        javaRuns: { total: s.usage?.javaRuns?.total || 0 }, 
        themeChanges: { total: s.usage?.themeChanges?.total || 0 } 
      },
      retentionMetrics: {
        upgradeRetention: 0,
        usageRetention: 0,
        overallRetention: 0,
        retentionGrade: 'N/A'
      },
      updatedAt: s.meta.updatedAt,
      file: CONFIG.STORE_FILE
    };
  }

  return {
    from: months[0] || 'N/A',
    to: months[months.length - 1] || 'N/A',
    windowMonths: months.length,
    totalMonths: months.length,
    installsTotal,
    monthlyInstalls: { months, counts },
    dailyInstalls: { dates: months, counts },
    byExt: s.byExt,
    byOs: s.byOs,
    byCountry: s.byCountry,
    versionTimeline: { months, versions: Object.keys(s.byExt), data: {} },
    osTimeline: { months, osTypes: Object.keys(s.byOs), data: {} },
    geoTimeline: { months, countries: Object.keys(s.byCountry), data: {} },
    seasonalPatterns: [],
    geographicGrowth: {},
    osGeoPreferences: {},
    versionMigration: {},
    growthTrajectory: null,
    platformTrends: {},
    upgradeAnalytics: { total: s.upgrades?.total || 0, months: [], timeline: [] },
    usageAnalytics: { 
      javaRuns: { total: s.usage?.javaRuns?.total || 0 }, 
      themeChanges: { total: s.usage?.themeChanges?.total || 0 } 
    },
    retentionMetrics: {
      upgradeRetention: 0,
      usageRetention: 0,
      overallRetention: 0,
      retentionGrade: 'N/A'
    },
    updatedAt: s.meta.updatedAt,
    file: CONFIG.STORE_FILE
  };
}

// Event validation
const EVENTS = new Set([
  'install.created',
  'extension.upgraded',
  'java.run.started',
  'feature.theme.change'
]);

function normalizeEvent(e) {
  if (!e) return null;
  const out = { ...e };
  if (typeof out.t === 'string') out.t = Number(out.t);
  if (typeof out.t !== 'number' || !isFinite(out.t)) return null;
  if (out.t < 1e12) out.t = out.t * 1000;

  if (typeof out.ext !== 'string') out.ext = '0.0.0';
  if (typeof out.vscode !== 'string') out.vscode = '0.0.0';
  if (typeof out.os !== 'string') out.os = 'unknown';

  if (typeof out.anon !== 'string' || !/^[a-f0-9]{32}$/.test(out.anon)) return null;
  if (typeof out.evt !== 'string' || !EVENTS.has(out.evt)) return null;
  if (out.ext.length > 30 || out.vscode.length > 30 || out.os.length > 30) return null;
  return out;
}

function validateEvent(e) {
  if (!e || typeof e.t !== 'number') return false;
  if (typeof e.anon !== 'string' || !/^[a-f0-9]{32}$/.test(e.anon)) return false;
  if (typeof e.evt !== 'string' || !EVENTS.has(e.evt)) return false;
  if (typeof e.ext !== 'string' || e.ext.length > 30) return false;
  if (typeof e.vscode !== 'string' || e.vscode.length > 30) return false;
  if (typeof e.os !== 'string' || e.os.length > 30) return false;
  return true;
}

// IP and geo utilities
function h(req, name) {
  const v = req.headers[name.toLowerCase()];
  return Array.isArray(v) ? v[0] : v;
}

function isPrivate(ip) {
  if (!ip) return true;
  return /^(10\.|127\.|192\.168\.|172\.(1[6-9]|2\d|3[0-1])\.|::1|fc00:|fe80:|fd00:)/.test(ip);
}

function parseForwardedFor(v) {
  if (!v) return [];
  return v.split(',').map(s => s.trim()).filter(Boolean);
}

function pickClientIp(req) {
  const chain = [
    h(req,'cf-connecting-ip'),
    h(req,'x-client-ip'),
    h(req,'x-real-ip'),
    ...parseForwardedFor(h(req,'x-forwarded-for')),
    req.connection.remoteAddress
  ].filter(Boolean);
  for (const ip of chain) { if (!isPrivate(ip)) return ip; }
  return chain[0] || req.connection.remoteAddress;
}

function extractGeoFromHeaders(req) {
  const country = h(req,'cf-ipcountry') || h(req,'x-geo-country') || h(req,'x-country-code') || h(req,'x-appengine-country') || h(req,'fastly-country-code') || undefined;
  const region = h(req,'cf-region-code') || h(req,'x-geo-region') || h(req,'x-appengine-region') || h(req,'x-region') || undefined;
  const out = {};
  if (country) out.country = country.toUpperCase();
  if (region) out.region = region.toUpperCase();
  return out;
}

function resolveGeo(req, ip) {
  const hdr = extractGeoFromHeaders(req);
  if (hdr.country) {
    return { country: hdr.country, region: hdr.region || '' };
  }
  const g = lookup(ip);
  return { country: g?.country || '', region: g?.region || '' };
}

// HTTP utilities
function sendJSON(res, data, statusCode = 200) {
  res.writeHead(statusCode, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
  });
  res.end(JSON.stringify(data, null, 2));
}

function readBody(req, callback) {
  let body = '';
  req.on('data', chunk => {
    body += chunk.toString();
    if (body.length > CONFIG.MAX_BODY) {
      callback(new Error('Body too large'), null);
      return;
    }
  });
  req.on('end', () => {
    callback(null, body);
  });
  req.on('error', err => {
    callback(err, null);
  });
}

// Route handlers
function handleStatsInstall(req, res) {
  log.info('req', { method: req.method, url: req.url, ip: req.connection.remoteAddress });
  
  try {
    const response = storeReadInstallStats(12);
    log.info('res', { method: req.method, url: req.url, status: 200 });
    sendJSON(res, response);
  } catch (e) {
    log.error('stats.install.error', { err: e.message });
    log.info('res', { method: req.method, url: req.url, status: 500 });
    sendJSON(res, { error: 'server_error' }, 500);
  }
}

function handleHealth(req, res) {
  sendJSON(res, {
    ok: true,
    ts: Date.now(),
    message: 'Enhanced HTTP server running',
    node: process.version,
    env: process.env.NODE_ENV || 'development'
  });
}

function handleTelemetry(req, res) {
  log.info('req', { method: req.method, url: req.url, ip: req.connection.remoteAddress });
  
  readBody(req, async (err, body) => {
    if (err) {
      log.error('telemetry.body.error', { err: err.message });
      log.info('res', { method: req.method, url: req.url, status: 400 });
      sendJSON(res, { error: 'bad_request' }, 400);
      return;
    }

    try {
      const raw = JSON.parse(body);
      const schema = typeof raw?.schema === 'string' ? raw.schema.trim().toLowerCase() : '';
      if (schema !== 'jwc.v1') {
        log.warn('schema not supported', { got: raw?.schema });
        log.info('res', { method: req.method, url: req.url, status: 400 });
        sendJSON(res, { error: 'schema_unsupported', expected: 'jwc.v1' }, 400);
        return;
      }

      const ip = pickClientIp(req);
      const geo = resolveGeo(req, ip);

      const batchRaw = Array.isArray(raw?.batch) ? raw.batch : [raw];
      let accepted = 0, skipped = 0;
      
      for (const evRaw of batchRaw) {
        const ev = normalizeEvent(evRaw);
        if (!ev || !validateEvent(ev)) { 
          skipped++; 
          log.warn('event skipped: invalid', { evRaw }); 
          continue; 
        }
        try {
          await storeUpsertInstall(ev, geo);
          accepted++;
          log.info('ingest: inserted', { ev, geo });
        } catch (e) {
          log.error('db insert failed', { err: e.message, ev });
          skipped++; 
          continue;
        }
      }
      
      log.info('ingest: batch result', { accepted, skipped });
      log.info('res', { method: req.method, url: req.url, status: 200 });
      sendJSON(res, { ok: true, accepted, skipped });
    } catch (e) {
      log.error('telemetry.error', { err: e.message });
      log.info('res', { method: req.method, url: req.url, status: 500 });
      sendJSON(res, { error: 'server_error' }, 500);
    }
  });
}

function handleDebugEnv(req, res) {
  sendJSON(res, {
    node: process.version,
    env: {
      NODE_ENV: process.env.NODE_ENV,
      PORT: process.env.PORT,
      PASSENGER_APP_ENV: process.env.PASSENGER_APP_ENV,
      LOG_FILE: process.env.LOG_FILE,
      LOG_LEVEL: process.env.LOG_LEVEL,
      GEO_DB: process.env.GEO_DB,
      GEO_MMDB: process.env.GEO_MMDB
    },
    config: {
      STORE_FILE: CONFIG.STORE_FILE,
      LOG_DIR: CONFIG.LOG_DIR,
      LAUNCH_DATE: CONFIG.LAUNCH_DATE,
      LAUNCH_DURATION: CONFIG.LAUNCH_DURATION
    },
    timestamp: new Date().toISOString()
  });
}

function handleDebugLogping(req, res) {
  log.info('debug.logping', { when: new Date().toISOString() });
  sendJSON(res, { ok: true, file: LOG_FILE, level: LOG_LEVEL });
}

// Main server
const server = http.createServer((req, res) => {
  const parsedUrl = url.parse(req.url, true);
  const path = parsedUrl.pathname;
  const method = req.method;

  // Handle CORS preflight
  if (method === 'OPTIONS') {
    res.writeHead(200, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization'
    });
    res.end();
    return;
  }

  // Route handling
  if (method === 'GET' && path === '/health') {
    handleHealth(req, res);
  } else if (method === 'GET' && path === '/stats/install') {
    handleStatsInstall(req, res);
  } else if (method === 'POST' && path === '/t') {
    handleTelemetry(req, res);
  } else if (method === 'GET' && path === '/debug/env') {
    handleDebugEnv(req, res);
  } else if (method === 'GET' && path === '/debug/logping') {
    handleDebugLogping(req, res);
  } else if (method === 'GET' && path === '/stats') {
    log.info('res', { method: req.method, url: req.url, status: 410 });
    sendJSON(res, { error: 'gone', note: 'Use /stats/install (JSON store)' }, 410);
  } else if (method === 'GET' && path === '/dbhealth') {
    sendJSON(res, { enabled: false, ok: false, note: 'DB removed; using JSON store', file: CONFIG.STORE_FILE });
  } else {
    // Catch all
    log.info('catchall', { method: method, url: path });
    sendJSON(res, {
      message: 'Enhanced HTTP server - route not found',
      method: method,
      url: path,
      timestamp: new Date().toISOString()
    });
  }
});

// Initialize and start
async function main() {
  const port = process.env.PORT || 3000;
  const isHosting = process.env.NODE_ENV === 'production' || process.env.PASSENGER_APP_ENV;

  console.log('=== Enhanced Telemetry Server ===');
  console.log('Node.js version:', process.version);
  console.log('Environment:', {
    NODE_ENV: process.env.NODE_ENV,
    PASSENGER_APP_ENV: process.env.PASSENGER_APP_ENV,
    PORT: process.env.PORT,
    isHosting
  });

  log.info('boot.start', { port: CONFIG.PORT });

  try {
    log.info('boot.geo.start');
    initGeo();
    log.info('boot.geo.done');
  } catch (e) {
    log.warn('boot.geo.failed', { error: e.message });
  }

  try {
    log.info('boot.store.start');
    await storeInit();
    log.info('boot.store.done', { file: CONFIG.STORE_FILE });
  } catch (e) {
    log.error('boot.store.failed', { error: e.message });
    // Continue without store for now
  }

  if (isHosting) {
    log.info('boot.hosting.detected', { env: process.env.NODE_ENV, passenger: process.env.PASSENGER_APP_ENV });
    console.log('✓ Enhanced server configured for hosting environment');
    module.exports = server;
  } else {
    server.listen(port, () => {
      log.info('boot.listen.ok', { port });
      console.log(`✓ Enhanced HTTP server listening on port ${port}`);
    });
  }
}

main().catch(err => {
  log.error('fatal', { error: err.message });
  console.error('FATAL ERROR:', err);
  process.exit(1);
});
