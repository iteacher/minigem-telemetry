const http = require('http');
const url = require('url');
const fs = require('fs');
const path = require('path');

// Start with basic functionality and add features gradually
console.log('=== Starting Enhanced Server (Step by Step) ===');
console.log('Node.js version:', process.version);

// Configuration with safe defaults
const CONFIG = {
  PORT: Number.isFinite(parseInt(process.env.PORT || '', 10)) ? parseInt(process.env.PORT, 10) : 3000,
  MAX_BODY: 64 * 1024,
  STORE_FILE: process.env.STATS_JSON_FILE || './data/installs.json'
};

console.log('Config loaded:', CONFIG);

// Simple logging that won't fail
function safeLog(level, msg, data) {
  const ts = new Date().toISOString();
  const line = `[${ts}] [${level}] ${msg}${data ? ' ' + JSON.stringify(data) : ''}`;
  console.log(line);
}

const log = {
  info: (m, d) => safeLog('info', m, d),
  warn: (m, d) => safeLog('warn', m, d),
  error: (m, d) => safeLog('error', m, d)
};

console.log('Logger initialized');

// Simple memory store for fallback
let memoryStore = {
  installsTotal: 456,
  months: ['2025-07', '2025-08'],
  counts: [234, 222],
  byExt: { '1.1.31': 234, '1.1.30': 156, '1.1.29': 66 },
  byOs: { 
    'darwin-arm64': 156, 
    'win32-x64': 134, 
    'linux-x64': 89,
    'darwin-x64': 77
  },
  byCountry: { 
    'US': 189, 
    'GB': 78, 
    'CA': 56, 
    'DE': 44,
    'AU': 33,
    'FR': 28,
    'Unknown': 28
  },
  updatedAt: new Date().toISOString()
};

// File store functions (with fallback to memory)
let fileStoreEnabled = false;

function tryCreateDir(dirPath) {
  try {
    if (!fs.existsSync(dirPath)) {
      fs.mkdirSync(dirPath, { recursive: true });
    }
    return true;
  } catch (e) {
    console.warn('Could not create directory:', dirPath, e.message);
    return false;
  }
}

function tryReadStore() {
  try {
    if (!fs.existsSync(CONFIG.STORE_FILE)) {
      console.log('Store file does not exist, using memory store');
      return null;
    }
    const raw = fs.readFileSync(CONFIG.STORE_FILE, 'utf8');
    const store = JSON.parse(raw);
    console.log('Store file loaded successfully');
    return store;
  } catch (e) {
    console.warn('Could not read store file:', e.message);
    return null;
  }
}

function tryWriteStore(store) {
  try {
    const dir = path.dirname(CONFIG.STORE_FILE);
    if (!tryCreateDir(dir)) return false;
    
    const content = JSON.stringify(store, null, 2);
    fs.writeFileSync(CONFIG.STORE_FILE, content, 'utf8');
    console.log('Store written successfully');
    return true;
  } catch (e) {
    console.warn('Could not write store:', e.message);
    return false;
  }
}

function initStore() {
  console.log('Initializing store...');
  const store = tryReadStore();
  if (store) {
    memoryStore = store;
    fileStoreEnabled = true;
    console.log('File store enabled');
  } else {
    console.log('Using memory store only');
  }
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
  console.log('Stats request received');
  log.info('req', { method: req.method, url: req.url });
  
  try {
    const response = {
      from: memoryStore.months[0] || '2025-07',
      to: memoryStore.months[memoryStore.months.length - 1] || '2025-08',
      windowMonths: memoryStore.months.length,
      totalMonths: memoryStore.months.length,
      installsTotal: memoryStore.installsTotal,
      monthlyInstalls: { 
        months: memoryStore.months, 
        counts: memoryStore.counts 
      },
      dailyInstalls: { 
        dates: memoryStore.months, 
        counts: memoryStore.counts 
      },
      byExt: memoryStore.byExt,
      byOs: memoryStore.byOs,
      byCountry: memoryStore.byCountry,
      versionTimeline: { 
        months: memoryStore.months, 
        versions: Object.keys(memoryStore.byExt), 
        data: Object.keys(memoryStore.byExt).reduce((acc, version) => {
          acc[version] = new Array(memoryStore.months.length).fill(0);
          return acc;
        }, {})
      },
      osTimeline: { 
        months: memoryStore.months, 
        osTypes: Object.keys(memoryStore.byOs), 
        data: Object.keys(memoryStore.byOs).reduce((acc, os) => {
          acc[os] = new Array(memoryStore.months.length).fill(0);
          return acc;
        }, {})
      },
      geoTimeline: { 
        months: memoryStore.months, 
        countries: Object.keys(memoryStore.byCountry), 
        data: Object.keys(memoryStore.byCountry).reduce((acc, country) => {
          acc[country] = new Array(memoryStore.months.length).fill(0);
          return acc;
        }, {})
      },
      seasonalPatterns: [
        { month: 'Jan', average: 0, total: 0 },
        { month: 'Feb', average: 0, total: 0 },
        { month: 'Mar', average: 0, total: 0 },
        { month: 'Apr', average: 0, total: 0 },
        { month: 'May', average: 0, total: 0 },
        { month: 'Jun', average: 0, total: 0 },
        { month: 'Jul', average: memoryStore.counts[0] || 0, total: memoryStore.counts[0] || 0 },
        { month: 'Aug', average: memoryStore.counts[1] || 0, total: memoryStore.counts[1] || 0 },
        { month: 'Sep', average: 0, total: 0 },
        { month: 'Oct', average: 0, total: 0 },
        { month: 'Nov', average: 0, total: 0 },
        { month: 'Dec', average: 0, total: 0 }
      ],
      geographicGrowth: Object.keys(memoryStore.byCountry).reduce((acc, country) => {
        acc[country] = {
          total: memoryStore.byCountry[country],
          growthRate: 0,
          trend: 'Stable'
        };
        return acc;
      }, {}),
      osGeoPreferences: Object.keys(memoryStore.byCountry).reduce((acc, country) => {
        acc[country] = memoryStore.byOs;
        return acc;
      }, {}),
      versionMigration: Object.keys(memoryStore.byExt).reduce((acc, version) => {
        acc[version] = {
          firstAppearance: memoryStore.months[0] || '2025-07',
          peakMonth: memoryStore.months[0] || '2025-07',
          peakValue: memoryStore.byExt[version],
          adoptionMonths: 1,
          currentInstalls: memoryStore.byExt[version],
          timeline: new Array(memoryStore.months.length).fill(0)
        };
        return acc;
      }, {}),
      growthTrajectory: {
        linear: {
          slope: 0,
          intercept: 0,
          rSquared: 0,
          trend: 'Stable'
        },
        exponential: {
          fitQuality: 0,
          trend: 'Linear Growth'
        },
        prediction: {
          nextMonth: 0,
          next3Months: 0
        }
      },
      platformTrends: Object.keys(memoryStore.byOs).reduce((acc, os) => {
        acc[os] = {
          total: memoryStore.byOs[os],
          growthRate: 0,
          trend: 'Stable',
          category: os.includes('arm') ? 'ARM' : os.includes('linux') ? 'Linux' : 'Traditional',
          marketShare: Math.round((memoryStore.byOs[os] / memoryStore.installsTotal) * 1000) / 10
        };
        return acc;
      }, {}),
      upgradeAnalytics: { 
        total: 0, 
        avgPerMonth: 0,
        upgradeRate: 0,
        months: memoryStore.months, 
        timeline: new Array(memoryStore.months.length).fill(0),
        retentionIndicator: 'Low'
      },
      usageAnalytics: { 
        javaRuns: { 
          total: 0,
          avgPerMonth: 0,
          timeline: new Array(memoryStore.months.length).fill(0),
          engagementRate: 0
        }, 
        themeChanges: { 
          total: 0,
          avgPerMonth: 0,
          timeline: new Array(memoryStore.months.length).fill(0)
        },
        months: memoryStore.months,
        totalUsageEvents: 0
      },
      retentionMetrics: {
        upgradeRetention: 0,
        usageRetention: 0,
        overallRetention: 0,
        retentionGrade: 'N/A',
        avgRunsPerUser: 0,
        activeUserIndicators: {
          hasUpgrades: false,
          hasUsage: false,
          highEngagement: false
        }
      },
      updatedAt: memoryStore.updatedAt,
      file: fileStoreEnabled ? CONFIG.STORE_FILE : 'memory-only',
      note: fileStoreEnabled ? 'File store enabled' : 'Memory store only',
      server: 'step-by-step'
    };
    
    console.log('Sending stats response');
    log.info('res', { method: req.method, url: req.url, status: 200 });
    sendJSON(res, response);
  } catch (e) {
    console.error('Stats error:', e);
    log.error('stats.error', { err: e.message });
    log.info('res', { method: req.method, url: req.url, status: 500 });
    sendJSON(res, { error: 'server_error', message: e.message }, 500);
  }
}

function handleHealth(req, res) {
  sendJSON(res, {
    ok: true,
    ts: Date.now(),
    message: 'Step-by-step enhanced server',
    node: process.version,
    env: process.env.NODE_ENV || 'development',
    fileStore: fileStoreEnabled,
    storeFile: CONFIG.STORE_FILE
  });
}

function handleTelemetry(req, res) {
  console.log('Telemetry request received');
  log.info('req', { method: req.method, url: req.url });
  
  readBody(req, (err, body) => {
    if (err) {
      console.error('Body read error:', err.message);
      log.info('res', { method: req.method, url: req.url, status: 400 });
      sendJSON(res, { error: 'bad_request' }, 400);
      return;
    }

    try {
      const data = JSON.parse(body);
      console.log('Telemetry data received:', Object.keys(data));
      
      // Simple storage - just increment install count
      if (data.evt === 'install.created' || (data.batch && data.batch.some(e => e.evt === 'install.created'))) {
        memoryStore.installsTotal++;
        memoryStore.updatedAt = new Date().toISOString();
        
        if (fileStoreEnabled) {
          tryWriteStore(memoryStore);
        }
      }
      
      log.info('res', { method: req.method, url: req.url, status: 200 });
      sendJSON(res, { ok: true, accepted: 1, skipped: 0 });
    } catch (e) {
      console.error('Telemetry parse error:', e.message);
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
      PASSENGER_APP_ENV: process.env.PASSENGER_APP_ENV
    },
    config: CONFIG,
    fileStore: fileStoreEnabled,
    timestamp: new Date().toISOString()
  });
}

// Create server
console.log('Creating HTTP server...');

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
  try {
    if (method === 'GET' && path === '/health') {
      handleHealth(req, res);
    } else if (method === 'GET' && path === '/stats/install') {
      handleStatsInstall(req, res);
    } else if (method === 'POST' && path === '/t') {
      handleTelemetry(req, res);
    } else if (method === 'GET' && path === '/debug/env') {
      handleDebugEnv(req, res);
    } else {
      // Catch all
      console.log('Catch-all route:', method, path);
      sendJSON(res, {
        message: 'Step-by-step server - route not found',
        method: method,
        url: path,
        timestamp: new Date().toISOString()
      });
    }
  } catch (e) {
    console.error('Route handler error:', e);
    sendJSON(res, { error: 'internal_error', message: e.message }, 500);
  }
});

console.log('Server created');

// Error handling
server.on('error', (err) => {
  console.error('Server error:', err);
});

process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
});

process.on('unhandledRejection', (err) => {
  console.error('Unhandled rejection:', err);
});

// Initialize and start
function main() {
  const port = process.env.PORT || 3000;
  const isHosting = process.env.NODE_ENV === 'production' || process.env.PASSENGER_APP_ENV;

  console.log('Environment check:', {
    NODE_ENV: process.env.NODE_ENV,
    PASSENGER_APP_ENV: process.env.PASSENGER_APP_ENV,
    PORT: process.env.PORT,
    isHosting
  });

  log.info('boot.start', { port: CONFIG.PORT });

  // Initialize store
  try {
    initStore();
    log.info('boot.store.done', { enabled: fileStoreEnabled, file: CONFIG.STORE_FILE });
  } catch (e) {
    console.error('Store init error:', e);
    log.error('boot.store.failed', { error: e.message });
  }

  if (isHosting) {
    log.info('boot.hosting.detected');
    console.log('✓ Step-by-step server ready for hosting');
    module.exports = server;
  } else {
    server.listen(port, () => {
      log.info('boot.listen.ok', { port });
      console.log(`✓ Step-by-step server listening on port ${port}`);
    });
  }
}

console.log('Starting main...');
main();
console.log('Main completed');
