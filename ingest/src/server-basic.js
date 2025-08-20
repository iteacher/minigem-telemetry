const http = require('http');
const url = require('url');

// Simple memory store with test data
const memoryStore = {
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

function sendJSON(res, data, statusCode = 200) {
  res.writeHead(statusCode, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
  });
  res.end(JSON.stringify(data, null, 2));
}

function handleStatsInstall(req, res) {
  console.log('Stats request received from:', req.connection.remoteAddress);
  
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
      data: {} 
    },
    osTimeline: { 
      months: memoryStore.months, 
      osTypes: Object.keys(memoryStore.byOs), 
      data: {} 
    },
    geoTimeline: { 
      months: memoryStore.months, 
      countries: Object.keys(memoryStore.byCountry), 
      data: {} 
    },
    seasonalPatterns: [],
    geographicGrowth: {},
    osGeoPreferences: {},
    versionMigration: {},
    growthTrajectory: null,
    platformTrends: {},
    upgradeAnalytics: { total: 0, months: [], timeline: [] },
    usageAnalytics: { 
      javaRuns: { total: 0 }, 
      themeChanges: { total: 0 } 
    },
    retentionMetrics: {
      upgradeRetention: 0,
      usageRetention: 0,
      overallRetention: 0,
      retentionGrade: 'N/A'
    },
    updatedAt: memoryStore.updatedAt,
    file: 'memory-store-http',
    note: 'Basic HTTP server for Node.js 16 compatibility'
  };
  
  console.log('Sending stats response');
  sendJSON(res, response);
}

function handleHealth(req, res) {
  sendJSON(res, {
    ok: true,
    ts: Date.now(),
    message: 'Basic HTTP server running',
    node: process.version,
    env: process.env.NODE_ENV || 'development'
  });
}

function handleTelemetry(req, res) {
  let body = '';
  req.on('data', chunk => {
    body += chunk.toString();
  });
  
  req.on('end', () => {
    try {
      const data = JSON.parse(body);
      console.log('Telemetry received:', JSON.stringify(data));
      sendJSON(res, { ok: true, accepted: 1, skipped: 0 });
    } catch (e) {
      console.error('Telemetry error:', e);
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
    timestamp: new Date().toISOString()
  });
}

const server = http.createServer((req, res) => {
  const parsedUrl = url.parse(req.url, true);
  const path = parsedUrl.pathname;
  const method = req.method;

  console.log(`${method} ${path}`);

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
  } else {
    // Catch all
    console.log('Catch-all route:', method, path);
    sendJSON(res, {
      message: 'Basic HTTP server - route not found',
      method: method,
      url: path,
      timestamp: new Date().toISOString()
    });
  }
});

const port = process.env.PORT || 3000;
const isHosting = process.env.NODE_ENV === 'production' || process.env.PASSENGER_APP_ENV;

console.log('=== Basic HTTP Telemetry Server ===');
console.log('Node.js version:', process.version);
console.log('Environment:', {
  NODE_ENV: process.env.NODE_ENV,
  PASSENGER_APP_ENV: process.env.PASSENGER_APP_ENV,
  PORT: process.env.PORT,
  isHosting
});

if (isHosting) {
  console.log('✓ Hosting environment detected');
  // For hosting environments that manage the port
  module.exports = server;
} else {
  // Local development
  server.listen(port, () => {
    console.log(`✓ Basic HTTP server listening on port ${port}`);
  });
}
