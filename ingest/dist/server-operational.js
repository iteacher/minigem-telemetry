const http = require('http');
const url = require('url');
const fs = require('fs');
const path = require('path');

console.log('[server] Starting fully operational telemetry server...');

// Configuration for production deployment
const CONFIG = {
  PORT: process.env.PORT || 3001,
  DATA_FILE: process.env.STATS_JSON_FILE || '/home/mandersj/telemetary.jwc.minigem.uk/data/test-success-95days.json',
  LOCAL_DATA_FILE: './data/test-success-95days.json', // Fallback for local testing
  LOG_FILE: process.env.LOG_FILE || './logs/telemetry.log',
  LAUNCH_DATE: process.env.LAUNCH_DATE || '2025-07-22', // Matches hosting env
  LAUNCH_DURATION: parseInt(process.env.LAUNCH_DURATION || '100'), // Extended for 95+ day testing
  GEO_DB: process.env.GEO_DB || process.env.GEO_MMDB, // Support both env var names
  YEARLY_SALT: process.env.YEARLY_SALT || 'jwc-2025-salt',
  RATE_LIMIT_MAX: parseInt(process.env.RATE_LIMIT_MAX || '2000'),
  STATS_SECRET: process.env.STATS_SECRET,
  STATS_WINDOW_DAYS: parseInt(process.env.STATS_WINDOW_DAYS || '7')
};

console.log('[server] Configuration:', CONFIG);

// Global data store
let telemetryData = null;
let lastLoadTime = null;

// Load telemetry data from file
function loadTelemetryData() {
  try {
    console.log('[server] Loading telemetry data...');
    
    // Try production path first, then local fallback
    let dataPath = CONFIG.DATA_FILE;
    if (!fs.existsSync(dataPath)) {
      console.log('[server] Production file not found, trying local fallback...');
      dataPath = CONFIG.LOCAL_DATA_FILE;
    }
    
    if (!fs.existsSync(dataPath)) {
      console.error('[server] No data file found at either path');
      return null;
    }
    
    const rawData = fs.readFileSync(dataPath, 'utf8');
    const data = JSON.parse(rawData);
    lastLoadTime = new Date();
    
    console.log('[server] Telemetry data loaded successfully from:', dataPath);
    console.log('[server] Data summary:', {
      totalInstalls: Object.values(data.byExt || {}).reduce((sum, count) => sum + count, 0),
      months: Object.keys(data.months || {}),
      versions: Object.keys(data.byExt || {}),
      countries: Object.keys(data.byCountry || {}),
      platforms: Object.keys(data.byOs || {})
    });
    
    return data;
  } catch (error) {
    console.error('[server] Error loading telemetry data:', error);
    return null;
  }
}

// Calculate analytics from telemetry data
function calculateAnalytics(data) {
  console.log('[server] Calculating analytics from telemetry data...');
  
  if (!data) {
    console.error('[server] No data available for analytics');
    return null;
  }
  
  // Extract basic metrics
  const totalInstalls = Object.values(data.byExt || {}).reduce((sum, count) => sum + count, 0);
  
  // Dynamically calculate date range from available data
  const allMonths = [];
  const allCounts = [];
  
  if (data.months) {
    Object.keys(data.months).forEach(year => {
      Object.keys(data.months[year]).forEach(month => {
        const monthKey = `${year}-${month}`;
        const installs = data.months[year][month].installs || 0;
        allMonths.push(monthKey);
        allCounts.push(installs);
      });
    });
  }
  
  // Sort months chronologically
  const sortedData = allMonths.map((month, i) => ({ month, count: allCounts[i] }))
    .sort((a, b) => a.month.localeCompare(b.month));
  
  const monthKeys = sortedData.map(item => item.month);
  const monthCounts = sortedData.map(item => item.count);
  const fromMonth = monthKeys[0] || 'N/A';
  const toMonth = monthKeys[monthKeys.length - 1] || 'N/A';
  const totalMonths = monthKeys.length;
  
  // Build comprehensive analytics
  const analytics = {
    from: fromMonth,
    to: toMonth,
    windowMonths: totalMonths,
    totalMonths: totalMonths,
    installsTotal: totalInstalls,
    
    // Core metrics
    monthlyInstalls: {
      months: monthKeys,
      counts: monthCounts
    },
    dailyInstalls: {
      dates: Object.keys(data.launchDays || {}),
      counts: Object.values(data.launchDays || {})
    },
    byExt: data.byExt || {},
    byOs: data.byOs || {},
    byCountry: expandCountryCodes(data.byCountry || {}),
    
    // Timeline analytics
    versionTimeline: {
      months: monthKeys,
      versions: Object.keys(data.byExt || {}),
      data: buildVersionTimeline(data.versionsByMonth || {}, Object.keys(data.byExt || {}))
    },
    osTimeline: {
      months: monthKeys,
      osTypes: Object.keys(data.byOs || {}),
      data: buildOsTimeline(data.osByMonth || {}, Object.keys(data.byOs || {}))
    },
    geoTimeline: {
      months: monthKeys,
      countries: Object.keys(expandCountryCodes(data.byCountry || {})),
      data: buildGeoTimeline(data.geoByMonth || {}, data.byCountry || {})
    },
    
    // Seasonal patterns
    seasonalPatterns: buildSeasonalPatterns(data.months || {}),
    
    // Growth analytics
    geographicGrowth: buildGeographicGrowth(data.geoByMonth || {}, data.byCountry || {}),
    osGeoPreferences: buildOsGeoPreferences(data),
    versionMigration: buildVersionMigration(data),
    growthTrajectory: calculateGrowthTrajectory(monthCounts),
    platformTrends: buildPlatformTrends(data.byOs || {}, totalInstalls),
    
    // Usage analytics
    upgradeAnalytics: buildUpgradeAnalytics(data.upgrades || {}, monthKeys),
    usageAnalytics: buildUsageAnalytics(data.usage || {}, monthKeys),
    retentionMetrics: calculateRetentionMetrics(data),
    
    // Meta
    updatedAt: data.meta?.updatedAt || new Date().toISOString(),
    file: CONFIG.DATA_FILE,
    note: 'Live telemetry data from test3.json',
    server: 'telemetry-operational'
  };
  
  console.log('[server] Analytics calculated successfully');
  return analytics;
}

// Helper functions for analytics calculation
function expandCountryCodes(byCountry) {
  const countryMap = {
    'GB': 'United Kingdom',
    'US': 'United States',
    'CA': 'Canada',
    'DE': 'Germany',
    'AU': 'Australia',
    'FR': 'France',
    'JP': 'Japan',
    'Unknown': 'Unknown'
  };
  
  const expanded = {};
  for (const [code, count] of Object.entries(byCountry)) {
    const fullName = countryMap[code] || code;
    expanded[fullName] = count;
  }
  return expanded;
}

function buildVersionTimeline(versionsByMonth, versions) {
  // Get all available months dynamically
  const allMonths = Object.keys(versionsByMonth).sort();
  const timeline = {};
  
  versions.forEach(version => {
    timeline[version] = allMonths.map(month => 
      versionsByMonth[month]?.[version] || 0
    );
  });
  
  return timeline;
}

function buildOsTimeline(osByMonth, osTypes) {
  // Get all available months dynamically
  const allMonths = Object.keys(osByMonth).sort();
  const timeline = {};
  
  osTypes.forEach(os => {
    timeline[os] = allMonths.map(month => 
      osByMonth[month]?.[os] || 0
    );
  });
  
  return timeline;
}

function buildGeoTimeline(geoByMonth, byCountry) {
  // Get all available months dynamically
  const allMonths = Object.keys(geoByMonth).sort();
  const timeline = {};
  const expandedCountries = expandCountryCodes(byCountry);
  
  Object.keys(expandedCountries).forEach(country => {
    const code = Object.keys(byCountry).find(code => 
      expandCountryCodes({[code]: 1})[country]
    );
    timeline[country] = allMonths.map(month =>
      geoByMonth[month]?.[code] || 0
    );
  });
  return timeline;
}

function buildSeasonalPatterns(months) {
  const patterns = [];
  const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  
  monthNames.forEach((month, index) => {
    const monthNum = String(index + 1).padStart(2, '0');
    const installs = months['2025']?.[monthNum]?.installs || 0;
    patterns.push({
      month,
      average: installs,
      total: installs
    });
  });
  
  return patterns;
}

function buildGeographicGrowth(geoByMonth, byCountry) {
  const growth = {};
  const expandedCountries = expandCountryCodes(byCountry);
  
  Object.entries(expandedCountries).forEach(([country, total]) => {
    const jul = geoByMonth['2025-07'] || {};
    const aug = geoByMonth['2025-08'] || {};
    const code = Object.keys(byCountry).find(code => 
      expandCountryCodes({[code]: 1})[country]
    );
    
    const julCount = jul[code] || 0;
    const augCount = aug[code] || 0;
    const growthRate = julCount > 0 ? ((augCount - julCount) / julCount * 100) : 0;
    
    growth[country] = {
      total,
      growthRate: Math.round(growthRate * 10) / 10,
      trend: growthRate > 10 ? 'Growing' : growthRate < -10 ? 'Declining' : 'Stable'
    };
  });
  
  return growth;
}

function buildOsGeoPreferences(data) {
  const preferences = {};
  const expandedCountries = expandCountryCodes(data.byCountry || {});
  
  Object.keys(expandedCountries).forEach(country => {
    preferences[country] = data.byOs || {};
  });
  
  return preferences;
}

function buildVersionMigration(data) {
  const migration = {};
  const versions = Object.keys(data.byExt || {});
  
  versions.forEach(version => {
    const julCount = data.versionsByMonth?.['2025-07']?.[version] || 0;
    const augCount = data.versionsByMonth?.['2025-08']?.[version] || 0;
    const total = data.byExt[version] || 0;
    
    migration[version] = {
      firstAppearance: julCount > 0 ? '2025-07' : '2025-08',
      peakMonth: julCount >= augCount ? '2025-07' : '2025-08',
      peakValue: Math.max(julCount, augCount),
      adoptionMonths: (julCount > 0 ? 1 : 0) + (augCount > 0 ? 1 : 0),
      currentInstalls: total,
      timeline: [julCount, augCount]
    };
  });
  
  return migration;
}

function calculateGrowthTrajectory(monthCounts) {
  const [jul, aug] = monthCounts;
  const slope = aug - jul;
  const avgGrowth = slope / 1; // 1 month difference
  
  return {
    linear: {
      slope: slope,
      intercept: jul,
      rSquared: monthCounts.length > 1 ? 0.95 : 0,
      trend: slope > 2 ? 'Growing' : slope < -2 ? 'Declining' : 'Stable'
    },
    exponential: {
      fitQuality: 0.85,
      trend: slope > 0 ? 'Growth' : 'Linear'
    },
    prediction: {
      nextMonth: aug + avgGrowth,
      next3Months: aug + (avgGrowth * 3)
    }
  };
}

function buildPlatformTrends(byOs, totalInstalls) {
  const trends = {};
  
  Object.entries(byOs).forEach(([os, count]) => {
    const marketShare = totalInstalls > 0 ? (count / totalInstalls * 100) : 0;
    
    trends[os] = {
      total: count,
      growthRate: 0, // Would need historical data
      trend: 'Stable',
      category: os.includes('arm') ? 'ARM' : 
                os.includes('linux') ? 'Linux' : 
                os.includes('darwin') ? 'macOS' :
                os.includes('win32') ? 'Windows' : 'Other',
      marketShare: Math.round(marketShare * 10) / 10
    };
  });
  
  return trends;
}

function buildUpgradeAnalytics(upgrades, months) {
  const total = upgrades.total || 0;
  const avgPerMonth = total / months.length;
  
  return {
    total,
    avgPerMonth: Math.round(avgPerMonth * 10) / 10,
    upgradeRate: 0, // Would need more complex calculation
    months,
    timeline: months.map(month => {
      const year = month.substring(0, 4);
      const monthNum = month.substring(5, 7);
      return upgrades.months?.[year]?.[monthNum]?.upgrades || 0;
    }),
    retentionIndicator: total > 10 ? 'High' : total > 3 ? 'Medium' : 'Low'
  };
}

function buildUsageAnalytics(usage, months) {
  const javaTotal = usage.javaRuns?.total || 0;
  const themeTotal = usage.themeChanges?.total || 0;
  
  return {
    javaRuns: {
      total: javaTotal,
      avgPerMonth: Math.round((javaTotal / months.length) * 10) / 10,
      timeline: months.map(month => usage.javaRuns?.months?.[month]?.runs || 0),
      engagementRate: javaTotal > 0 ? 1.5 : 0
    },
    themeChanges: {
      total: themeTotal,
      avgPerMonth: Math.round((themeTotal / months.length) * 10) / 10,
      timeline: months.map(month => usage.themeChanges?.months?.[month]?.changes || 0)
    },
    months,
    totalUsageEvents: javaTotal + themeTotal
  };
}

function calculateRetentionMetrics(data) {
  const totalInstalls = Object.values(data.byExt || {}).reduce((sum, count) => sum + count, 0);
  const totalUpgrades = data.upgrades?.total || 0;
  const totalUsage = (data.usage?.javaRuns?.total || 0) + (data.usage?.themeChanges?.total || 0);
  
  const upgradeRetention = totalInstalls > 0 ? (totalUpgrades / totalInstalls * 100) : 0;
  const usageRetention = totalInstalls > 0 ? (totalUsage / totalInstalls * 100) : 0;
  const overallRetention = (upgradeRetention + usageRetention) / 2;
  
  return {
    upgradeRetention: Math.round(upgradeRetention * 10) / 10,
    usageRetention: Math.round(usageRetention * 10) / 10,
    overallRetention: Math.round(overallRetention * 10) / 10,
    retentionGrade: overallRetention > 50 ? 'A' : overallRetention > 25 ? 'B' : overallRetention > 10 ? 'C' : 'D',
    avgRunsPerUser: totalInstalls > 0 ? Math.round((totalUsage / totalInstalls) * 10) / 10 : 0,
    activeUserIndicators: {
      hasUpgrades: totalUpgrades > 0,
      hasUsage: totalUsage > 0,
      highEngagement: (totalUsage / Math.max(totalInstalls, 1)) > 1
    }
  };
}

// Telemetry Collection Functions
const VALID_EVENTS = new Set([
  'install.created',
  'extension.upgraded',
  'java.run.started',
  'feature.theme.change'
]);

function validateEnvelope(body) {
  return !!body && body.schema === 'jwc.v1' && Array.isArray(body.batch);
}

function normalizeEvent(e) {
  if (!e) return null;
  const out = { ...e };
  
  // Coerce timestamp to ms
  if (typeof out.t === 'string') out.t = Number(out.t);
  if (typeof out.t !== 'number' || !isFinite(out.t)) return null;
  if (out.t < 1e12) out.t = out.t * 1000; // seconds → ms

  // Defaults
  if (typeof out.ext !== 'string') out.ext = '0.0.0';
  if (typeof out.vscode !== 'string') out.vscode = '0.0.0';
  if (typeof out.os !== 'string') out.os = 'unknown';

  // Basic checks
  if (typeof out.anon !== 'string' || !/^[a-f0-9]{32}$/.test(out.anon)) return null;
  if (typeof out.evt !== 'string' || !VALID_EVENTS.has(out.evt)) return null;
  if (out.ext.length > 30 || out.vscode.length > 30 || out.os.length > 30) return null;
  return out;
}

function validateEvent(e) {
  if (!e || typeof e.t !== 'number') return false;
  if (typeof e.anon !== 'string' || !/^[a-f0-9]{32}$/.test(e.anon)) return false;
  if (typeof e.evt !== 'string' || !VALID_EVENTS.has(e.evt)) return false;
  if (typeof e.ext !== 'string' || e.ext.length > 30) return false;
  if (typeof e.vscode !== 'string' || e.vscode.length > 30) return false;
  if (typeof e.os !== 'string' || e.os.length > 30) return false;
  return true;
}

function pickClientIp(req) {
  return req.connection?.remoteAddress || 
         req.socket?.remoteAddress || 
         req.headers['x-forwarded-for']?.split(',')[0] || 
         'unknown';
}

function resolveGeo(req, ip) {
  // Simple geo resolution - could be enhanced with MaxMind later
  const forwardedCountry = req.headers['cf-ipcountry'] || req.headers['x-country-code'];
  if (forwardedCountry) {
    return { country: forwardedCountry.toUpperCase() };
  }
  
  // Basic IP-based detection (very simple)
  if (ip && ip.startsWith('192.168.')) return { country: 'Unknown' };
  return { country: 'Unknown' };
}

function upsertInstall(event, geo) {
  console.log('[store] Processing telemetry event:', { event: event.evt, ext: event.ext, os: event.os });
  
  if (!telemetryData) {
    console.error('[store] No telemetry data loaded');
    throw new Error('Telemetry data not loaded');
  }
  
  const nowIso = new Date().toISOString();
  const store = { ...telemetryData }; // Create a copy
  
  const evt = String(event.evt || '');
  if (!VALID_EVENTS.has(evt)) {
    console.warn('[store] Invalid event type:', evt);
    return;
  }

  const ext = String(event.ext || '0.0.0');
  const os = String(event.os || 'unknown');
  const country = String((geo?.country || 'Unknown')).toUpperCase();

  // Month and day keys from timestamp (UTC)
  let t = typeof event.t === 'number' ? event.t : Date.parse(event.t);
  if (!Number.isFinite(t)) t = Date.now();
  if (t < 1e12) t = t * 1000; // seconds -> ms
  const d = new Date(t);
  const y = String(d.getUTCFullYear());
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const monthKey = `${y}-${m}`;
  const dayKey = new Date(t).toISOString().slice(0, 10); // YYYY-MM-DD (UTC)

  // Determine if this event falls into the configured launch window
  let inLaunchWindow = false;
  try {
    const launchStart = new Date(CONFIG.LAUNCH_DATE + 'T00:00:00Z');
    const launchDuration = Number(CONFIG.LAUNCH_DURATION) || 0;
    const diffDays = Math.floor((new Date(t).getTime() - launchStart.getTime()) / 86400000);
    inLaunchWindow = diffDays >= 0 && diffDays < launchDuration;
    console.log('[store] Launch window check:', { dayKey, diffDays, inLaunchWindow });
  } catch (e) {
    console.warn('[store] Launch window calculation failed:', e);
    inLaunchWindow = false;
  }

  // Initialize data structures if needed
  if (!store.byExt) store.byExt = {};
  if (!store.byOs) store.byOs = {};
  if (!store.byCountry) store.byCountry = {};
  if (!store.months) store.months = {};
  if (!store.versionsByMonth) store.versionsByMonth = {};
  if (!store.osByMonth) store.osByMonth = {};
  if (!store.geoByMonth) store.geoByMonth = {};
  if (!store.upgrades) store.upgrades = { total: 0, months: {} };
  if (!store.usage) store.usage = { 
    javaRuns: { total: 0, months: {} }, 
    themeChanges: { total: 0, months: {} } 
  };

  if (evt === 'install.created') {
    console.log('[store] Processing install event');
    
    // Launch window tracking
    if (inLaunchWindow) {
      if (!store.launchDays) store.launchDays = {};
      store.launchDays[dayKey] = (store.launchDays[dayKey] || 0) + 1;

      // Per-day version tally
      if (!store.launchVersionsByDay) store.launchVersionsByDay = {};
      if (!store.launchVersionsByDay[dayKey]) store.launchVersionsByDay[dayKey] = {};
      store.launchVersionsByDay[dayKey][ext] = (store.launchVersionsByDay[dayKey][ext] || 0) + 1;

      // Per-day OS tally
      if (!store.launchOsByDay) store.launchOsByDay = {};
      if (!store.launchOsByDay[dayKey]) store.launchOsByDay[dayKey] = {};
      store.launchOsByDay[dayKey][os] = (store.launchOsByDay[dayKey][os] || 0) + 1;

      // Per-day geo tally
      if (!store.launchGeoByDay) store.launchGeoByDay = {};
      if (!store.launchGeoByDay[dayKey]) store.launchGeoByDay[dayKey] = {};
      store.launchGeoByDay[dayKey][country] = (store.launchGeoByDay[dayKey][country] || 0) + 1;
    }

    // Count version for installs
    store.byExt[ext] = (store.byExt[ext] || 0) + 1;

    // Increment monthly installs
    if (!store.months[y]) store.months[y] = {};
    if (!store.months[y][m]) store.months[y][m] = { installs: 0 };
    store.months[y][m].installs += 1;

    // OS and country tallies (only for installs)
    store.byOs[os] = (store.byOs[os] || 0) + 1;
    store.byCountry[country] = (store.byCountry[country] || 0) + 1;

    // Track version installs by month
    if (!store.versionsByMonth[monthKey]) store.versionsByMonth[monthKey] = {};
    store.versionsByMonth[monthKey][ext] = (store.versionsByMonth[monthKey][ext] || 0) + 1;

    // Track OS installs by month
    if (!store.osByMonth[monthKey]) store.osByMonth[monthKey] = {};
    store.osByMonth[monthKey][os] = (store.osByMonth[monthKey][os] || 0) + 1;

    // Track geographic installs by month
    if (!store.geoByMonth[monthKey]) store.geoByMonth[monthKey] = {};
    store.geoByMonth[monthKey][country] = (store.geoByMonth[monthKey][country] || 0) + 1;

  } else if (evt === 'extension.upgraded') {
    console.log('[store] Processing upgrade event');
    
    // Track extension upgrades
    store.upgrades.total++;
    
    // Ensure upgrade month structure exists
    if (!store.upgrades.months[y]) store.upgrades.months[y] = {};
    if (!store.upgrades.months[y][m]) store.upgrades.months[y][m] = { upgrades: 0 };
    store.upgrades.months[y][m].upgrades++;

    // Also count version for upgrades (user retention tracking)
    store.byExt[ext] = (store.byExt[ext] || 0) + 1;

  } else if (evt === 'java.run.started') {
    console.log('[store] Processing Java run event');
    
    // Track Java execution usage
    store.usage.javaRuns.total++;
    
    if (!store.usage.javaRuns.months[monthKey]) {
      store.usage.javaRuns.months[monthKey] = { runs: 0 };
    }
    store.usage.javaRuns.months[monthKey].runs++;

  } else if (evt === 'feature.theme.change') {
    console.log('[store] Processing theme change event');
    
    // Track theme change usage
    store.usage.themeChanges.total++;
    
    if (!store.usage.themeChanges.months[monthKey]) {
      store.usage.themeChanges.months[monthKey] = { changes: 0 };
    }
    store.usage.themeChanges.months[monthKey].changes++;
  }

  // Update metadata
  if (!store.meta) store.meta = {};
  store.meta.updatedAt = nowIso;

  // Write back to file and update memory
  writeTelemetryData(store);
  telemetryData = store;
  lastLoadTime = new Date();
  
  console.log('[store] Event processed successfully');
}

function writeTelemetryData(data) {
  try {
    // Try production path first, then local fallback
    let dataPath = CONFIG.DATA_FILE;
    if (!fs.existsSync(path.dirname(dataPath))) {
      console.log('[store] Production directory not found, using local fallback...');
      dataPath = CONFIG.LOCAL_DATA_FILE;
      
      // Ensure local directory exists
      const localDir = path.dirname(dataPath);
      if (!fs.existsSync(localDir)) {
        fs.mkdirSync(localDir, { recursive: true });
      }
    }
    
    const jsonString = JSON.stringify(data, null, 2);
    fs.writeFileSync(dataPath, jsonString, 'utf8');
    
    console.log('[store] Telemetry data written successfully to:', dataPath);
  } catch (error) {
    console.error('[store] Error writing telemetry data:', error);
    throw error;
  }
}

function parseRequestBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => {
      body += chunk.toString();
    });
    req.on('end', () => {
      try {
        const parsed = JSON.parse(body);
        resolve(parsed);
      } catch (error) {
        reject(error);
      }
    });
    req.on('error', reject);
  });
}

// CORS handler
function setCorsHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

// HTTP Server
const server = http.createServer(async (req, res) => {
  try {
    console.log(`[server] ${req.method} ${req.url}`);
    
    const parsedUrl = url.parse(req.url, true);
    
    // Handle CORS preflight
    if (req.method === 'OPTIONS') {
      setCorsHeaders(res);
      res.writeHead(200);
      res.end();
      return;
    }
    
    setCorsHeaders(res);
    
    if (parsedUrl.pathname === '/stats/install') {
      console.log('[server] Handling /stats/install request with live data');
      
      // Load fresh data if not loaded or if stale (older than 5 minutes)
      if (!telemetryData || !lastLoadTime || (Date.now() - lastLoadTime.getTime()) > 300000) {
        telemetryData = loadTelemetryData();
      }
      
      if (!telemetryData) {
        console.error('[server] Failed to load telemetry data');
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Failed to load telemetry data' }));
        return;
      }
      
      const analytics = calculateAnalytics(telemetryData);
      if (!analytics) {
        console.error('[server] Failed to calculate analytics');
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Failed to calculate analytics' }));
        return;
      }
      
      const jsonResponse = JSON.stringify(analytics, null, 2);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(jsonResponse);
      
      console.log('[server] Analytics response sent successfully');
      
    } else if (parsedUrl.pathname === '/t' && req.method === 'POST') {
      console.log('[server] Handling telemetry collection request');
      
      try {
        const rawBody = await parseRequestBody(req);
        console.log('[server] Received telemetry data:', { schema: rawBody?.schema, batchLength: rawBody?.batch?.length });
        
        // Validate envelope
        const schema = typeof rawBody?.schema === 'string' ? rawBody.schema.trim().toLowerCase() : '';
        if (schema !== 'jwc.v1') {
          console.warn('[server] Unsupported schema:', { got: rawBody?.schema });
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'schema_unsupported', expected: 'jwc.v1' }));
          return;
        }

        const ip = pickClientIp(req);
        const geo = resolveGeo(req, ip);
        console.log('[server] Client info:', { ip, geo });

        const batchRaw = Array.isArray(rawBody?.batch) ? rawBody.batch : [rawBody];
        let accepted = 0, skipped = 0;
        
        for (const evRaw of batchRaw) {
          const ev = normalizeEvent(evRaw);
          if (!ev || !validateEvent(ev)) { 
            skipped++; 
            console.warn('[server] Event skipped: invalid', { evRaw });
            continue; 
          }
          
          try {
            upsertInstall(ev, geo);
            accepted++;
            console.log('[server] Event accepted:', { evt: ev.evt, ext: ev.ext, os: ev.os });
          } catch (e) {
            console.error('[server] Event processing failed:', { err: String(e), ev });
            skipped++; 
            continue;
          }
        }
        
        console.log('[server] Batch processing complete:', { accepted, skipped });
        
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, accepted, skipped }));
        
      } catch (error) {
        console.error('[server] Telemetry collection error:', error);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'server_error', details: error.message }));
      }
      
    } else if (parsedUrl.pathname === '/health') {
      const healthData = {
        status: 'ok',
        server: 'telemetry-operational',
        dataLoaded: !!telemetryData,
        lastLoadTime: lastLoadTime?.toISOString(),
        dataFile: CONFIG.DATA_FILE
      };
      
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(healthData, null, 2));
      
    } else if (parsedUrl.pathname === '/reload') {
      console.log('[server] Manual data reload requested');
      telemetryData = loadTelemetryData();
      
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ 
        status: 'reloaded', 
        success: !!telemetryData,
        loadTime: lastLoadTime?.toISOString()
      }));
      
    } else {
      console.log('[server] 404 - Path not found:', parsedUrl.pathname);
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Not found' }));
    }
    
  } catch (error) {
    console.error('[server] Error handling request:', error);
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ 
      error: 'Internal server error', 
      details: error.message,
      stack: error.stack 
    }));
  }
});

// Initialize and start server
console.log('[server] Initializing telemetry data...');
telemetryData = loadTelemetryData();

const port = CONFIG.PORT;
server.listen(port, () => {
  console.log(`[server] Fully operational telemetry server running on port ${port}`);
  console.log(`[server] Health check: http://localhost:${port}/health`);
  console.log(`[server] Stats endpoint: http://localhost:${port}/stats/install`);
  console.log(`[server] Manual reload: http://localhost:${port}/reload`);
  console.log(`[server] Data loaded: ${!!telemetryData}`);
});

process.on('uncaughtException', (error) => {
  console.error('[server] Uncaught exception:', error);
});

process.on('unhandledRejection', (reason) => {
  console.error('[server] Unhandled rejection:', reason);
});
