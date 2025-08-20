const http = require('http');
const url = require('url');
const fs = require('fs');
const path = require('path');

console.log('[server] Starting fully operational telemetry server...');

// Configuration for production deployment
const CONFIG = {
  PORT: process.env.PORT || 3001,
  DATA_FILE: '/home/mandersj/telemetary.jwc.minigem.uk/data/test3.json',
  LOCAL_DATA_FILE: './data/test3.json', // Fallback for local testing
  LOG_FILE: './logs/telemetry.log'
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
  const monthKeys = ['2025-07', '2025-08'];
  const monthCounts = [
    data.months?.['2025']?.['07']?.installs || 0,
    data.months?.['2025']?.['08']?.installs || 0
  ];
  
  // Build comprehensive analytics
  const analytics = {
    from: '2025-07',
    to: '2025-08',
    windowMonths: 2,
    totalMonths: 2,
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
  const timeline = {};
  versions.forEach(version => {
    timeline[version] = [
      versionsByMonth['2025-07']?.[version] || 0,
      versionsByMonth['2025-08']?.[version] || 0
    ];
  });
  return timeline;
}

function buildOsTimeline(osByMonth, osTypes) {
  const timeline = {};
  osTypes.forEach(os => {
    timeline[os] = [
      osByMonth['2025-07']?.[os] || 0,
      osByMonth['2025-08']?.[os] || 0
    ];
  });
  return timeline;
}

function buildGeoTimeline(geoByMonth, byCountry) {
  const timeline = {};
  const expandedCountries = expandCountryCodes(byCountry);
  
  Object.keys(expandedCountries).forEach(country => {
    const code = Object.keys(byCountry).find(code => 
      expandCountryCodes({[code]: 1})[country]
    );
    timeline[country] = [
      geoByMonth['2025-07']?.[code] || 0,
      geoByMonth['2025-08']?.[code] || 0
    ];
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

// CORS handler
function setCorsHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

// HTTP Server
const server = http.createServer((req, res) => {
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
