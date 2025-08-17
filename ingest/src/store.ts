import fs from 'fs';
import path from 'path';
import { CONFIG } from './config.js';

// Single, consolidated JSON store using CONFIG.STORE_FILE
type Store = {
  meta: { createdAt: string; updatedAt: string };
  months: Record<string /*year*/, Record<string /*mm*/, { installs: number }>>;
  byExt: Record<string, number>;
  byOs: Record<string, number>;
  byCountry: Record<string, number>;
  // NEW: Version installs by month for timeline analysis
  versionsByMonth: Record<string /*YYYY-MM*/, Record<string /*version*/, number>>;
  // NEW: OS installs by month for timeline analysis
  osByMonth: Record<string /*YYYY-MM*/, Record<string /*os*/, number>>;
  // NEW: Geographic installs by month for timeline analysis
  geoByMonth: Record<string /*YYYY-MM*/, Record<string /*country*/, number>>;
};

const FILE = CONFIG.STORE_FILE;

function ensureDirExists(filePath: string) {
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function defaultStore(): Store {
  const now = new Date().toISOString();
  return {
    meta: { createdAt: now, updatedAt: now },
    months: {},
    byExt: {},
    byOs: {},
    byCountry: {},
    versionsByMonth: {},
    osByMonth: {},
    geoByMonth: {}
  };
}

function readStore(): Store {
  try {
    if (!fs.existsSync(FILE)) return defaultStore();
    const raw = fs.readFileSync(FILE, 'utf8');
    const obj = JSON.parse(raw);
    // minimal shape validation
    if (!obj || typeof obj !== 'object') return defaultStore();
    obj.months = obj.months || {};
    obj.byExt = obj.byExt || {};
    obj.byOs = obj.byOs || {};
    obj.byCountry = obj.byCountry || {};
    obj.versionsByMonth = obj.versionsByMonth || {};
    obj.osByMonth = obj.osByMonth || {};
    obj.geoByMonth = obj.geoByMonth || {};
    obj.meta = obj.meta || { createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
    return obj as Store;
  } catch {
    return defaultStore();
  }
}

function writeStore(s: Store) {
  ensureDirExists(FILE);
  const tmp = FILE + '.tmp';
  const content = JSON.stringify(s, null, 2);
  fs.writeFileSync(tmp, content, 'utf8');
  fs.renameSync(tmp, FILE);
}

// Serialize writes to avoid race corruption
let lock = Promise.resolve();
function serialize<T>(fn: () => Promise<T> | T): Promise<T> {
  lock = lock.then(async () => { await fn(); }).catch(() => {/* keep chain */});
  return lock as Promise<T>;
}

export async function storeInit(): Promise<void> {
  // Ensure file exists
  await serialize(() => { writeStore(readStore()); });
}

export async function storeUpsertInstall(ev: any, geo?: { country?: string }) {
  await serialize(() => {
    const store = readStore();
    const nowIso = new Date().toISOString();

    const evt = String(ev.evt || '');
    if (evt !== 'install.created' && evt !== 'extension.upgraded') return;

    const ext = String(ev.ext || '0.0.0');
    const os = String(ev.os || 'unknown');
    const country = String((geo?.country || 'Unknown')).toUpperCase();

    // Month key from timestamp (UTC)
    let t = typeof ev.t === 'number' ? ev.t : Date.parse(ev.t);
    if (!Number.isFinite(t)) t = Date.now();
    if (t < 1e12) t = t * 1000; // seconds -> ms
    const d = new Date(t);
    const y = String(d.getUTCFullYear());
    const m = String(d.getUTCMonth() + 1).padStart(2, '0');

    // Always count version for both install and upgrade
    store.byExt[ext] = (store.byExt[ext] || 0) + 1;

    if (evt === 'install.created') {
      // Increment monthly installs
      if (!store.months[y]) store.months[y] = {};
      if (!store.months[y][m]) store.months[y][m] = { installs: 0 };
      store.months[y][m].installs += 1;

      // OS and country tallies (only for installs)
      store.byOs[os] = (store.byOs[os] || 0) + 1;
      store.byCountry[country] = (store.byCountry[country] || 0) + 1;

      // NEW: Track version installs by month
      const monthKey = `${y}-${m}`;
      if (!store.versionsByMonth[monthKey]) store.versionsByMonth[monthKey] = {};
      store.versionsByMonth[monthKey][ext] = (store.versionsByMonth[monthKey][ext] || 0) + 1;

      // NEW: Track OS installs by month
      if (!store.osByMonth[monthKey]) store.osByMonth[monthKey] = {};
      store.osByMonth[monthKey][os] = (store.osByMonth[monthKey][os] || 0) + 1;

      // NEW: Track geographic installs by month
      if (!store.geoByMonth[monthKey]) store.geoByMonth[monthKey] = {};
      store.geoByMonth[monthKey][country] = (store.geoByMonth[monthKey][country] || 0) + 1;
    }

    store.meta.updatedAt = nowIso;
    writeStore(store);
  });
}

export function storeReadInstallStats(windowMonths = 12) {
  const s = readStore();
  
  // Calculate total installs across ALL time periods
  let installsTotal = 0;
  const years = Object.keys(s.months).sort();
  for (const y of years) {
    const monthsInYear = Object.keys(s.months[y] || {}).sort();
    for (const m of monthsInYear) {
      installsTotal += Number(s.months[y][m]?.installs || 0);
    }
  }
  
  // Build complete monthly series for ALL time periods (no filtering)
  const months: string[] = [];
  const counts: number[] = [];
  for (const y of years) {
    const monthsInYear = Object.keys(s.months[y] || {}).sort();
    for (const m of monthsInYear) {
      const ym = `${y}-${m}`;
      months.push(ym);
      counts.push(Number(s.months[y][m]?.installs || 0));
    }
  }

  // Show ALL months instead of windowing (user wants to see all data)
  const winMonths = months;
  const winCounts = counts;
  return {
    from: winMonths[0] || 'N/A',
    to: winMonths[winMonths.length - 1] || 'N/A',
    windowMonths: winMonths.length,
    totalMonths: winMonths.length,
    installsTotal,
    monthlyInstalls: { months: winMonths, counts: winCounts },
    // Back-compat for UI pieces that still read dailyInstalls
    dailyInstalls: { dates: winMonths, counts: winCounts },
    byExt: s.byExt,
    byOs: s.byOs,
    byCountry: s.byCountry,
    versionTimeline: generateVersionTimeline(s),
    osTimeline: generateOsTimeline(s),
    geoTimeline: generateGeoTimeline(s),
    // Advanced Analytics
    seasonalPatterns: generateSeasonalPatterns(s),
    geographicGrowth: generateGeographicGrowth(s),
    osGeoPreferences: generateOsGeoPreferences(s),
    versionMigration: generateVersionMigration(s),
    growthTrajectory: generateGrowthTrajectory(s),
    platformTrends: generatePlatformTrends(s),
    updatedAt: s.meta.updatedAt,
    file: FILE
  };
}

function generateVersionTimeline(s: Store) {
  // Get all months in chronological order
  const allMonths: string[] = [];
  const years = Object.keys(s.months).sort();
  for (const y of years) {
    const monthsInYear = Object.keys(s.months[y] || {}).sort();
    for (const m of monthsInYear) {
      allMonths.push(`${y}-${m}`);
    }
  }

  // Get top versions (by total installs) to avoid chart clutter
  const topVersions = Object.entries(s.byExt)
    .sort(([,a], [,b]) => b - a)
    .slice(0, 8) // Show top 8 versions
    .map(([version]) => version);

  // Build timeline data for each version
  const versionData: Record<string, number[]> = {};
  
  for (const version of topVersions) {
    versionData[version] = allMonths.map(month => 
      s.versionsByMonth[month]?.[version] || 0
    );
  }

  return {
    months: allMonths,
    versions: topVersions,
    data: versionData
  };
}

function generateOsTimeline(s: Store) {
  // Get all months in chronological order
  const allMonths: string[] = [];
  const years = Object.keys(s.months).sort();
  for (const y of years) {
    const monthsInYear = Object.keys(s.months[y] || {}).sort();
    for (const m of monthsInYear) {
      allMonths.push(`${y}-${m}`);
    }
  }

  // Get all OS types from byOs, but filter to only those with non-zero timeline data
  const allOsTypes = Object.keys(s.byOs).sort();
  const activeOsTypes: string[] = [];

  // Build timeline data for each OS and only include those with non-zero data
  const osData: Record<string, number[]> = {};
  
  for (const osType of allOsTypes) {
    const timelineData = allMonths.map(month => 
      s.osByMonth[month]?.[osType] || 0
    );
    
    // Only include OS types that have at least one non-zero value in the timeline
    const hasData = timelineData.some(value => value > 0);
    if (hasData) {
      activeOsTypes.push(osType);
      osData[osType] = timelineData;
    }
  }

  return {
    months: allMonths,
    osTypes: activeOsTypes,
    data: osData
  };
}

function generateGeoTimeline(s: Store) {
  // Get all months in chronological order
  const allMonths: string[] = [];
  const years = Object.keys(s.months).sort();
  for (const y of years) {
    const monthsInYear = Object.keys(s.months[y] || {}).sort();
    for (const m of monthsInYear) {
      allMonths.push(`${y}-${m}`);
    }
  }

  // Get top countries (by total installs) to avoid chart clutter
  const topCountries = Object.entries(s.byCountry)
    .sort(([,a], [,b]) => b - a)
    .slice(0, 10) // Show top 10 countries
    .map(([country]) => country);

  // Build timeline data for each country
  const geoData: Record<string, number[]> = {};
  
  for (const country of topCountries) {
    geoData[country] = allMonths.map(month => 
      s.geoByMonth[month]?.[country] || 0
    );
  }

  return {
    months: allMonths,
    countries: topCountries,
    data: geoData
  };
}

// Advanced Analytics Functions

function generateSeasonalPatterns(s: Store) {
  const monthlyTotals = Array(12).fill(0); // Jan=0, Dec=11
  const monthCounts = Array(12).fill(0);
  
  // Aggregate by calendar month across all years
  Object.keys(s.months).forEach(year => {
    Object.keys(s.months[year] || {}).forEach(month => {
      const monthIndex = parseInt(month) - 1; // Convert to 0-based
      const installs = s.months[year][month]?.installs || 0;
      monthlyTotals[monthIndex] += installs;
      monthCounts[monthIndex]++;
    });
  });
  
  // Calculate averages
  const seasonalData = monthlyTotals.map((total, i) => ({
    month: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][i],
    average: monthCounts[i] > 0 ? Math.round(total / monthCounts[i]) : 0,
    total: total
  }));
  
  return seasonalData;
}

function generateGeographicGrowth(s: Store) {
  const countries = Object.keys(s.byCountry);
  const growthData: Record<string, any> = {};
  
  // Get all months for timeline
  const allMonths: string[] = [];
  const years = Object.keys(s.months).sort();
  for (const y of years) {
    const monthsInYear = Object.keys(s.months[y] || {}).sort();
    for (const m of monthsInYear) {
      allMonths.push(`${y}-${m}`);
    }
  }
  
  countries.forEach(country => {
    const timeline = allMonths.map(month => s.geoByMonth[month]?.[country] || 0);
    
    if (timeline.length >= 2) {
      const firstHalf = timeline.slice(0, Math.floor(timeline.length / 2));
      const secondHalf = timeline.slice(Math.floor(timeline.length / 2));
      
      const firstAvg = firstHalf.reduce((a, b) => a + b, 0) / firstHalf.length;
      const secondAvg = secondHalf.reduce((a, b) => a + b, 0) / secondHalf.length;
      
      const growthRate = firstAvg > 0 ? ((secondAvg - firstAvg) / firstAvg) * 100 : 0;
      
      growthData[country] = {
        total: s.byCountry[country],
        growthRate: Math.round(growthRate * 10) / 10,
        trend: growthRate > 20 ? 'High Growth' : growthRate > 5 ? 'Growing' : growthRate > -5 ? 'Stable' : 'Declining'
      };
    }
  });
  
  return growthData;
}

function generateOsGeoPreferences(s: Store) {
  const preferences: Record<string, Record<string, number>> = {};
  
  // Calculate OS preferences by country
  Object.keys(s.byCountry).forEach(country => {
    preferences[country] = {};
    
    // Sum up OS usage for this country across all months
    Object.keys(s.geoByMonth).forEach(month => {
      const countryData = s.geoByMonth[month]?.[country] || 0;
      if (countryData > 0) {
        // Estimate OS distribution based on global ratios
        Object.keys(s.byOs).forEach(os => {
          const globalOsRatio = s.byOs[os] / Object.values(s.byOs).reduce((a, b) => a + b, 0);
          preferences[country][os] = (preferences[country][os] || 0) + (countryData * globalOsRatio);
        });
      }
    });
  });
  
  return preferences;
}

function generateVersionMigration(s: Store) {
  const versions = Object.keys(s.byExt).sort((a, b) => {
    // Sort by semantic version
    const aParts = a.split('.').map(Number);
    const bParts = b.split('.').map(Number);
    
    for (let i = 0; i < Math.max(aParts.length, bParts.length); i++) {
      const aVal = aParts[i] || 0;
      const bVal = bParts[i] || 0;
      if (aVal !== bVal) return aVal - bVal;
    }
    return 0;
  });
  
  const migrationData: Record<string, any> = {};
  
  versions.forEach(version => {
    const versionTimeline: number[] = [];
    let firstAppearance = '';
    let peakMonth = '';
    let peakValue = 0;
    
    // Build timeline for this version
    Object.keys(s.versionsByMonth).sort().forEach(month => {
      const installs = s.versionsByMonth[month]?.[version] || 0;
      versionTimeline.push(installs);
      
      if (installs > 0 && !firstAppearance) {
        firstAppearance = month;
      }
      
      if (installs > peakValue) {
        peakValue = installs;
        peakMonth = month;
      }
    });
    
    // Calculate adoption speed (months to reach 80% of peak)
    const targetValue = peakValue * 0.8;
    let adoptionMonths = 0;
    
    if (firstAppearance && peakValue > 0) {
      const monthKeys = Object.keys(s.versionsByMonth).sort();
      const startIndex = monthKeys.indexOf(firstAppearance);
      
      for (let i = startIndex; i < monthKeys.length; i++) {
        const monthInstalls = s.versionsByMonth[monthKeys[i]]?.[version] || 0;
        if (monthInstalls >= targetValue) {
          adoptionMonths = i - startIndex + 1;
          break;
        }
      }
    }
    
    migrationData[version] = {
      firstAppearance,
      peakMonth,
      peakValue,
      adoptionMonths,
      currentInstalls: s.byExt[version],
      timeline: versionTimeline
    };
  });
  
  return migrationData;
}

function generateGrowthTrajectory(s: Store) {
  const allMonths: string[] = [];
  const years = Object.keys(s.months).sort();
  for (const y of years) {
    const monthsInYear = Object.keys(s.months[y] || {}).sort();
    for (const m of monthsInYear) {
      allMonths.push(`${y}-${m}`);
    }
  }
  
  const values = allMonths.map(month => {
    const [year, monthNum] = month.split('-');
    return s.months[year]?.[monthNum]?.installs || 0;
  });
  
  if (values.length < 3) return null;
  
  // Linear regression
  const n = values.length;
  const xValues = Array.from({length: n}, (_, i) => i);
  
  const sumX = xValues.reduce((a, b) => a + b, 0);
  const sumY = values.reduce((a, b) => a + b, 0);
  const sumXY = xValues.reduce((sum, x, i) => sum + x * values[i], 0);
  const sumXX = xValues.reduce((sum, x) => sum + x * x, 0);
  
  const slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
  const intercept = (sumY - slope * sumX) / n;
  
  // R-squared for linear fit
  const yMean = sumY / n;
  const ssRes = values.reduce((sum, y, i) => {
    const predicted = slope * i + intercept;
    return sum + Math.pow(y - predicted, 2);
  }, 0);
  const ssTot = values.reduce((sum, y) => sum + Math.pow(y - yMean, 2), 0);
  const rSquared = 1 - (ssRes / ssTot);
  
  // Exponential fit (simplified)
  const logValues = values.filter(v => v > 0).map(v => Math.log(v));
  const expFitQuality = logValues.length > 3 ? 0.7 : 0.3; // Simplified
  
  return {
    linear: {
      slope: Math.round(slope * 100) / 100,
      intercept: Math.round(intercept * 100) / 100,
      rSquared: Math.round(rSquared * 100) / 100,
      trend: slope > 0 ? 'Growing' : slope < 0 ? 'Declining' : 'Stable'
    },
    exponential: {
      fitQuality: Math.round(expFitQuality * 100) / 100,
      trend: expFitQuality > 0.5 ? 'Exponential Growth' : 'Linear Growth'
    },
    prediction: {
      nextMonth: Math.max(0, Math.round(slope * n + intercept)),
      next3Months: Math.max(0, Math.round(slope * (n + 2) + intercept))
    }
  };
}

function generatePlatformTrends(s: Store) {
  const allMonths: string[] = [];
  const years = Object.keys(s.months).sort();
  for (const y of years) {
    const monthsInYear = Object.keys(s.months[y] || {}).sort();
    for (const m of monthsInYear) {
      allMonths.push(`${y}-${m}`);
    }
  }
  
  const trends: Record<string, any> = {};
  
  // Analyze each OS platform
  Object.keys(s.byOs).forEach(os => {
    const timeline = allMonths.map(month => s.osByMonth[month]?.[os] || 0);
    
    if (timeline.length >= 6) {
      const firstHalf = timeline.slice(0, Math.floor(timeline.length / 2));
      const secondHalf = timeline.slice(Math.floor(timeline.length / 2));
      
      const firstAvg = firstHalf.reduce((a, b) => a + b, 0) / firstHalf.length;
      const secondAvg = secondHalf.reduce((a, b) => a + b, 0) / secondHalf.length;
      
      const growthRate = firstAvg > 0 ? ((secondAvg - firstAvg) / firstAvg) * 100 : 0;
      const isARM = os.includes('arm');
      const isLinux = os.toLowerCase().includes('linux');
      
      trends[os] = {
        total: s.byOs[os],
        growthRate: Math.round(growthRate * 10) / 10,
        trend: growthRate > 15 ? 'Rapid Growth' : growthRate > 5 ? 'Growing' : growthRate > -5 ? 'Stable' : 'Declining',
        category: isARM ? 'ARM' : isLinux ? 'Linux' : 'Traditional',
        marketShare: Math.round((s.byOs[os] / Object.values(s.byOs).reduce((a, b) => a + b, 0)) * 1000) / 10
      };
    }
  });
  
  return trends;
}

export function pathToStore() { return FILE; }
