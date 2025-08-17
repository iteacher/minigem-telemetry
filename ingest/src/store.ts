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

export function pathToStore() { return FILE; }
