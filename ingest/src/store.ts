import fs from 'fs';
import path from 'path';
import { CONFIG } from './config.js';

type Store = {
  meta: { createdAt: string; updatedAt: string };
  months: Record<string /*year*/, Record<string /*mm*/, { installs: number }>>;
  byExt: Record<string, number>;
  byOs: Record<string, number>;
  byCountry: Record<string, number>;
};

const FILE = process.env.STATS_JSON_FILE || path.join(CONFIG.LOG_DIR || '/opt/jwc-telemetry/logs', 'installs.json');

function ensureDirExists(filePath: string) {
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function defaultStore(): Store {
  const now = new Date().toISOString();
  return { meta: { createdAt: now, updatedAt: now }, months: {}, byExt: {}, byOs: {}, byCountry: {} };
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

// Simple in-process mutex to serialize updates
let lock = Promise.resolve();
function serialize<T>(fn: () => Promise<T> | T): Promise<T> {
  lock = lock.then(async () => { await fn(); }).catch(() => {/* swallow to keep chain alive */});
  return lock as Promise<T>;
}

export async function upsertFromEvent(ev: any, geo?: { country?: string }) {
  await serialize(() => {
    const store = readStore();
    const nowIso = new Date().toISOString();

    const evt = String(ev.evt || '');
    const ext = String(ev.ext || '0.0.0');
    const os = String(ev.os || 'unknown');
    const country = String((geo?.country || 'Unknown')).toUpperCase();

    // Month key from timestamp (UTC)
    let t = typeof ev.t === 'number' ? ev.t : Date.parse(ev.t);
    if (!Number.isFinite(t)) t = Date.now();
    if (t < 1e12) t = t * 1000;
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
    }

    store.meta.updatedAt = nowIso;
    writeStore(store);
  });
}

export function readStatsMonthly() {
  const s = readStore();
  // Build ordered monthly series from first stored month to latest present in file
  const months: string[] = [];
  const counts: number[] = [];
  const years = Object.keys(s.months).sort();
  for (const y of years) {
    const monthsInYear = Object.keys(s.months[y] || {}).sort();
    for (const m of monthsInYear) {
  const ym = `${y}-${m}`;
  if (ym < '2025-08') continue; // Only track from Aug 2025 onwards
  months.push(ym);
  counts.push(Number(s.months[y][m]?.installs || 0));
    }
  }
  const installsTotal = counts.reduce((a, b) => a + b, 0);
  return {
    from: months[0] || new Date().toISOString().slice(0, 7),
    to: months[months.length - 1] || new Date().toISOString().slice(0, 7),
    windowMonths: months.length,
    installsTotal,
    monthlyInstalls: { months, counts },
    byExt: s.byExt,
    byOs: s.byOs,
    byCountry: s.byCountry,
    updatedAt: s.meta.updatedAt
  };
}

export function pathToStore() { return FILE; }
