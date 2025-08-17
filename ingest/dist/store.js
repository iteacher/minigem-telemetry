import fs from 'fs';
import path from 'path';
import { CONFIG } from './config.js';
const FILE = CONFIG.STORE_FILE;
function ensureDirExists(filePath) {
    const dir = path.dirname(filePath);
    if (!fs.existsSync(dir))
        fs.mkdirSync(dir, { recursive: true });
}
function defaultStore() {
    const now = new Date().toISOString();
    return { meta: { createdAt: now, updatedAt: now }, months: {}, byExt: {}, byOs: {}, byCountry: {} };
}
function readStore() {
    try {
        if (!fs.existsSync(FILE))
            return defaultStore();
        const raw = fs.readFileSync(FILE, 'utf8');
        const obj = JSON.parse(raw);
        // minimal shape validation
        if (!obj || typeof obj !== 'object')
            return defaultStore();
        obj.months = obj.months || {};
        obj.byExt = obj.byExt || {};
        obj.byOs = obj.byOs || {};
        obj.byCountry = obj.byCountry || {};
        obj.meta = obj.meta || { createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
        return obj;
    }
    catch {
        return defaultStore();
    }
}
function writeStore(s) {
    ensureDirExists(FILE);
    const tmp = FILE + '.tmp';
    const content = JSON.stringify(s, null, 2);
    fs.writeFileSync(tmp, content, 'utf8');
    fs.renameSync(tmp, FILE);
}
// Serialize writes to avoid race corruption
let lock = Promise.resolve();
function serialize(fn) {
    lock = lock.then(async () => { await fn(); }).catch(() => { });
    return lock;
}
export async function storeInit() {
    // Ensure file exists
    await serialize(() => { writeStore(readStore()); });
}
export async function storeUpsertInstall(ev, geo) {
    await serialize(() => {
        const store = readStore();
        const nowIso = new Date().toISOString();
        const evt = String(ev.evt || '');
        if (evt !== 'install.created' && evt !== 'extension.upgraded')
            return;
        const ext = String(ev.ext || '0.0.0');
        const os = String(ev.os || 'unknown');
        const country = String((geo?.country || 'Unknown')).toUpperCase();
        // Month key from timestamp (UTC)
        let t = typeof ev.t === 'number' ? ev.t : Date.parse(ev.t);
        if (!Number.isFinite(t))
            t = Date.now();
        if (t < 1e12)
            t = t * 1000; // seconds -> ms
        const d = new Date(t);
        const y = String(d.getUTCFullYear());
        const m = String(d.getUTCMonth() + 1).padStart(2, '0');
        // Always count version for both install and upgrade
        store.byExt[ext] = (store.byExt[ext] || 0) + 1;
        if (evt === 'install.created') {
            // Increment monthly installs
            if (!store.months[y])
                store.months[y] = {};
            if (!store.months[y][m])
                store.months[y][m] = { installs: 0 };
            store.months[y][m].installs += 1;
            // OS and country tallies (only for installs)
            store.byOs[os] = (store.byOs[os] || 0) + 1;
            store.byCountry[country] = (store.byCountry[country] || 0) + 1;
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
        updatedAt: s.meta.updatedAt,
        file: FILE
    };
}
export function pathToStore() { return FILE; }
