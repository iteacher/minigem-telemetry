import fs from 'fs';
import path from 'path';
import { CONFIG } from './config.js';
const CONTINENT = {
    // Minimal ISO country → continent mapping (extend as needed)
    US: 'NA', CA: 'NA', MX: 'NA', BR: 'SA', AR: 'SA', CL: 'SA', CO: 'SA',
    GB: 'EU', DE: 'EU', FR: 'EU', ES: 'EU', IT: 'EU', NL: 'EU', SE: 'EU', PL: 'EU', IE: 'EU', PT: 'EU', CZ: 'EU', RO: 'EU', HU: 'EU',
    RU: 'EU', UA: 'EU', TR: 'AS',
    CN: 'AS', JP: 'AS', KR: 'AS', IN: 'AS', SG: 'AS', HK: 'AS', TW: 'AS', ID: 'AS', PH: 'AS', TH: 'AS', VN: 'AS', MY: 'AS', PK: 'AS', BD: 'AS',
    AU: 'OC', NZ: 'OC',
    ZA: 'AF', NG: 'AF', EG: 'AF', MA: 'AF', KE: 'AF', ET: 'AF', DZ: 'AF', GH: 'AF', TZ: 'AF', CI: 'AF', SN: 'AF'
};
function readJsonLines(file) {
    if (!fs.existsSync(file))
        return [];
    const out = [];
    const lines = fs.readFileSync(file, 'utf8').split(/\r?\n/);
    for (const line of lines) {
        if (!line)
            continue;
        try {
            out.push(JSON.parse(line));
        }
        catch { }
    }
    return out;
}
function percentile(sorted, p) {
    if (!sorted.length)
        return 0;
    const idx = Math.ceil((p / 100) * sorted.length) - 1;
    return sorted[Math.max(0, Math.min(sorted.length - 1, idx))];
}
function toTopRows(obj, denom, limit = 20) {
    return Object.entries(obj || {})
        .sort((a, b) => b[1] - a[1])
        .slice(0, limit)
        .map(([key, count]) => ({ key, count, pct: denom ? count / denom : 0 }));
}
export function readWindowStats() {
    const now = new Date();
    const baseDir = path.join(CONFIG.LOG_DIR, 'json');
    const days = [];
    for (let i = CONFIG.STATS_WINDOW_DAYS - 1; i >= 0; i--) {
        const d = new Date(now.getTime() - i * 86400000).toISOString().slice(0, 10);
        days.push(d);
    }
    const byEvent = {};
    const byOs = {};
    const byExt = {};
    const byVscode = {};
    const hourly = new Array(24).fill(0);
    const dow = new Array(7).fill(0);
    const dayHits = new Array(days.length).fill(0);
    const dayUniques = days.map(() => new Set());
    const globalUniques = new Set();
    const dailyOsSeries = new Map();
    let total = 0;
    let started = 0, completed = 0;
    let compileErr = 0, runtimeErr = 0;
    const durations = [];
    let waitMsSum = 0;
    let interactive = 0;
    let truncations = 0;
    const exitCodes = {};
    const outputBuckets = {};
    const topExceptions = {};
    const hourlyVisitors = new Array(24).fill(0);
    const byCountry = {};
    const byContinent = {};
    const seenHourAnon = new Map();
    for (let di = 0; di < days.length; di++) {
        const day = days[di];
        const file = path.join(baseDir, `${day}.jsonl`);
        const arr = readJsonLines(file);
        for (const e of arr) {
            total++;
            const evt = e.evt || 'unknown';
            const os = e.os || 'unknown';
            const ext = e.ext || '0.0.0';
            const vsc = e.vscode || 'unknown';
            let ts = typeof e.t === 'number' ? e.t : Date.parse(e.t);
            if (ts && ts < 1e12)
                ts = ts * 1000; // seconds → ms safeguard
            const dt = new Date(ts || Date.now());
            const h = dt.getUTCHours();
            const dwi = dt.getUTCDay();
            hourly[h]++;
            dow[dwi]++;
            byEvent[evt] = (byEvent[evt] || 0) + 1;
            byOs[os] = (byOs[os] || 0) + 1;
            byExt[ext] = (byExt[ext] || 0) + 1;
            byVscode[vsc] = (byVscode[vsc] || 0) + 1;
            if (!dailyOsSeries.has(os))
                dailyOsSeries.set(os, new Array(days.length).fill(0));
            dailyOsSeries.get(os)[di]++;
            const anon = e.anon || '';
            if (anon) {
                dayUniques[di].add(anon);
                globalUniques.add(anon);
            }
            dayHits[di]++;
            const country = (e.country || '-').toUpperCase();
            const cont = CONTINENT[country] || (country === '-' ? 'UN' : 'UN');
            byCountry[country] = byCountry[country] || { hits: 0, visitors: 0 };
            byContinent[cont] = byContinent[cont] || { hits: 0, visitors: 0 };
            byCountry[country].hits++;
            byContinent[cont].hits++;
            // unique visitors per country/continent (by anon within window)
            if (e.anon) {
                // per-country unique
                const keyC = `c:${country}`;
                global[keyC] = global[keyC] || new Set();
                const setC = global[keyC];
                if (!setC.has(e.anon)) {
                    setC.add(e.anon);
                    byCountry[country].visitors++;
                }
                // per-continent unique
                const keyT = `t:${cont}`;
                global[keyT] = global[keyT] || new Set();
                const setT = global[keyT];
                if (!setT.has(e.anon)) {
                    setT.add(e.anon);
                    byContinent[cont].visitors++;
                }
            }
            // hourly visitors (unique anon per hour bucket)
            const hour = dt.getUTCHours();
            if (!seenHourAnon.has(hour))
                seenHourAnon.set(hour, new Set());
            const sh = seenHourAnon.get(hour);
            if (e.anon && !sh.has(e.anon)) {
                sh.add(e.anon);
                hourlyVisitors[hour]++;
            }
            if (evt === 'java.run.started')
                started++;
            if (evt === 'java.run.completed') {
                completed++;
                const dur = Number(e.m?.durationMs) || 0;
                if (dur > 0)
                    durations.push(dur);
                waitMsSum += Number(e.m?.waitMsTotal) || 0;
                const exit = e.m?.exit ?? 0;
                exitCodes[exit] = (exitCodes[exit] || 0) + 1;
                const bucket = e.m?.cumulativeBytesBucket || '-';
                outputBuckets[bucket] = (outputBuckets[bucket] || 0) + 1;
                if (e.m?.scannerUsage === true)
                    interactive++;
                if (e.m?.truncatedOutput === true)
                    truncations++;
            }
            if (evt === 'java.run.error') {
                const phase = e.m?.phase || 'runtime';
                if (phase === 'compile')
                    compileErr++;
                else
                    runtimeErr++;
                const ex = e.m?.exceptionHash;
                if (ex)
                    topExceptions[ex] = (topExceptions[ex] || 0) + 1;
            }
        }
    }
    durations.sort((a, b) => a - b);
    const med = percentile(durations, 50);
    const p90 = percentile(durations, 90);
    const histEdges = [0, 500, 1000, 3000, 10000, 30000, 60000, 120000];
    const histVals = new Array(histEdges.length).fill(0);
    for (const d of durations) {
        let idx = histEdges.findIndex((edge, i) => i < histEdges.length - 1 && d >= edge && d < histEdges[i + 1]);
        if (idx === -1)
            idx = histEdges.length - 1;
        histVals[idx]++;
    }
    const histLabels = histEdges.map((v, i) => (i < histEdges.length - 1 ? `${v}-${histEdges[i + 1]}ms` : `≥${histEdges[i]}ms`));
    const daily = { dates: days, hits: dayHits, uniques: dayUniques.map(s => s.size) };
    const compRateDen = Math.max(1, started);
    const errors = { compile: compileErr, runtime: runtimeErr, compileRate: compileErr / compRateDen, runtimeRate: runtimeErr / compRateDen };
    const durationsOut = { count: durations.length, median: med, p90, hist: { labels: histLabels, values: histVals }, avgWaitMs: completed ? waitMsSum / completed : 0 };
    const interactiveRate = completed ? interactive / completed : 0;
    const truncationRate = completed ? truncations / completed : 0;
    const dailyOs = { labels: days, series: Array.from(dailyOsSeries.entries()).map(([key, data]) => ({ key, data })) };
    return {
        from: days[0],
        to: days[days.length - 1],
        windowDays: CONFIG.STATS_WINDOW_DAYS,
        total,
        uniques: globalUniques.size,
        osTypes: Object.keys(byOs).length,
        extTypes: Object.keys(byExt).length,
        vscodeTypes: Object.keys(byVscode).length,
        daily,
        dailyOs,
        byEvent,
        byOs,
        byExt,
        byVscode,
        hourly,
        dow,
        runs: { started, completed },
        errors,
        durations: durationsOut,
        exitCodes,
        outputBuckets,
        interactiveRate,
        truncationRate,
        topExceptions,
        geo: { byContinent, byCountry },
        hourlyVisitors,
        tables: {
            eventsTop: toTopRows(byEvent, total),
            extTop: toTopRows(byExt, total),
            vscodeTop: toTopRows(byVscode, total),
            exitTop: toTopRows(Object.fromEntries(Object.entries(exitCodes).map(([k, v]) => [String(k), v])), total),
            exceptionsTop: toTopRows(topExceptions, total)
        }
    };
}
