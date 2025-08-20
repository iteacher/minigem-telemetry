"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.storeInit = storeInit;
exports.rollupLaunchWindowIfExpired = rollupLaunchWindowIfExpired;
exports.storeUpsertInstall = storeUpsertInstall;
exports.storeReadInstallStats = storeReadInstallStats;
exports.pathToStore = pathToStore;
const fs_1 = __importDefault(require("fs"));
const path_1 = __importDefault(require("path"));
const config_js_1 = require("./config.js");
const FILE = config_js_1.CONFIG.STORE_FILE;
const logger_js_1 = require("./logger.js");
function ensureDirExists(filePath) {
    const dir = path_1.default.dirname(filePath);
    if (!fs_1.default.existsSync(dir))
        fs_1.default.mkdirSync(dir, { recursive: true });
}
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
        // NEW: Track extension upgrades separately
        upgrades: {
            total: 0,
            months: {
                [currentYear]: {
                    [currentMonth]: { upgrades: 0 }
                }
            }
        },
        // NEW: Track usage events
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
        byExt: {
            "0.0.0": 0
        },
        byOs: {
            "darwin-arm64": 0,
            "darwin-x64": 0,
            "win32-x64": 0,
            "win32-arm64": 0,
            "linux-x64": 0,
            "linux-arm64": 0,
            "Unknown": 0
        },
        byCountry: {
            "US": 0,
            "GB": 0,
            "CA": 0,
            "DE": 0,
            "FR": 0,
            "AU": 0,
            "JP": 0,
            "Unknown": 0
        },
        versionsByMonth: {
            [currentMonthKey]: {
                "0.0.0": 0
            }
        },
        osByMonth: {
            [currentMonthKey]: {
                "darwin-arm64": 0,
                "darwin-x64": 0,
                "win32-x64": 0,
                "win32-arm64": 0,
                "linux-x64": 0,
                "linux-arm64": 0,
                "Unknown": 0
            }
        },
        geoByMonth: {
            [currentMonthKey]: {
                "US": 0,
                "GB": 0,
                "CA": 0,
                "DE": 0,
                "FR": 0,
                "AU": 0,
                "JP": 0,
                "Unknown": 0
            }
        },
        // Launch window per-day tallies (empty by default)
        launchDays: {},
        // Per-day breakdowns for launch window (YYYY-MM-DD -> { version: count })
        launchVersionsByDay: {},
        launchOsByDay: {},
        launchGeoByDay: {},
        launchRolledUp: false
    };
}
function readStore() {
    try {
        if (!fs_1.default.existsSync(FILE))
            return defaultStore();
        const raw = fs_1.default.readFileSync(FILE, 'utf8');
        const obj = JSON.parse(raw);
        // minimal shape validation
        if (!obj || typeof obj !== 'object')
            return defaultStore();
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
        // NEW: Ensure upgrade and usage structures exist
        obj.upgrades = obj.upgrades || {
            total: 0,
            months: {}
        };
        obj.usage = obj.usage || {
            javaRuns: {
                total: 0,
                months: {}
            },
            themeChanges: {
                total: 0,
                months: {}
            }
        };
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
    fs_1.default.writeFileSync(tmp, content, 'utf8');
    fs_1.default.renameSync(tmp, FILE);
}
// Serialize writes to avoid race corruption
let lock = Promise.resolve();
function serialize(fn) {
    lock = lock.then(async () => { await fn(); }).catch(() => { });
    return lock;
}
async function storeInit() {
    // Ensure file exists
    await serialize(async () => {
        logger_js_1.log.info('store.init', { file: FILE });
        try {
            // Ensure store file exists and perform rollup if launch window expired
            const s = readStore();
            // Write to ensure file exists
            writeStore(s);
            logger_js_1.log.info('store.init.file.created', { file: FILE });
        }
        catch (e) {
            logger_js_1.log.error('store.init.file.error', { file: FILE, error: String(e) });
            throw e;
        }
        try {
            logger_js_1.log.info('store.rollup.check', { launchDate: config_js_1.CONFIG.LAUNCH_DATE, duration: config_js_1.CONFIG.LAUNCH_DURATION });
            await rollupLaunchWindowIfExpired();
            logger_js_1.log.info('store.rollup.check.done');
        }
        catch (e) {
            // swallow - rollup failures should not block startup
            logger_js_1.log.warn('store.rollup.error', String(e));
        }
    });
}
// Roll up (clear) per-day launch data after launch window has elapsed to avoid unbounded growth.
async function rollupLaunchWindowIfExpired() {
    return serialize(() => {
        const s = readStore();
        try {
            const launchStart = new Date(config_js_1.CONFIG.LAUNCH_DATE + 'T00:00:00Z');
            const launchDuration = Number(config_js_1.CONFIG.LAUNCH_DURATION) || 0;
            const launchEnd = launchStart.getTime() + (launchDuration * 86400000);
            if (Date.now() < launchEnd)
                return; // still in window
            if (s.launchRolledUp)
                return; // already rolled up
            // We expect monthly tallies to already include these installs (upsert increments monthly buckets),
            // so just remove the per-day detailed structures to free space.
            if (s.launchDays && Object.keys(s.launchDays).length > 0) {
                s.launchDays = {};
            }
            if (s.launchVersionsByDay && Object.keys(s.launchVersionsByDay).length > 0) {
                s.launchVersionsByDay = {};
            }
            if (s.launchOsByDay && Object.keys(s.launchOsByDay).length > 0) {
                s.launchOsByDay = {};
            }
            if (s.launchGeoByDay && Object.keys(s.launchGeoByDay).length > 0) {
                s.launchGeoByDay = {};
            }
            s.launchRolledUp = true;
            writeStore(s);
            logger_js_1.log.info('store.rollup.done');
        }
        catch (e) {
            // ignore errors during rollup
            logger_js_1.log.warn('store.rollup.failed', String(e));
        }
    });
}
async function storeUpsertInstall(ev, geo) {
    await serialize(() => {
        const store = readStore();
        const nowIso = new Date().toISOString();
        const evt = String(ev.evt || '');
        if (!['install.created', 'extension.upgraded', 'java.run.started', 'feature.theme.change'].includes(evt))
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
        const monthKey = `${y}-${m}`;
        const dayKey = new Date(t).toISOString().slice(0, 10); // YYYY-MM-DD (UTC)
        // Determine if this event falls into the configured launch window
        let inLaunchWindow = false;
        try {
            const launchStart = new Date(config_js_1.CONFIG.LAUNCH_DATE + 'T00:00:00Z');
            const launchDuration = Number(config_js_1.CONFIG.LAUNCH_DURATION) || 0;
            const diffDays = Math.floor((new Date(t).getTime() - launchStart.getTime()) / 86400000);
            inLaunchWindow = diffDays >= 0 && diffDays < launchDuration;
        }
        catch {
            inLaunchWindow = false;
        }
        if (evt === 'install.created') {
            // If inside the launch window, increment daily tally (separate from monthly tallies)
            if (inLaunchWindow) {
                store.launchDays = store.launchDays || {};
                store.launchDays[dayKey] = (store.launchDays[dayKey] || 0) + 1;
                // Per-day version tally
                store.launchVersionsByDay = store.launchVersionsByDay || {};
                store.launchVersionsByDay[dayKey] = store.launchVersionsByDay[dayKey] || {};
                store.launchVersionsByDay[dayKey][ext] = (store.launchVersionsByDay[dayKey][ext] || 0) + 1;
                // Per-day OS tally
                store.launchOsByDay = store.launchOsByDay || {};
                store.launchOsByDay[dayKey] = store.launchOsByDay[dayKey] || {};
                store.launchOsByDay[dayKey][os] = (store.launchOsByDay[dayKey][os] || 0) + 1;
                // Per-day geo tally
                store.launchGeoByDay = store.launchGeoByDay || {};
                store.launchGeoByDay[dayKey] = store.launchGeoByDay[dayKey] || {};
                store.launchGeoByDay[dayKey][country] = (store.launchGeoByDay[dayKey][country] || 0) + 1;
            }
            // Count version for installs
            store.byExt[ext] = (store.byExt[ext] || 0) + 1;
            // Increment monthly installs
            if (!store.months[y])
                store.months[y] = {};
            if (!store.months[y][m])
                store.months[y][m] = { installs: 0 };
            store.months[y][m].installs += 1;
            // OS and country tallies (only for installs)
            store.byOs[os] = (store.byOs[os] || 0) + 1;
            store.byCountry[country] = (store.byCountry[country] || 0) + 1;
            // Track version installs by month
            if (!store.versionsByMonth[monthKey])
                store.versionsByMonth[monthKey] = {};
            store.versionsByMonth[monthKey][ext] = (store.versionsByMonth[monthKey][ext] || 0) + 1;
            // Track OS installs by month
            if (!store.osByMonth[monthKey])
                store.osByMonth[monthKey] = {};
            store.osByMonth[monthKey][os] = (store.osByMonth[monthKey][os] || 0) + 1;
            // Track geographic installs by month
            if (!store.geoByMonth[monthKey])
                store.geoByMonth[monthKey] = {};
            store.geoByMonth[monthKey][country] = (store.geoByMonth[monthKey][country] || 0) + 1;
        }
        else if (evt === 'extension.upgraded') {
            // Track extension upgrades
            store.upgrades.total++;
            // Ensure upgrade month structure exists
            if (!store.upgrades.months[y]) {
                store.upgrades.months[y] = {};
            }
            if (!store.upgrades.months[y][m]) {
                store.upgrades.months[y][m] = { upgrades: 0 };
            }
            store.upgrades.months[y][m].upgrades++;
            // Also count version for upgrades (user retention tracking)
            store.byExt[ext] = (store.byExt[ext] || 0) + 1;
        }
        else if (evt === 'java.run.started') {
            // Track Java execution usage
            store.usage.javaRuns.total++;
            if (!store.usage.javaRuns.months[monthKey]) {
                store.usage.javaRuns.months[monthKey] = { runs: 0 };
            }
            store.usage.javaRuns.months[monthKey].runs++;
        }
        else if (evt === 'feature.theme.change') {
            // Track theme change usage
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
    // Detect if we're inside a configured launch window (use daily tallies)
    let inLaunchWindow = false;
    try {
        const launchStart = new Date(config_js_1.CONFIG.LAUNCH_DATE + 'T00:00:00Z');
        const launchDuration = Number(config_js_1.CONFIG.LAUNCH_DURATION) || 0;
        const diffDaysNow = Math.floor((Date.now() - launchStart.getTime()) / 86400000);
        inLaunchWindow = diffDaysNow >= 0 && diffDaysNow < launchDuration;
    }
    catch {
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
            versionTimeline: generateVersionTimelineDays(s, days),
            osTimeline: generateOsTimelineDays(s, days),
            geoTimeline: generateGeoTimelineDays(s, days),
            // Advanced Analytics preserved
            seasonalPatterns: generateSeasonalPatterns(s),
            geographicGrowth: generateGeographicGrowth(s),
            osGeoPreferences: generateOsGeoPreferences(s),
            versionMigration: generateVersionMigration(s),
            growthTrajectory: generateGrowthTrajectory(s),
            platformTrends: generatePlatformTrends(s),
            upgradeAnalytics: generateUpgradeAnalytics(s),
            usageAnalytics: generateUsageAnalytics(s),
            retentionMetrics: generateRetentionMetrics(s),
            updatedAt: s.meta.updatedAt,
            file: FILE
        };
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
        // NEW: Enhanced Analytics for Retention and Usage
        upgradeAnalytics: generateUpgradeAnalytics(s),
        usageAnalytics: generateUsageAnalytics(s),
        retentionMetrics: generateRetentionMetrics(s),
        updatedAt: s.meta.updatedAt,
        file: FILE
    };
}
function generateVersionTimeline(s) {
    // Get all months in chronological order
    const allMonths = [];
    const years = Object.keys(s.months).sort();
    for (const y of years) {
        const monthsInYear = Object.keys(s.months[y] || {}).sort();
        for (const m of monthsInYear) {
            allMonths.push(`${y}-${m}`);
        }
    }
    // Get top versions (by total installs) to avoid chart clutter
    const topVersions = Object.entries(s.byExt)
        .sort(([, a], [, b]) => b - a)
        .slice(0, 8) // Show top 8 versions
        .map(([version]) => version);
    // Build timeline data for each version
    const versionData = {};
    for (const version of topVersions) {
        versionData[version] = allMonths.map(month => s.versionsByMonth[month]?.[version] || 0);
    }
    return {
        months: allMonths,
        versions: topVersions,
        data: versionData
    };
}
function generateOsTimeline(s) {
    // Get all months in chronological order
    const allMonths = [];
    const years = Object.keys(s.months).sort();
    for (const y of years) {
        const monthsInYear = Object.keys(s.months[y] || {}).sort();
        for (const m of monthsInYear) {
            allMonths.push(`${y}-${m}`);
        }
    }
    // Get all OS types from byOs, but filter to only those with non-zero timeline data
    const allOsTypes = Object.keys(s.byOs).sort();
    const activeOsTypes = [];
    // Build timeline data for each OS and only include those with non-zero data
    const osData = {};
    for (const osType of allOsTypes) {
        const timelineData = allMonths.map(month => s.osByMonth[month]?.[osType] || 0);
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
function generateGeoTimeline(s) {
    // Get all months in chronological order
    const allMonths = [];
    const years = Object.keys(s.months).sort();
    for (const y of years) {
        const monthsInYear = Object.keys(s.months[y] || {}).sort();
        for (const m of monthsInYear) {
            allMonths.push(`${y}-${m}`);
        }
    }
    // Get top countries (by total installs) to avoid chart clutter
    const topCountries = Object.entries(s.byCountry)
        .sort(([, a], [, b]) => b - a)
        .slice(0, 10) // Show top 10 countries
        .map(([country]) => country);
    // Build timeline data for each country
    const geoData = {};
    for (const country of topCountries) {
        geoData[country] = allMonths.map(month => s.geoByMonth[month]?.[country] || 0);
    }
    return {
        months: allMonths,
        countries: topCountries,
        data: geoData
    };
}
// Generate per-day version timeline during launch window
function generateVersionTimelineDays(s, days) {
    const topVersions = Object.entries(s.byExt)
        .sort(([, a], [, b]) => b - a)
        .slice(0, 8)
        .map(([v]) => v);
    const data = {};
    for (const v of topVersions) {
        data[v] = days.map(day => {
            // If we have per-day breakdown, use it
            const perDay = s.launchVersionsByDay?.[day]?.[v];
            if (typeof perDay === 'number')
                return perDay;
            // Otherwise, fallback: distribute monthly version totals proportionally across launchDays
            // Find month key for this day
            const m = day.slice(0, 7);
            const monthTotalForVersion = s.versionsByMonth?.[m]?.[v] || 0;
            const monthDays = days.filter(d => d.startsWith(m));
            const denom = monthDays.reduce((sum, d) => sum + (s.launchDays?.[d] || 0), 0) || monthDays.length || 1;
            const proportion = (s.launchDays?.[day] || 0) / denom;
            return Math.round(monthTotalForVersion * proportion);
        });
    }
    return { dates: days, versions: topVersions, data };
}
// Generate per-day OS timeline during launch window
function generateOsTimelineDays(s, days) {
    const osTypes = Object.keys(s.byOs).sort();
    const data = {};
    for (const os of osTypes) {
        data[os] = days.map(day => {
            const perDay = s.launchOsByDay?.[day]?.[os];
            if (typeof perDay === 'number')
                return perDay;
            // fallback proportional distribution based on monthly osByMonth
            const m = day.slice(0, 7);
            const monthTotalForOs = s.osByMonth?.[m]?.[os] || 0;
            const monthDays = days.filter(d => d.startsWith(m));
            const denom = monthDays.reduce((sum, d) => sum + (s.launchDays?.[d] || 0), 0) || monthDays.length || 1;
            const proportion = (s.launchDays?.[day] || 0) / denom;
            return Math.round(monthTotalForOs * proportion);
        });
    }
    return { dates: days, osTypes, data };
}
// Generate per-day geo (country) timeline during launch window
function generateGeoTimelineDays(s, days) {
    const countries = Object.keys(s.byCountry).sort();
    const data = {};
    for (const c of countries) {
        data[c] = days.map(day => {
            const perDay = s.launchGeoByDay?.[day]?.[c];
            if (typeof perDay === 'number')
                return perDay;
            const m = day.slice(0, 7);
            const monthTotalForCountry = s.geoByMonth?.[m]?.[c] || 0;
            const monthDays = days.filter(d => d.startsWith(m));
            const denom = monthDays.reduce((sum, d) => sum + (s.launchDays?.[d] || 0), 0) || monthDays.length || 1;
            const proportion = (s.launchDays?.[day] || 0) / denom;
            return Math.round(monthTotalForCountry * proportion);
        });
    }
    return { dates: days, countries, data };
}
// Advanced Analytics Functions
function generateSeasonalPatterns(s) {
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
function generateGeographicGrowth(s) {
    const countries = Object.keys(s.byCountry);
    const growthData = {};
    // Get all months for timeline
    const allMonths = [];
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
function generateOsGeoPreferences(s) {
    const preferences = {};
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
function generateVersionMigration(s) {
    const versions = Object.keys(s.byExt).sort((a, b) => {
        // Sort by semantic version
        const aParts = a.split('.').map(Number);
        const bParts = b.split('.').map(Number);
        for (let i = 0; i < Math.max(aParts.length, bParts.length); i++) {
            const aVal = aParts[i] || 0;
            const bVal = bParts[i] || 0;
            if (aVal !== bVal)
                return aVal - bVal;
        }
        return 0;
    });
    const migrationData = {};
    versions.forEach(version => {
        const versionTimeline = [];
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
function generateGrowthTrajectory(s) {
    const allMonths = [];
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
    if (values.length < 3)
        return null;
    // Linear regression
    const n = values.length;
    const xValues = Array.from({ length: n }, (_, i) => i);
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
function generatePlatformTrends(s) {
    const allMonths = [];
    const years = Object.keys(s.months).sort();
    for (const y of years) {
        const monthsInYear = Object.keys(s.months[y] || {}).sort();
        for (const m of monthsInYear) {
            allMonths.push(`${y}-${m}`);
        }
    }
    const trends = {};
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
// NEW: Generate upgrade analytics
function generateUpgradeAnalytics(s) {
    if (!s.upgrades)
        return { total: 0, months: [], timeline: [] };
    // Get all months for upgrade timeline
    const allMonths = [];
    const years = Object.keys(s.months).sort();
    for (const y of years) {
        const monthsInYear = Object.keys(s.months[y] || {}).sort();
        for (const m of monthsInYear) {
            allMonths.push(`${y}-${m}`);
        }
    }
    // Build upgrade timeline
    const upgradeTimeline = allMonths.map(month => {
        const [year, monthNum] = month.split('-');
        return s.upgrades.months[year]?.[monthNum]?.upgrades || 0;
    });
    // Calculate upgrade statistics
    const totalUpgrades = s.upgrades.total;
    const avgUpgradesPerMonth = upgradeTimeline.length > 0 ?
        Math.round((totalUpgrades / upgradeTimeline.length) * 10) / 10 : 0;
    // Calculate upgrade rate vs installs
    const totalInstalls = Object.values(s.byExt).reduce((a, b) => a + b, 0);
    const upgradeRate = totalInstalls > 0 ?
        Math.round((totalUpgrades / totalInstalls) * 1000) / 10 : 0;
    return {
        total: totalUpgrades,
        avgPerMonth: avgUpgradesPerMonth,
        upgradeRate: upgradeRate, // percentage of users who upgrade
        months: allMonths,
        timeline: upgradeTimeline,
        retentionIndicator: upgradeRate > 50 ? 'High' : upgradeRate > 25 ? 'Medium' : 'Low'
    };
}
// NEW: Generate usage analytics
function generateUsageAnalytics(s) {
    if (!s.usage)
        return { javaRuns: { total: 0 }, themeChanges: { total: 0 } };
    // Get all months for timeline
    const allMonths = [];
    const years = Object.keys(s.months).sort();
    for (const y of years) {
        const monthsInYear = Object.keys(s.months[y] || {}).sort();
        for (const m of monthsInYear) {
            allMonths.push(`${y}-${m}`);
        }
    }
    // Java runs analytics
    const javaRunsTimeline = allMonths.map(month => s.usage.javaRuns.months[month]?.runs || 0);
    const avgJavaRunsPerMonth = javaRunsTimeline.length > 0 ?
        Math.round((s.usage.javaRuns.total / javaRunsTimeline.length) * 10) / 10 : 0;
    // Theme changes analytics
    const themeChangesTimeline = allMonths.map(month => s.usage.themeChanges.months[month]?.changes || 0);
    const avgThemeChangesPerMonth = themeChangesTimeline.length > 0 ?
        Math.round((s.usage.themeChanges.total / themeChangesTimeline.length) * 10) / 10 : 0;
    // Calculate engagement metrics
    const totalInstalls = Object.values(s.byExt).reduce((a, b) => a + b, 0);
    const javaEngagementRate = totalInstalls > 0 ?
        Math.round((s.usage.javaRuns.total / totalInstalls) * 10) / 10 : 0;
    return {
        javaRuns: {
            total: s.usage.javaRuns.total,
            avgPerMonth: avgJavaRunsPerMonth,
            timeline: javaRunsTimeline,
            engagementRate: javaEngagementRate // runs per user
        },
        themeChanges: {
            total: s.usage.themeChanges.total,
            avgPerMonth: avgThemeChangesPerMonth,
            timeline: themeChangesTimeline
        },
        months: allMonths,
        totalUsageEvents: s.usage.javaRuns.total + s.usage.themeChanges.total
    };
}
// NEW: Generate retention metrics
function generateRetentionMetrics(s) {
    const totalInstalls = Object.values(s.byExt).reduce((a, b) => a + b, 0);
    if (!s.upgrades || !s.usage || totalInstalls === 0) {
        return {
            upgradeRetention: 0,
            usageRetention: 0,
            overallRetention: 0,
            retentionGrade: 'N/A'
        };
    }
    // Calculate different retention metrics
    const upgradeRetention = Math.round((s.upgrades.total / totalInstalls) * 1000) / 10;
    const usageRetention = Math.round(((s.usage.javaRuns.total > 0 ? 1 : 0) * totalInstalls / totalInstalls) * 1000) / 10;
    // Overall retention score (weighted average)
    const overallRetention = Math.round(((upgradeRetention * 0.6) + (usageRetention * 0.4)) * 10) / 10;
    // Retention grade
    let retentionGrade = 'F';
    if (overallRetention >= 80)
        retentionGrade = 'A';
    else if (overallRetention >= 65)
        retentionGrade = 'B';
    else if (overallRetention >= 50)
        retentionGrade = 'C';
    else if (overallRetention >= 35)
        retentionGrade = 'D';
    // Additional metrics
    const avgRunsPerUser = totalInstalls > 0 ?
        Math.round((s.usage.javaRuns.total / totalInstalls) * 10) / 10 : 0;
    return {
        upgradeRetention,
        usageRetention,
        overallRetention,
        retentionGrade,
        avgRunsPerUser,
        activeUserIndicators: {
            hasUpgrades: s.upgrades.total > 0,
            hasUsage: s.usage.javaRuns.total > 0,
            highEngagement: avgRunsPerUser > 10
        }
    };
}
function pathToStore() { return FILE; }
