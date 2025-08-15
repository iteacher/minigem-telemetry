import Fastify from 'fastify';
import rateLimit from '@fastify/rate-limit';
import { CONFIG } from './config.js';
import { validateEvent, normalizeEvent } from './validate.js';
import { initGeo, lookup } from './geo.js';
import { dbEnabled, dbInit, dbInsertEvent, dbReadStats, dbHealth, dbRecent, dbCounts } from './db.js';
import { log } from './logger.js';
function h(req, name) {
    const v = req.headers[name.toLowerCase()];
    return Array.isArray(v) ? v[0] : v;
}
function isPrivate(ip) {
    if (!ip)
        return true;
    return /^(10\.|127\.|192\.168\.|172\.(1[6-9]|2\d|3[0-1])\.|::1|fc00:|fe80:|fd00:)/.test(ip);
}
function parseForwardedFor(v) {
    if (!v)
        return [];
    return v.split(',').map(s => s.trim()).filter(Boolean);
}
function pickClientIp(req) {
    const chain = [
        h(req, 'cf-connecting-ip'),
        h(req, 'x-client-ip'),
        h(req, 'x-real-ip'),
        ...parseForwardedFor(h(req, 'x-forwarded-for')),
        req.ip
    ].filter(Boolean);
    for (const ip of chain) {
        if (!isPrivate(ip))
            return ip;
    }
    return chain[0] || req.ip;
}
function extractGeoFromHeaders(req) {
    // Common provider headers
    const country = h(req, 'cf-ipcountry') || h(req, 'x-geo-country') || h(req, 'x-country-code') || h(req, 'x-appengine-country') || h(req, 'fastly-country-code') || undefined;
    const region = h(req, 'cf-region-code') || h(req, 'x-geo-region') || h(req, 'x-appengine-region') || h(req, 'x-region') || undefined;
    const out = {};
    if (country)
        out.country = country.toUpperCase();
    if (region)
        out.region = region.toUpperCase();
    return out;
}
function resolveGeo(req, ip) {
    const hdr = extractGeoFromHeaders(req);
    if (hdr.country) {
        return { country: hdr.country, region: hdr.region || '' };
    }
    const g = lookup(ip); // MaxMind lookup
    return { country: g?.country || '', region: g?.region || '' };
}
async function main() {
    const app = Fastify({ logger: false, bodyLimit: CONFIG.MAX_BODY, trustProxy: true });
    // Global and per-request logging
    process.on('uncaughtException', (e) => log.error('uncaughtException', String(e)));
    process.on('unhandledRejection', (e) => log.error('unhandledRejection', String(e)));
    app.addHook('onRequest', async (req) => { log.info('req', { method: req.method, url: req.url, ip: req.ip }); });
    app.addHook('onResponse', async (req, reply) => { log.info('res', { method: req.method, url: req.url, status: reply.statusCode }); });
    log.info('boot.start', { port: CONFIG.PORT });
    await initGeo();
    await dbInit();
    log.info('boot.dbInit.done', { enabled: dbEnabled() });
    await app.register(rateLimit, { max: CONFIG.RATE_LIMIT_MAX, timeWindow: CONFIG.RATE_LIMIT_TIME_WINDOW });
    app.get('/health', async () => ({ ok: true, ts: Date.now() }));
    app.get('/dbhealth', async () => { try {
        return await dbHealth();
    }
    catch (e) {
        return { enabled: dbEnabled(), error: String(e) };
    } });
    // Debug: write to log file and return path
    app.get('/debug/logping', async () => {
        log.info('debug.logping', { when: new Date().toISOString() });
        return { ok: true, file: log.file, level: log.level };
    });
    // Debug: show non-sensitive env for process
    app.get('/debug/env', async () => {
        return {
            node: process.version,
            port: CONFIG.PORT,
            env: {
                PORT: process.env.PORT || null,
                DATABASE_URL: !!process.env.DATABASE_URL,
                PGHOST: process.env.PGHOST || null,
                PGUSER: process.env.PGUSER || null,
                PGDATABASE: process.env.PGDATABASE || null,
                PGPORT: process.env.PGPORT || null,
                PGSSL: process.env.PGSSL || null,
                LOG_FILE: process.env.LOG_FILE || null,
                LOG_LEVEL: process.env.LOG_LEVEL || null,
                DEBUG_DB: process.env.DEBUG_DB || null,
                DEBUG_STATS_TRACE: process.env.DEBUG_STATS_TRACE || null
            }
        };
    });
    app.get('/debug/recent', async (req, reply) => {
        if (CONFIG.STATS_SECRET) {
            const key = req.headers['x-stats-key'] || req.query?.key;
            if (key !== CONFIG.STATS_SECRET)
                return reply.code(401).send({ error: 'unauthorized' });
        }
        const lim = Number(req.query?.limit || 50);
        return { rows: await dbRecent(lim) };
    });
    app.get('/debug/counts', async (req, reply) => {
        if (CONFIG.STATS_SECRET) {
            const key = req.headers['x-stats-key'] || req.query?.key;
            if (key !== CONFIG.STATS_SECRET)
                return reply.code(401).send({ error: 'unauthorized' });
        }
        const hrs = Number(req.query?.hours || 24);
        return await dbCounts(hrs);
    });
    app.get('/stats', async (req, reply) => {
        try {
            // Public stats endpoint: DB is the single source of truth.
            if (!dbEnabled()) {
                return reply.code(503).send({ error: 'db_disabled', message: 'Database not configured. Stats require DB-only mode.' });
            }
            const q = req.query || {};
            const from = typeof q.from === 'string' ? q.from : undefined;
            const to = typeof q.to === 'string' ? q.to : undefined;
            const data = await dbReadStats(CONFIG.STATS_WINDOW_DAYS, from, to);
            if (CONFIG.DEBUG_STATS_TRACE && q.debug === '1') {
                log.info('stats.trace', { from, to, trace: data?._trace });
            }
            if (data)
                return data; // DB-backed stats
            return reply.code(204).send();
        }
        catch (e) {
            req.log?.error?.(e);
            const today = new Date().toISOString().slice(0, 10);
            return {
                from: today,
                to: today,
                windowDays: CONFIG.STATS_WINDOW_DAYS,
                total: 0,
                uniques: 0,
                osTypes: 0,
                extTypes: 0,
                vscodeTypes: 0,
                daily: { dates: [], hits: [], uniques: [] },
                dailyOs: { labels: [], series: [] },
                byEvent: {}, byOs: {}, byExt: {}, byVscode: {},
                hourly: new Array(24).fill(0),
                hourlyVisitors: new Array(24).fill(0),
                dow: new Array(7).fill(0),
                runs: { started: 0, completed: 0 },
                errors: { compile: 0, runtime: 0, compileRate: 0, runtimeRate: 0 },
                durations: { count: 0, median: 0, p90: 0, hist: { labels: [], values: [] }, avgWaitMs: 0 },
                exitCodes: {}, outputBuckets: {},
                interactiveRate: 0, truncationRate: 0, topExceptions: {},
                geo: { byContinent: {}, byCountry: {} },
                tables: { eventsTop: [], extTop: [], vscodeTop: [], exitTop: [], exceptionsTop: [] }
            };
        }
    });
    app.post('/t', async (req, reply) => {
        try {
            // Ingest requires DB; no file fallbacks.
            if (!dbEnabled()) {
                return reply.code(503).send({ error: 'db_disabled', message: 'Database not configured. Ingestion requires DB-only mode.' });
            }
            const raw = req.body;
            const schema = typeof raw?.schema === 'string' ? raw.schema.trim().toLowerCase() : '';
            if (schema !== 'jwc.v1') {
                log.warn('schema not supported', { got: raw?.schema });
                return reply.code(400).send({ error: 'schema_unsupported', expected: 'jwc.v1' });
            }
            const ip = pickClientIp(req);
            const geo = resolveGeo(req, ip);
            const batchRaw = Array.isArray(raw?.batch) ? raw.batch : [raw];
            let accepted = 0, skipped = 0;
            for (const evRaw of batchRaw) {
                const ev = normalizeEvent(evRaw);
                if (!ev || !validateEvent(ev)) {
                    skipped++;
                    log.warn('event skipped: invalid', { evRaw });
                    continue;
                }
                try {
                    await dbInsertEvent(ev, geo);
                    accepted++;
                    log.info('ingest: inserted', { ev, geo });
                }
                catch (e) {
                    log.error('db insert failed', { err: String(e), ev });
                    skipped++;
                    continue;
                }
            }
            log.info('ingest: batch result', { accepted, skipped });
            return { ok: true, accepted, skipped };
        }
        catch (e) {
            log.error('server_error', e);
            return reply.code(500).send({ error: 'server_error' });
        }
    });
    await app.listen({ port: CONFIG.PORT, host: '0.0.0.0' });
    log.info('boot.listen', { url: `http://0.0.0.0:${CONFIG.PORT}` });
}
main().catch(err => { console.error('fatal:', err); process.exit(1); });
