"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const fastify_1 = __importDefault(require("fastify"));
const rate_limit_1 = __importDefault(require("@fastify/rate-limit"));
const config_1 = require("./config");
const validate_1 = require("./validate");
const geo_1 = require("./geo");
const logger_1 = require("./logger");
const store_1 = require("./store");
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
    const g = (0, geo_1.lookup)(ip); // MaxMind lookup
    return { country: g?.country || '', region: g?.region || '' };
}
async function main() {
    const app = (0, fastify_1.default)({ logger: false, bodyLimit: config_1.CONFIG.MAX_BODY, trustProxy: true });
    // Global and per-request logging
    process.on('uncaughtException', (e) => logger_1.log.error('uncaughtException', String(e)));
    process.on('unhandledRejection', (e) => logger_1.log.error('unhandledRejection', String(e)));
    // Log ALL incoming requests to debug routing issues
    app.addHook('onRequest', async (req) => {
        logger_1.log.info('req', { method: req.method, url: req.url, ip: req.ip, headers: req.headers });
    });
    app.addHook('onResponse', async (req, reply) => {
        logger_1.log.info('res', { method: req.method, url: req.url, status: reply.statusCode });
    });
    logger_1.log.info('boot.start', { port: config_1.CONFIG.PORT });
    await (0, geo_1.initGeo)();
    await (0, store_1.storeInit)();
    logger_1.log.info('boot.store.ready', { file: (0, store_1.pathToStore)() });
    logger_1.log.info('boot.register.rateLimit.start');
    await app.register(rate_limit_1.default, { max: config_1.CONFIG.RATE_LIMIT_MAX, timeWindow: config_1.CONFIG.RATE_LIMIT_TIME_WINDOW });
    logger_1.log.info('boot.register.rateLimit.done');
    app.get('/health', async () => ({ ok: true, ts: Date.now() }));
    app.get('/dbhealth', async () => ({ enabled: false, ok: false, note: 'DB removed; using JSON store', file: (0, store_1.pathToStore)() }));
    // Debug: write to log file and return path
    app.get('/debug/logping', async () => {
        logger_1.log.info('debug.logping', { when: new Date().toISOString() });
        return { ok: true, file: logger_1.log.file, level: logger_1.log.level };
    });
    // Debug: show non-sensitive env for process
    app.get('/debug/env', async () => {
        return {
            node: process.version,
            port: config_1.CONFIG.PORT,
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
    app.get('/debug/recent', async (_req, _reply) => ({ note: 'Not available without DB' }));
    app.get('/debug/counts', async (_req, _reply) => ({ note: 'Not available without DB' }));
    app.get('/stats', async (_req, reply) => reply.code(410).send({ error: 'gone', note: 'Use /stats/install (JSON store)' }));
    // Minimal installs-only stats for simplified dashboard
    app.get('/stats/install', async (_req, reply) => {
        try {
            return (0, store_1.storeReadInstallStats)(12);
        }
        catch (e) {
            logger_1.log.error('stats.install.error', { err: String(e) });
            return reply.code(500).send({ error: 'server_error' });
        }
    });
    app.post('/t', async (req, reply) => {
        try {
            const raw = req.body;
            const schema = typeof raw?.schema === 'string' ? raw.schema.trim().toLowerCase() : '';
            if (schema !== 'jwc.v1') {
                logger_1.log.warn('schema not supported', { got: raw?.schema });
                return reply.code(400).send({ error: 'schema_unsupported', expected: 'jwc.v1' });
            }
            const ip = pickClientIp(req);
            const geo = resolveGeo(req, ip);
            const batchRaw = Array.isArray(raw?.batch) ? raw.batch : [raw];
            let accepted = 0, skipped = 0;
            for (const evRaw of batchRaw) {
                const ev = (0, validate_1.normalizeEvent)(evRaw);
                if (!ev || !(0, validate_1.validateEvent)(ev)) {
                    skipped++;
                    logger_1.log.warn('event skipped: invalid', { evRaw });
                    continue;
                }
                try {
                    await (0, store_1.storeUpsertInstall)(ev, geo);
                    accepted++;
                    logger_1.log.info('ingest: inserted', { ev, geo });
                }
                catch (e) {
                    logger_1.log.error('db insert failed', { err: String(e), ev });
                    skipped++;
                    continue;
                }
            }
            logger_1.log.info('ingest: batch result', { accepted, skipped });
            return { ok: true, accepted, skipped };
        }
        catch (e) {
            logger_1.log.error('server_error', e);
            return reply.code(500).send({ error: 'server_error' });
        }
    });
    // Add a catch-all route to debug what requests are coming in
    app.all('*', async (req, reply) => {
        logger_1.log.info('catchall', { method: req.method, url: req.url });
        return reply.code(200).send({
            message: 'Debug - request received but no route matched',
            method: req.method,
            url: req.url,
            timestamp: new Date().toISOString()
        });
    });
    try {
        // For Passenger: if no PORT env var, let system assign port (use 0)
        const listenPort = process.env.PORT ? parseInt(process.env.PORT) : 0;
        const listenHost = process.env.PORT ? '0.0.0.0' : 'localhost';
        logger_1.log.info('boot.listen.start', {
            configPort: config_1.CONFIG.PORT,
            envPort: process.env.PORT,
            listenPort,
            listenHost
        });
        await app.listen({ port: listenPort, host: listenHost });
        const address = app.server.address();
        logger_1.log.info('boot.listen.ok', { address, configPort: config_1.CONFIG.PORT });
    }
    catch (e) {
        logger_1.log.error('boot.listen.error', { error: String(e?.message || e) });
        throw e;
    }
}
main().catch(err => { logger_1.log.error('fatal', String(err)); process.exit(1); });
