import Fastify, { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import rateLimit from '@fastify/rate-limit';
import cors from '@fastify/cors';
import { CONFIG } from './config.js';
import { validateEnvelope, validateEvent, normalizeEvent } from './validate.js';
import { initGeo, lookup } from './geo.js';
import { log } from './logger.js';
import { storeInit, storeUpsertInstall, storeReadInstallStats, pathToStore } from './store.js';

function h(req: FastifyRequest, name: string): string | undefined {
  const v = req.headers[name.toLowerCase()];
  return Array.isArray(v) ? v[0] : v as string | undefined;
}

function isPrivate(ip?: string) {
  if (!ip) return true;
  return /^(10\.|127\.|192\.168\.|172\.(1[6-9]|2\d|3[0-1])\.|::1|fc00:|fe80:|fd00:)/.test(ip);
}

function parseForwardedFor(v?: string): string[] {
  if (!v) return [];
  return v.split(',').map(s => s.trim()).filter(Boolean);
}

function pickClientIp(req: FastifyRequest): string {
  const chain = [
    h(req,'cf-connecting-ip'),
    h(req,'x-client-ip'),
    h(req,'x-real-ip'),
    ...parseForwardedFor(h(req,'x-forwarded-for')),
    (req as any).ip
  ].filter(Boolean) as string[];
  for (const ip of chain) { if (!isPrivate(ip)) return ip; }
  return chain[0] || (req as any).ip;
}

function extractGeoFromHeaders(req: FastifyRequest): { country?: string; region?: string } {
  // Common provider headers
  const country = h(req,'cf-ipcountry') || h(req,'x-geo-country') || h(req,'x-country-code') || h(req,'x-appengine-country') || h(req,'fastly-country-code') || undefined;
  const region = h(req,'cf-region-code') || h(req,'x-geo-region') || h(req,'x-appengine-region') || h(req,'x-region') || undefined;
  const out: any = {};
  if (country) out.country = country.toUpperCase();
  if (region) out.region = region.toUpperCase();
  return out;
}

function resolveGeo(req: FastifyRequest, ip: string): { country: string; region: string } {
  const hdr = extractGeoFromHeaders(req);
  if (hdr.country) {
    return { country: hdr.country, region: hdr.region || '' };
  }
  const g = lookup(ip) as any; // MaxMind lookup
  return { country: g?.country || '', region: g?.region || '' };
}

async function main() {
  const app: FastifyInstance = Fastify({ logger: false, bodyLimit: CONFIG.MAX_BODY, trustProxy: true });
  // Global and per-request logging
  process.on('uncaughtException', (e) => log.error('uncaughtException', String(e)));
  process.on('unhandledRejection', (e) => log.error('unhandledRejection', String(e)));
  app.addHook('onRequest', async (req) => { log.info('req', { method: req.method, url: req.url, ip: (req as any).ip }); });
  app.addHook('onResponse', async (req, reply) => { log.info('res', { method: req.method, url: req.url, status: reply.statusCode }); });
  log.info('boot.start', { port: CONFIG.PORT });
  await initGeo();
  await storeInit();
  log.info('boot.store.ready', { file: pathToStore() });

  log.info('boot.register.rateLimit.start');
  await app.register(rateLimit, { max: CONFIG.RATE_LIMIT_MAX, timeWindow: CONFIG.RATE_LIMIT_TIME_WINDOW });
  log.info('boot.register.rateLimit.done');

  // CORS to allow dashboard hosted on another origin to call this API
  await app.register(cors, {
    origin: (origin, cb) => cb(null, true), // allow all origins for now
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type']
  });

  app.get('/health', async () => ({ ok: true, ts: Date.now() }));

  app.get('/dbhealth', async () => ({ enabled: false, ok: false, note: 'DB removed; using JSON store', file: pathToStore() }));

  // Debug: write to log file and return path
  app.get('/debug/logping', async () => {
    log.info('debug.logping', { when: new Date().toISOString() });
    return { ok: true, file: (log as any).file, level: (log as any).level };
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

  app.get('/debug/recent', async (_req, _reply) => ({ note: 'Not available without DB' }));

  app.get('/debug/counts', async (_req, _reply) => ({ note: 'Not available without DB' }));

  app.get('/stats', async (_req: FastifyRequest, reply: FastifyReply) => reply.code(410).send({ error: 'gone', note: 'Use /stats/install (JSON store)' }));

  // Minimal installs-only stats for simplified dashboard
  app.get('/stats/install', async (_req: FastifyRequest, reply: FastifyReply) => {
    try { return storeReadInstallStats(12); } catch (e) { log.error('stats.install.error', { err: String(e) }); return reply.code(500).send({ error: 'server_error' }); }
  });

  app.post('/t', async (req: FastifyRequest, reply: FastifyReply) => {
    try {
      const raw: any = (req as any).body;
      const schema = typeof raw?.schema === 'string' ? raw.schema.trim().toLowerCase() : '';
      if (schema !== 'jwc.v1') {
  log.warn('schema not supported', { got: raw?.schema });
        return reply.code(400).send({ error: 'schema_unsupported', expected: 'jwc.v1' });
      }

      const ip = pickClientIp(req);
      const geo = resolveGeo(req, ip);

      const batchRaw: any[] = Array.isArray(raw?.batch) ? raw.batch : [raw];
      let accepted = 0, skipped = 0;
      for (const evRaw of batchRaw) {
        const ev = normalizeEvent(evRaw);
        if (!ev || !validateEvent(ev)) { skipped++; log.warn('event skipped: invalid', { evRaw }); continue; }
        try {
          await storeUpsertInstall(ev, geo);
          accepted++;
          log.info('ingest: inserted', { ev, geo });
        }
        catch (e) {
          log.error('db insert failed', { err: String(e), ev });
          skipped++; continue;
        }
      }
      log.info('ingest: batch result', { accepted, skipped });
      return { ok: true, accepted, skipped };
    } catch (e) {
      log.error('server_error', e);
      return reply.code(500).send({ error: 'server_error' });
    }
  });

  try {
    log.info('boot.listen.start', { port: CONFIG.PORT, host: '0.0.0.0' });
    await app.listen({ port: CONFIG.PORT, host: '0.0.0.0' });
    log.info('boot.listen.ok', { url: `http://0.0.0.0:${CONFIG.PORT}` });
  } catch (e: any) {
    log.error('boot.listen.error', { error: String(e?.message || e) });
    throw e;
  }
}

main().catch(err => { log.error('fatal', String(err)); process.exit(1); });