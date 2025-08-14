import Fastify, { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import rateLimit from '@fastify/rate-limit';
import { CONFIG } from './config.js';
import { validateEnvelope, validateEvent, normalizeEvent } from './validate.js';
import { initGeo, lookup } from './geo.js';
import { lineForEvent, appendLine, appendJsonEvent } from './transform.js';
import { readWindowStats } from './stats.js';
import { dbEnabled, dbInit, dbInsertEvent, dbReadStats, dbHealth, dbRecent, dbCounts } from './db.js';

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
  const app: FastifyInstance = Fastify({ logger: true, bodyLimit: CONFIG.MAX_BODY, trustProxy: true });
  await initGeo();
  await dbInit();

  await app.register(rateLimit, { max: CONFIG.RATE_LIMIT_MAX, timeWindow: CONFIG.RATE_LIMIT_TIME_WINDOW });

  app.get('/health', async () => ({ ok: true, ts: Date.now() }));

  app.get('/dbhealth', async () => { try { return await dbHealth(); } catch (e) { return { enabled: dbEnabled(), error: String(e) }; } });

  app.get('/debug/recent', async (req, reply) => {
    if (CONFIG.STATS_SECRET) {
      const key = (req.headers['x-stats-key'] as string) || (req.query as any)?.key;
      if (key !== CONFIG.STATS_SECRET) return reply.code(401).send({ error: 'unauthorized' });
    }
    const lim = Number((req.query as any)?.limit||50);
    return { rows: await dbRecent(lim) };
  });

  app.get('/debug/counts', async (req, reply) => {
    if (CONFIG.STATS_SECRET) {
      const key = (req.headers['x-stats-key'] as string) || (req.query as any)?.key;
      if (key !== CONFIG.STATS_SECRET) return reply.code(401).send({ error: 'unauthorized' });
    }
    const hrs = Number((req.query as any)?.hours||24);
    return await dbCounts(hrs);
  });

  app.get('/stats', async (req: FastifyRequest, reply: FastifyReply) => {
    try {
  // Public stats endpoint: no auth required.
      if (dbEnabled()) {
        const q: any = (req as any).query || {};
        const from = typeof q.from === 'string' ? q.from : undefined;
        const to = typeof q.to === 'string' ? q.to : undefined;
        const data = await dbReadStats(CONFIG.STATS_WINDOW_DAYS, from, to);
        if (data) return data; // minimal DB stats
      }
      return readWindowStats();
    } catch (e) {
      (req as any).log?.error?.(e);
      const today = new Date().toISOString().slice(0,10);
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
      } as any;
    }
  });

  app.post('/t', async (req: FastifyRequest, reply: FastifyReply) => {
    try {
      const raw: any = (req as any).body;
      const schema = typeof raw?.schema === 'string' ? raw.schema.trim().toLowerCase() : '';
      if (schema !== 'jwc.v1') {
        (req as any).log.warn({ got: raw?.schema }, 'schema not supported');
        return reply.code(400).send({ error: 'schema_unsupported', expected: 'jwc.v1' });
      }

      const ip = pickClientIp(req);
      const geo = resolveGeo(req, ip);

      const batchRaw: any[] = Array.isArray(raw?.batch) ? raw.batch : [raw];
      let accepted = 0, skipped = 0;
      for (const evRaw of batchRaw) {
        const ev = normalizeEvent(evRaw);
        if (!ev || !validateEvent(ev)) { skipped++; (req as any).log.warn({ evRaw }, 'event skipped: invalid'); continue; }
        try {
          if (dbEnabled()) await dbInsertEvent(ev, geo);
        } catch (e) {
          (req as any).log.error({ err: String(e), ev }, 'db insert failed');
          skipped++; continue;
        }
        try { appendLine(lineForEvent(ev, geo)); } catch {}
        try { appendJsonEvent(ev, geo); } catch {}
        accepted++;
      }
      return { ok: true, accepted, skipped };
    } catch (e) {
      (req as any).log.error(e);
      return reply.code(500).send({ error: 'server_error' });
    }
  });

  await app.listen({ port: CONFIG.PORT, host: '127.0.0.1' });
  app.log.info(`telemetry ingest listening on ${CONFIG.PORT}`);
}

main().catch(err => { console.error('fatal:', err); process.exit(1); });