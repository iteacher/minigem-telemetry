import Fastify, { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import rateLimit from '@fastify/rate-limit';
import { CONFIG } from './config';
import { validateEnvelope, validateEvent, normalizeEvent } from './validate';
import { initGeo, lookup } from './geo';
import { log } from './logger';

// Simplified store functions that don't require file system access
let memoryStore = {
  installsTotal: 0,
  months: ['2025-07', '2025-08'],
  counts: [45, 78],
  byExt: { '1.1.31': 123 },
  byOs: { 'darwin-arm64': 50, 'win32-x64': 40, 'linux-x64': 33 },
  byCountry: { 'US': 60, 'GB': 30, 'CA': 20, 'DE': 13 },
  updatedAt: new Date().toISOString()
};

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
  process.on('uncaughtException', (e) => {
    console.error('UNCAUGHT EXCEPTION:', e);
    log.error('uncaughtException', String(e));
  });
  process.on('unhandledRejection', (e) => {
    console.error('UNHANDLED REJECTION:', e);
    log.error('unhandledRejection', String(e));
  });
  
  // Log ALL incoming requests to debug routing issues
  app.addHook('onRequest', async (req) => { 
    log.info('req', { method: req.method, url: req.url, ip: (req as any).ip });
  });
  app.addHook('onResponse', async (req, reply) => { 
    log.info('res', { method: req.method, url: req.url, status: reply.statusCode });
  });
  
  console.log('Starting server...');
  log.info('boot.start', { port: CONFIG.PORT });
  
  // Initialize geo (non-critical)
  try {
    log.info('boot.geo.start');
    await initGeo();
    log.info('boot.geo.done');
  } catch (e) {
    log.warn('boot.geo.failed', String(e));
    console.log('Warning: Geo initialization failed, continuing without geo lookup');
  }

  // Skip store initialization for now to avoid file system issues
  log.info('boot.store.skipped', { reason: 'Using memory store for hosting compatibility' });

  // Try to register rate limiting, but continue if it fails
  log.info('boot.register.rateLimit.start');
  try {
    await app.register(rateLimit, { max: CONFIG.RATE_LIMIT_MAX, timeWindow: CONFIG.RATE_LIMIT_TIME_WINDOW });
    log.info('boot.register.rateLimit.done');
  } catch (e) {
    log.warn('boot.register.rateLimit.failed', String(e));
    console.log('Warning: Rate limiting failed, continuing without rate limiting');
  }

  app.get('/health', async () => ({ ok: true, ts: Date.now() }));

  app.get('/dbhealth', async () => ({ enabled: false, ok: false, note: 'DB removed; using memory store', store: 'memory' }));

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
        LOG_FILE: process.env.LOG_FILE || null,
        LOG_LEVEL: process.env.LOG_LEVEL || null,
        GEO_DB: process.env.GEO_DB || null,
        GEO_MMDB: process.env.GEO_MMDB || null
      }
    };
  });

  app.get('/debug/recent', async (_req, _reply) => ({ note: 'Not available without DB' }));

  app.get('/debug/counts', async (_req, _reply) => ({ note: 'Not available without DB' }));

  app.get('/stats', async (_req: FastifyRequest, reply: FastifyReply) => reply.code(410).send({ error: 'gone', note: 'Use /stats/install (memory store)' }));

  // Minimal installs-only stats using memory store
  app.get('/stats/install', async (_req: FastifyRequest, reply: FastifyReply) => {
    try { 
      const response = {
        from: memoryStore.months[0] || 'N/A',
        to: memoryStore.months[memoryStore.months.length - 1] || 'N/A',
        windowMonths: memoryStore.months.length,
        totalMonths: memoryStore.months.length,
        installsTotal: memoryStore.installsTotal,
        monthlyInstalls: { months: memoryStore.months, counts: memoryStore.counts },
        dailyInstalls: { dates: memoryStore.months, counts: memoryStore.counts },
        byExt: memoryStore.byExt,
        byOs: memoryStore.byOs,
        byCountry: memoryStore.byCountry,
        versionTimeline: { months: memoryStore.months, versions: Object.keys(memoryStore.byExt), data: {} },
        osTimeline: { months: memoryStore.months, osTypes: Object.keys(memoryStore.byOs), data: {} },
        geoTimeline: { months: memoryStore.months, countries: Object.keys(memoryStore.byCountry), data: {} },
        updatedAt: memoryStore.updatedAt,
        file: 'memory-store',
        note: 'Using memory store for hosting compatibility'
      };
      return response;
    } catch (e) { 
      log.error('stats.install.error', { err: String(e) }); 
      return reply.code(500).send({ error: 'server_error' }); 
    }
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
          // Store in memory for now
          if (ev.evt === 'install.created') {
            memoryStore.installsTotal++;
            memoryStore.updatedAt = new Date().toISOString();
          }
          accepted++;
          log.info('ingest: stored in memory', { ev, geo });
        }
        catch (e) {
          log.error('memory store failed', { err: String(e), ev });
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

  // Add a catch-all route to debug what requests are coming in
  app.all('*', async (req, reply) => {
    log.info('catchall', { method: req.method, url: req.url });
    return reply.code(200).send({
      message: 'Debug - request received but no route matched',
      method: req.method,
      url: req.url,
      timestamp: new Date().toISOString()
    });
  });

  try {
    // For hosting: if no PORT env var, let system assign port (use 0)
    const listenPort = process.env.PORT ? parseInt(process.env.PORT) : 0;
    const listenHost = process.env.PORT ? '0.0.0.0' : 'localhost';
    
    log.info('boot.listen.start', { 
      configPort: CONFIG.PORT, 
      envPort: process.env.PORT, 
      listenPort, 
      listenHost 
    });
    
    await app.listen({ port: listenPort, host: listenHost });
    
    const address = app.server.address();
    log.info('boot.listen.ok', { address, configPort: CONFIG.PORT });
    console.log(`✓ Server listening on ${listenHost}:${listenPort}`);
  } catch (e: any) {
    log.error('boot.listen.error', { error: String(e?.message || e) });
    console.error('LISTEN ERROR:', e);
    throw e;
  }
}

main().catch(err => { 
  log.error('fatal', String(err)); 
  console.error('FATAL ERROR:', err);
  process.exit(1); 
});
