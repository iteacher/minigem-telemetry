import Fastify, { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';

// Simple memory store with test data
const memoryStore = {
  installsTotal: 456,
  months: ['2025-07', '2025-08'],
  counts: [234, 222],
  byExt: { '1.1.31': 234, '1.1.30': 156, '1.1.29': 66 },
  byOs: { 
    'darwin-arm64': 156, 
    'win32-x64': 134, 
    'linux-x64': 89,
    'darwin-x64': 77
  },
  byCountry: { 
    'US': 189, 
    'GB': 78, 
    'CA': 56, 
    'DE': 44,
    'AU': 33,
    'FR': 28,
    'Unknown': 28
  },
  updatedAt: new Date().toISOString()
};

async function createApp(): Promise<FastifyInstance> {
  const app: FastifyInstance = Fastify({ 
    logger: false, 
    bodyLimit: 64 * 1024, 
    trustProxy: true 
  });

  // Basic health check
  app.get('/health', async () => ({ 
    ok: true, 
    ts: Date.now(),
    message: 'Simple server running'
  }));

  // Stats endpoint that your dashboard needs
  app.get('/stats/install', async (_req: FastifyRequest, _reply: FastifyReply) => {
    const response = {
      from: memoryStore.months[0] || '2025-07',
      to: memoryStore.months[memoryStore.months.length - 1] || '2025-08',
      windowMonths: memoryStore.months.length,
      totalMonths: memoryStore.months.length,
      installsTotal: memoryStore.installsTotal,
      monthlyInstalls: { 
        months: memoryStore.months, 
        counts: memoryStore.counts 
      },
      dailyInstalls: { 
        dates: memoryStore.months, 
        counts: memoryStore.counts 
      },
      byExt: memoryStore.byExt,
      byOs: memoryStore.byOs,
      byCountry: memoryStore.byCountry,
      versionTimeline: { 
        months: memoryStore.months, 
        versions: Object.keys(memoryStore.byExt), 
        data: {} 
      },
      osTimeline: { 
        months: memoryStore.months, 
        osTypes: Object.keys(memoryStore.byOs), 
        data: {} 
      },
      geoTimeline: { 
        months: memoryStore.months, 
        countries: Object.keys(memoryStore.byCountry), 
        data: {} 
      },
      seasonalPatterns: [],
      geographicGrowth: {},
      osGeoPreferences: {},
      versionMigration: {},
      growthTrajectory: null,
      platformTrends: {},
      upgradeAnalytics: { total: 0, months: [], timeline: [] },
      usageAnalytics: { 
        javaRuns: { total: 0 }, 
        themeChanges: { total: 0 } 
      },
      retentionMetrics: {
        upgradeRetention: 0,
        usageRetention: 0,
        overallRetention: 0,
        retentionGrade: 'N/A'
      },
      updatedAt: memoryStore.updatedAt,
      file: 'memory-store-simple',
      note: 'Simple hosting-compatible server'
    };
    
    return response;
  });

  // Basic telemetry endpoint
  app.post('/t', async (req: FastifyRequest, reply: FastifyReply) => {
    try {
      // Just accept and log for now
      console.log('Telemetry received:', JSON.stringify((req as any).body));
      return { ok: true, accepted: 1, skipped: 0 };
    } catch (e) {
      console.error('Telemetry error:', e);
      return reply.code(500).send({ error: 'server_error' });
    }
  });

  // Catch all other routes
  app.all('*', async (req, reply) => {
    return reply.code(200).send({
      message: 'Simple server - route not found',
      method: req.method,
      url: req.url,
      timestamp: new Date().toISOString()
    });
  });

  return app;
}

// Check if we're in a hosting environment
const isHosting = process.env.NODE_ENV === 'production' || 
                  process.env.PASSENGER_APP_ENV || 
                  process.env.PORT;

async function main() {
  try {
    console.log('=== Simple Telemetry Server Starting ===');
    console.log('Environment check:', {
      NODE_ENV: process.env.NODE_ENV,
      PASSENGER_APP_ENV: process.env.PASSENGER_APP_ENV,
      PORT: process.env.PORT,
      isHosting
    });

    const app = await createApp();
    console.log('✓ App created successfully');

    if (isHosting) {
      console.log('✓ Hosting environment detected - letting host manage port');
      // In hosting environment, don't call listen - let the host handle it
      return app;
    } else {
      // Local development - bind to port
      const port = parseInt(process.env.PORT || '3000');
      await app.listen({ port, host: '0.0.0.0' });
      console.log(`✓ Server listening on port ${port}`);
      return app;
    }
  } catch (error) {
    console.error('FATAL ERROR:', error);
    throw error;
  }
}

// Handle different environments
if (isHosting) {
  // Export for hosting environments
  main().then(app => {
    if (app) {
      console.log('✓ Server ready for hosting environment');
      // For some hosting environments, we might need to export the app
      module.exports = app;
    }
  }).catch(err => {
    console.error('Startup failed:', err);
    process.exit(1);
  });
} else {
  // Run directly for local development
  main().catch(err => {
    console.error('Startup failed:', err);
    process.exit(1);
  });
}
