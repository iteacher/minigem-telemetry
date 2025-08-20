import Fastify, { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';

async function main() {
  const app: FastifyInstance = Fastify({ logger: false, trustProxy: true });
  
  // Global error handling
  process.on('uncaughtException', (e) => console.error('uncaughtException', String(e)));
  process.on('unhandledRejection', (e) => console.error('unhandledRejection', String(e)));
  
  console.log('boot.start', { timestamp: new Date().toISOString() });

  // Simple routes for testing
  app.get('/health', async () => ({ 
    ok: true, 
    ts: Date.now(),
    message: 'Minimal server running' 
  }));

  app.get('/stats/install', async () => ({
    ok: true,
    installsTotal: 42,
    message: 'Minimal stats endpoint',
    timestamp: new Date().toISOString()
  }));

  app.get('/', async () => ({
    status: 'ok',
    message: 'Minimal Fastify server',
    timestamp: new Date().toISOString()
  }));

  try {
    // For CloudLinux: if no PORT env var, let system assign port
    const listenPort = process.env.PORT ? parseInt(process.env.PORT) : 0;
    const listenHost = process.env.PORT ? '0.0.0.0' : 'localhost';
    
    console.log('boot.listen.start', { 
      envPort: process.env.PORT, 
      listenPort, 
      listenHost 
    });
    
    await app.listen({ port: listenPort, host: listenHost });
    
    const address = app.server.address();
    console.log('boot.listen.ok', { address });
  } catch (e: any) {
    console.error('boot.listen.error', { error: String(e?.message || e) });
    throw e;
  }
}

main().catch(err => { 
  console.error('fatal', String(err)); 
  process.exit(1); 
});
