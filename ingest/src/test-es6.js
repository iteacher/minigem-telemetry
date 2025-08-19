// Minimal ES6 module test
console.log('[ES6-TEST] Starting...');

try {
  console.log('[ES6-TEST] Testing basic imports...');
  
  // Test if we can import our own modules
  import('./config.js').then(configModule => {
    console.log('[ES6-TEST] Config import successful');
    console.log('[ES6-TEST] PORT:', configModule.CONFIG.PORT);
    
    // Test if we can import Fastify
    return import('fastify');
  }).then(fastifyModule => {
    console.log('[ES6-TEST] Fastify import successful');
    
    const fastify = fastifyModule.default();
    
    fastify.get('/test', async () => {
      return { status: 'ok', test: true };
    });
    
    const port = process.env.PORT || 3000;
    console.log('[ES6-TEST] Starting Fastify on port:', port);
    
    return fastify.listen({ port, host: '0.0.0.0' });
  }).then(() => {
    console.log('[ES6-TEST] Server started successfully!');
  }).catch(err => {
    console.error('[ES6-TEST] Error:', err);
    process.exit(1);
  });
  
} catch (err) {
  console.error('[ES6-TEST] Sync error:', err);
  process.exit(1);
}

process.on('uncaughtException', (err) => {
  console.error('[ES6-TEST] Uncaught exception:', err);
});

process.on('unhandledRejection', (err) => {
  console.error('[ES6-TEST] Unhandled rejection:', err);
});
