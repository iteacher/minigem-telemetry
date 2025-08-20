#!/usr/bin/env node

// Debug script to isolate the server startup issue
console.log('=== Debug Server Startup ===');

async function testComponents() {
  console.log('\n1. Testing basic requires...');
  
  try {
    const { CONFIG } = require('./config.js');
    console.log('✓ Config loaded');
    console.log('  - PORT:', CONFIG.PORT);
    console.log('  - STORE_FILE:', CONFIG.STORE_FILE);
  } catch (e) {
    console.log('✗ Config failed:', e.message);
    return;
  }

  try {
    const { log } = require('./logger.js');
    console.log('✓ Logger loaded');
    log.info('debug.test', { message: 'test log entry' });
  } catch (e) {
    console.log('✗ Logger failed:', e.message);
    return;
  }

  try {
    const geo = require('./geo.js');
    console.log('✓ Geo module loaded');
    
    // Test geo initialization
    console.log('\n2. Testing geo initialization...');
    await geo.initGeo();
    console.log('✓ Geo initialization completed');
  } catch (e) {
    console.log('✗ Geo failed:', e.message);
    console.log('Stack:', e.stack);
  }

  try {
    const store = require('./store.js');
    console.log('✓ Store module loaded');
    
    // Test store initialization
    console.log('\n3. Testing store initialization...');
    await store.storeInit();
    console.log('✓ Store initialization completed');
    
    // Test store read
    const stats = store.storeReadInstallStats(12);
    console.log('✓ Store read test:', Object.keys(stats));
  } catch (e) {
    console.log('✗ Store failed:', e.message);
    console.log('Stack:', e.stack);
  }

  try {
    console.log('\n4. Testing Fastify setup...');
    const Fastify = require('fastify');
    const app = Fastify({ logger: false, trustProxy: true });
    
    app.get('/test', async () => ({ ok: true }));
    
    console.log('✓ Fastify setup completed');
    
    // Test listening
    const port = process.env.PORT || 3000;
    await app.listen({ port: port, host: '0.0.0.0' });
    console.log(`✓ Fastify listening on port ${port}`);
    
    // Stop the server
    await app.close();
    console.log('✓ Fastify closed cleanly');
    
  } catch (e) {
    console.log('✗ Fastify failed:', e.message);
    console.log('Stack:', e.stack);
  }

  console.log('\n=== Component test complete ===');
}

testComponents().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
