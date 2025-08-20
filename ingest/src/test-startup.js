#!/usr/bin/env node

// Simple startup test to debug server issues
const fs = require('fs');
const path = require('path');

console.log('=== JWC Telemetry Server Startup Debug ===');
console.log('Node.js version:', process.version);
console.log('Working directory:', process.cwd());
console.log('Arguments:', process.argv);

console.log('\n=== Environment Variables ===');
const envVars = [
  'PORT', 'LOG_FILE', 'LOG_LEVEL', 'GEO_DB', 'GEO_MMDB', 
  'STATS_JSON_FILE', 'LOG_DIR', 'LAUNCH_DATE', 'LAUNCH_DURATION'
];

envVars.forEach(name => {
  console.log(`${name}: ${process.env[name] || '(not set)'}`);
});

console.log('\n=== File System Checks ===');
const checkPaths = [
  '/tmp/jwc-telemetry.log',
  '/home/mandersj/telemetary.jwc.minigem.uk/data/test3.json',
  '/home/mandersj/mmdb/GeoLite2-Country.mmdb',
  '/opt/jwc-telemetry/geo/GeoLite2-City.mmdb'
];

checkPaths.forEach(filePath => {
  try {
    const exists = fs.existsSync(filePath);
    if (exists) {
      const stats = fs.statSync(filePath);
      console.log(`✓ ${filePath} (${stats.size} bytes, ${stats.mtime})`);
    } else {
      console.log(`✗ ${filePath} (does not exist)`);
    }
  } catch (e) {
    console.log(`✗ ${filePath} (error: ${e.message})`);
  }
});

console.log('\n=== Directory Permissions ===');
const checkDirs = [
  '/tmp',
  '/home/mandersj/telemetary.jwc.minigem.uk/data',
  path.dirname('/tmp/jwc-telemetry.log')
];

checkDirs.forEach(dirPath => {
  try {
    fs.accessSync(dirPath, fs.constants.W_OK);
    console.log(`✓ ${dirPath} (writable)`);
  } catch (e) {
    console.log(`✗ ${dirPath} (not writable: ${e.message})`);
  }
});

console.log('\n=== Port Check ===');
const net = require('net');
const testPort = process.env.PORT || 3000;

const server = net.createServer();
server.listen(testPort, '0.0.0.0', () => {
  console.log(`✓ Port ${testPort} is available`);
  server.close();
}).on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.log(`✗ Port ${testPort} is already in use`);
  } else {
    console.log(`✗ Port ${testPort} error: ${err.message}`);
  }
});

console.log('\n=== Attempting to load server modules ===');
try {
  console.log('Loading config...');
  const { CONFIG } = require('./config.js');
  console.log('✓ Config loaded:', JSON.stringify(CONFIG, null, 2));
} catch (e) {
  console.log('✗ Config load failed:', e.message);
}

try {
  console.log('Loading logger...');
  const { log } = require('./logger.js');
  log.info('test.startup', { message: 'Logger test successful' });
  console.log('✓ Logger loaded and tested');
} catch (e) {
  console.log('✗ Logger load failed:', e.message);
}

try {
  console.log('Loading store...');
  const { storeInit, pathToStore } = require('./store.js');
  console.log('✓ Store module loaded, file path:', pathToStore());
} catch (e) {
  console.log('✗ Store load failed:', e.message);
}

try {
  console.log('Loading geo...');
  const { initGeo } = require('./geo.js');
  console.log('✓ Geo module loaded');
} catch (e) {
  console.log('✗ Geo load failed:', e.message);
}

console.log('\n=== Debug Complete ===');
