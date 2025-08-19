// Diagnostic script - check basic Node.js functionality
console.log('[DIAG] Node.js version:', process.version);
console.log('[DIAG] Process argv:', process.argv);
console.log('[DIAG] Current working directory:', process.cwd());
console.log('[DIAG] Environment PORT:', process.env.PORT);

// Check if we can access file system
const fs = require('fs');
const path = require('path');

try {
  console.log('[DIAG] Checking current directory contents...');
  const files = fs.readdirSync('.');
  console.log('[DIAG] Files in current dir:', files.slice(0, 10));
} catch (err) {
  console.error('[DIAG] Error reading directory:', err.message);
}

// Check if package.json exists
try {
  const pkg = require('./package.json');
  console.log('[DIAG] Package name:', pkg.name);
  console.log('[DIAG] Package dependencies:', Object.keys(pkg.dependencies || {}));
} catch (err) {
  console.error('[DIAG] Error reading package.json:', err.message);
}

// Check if node_modules exists
try {
  const nodeModulesPath = path.join(process.cwd(), 'node_modules');
  const hasNodeModules = fs.existsSync(nodeModulesPath);
  console.log('[DIAG] node_modules exists:', hasNodeModules);
  
  if (hasNodeModules) {
    const modules = fs.readdirSync(nodeModulesPath).slice(0, 10);
    console.log('[DIAG] Some installed modules:', modules);
  }
} catch (err) {
  console.error('[DIAG] Error checking node_modules:', err.message);
}

// Test basic HTTP server
const http = require('http');
const port = process.env.PORT || 3000;

console.log('[DIAG] Attempting to create HTTP server on port:', port);

const server = http.createServer((req, res) => {
  console.log('[DIAG] Request received:', req.method, req.url);
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    status: 'ok',
    message: 'Diagnostic server running',
    timestamp: new Date().toISOString(),
    nodeVersion: process.version,
    port: port
  }));
});

server.listen(port, '0.0.0.0', () => {
  console.log('[DIAG] HTTP server started successfully on port:', port);
});

server.on('error', (err) => {
  console.error('[DIAG] Server error:', err);
});

// Keep process alive
setTimeout(() => {
  console.log('[DIAG] Diagnostic complete - server should be running');
}, 1000);
