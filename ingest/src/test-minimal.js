// Minimal test server - CommonJS format for maximum compatibility
const http = require('http');

console.log('[TEST] Starting minimal server...');

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    url: req.url,
    method: req.method
  }));
});

const port = process.env.PORT || 3000;
console.log('[TEST] Attempting to listen on port:', port);

server.listen(port, '0.0.0.0', () => {
  console.log('[TEST] Server running on port:', port);
});

server.on('error', (err) => {
  console.error('[TEST] Server error:', err);
});

process.on('uncaughtException', (err) => {
  console.error('[TEST] Uncaught exception:', err);
});

process.on('unhandledRejection', (err) => {
  console.error('[TEST] Unhandled rejection:', err);
});
