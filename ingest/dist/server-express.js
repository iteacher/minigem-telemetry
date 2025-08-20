"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const app = (0, express_1.default)();
// Middleware
app.use(express_1.default.json({ limit: '64kb' }));
app.use(express_1.default.urlencoded({ extended: true }));
// Enable CORS for all routes
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    if (req.method === 'OPTIONS') {
        res.sendStatus(200);
    }
    else {
        next();
    }
});
// Simple memory store with realistic data
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
// Basic health check
app.get('/health', (req, res) => {
    res.json({
        ok: true,
        ts: Date.now(),
        message: 'Express server running',
        env: process.env.NODE_ENV || 'development'
    });
});
// Main stats endpoint for dashboard
app.get('/stats/install', (req, res) => {
    console.log('Stats request received from:', req.ip);
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
        file: 'memory-store-express',
        note: 'Express.js hosting-compatible server'
    };
    console.log('Sending stats response');
    res.json(response);
});
// Telemetry endpoint
app.post('/t', (req, res) => {
    try {
        console.log('Telemetry received:', JSON.stringify(req.body));
        res.json({ ok: true, accepted: 1, skipped: 0 });
    }
    catch (e) {
        console.error('Telemetry error:', e);
        res.status(500).json({ error: 'server_error' });
    }
});
// Debug endpoint
app.get('/debug/env', (req, res) => {
    res.json({
        node: process.version,
        env: {
            NODE_ENV: process.env.NODE_ENV,
            PORT: process.env.PORT,
            PASSENGER_APP_ENV: process.env.PASSENGER_APP_ENV
        },
        timestamp: new Date().toISOString()
    });
});
// Catch all
app.all('*', (req, res) => {
    console.log('Catch-all route:', req.method, req.url);
    res.json({
        message: 'Express server - route not found',
        method: req.method,
        url: req.url,
        timestamp: new Date().toISOString()
    });
});
// Error handler
app.use((err, req, res, next) => {
    console.error('Express error:', err);
    res.status(500).json({ error: 'Internal server error', message: err.message });
});
const port = process.env.PORT || 3000;
const isHosting = process.env.NODE_ENV === 'production' || process.env.PASSENGER_APP_ENV;
console.log('=== Express Telemetry Server ===');
console.log('Environment:', {
    NODE_ENV: process.env.NODE_ENV,
    PASSENGER_APP_ENV: process.env.PASSENGER_APP_ENV,
    PORT: process.env.PORT,
    isHosting
});
if (isHosting) {
    console.log('✓ Hosting environment detected');
    // For Passenger/hosting, just export the app
    module.exports = app;
}
else {
    // Local development
    app.listen(port, () => {
        console.log(`✓ Express server listening on port ${port}`);
    });
}
// Also export for compatibility
exports.default = app;
