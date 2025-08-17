# Minigem Telemetry Analytics

[![MIT License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GitHub](https://img.shields.io/github/license/iteacher/minigem-telemetry)](https://github.com/iteacher/minigem-telemetry/blob/main/LICENSE)

A privacy-focused, lightweight telemetry system for tracking Java Web Console extension installations and usage patterns. Built with TypeScript/Node.js and designed for minimal resource usage while providing rich analytics insights.

## Overview

This system collects installation telemetry from the Java Web Console VS Code extension and provides a comprehensive analytics dashboard. The design prioritizes privacy (anonymous user tracking), performance (JSON file storage), and ease of deployment (single Node.js service with static dashboard).

## Architecture

- **Ingestion Server**: FastAPI-based TypeScript service with rate limiting and validation
- **Storage**: JSON file-based storage for minimal overhead and easy backup
- **Analytics Dashboard**: Professional web interface with Chart.js visualizations
- **Geographic Data**: Optional MaxMind GeoLite2 integration for location insights
- **Privacy-First**: Anonymous user hashing with yearly salt rotation

## Features

### 🚀 **Telemetry Ingestion**
- High-performance telemetry collection with rate limiting (2000 req/hour default)
- Batch request support for efficient data transmission
- Schema validation and event normalization
- IP-based geographic enrichment with privacy protection
- Comprehensive request logging and error handling

### 📊 **Analytics Dashboard**
- **Installation Metrics**: Total installs, monthly trends, geographic distribution
- **Version Analytics**: Extension version adoption patterns and lifecycle analysis
- **Platform Intelligence**: Operating system breakdown and market share analysis
- **Advanced Insights**: Seasonal patterns, growth trajectory analysis, version migration tracking
- **Real-time Visualization**: Interactive Chart.js charts with professional Material Design UI

### 🔒 **Privacy & Security**
- Anonymous user identification with cryptographic hashing
- No personally identifiable information stored
- Yearly salt rotation for enhanced privacy
- Rate limiting and input validation
- Optional debug endpoints with configurable access control

### 🛠 **Developer Experience**
- TypeScript codebase with full type safety
- Modular architecture for easy extension
- Comprehensive logging and debugging capabilities
- Environment-based configuration
- Production-ready deployment options

## Quick Start

### Prerequisites

- Node.js 18+ 
- Optional: MaxMind GeoLite2 database for geographic data

### Installation

```bash
git clone https://github.com/iteacher/minigem-telemetry.git
cd minigem-telemetry/ingest
npm install
npm run build
```

### Configuration

Create a `.env` file or set environment variables:

```bash
# Required
PORT=8088                    # Server port (default: 3000)

# Optional - Storage
LOG_DIR=/opt/jwc-telemetry/logs/events-transformed    # Log directory
STATS_JSON_FILE=/opt/jwc-telemetry/logs/installs.json # Analytics data store

# Optional - Geographic Data
GEO_DB=/opt/jwc-telemetry/geo/GeoLite2-City.mmdb     # MaxMind database path

# Optional - Security & Performance
YEARLY_SALT=your-secret-salt-2025    # Anonymous user hashing salt
RATE_LIMIT_MAX=2000                  # Max requests per time window
RATE_LIMIT_TIME_WINDOW="1 hour"      # Rate limit window
STATS_SECRET=admin-secret            # Debug endpoint access key

# Optional - Analytics
STATS_WINDOW_DAYS=7                  # Statistics analysis window
DEBUG_STATS_TRACE=false             # Enable detailed analytics logging
```

### Running the Server

```bash
# Development with auto-reload
npm run dev

# Production
npm start

# Direct execution
node dist/server.js
```

The server starts on the configured port (default: 3000) and logs startup information.

## API Reference

### Core Endpoints

#### `POST /t` - Telemetry Ingestion

Primary endpoint for receiving telemetry data from extensions.

**Single Event:**
```bash
curl -X POST http://localhost:8088/t \
  -H "Content-Type: application/json" \
  -d '{
    "schema": "jwc.v1",
    "anon": "user_abc123",
    "evt": "install",
    "t": 1692230400000,
    "os": "Windows_NT",
    "ext": "1.2.3",
    "vscode": "1.81.0",
    "m": {
      "installMethod": "marketplace",
      "firstInstall": true
    }
  }'
```

**Batch Events:**
```bash
curl -X POST http://localhost:8088/t \
  -H "Content-Type: application/json" \
  -d '{
    "schema": "jwc.v1",
    "batch": [
      {
        "anon": "user_abc123",
        "evt": "install", 
        "t": 1692230400000,
        "os": "Windows_NT",
        "ext": "1.2.3"
      },
      {
        "anon": "user_abc123",
        "evt": "first_use",
        "t": 1692230460000,
        "m": {"feature": "web_console"}
      }
    ]
  }'
```

**Event Schema:**
- `schema` (required): Must be `"jwc.v1"`
- `anon` (required): Anonymous user identifier (string)
- `evt` (required): Event type (e.g., "install", "uninstall", "first_use")
- `t` (required): Timestamp (Unix milliseconds or ISO string)
- `os` (optional): Operating system identifier
- `ext` (optional): Extension version
- `vscode` (optional): VS Code version
- `m` (optional): Additional metadata object

**Response:**
```json
{
  "ok": true,
  "accepted": 2,
  "skipped": 0
}
```

#### `GET /health` - Health Check

Simple health check endpoint for monitoring.

```json
{
  "ok": true,
  "ts": 1692230400000
}
```

#### `GET /stats/install` - Analytics Data

Returns comprehensive installation analytics for the dashboard.

**Response includes:**
- Installation totals and time ranges
- Monthly installation trends
- Extension version breakdowns
- Operating system distribution
- Geographic data (country-level)
- Advanced analytics (seasonal patterns, growth analysis, etc.)

### Administrative Endpoints

#### `GET /debug/env` - Environment Information

Returns non-sensitive configuration details.

#### `GET /debug/logping` - Logging Test

Tests log file writing and returns log configuration.

## Dashboard

### Accessing the Dashboard

The analytics dashboard is available as static HTML files in the `/dashboard` directory:

- **Main Dashboard**: `dashboard/installs.html` - Comprehensive analytics interface
- **Redirect Page**: `dashboard/index.html` - Automatically redirects to main dashboard

### Dashboard Features

**📈 Installation Overview**
- Total installation count with time period
- Monthly installation trends with interactive charts
- Key metrics summary cards

**🔧 Version Analytics**
- Extension version adoption rates
- Version lifecycle analysis with bubble charts
- Migration patterns and adoption speed metrics

**💻 Platform Intelligence**
- Operating system market share breakdown
- Platform trends analysis with scatter plots
- Geographic distribution with country-level insights

**📊 Advanced Analytics**
- Seasonal installation patterns
- Growth trajectory analysis with predictive modeling
- Version migration heat maps
- Geographic growth velocity analysis

### Dashboard Technology

- **Chart.js v4**: Professional chart visualizations
- **Material Symbols**: Google's icon system for consistent UI
- **Inter Font**: Professional typography
- **Responsive Design**: Works on desktop and mobile devices
- **Dark Theme**: Easy-on-eyes dark interface
- **Real-time Updates**: Data refreshes automatically

## Integration Examples

### JavaScript/TypeScript Client

```typescript
interface TelemetryEvent {
  schema: 'jwc.v1';
  anon: string;
  evt: string;
  t: number;
  os?: string;
  ext?: string;
  vscode?: string;
  m?: Record<string, any>;
}

class TelemetryClient {
  private endpoint: string;
  private userId: string;

  constructor(endpoint: string) {
    this.endpoint = endpoint;
    this.userId = this.generateUserId();
  }

  private generateUserId(): string {
    // Generate stable anonymous ID (implement based on your needs)
    return 'user_' + Math.random().toString(36).substr(2, 12);
  }

  async track(event: string, metadata?: Record<string, any>): Promise<void> {
    try {
      const payload: TelemetryEvent = {
        schema: 'jwc.v1',
        anon: this.userId,
        evt: event,
        t: Date.now(),
        os: navigator.platform,
        ext: '1.2.3', // Your extension version
        m: metadata
      };

      const response = await fetch(`${this.endpoint}/t`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
    } catch (error) {
      console.warn('Telemetry failed:', error);
      // Fail silently in production
    }
  }

  async trackBatch(events: Array<{evt: string, metadata?: Record<string, any>}>): Promise<void> {
    try {
      const payload = {
        schema: 'jwc.v1',
        batch: events.map(({evt, metadata}) => ({
          anon: this.userId,
          evt,
          t: Date.now(),
          os: navigator.platform,
          ext: '1.2.3',
          m: metadata
        }))
      };

      await fetch(`${this.endpoint}/t`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
    } catch (error) {
      console.warn('Batch telemetry failed:', error);
    }
  }
}

// Usage
const telemetry = new TelemetryClient('https://telemetry.minigem.org');

// Track extension installation
await telemetry.track('install', {
  installMethod: 'marketplace',
  firstInstall: true
});

// Track feature usage
await telemetry.track('feature_used', {
  feature: 'web_console',
  duration_ms: 1500
});
```

### Python Client

```python
import json
import time
import uuid
import requests
from typing import Dict, List, Optional, Any

class TelemetryClient:
    def __init__(self, endpoint: str, app_version: str = '1.0.0'):
        self.endpoint = endpoint
        self.app_version = app_version
        self.user_id = f"user_{uuid.uuid4().hex[:12]}"
    
    def track(self, event: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Track a single telemetry event."""
        try:
            payload = {
                'schema': 'jwc.v1',
                'anon': self.user_id,
                'evt': event,
                't': int(time.time() * 1000),
                'ext': self.app_version,
                'm': metadata or {}
            }
            
            response = requests.post(
                f"{self.endpoint}/t",
                json=payload,
                timeout=10
            )
            return response.status_code == 200
            
        except Exception as e:
            print(f"Telemetry failed: {e}")
            return False
    
    def track_batch(self, events: List[Dict[str, Any]]) -> bool:
        """Track multiple events in a single request."""
        try:
            payload = {
                'schema': 'jwc.v1',
                'batch': [
                    {
                        'anon': self.user_id,
                        'evt': event['evt'],
                        't': int(time.time() * 1000),
                        'ext': self.app_version,
                        'm': event.get('metadata', {})
                    }
                    for event in events
                ]
            }
            
            response = requests.post(
                f"{self.endpoint}/t",
                json=payload,
                timeout=15
            )
            return response.status_code == 200
            
        except Exception as e:
            print(f"Batch telemetry failed: {e}")
            return False

# Usage example
telemetry = TelemetryClient('https://telemetry.minigem.org', '2.1.0')

# Single event tracking
telemetry.track('app_started', {
    'startup_time_ms': 1200,
    'features_enabled': ['console', 'debugger']
})

# Batch event tracking
events = [
    {'evt': 'feature_used', 'metadata': {'feature': 'debugger'}},
    {'evt': 'file_opened', 'metadata': {'file_type': 'java', 'size': 2048}},
    {'evt': 'session_ended', 'metadata': {'duration_minutes': 45}}
]
telemetry.track_batch(events)
```

## Deployment

### Docker Deployment

**Dockerfile:**
```dockerfile
FROM node:18-alpine

WORKDIR /app

# Install dependencies
COPY ingest/package*.json ./
RUN npm ci --only=production && npm cache clean --force

# Copy application code
COPY ingest/dist ./dist
COPY dashboard ./dashboard

# Create required directories
RUN mkdir -p /opt/jwc-telemetry/logs/events-transformed

# Set up non-root user
RUN addgroup -g 1001 -S nodejs && adduser -S nextjs -u 1001
RUN chown -R nextjs:nodejs /app /opt/jwc-telemetry
USER nextjs

EXPOSE 8088

CMD ["node", "dist/server.js"]
```

**docker-compose.yml:**
```yaml
version: '3.8'

services:
  telemetry:
    build: .
    ports:
      - "8088:8088"
    environment:
      - PORT=8088
      - LOG_DIR=/opt/jwc-telemetry/logs/events-transformed
      - YEARLY_SALT=your-production-salt-2025
      - RATE_LIMIT_MAX=5000
    volumes:
      - ./data:/opt/jwc-telemetry/logs
      - ./geo:/opt/jwc-telemetry/geo
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8088/health"]
      interval: 30s
      timeout: 10s
      retries: 3
```

### Production Environment

**Environment Variables:**
```bash
# Core Configuration
PORT=8088
NODE_ENV=production

# Storage Configuration
LOG_DIR=/opt/jwc-telemetry/logs/events-transformed
STATS_JSON_FILE=/opt/jwc-telemetry/logs/installs.json

# Security Configuration
YEARLY_SALT=your-unique-production-salt-2025
STATS_SECRET=your-admin-secret-key

# Performance Configuration
RATE_LIMIT_MAX=5000
RATE_LIMIT_TIME_WINDOW="1 hour"

# Geographic Data (Optional)
GEO_DB=/opt/jwc-telemetry/geo/GeoLite2-City.mmdb

# Analytics Configuration
STATS_WINDOW_DAYS=30
DEBUG_STATS_TRACE=false
```

### Reverse Proxy Setup (Nginx)

```nginx
server {
    listen 443 ssl http2;
    server_name telemetry.yourdomain.com;
    
    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;
    
    # Rate limiting
    limit_req_zone $binary_remote_addr zone=telemetry:10m rate=100r/m;
    
    # Telemetry ingestion endpoint
    location /t {
        limit_req zone=telemetry burst=20 nodelay;
        proxy_pass http://localhost:8088;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
    
    # Health check
    location /health {
        proxy_pass http://localhost:8088;
        access_log off;
    }
    
    # Analytics dashboard
    location /dashboard/ {
        alias /path/to/minigem-telemetry/dashboard/;
        index index.html;
        try_files $uri $uri/ /dashboard/index.html;
        
        # Optional: Basic auth for dashboard
        # auth_basic "Analytics Dashboard";
        # auth_basic_user_file /etc/nginx/.htpasswd;
    }
    
    # Analytics API
    location /stats/install {
        proxy_pass http://localhost:8088;
        proxy_cache_valid 200 5m;
    }
    
    # Security headers
    add_header X-Frame-Options DENY;
    add_header X-Content-Type-Options nosniff;
    add_header X-XSS-Protection "1; mode=block";
}
```

## Data Privacy & GDPR Compliance

### Privacy Features

- **Anonymous Tracking**: No personal identifiers stored
- **Cryptographic Hashing**: User IDs are hashed with yearly salt rotation
- **Minimal Data Collection**: Only essential metrics collected
- **Geographic Aggregation**: Location data at country level only
- **Data Retention**: Configurable retention periods

### Data Handling

- All user identification is anonymized before storage
- IP addresses are used only for geographic enrichment, not stored
- No tracking of personal content or file names
- Data aggregation prevents individual user behavior analysis

## Monitoring & Maintenance

### Health Monitoring

```bash
# Health check
curl http://localhost:8088/health

# Environment check
curl http://localhost:8088/debug/env

# Log file test
curl http://localhost:8088/debug/logping
```

### Log Rotation

Set up logrotate for the telemetry logs:

```bash
# /etc/logrotate.d/jwc-telemetry
/opt/jwc-telemetry/logs/events-transformed/*.log {
    daily
    rotate 30
    compress
    missingok
    notifempty
    create 644 nodejs nodejs
}
```

### Backup Strategy

```bash
#!/bin/bash
# Backup analytics data
cp /opt/jwc-telemetry/logs/installs.json /backup/installs-$(date +%Y%m%d).json

# Compress and archive old logs
tar -czf /backup/logs-$(date +%Y%m%d).tar.gz /opt/jwc-telemetry/logs/events-transformed/
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-analytics`
3. Make your changes with appropriate tests
4. Commit your changes: `git commit -am 'Add new analytics feature'`
5. Push to the branch: `git push origin feature/new-analytics`
6. Create a Pull Request

### Development Setup

```bash
# Clone and setup
git clone https://github.com/iteacher/minigem-telemetry.git
cd minigem-telemetry/ingest

# Install dependencies
npm install

# Build TypeScript
npm run build

# Start development server with auto-reload
npm run dev
```

### Code Standards

- TypeScript with strict mode enabled
- ESLint for code quality
- Comprehensive error handling and logging
- Type safety for all interfaces and functions

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/iteacher/minigem-telemetry/issues)
- **Documentation**: This README and inline code comments
- **Community**: GitHub Discussions for questions and feature requests

---

*Built with ❤️ for the Java Web Console community*
