# Minigem Telemetry Analytics

[![MIT License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GitHub](https://img.shields.io/github/license/iteacher/minigem-telemetry)](https://github.com/iteacher/minigem-telemetry/blob/main/LICENSE)

A privacy-focused, lightweight telemetry system for tracking application installations and usage patterns. Built with TypeScript/Node.js and designed for minimal resource usage while providing rich analytics insights for any application or service.

## Overview

This system collects installation and usage telemetry from any application and provides a comprehensive analytics dashboard. The design prioritizes privacy (anonymous user tracking), performance (JSON file storage), and ease of deployment (single Node.js service with static dashboard).

## Dashboard Preview

<div align="center">
  <table>
    <tr>
      <td align="center">
        <img src="dashboard/assets/short-term-dashboard.png" alt="Short Term Launch Window" width="400"/>
        <br />
        <strong>Short Term Launch Window</strong>
        <br />
        <em>Weekly granularity for applications in launch phase</em>
      </td>
      <td align="center">
        <img src="dashboard/assets/long-term-dashboard.png" alt="Long Term Analysis" width="400"/>
        <br />
        <strong>Long Term Analysis</strong>
        <br />
        <em>Monthly granularity for mature applications</em>
      </td>
    </tr>
  </table>
</div>

*Professional analytics dashboard showing adaptive timeline visualization - automatically switches from weekly to monthly granularity based on application maturity*

## Architecture

- **Ingestion Server**: Pure Node.js HTTP server optimized for shared hosting platforms
- **Storage**: JSON file-based storage for minimal overhead and easy backup
- **Analytics Dashboard**: Professional web interface with Chart.js visualizations
- **Geographic Data**: Simple IP-based geographic enrichment
- **Privacy-First**: Anonymous user hashing with yearly salt rotation
- **Hosting Compatibility**: Designed for shared hosting platforms like A2Hosting with Node.js 16+ support

## Server Configuration

### Shared Hosting Architecture

The telemetry server is specifically designed for shared hosting platforms such as A2Hosting. The architecture uses:

- **Pure Node.js Built-ins**: No external dependencies for core functionality
- **Node.js 16+ Compatibility**: Optimized for Node.js v16.20.2 and higher
- **JSON File Storage**: File-based persistence compatible with shared hosting file systems
- **Relative Path Configuration**: Uses relative directories (./data/, ./logs/) for hosting compatibility
- **Environment Detection**: Automatic detection of hosting vs local environments
- **Graceful Fallbacks**: Robust error handling for varying hosting environments

### Hosting Platform Requirements

- **Node.js 16.20.2+**: Required for hosting compatibility
- **File System Access**: Read/write permissions for data and log directories
- **HTTP Server**: Ability to run persistent Node.js HTTP server
- **Port Management**: Hosting platform manages port binding automatically

## Features

### 🚀 **Telemetry Ingestion**
- High-performance telemetry collection with rate limiting (2000 req/hour default)
- Batch request support for efficient data transmission
- Schema validation and event normalization
- IP-based geographic enrichment with privacy protection
- Comprehensive request logging and error handling

### 📊 **Analytics Dashboard**
- **Installation Metrics**: Total installs, monthly trends, geographic distribution
- **Version Analytics**: Application version adoption patterns and lifecycle analysis
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

- Node.js 16.20.2+ (optimized for shared hosting platforms like A2Hosting)
- Optional: Basic geographic IP detection (built-in)

### Installation

```bash
git clone https://github.com/iteacher/minigem-telemetry.git
cd minigem-telemetry/ingest
npm install
npm run build
```

### Shared Hosting Deployment

For shared hosting platforms (A2Hosting, etc.):

```bash
# Build the operational server
npm run build

# Deploy the hosting-compatible server
# Use dist/server-operational.js for production
# This version is optimized for Node.js 16+ shared hosting
```

### Configuration

Create a `.env` file or set environment variables:

```bash
# Required
PORT=3001                    # Server port (hosting platform manages binding)

# Storage Configuration (Shared Hosting Compatible)
STATS_JSON_FILE=/home/username/domain.com/data/test3.json  # Primary data file
LOG_FILE=/home/username/tmp/jwc-telemetry.log             # Log file path

# Launch Window Configuration
LAUNCH_DATE=2025-07-22       # Launch tracking start date
LAUNCH_DURATION=90           # Days for launch window tracking

# Geographic Data (Optional)
GEO_DB=/home/username/mmdb/GeoLite2-City.mmdb           # MaxMind City database
GEO_MMDB=/home/username/mmdb/GeoLite2-Country.mmdb      # MaxMind Country database

# Security & Performance
YEARLY_SALT=jwc-2025-salt                # Anonymous user hashing salt
RATE_LIMIT_MAX=2000                      # Max requests per time window
RATE_LIMIT_TIME_WINDOW="1 hour"          # Rate limit window
STATS_SECRET=your-admin-secret           # Debug endpoint access key

# Optional - Analytics
STATS_WINDOW_DAYS=7                      # Statistics analysis window
DEBUG_STATS_TRACE=1                      # Enable detailed analytics logging
DEBUG_DB=1                               # Enable database debugging
NODE_ENV=production                      # Environment mode
LOG_LEVEL=debug                          # Logging level
```

### Running the Server

```bash
# Development with auto-reload
npm run dev

# Production (standard server)
npm start

# Shared Hosting Production (optimized for Node.js 16+)
node dist/server-operational.js

# Direct execution
node dist/server.js
```

The server starts on the configured port and logs startup information. For shared hosting platforms, use `server-operational.js` which includes hosting-specific optimizations.

## API Reference

### Core Endpoints

#### `POST /t` - Telemetry Ingestion

Primary endpoint for receiving telemetry data from any application or service. Supports both single events and batch processing.

**Supported Event Types:**
- `install.created` - New installation events
- `extension.upgraded` - Version upgrade events  
- `java.run.started` - Usage/execution events
- `feature.theme.change` - Feature usage events

**Single Event:**
```bash
curl -X POST https://yourdomain.com/t \
  -H "Content-Type: application/json" \
  -d '{
    "schema": "jwc.v1",
    "batch": [{
      "evt": "install.created",
      "anon": "1234567890abcdef1234567890abcdef",
      "ext": "1.2.3",
      "vscode": "1.81.0",
      "os": "darwin-arm64",
      "t": '$(date +%s000)'
    }]
  }'
```

**Event Schema:**
- `schema` (required): Must be `"jwc.v1"` - telemetry schema version
- `batch` (required): Array of events to process
- `evt` (required): Event type (install.created, extension.upgraded, etc.)
- `anon` (required): Anonymous user identifier (32-char hex string)
- `ext` (optional): Application/extension version (e.g., "1.2.3")
- `vscode` (optional): Host application version (if applicable)
- `os` (optional): Operating system identifier (e.g., "darwin-arm64", "win32-x64")
- `t` (required): Timestamp (Unix milliseconds)

**Response:**
```json
{
  "ok": true,
  "accepted": 1,
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
- Application version breakdowns
- Operating system distribution
- Geographic data (country-level)
- Advanced analytics (seasonal patterns, growth analysis, etc.)

### Administrative Endpoints

#### `GET /debug/env` - Environment Information

Returns non-sensitive configuration details.

#### `GET /debug/logping` - Logging Test

Tests log file writing and returns log configuration.

## How to Send Telemetry Data

### Understanding the Schema

The telemetry system uses a simple JSON schema that any application can implement:

```javascript
{
  "schema": "jwc.v1",        // Fixed schema version
  "anon": "user_12345",      // Anonymous user ID (consistent per user)
  "evt": "app_started",      // Event name (your choice)
  "t": 1692230400000,        // Timestamp (Unix milliseconds)
  "os": "Windows_NT",        // OS (optional but recommended)
  "ext": "2.1.0",           // Your app version (optional)
  "m": {                     // Custom metadata (optional)
    "feature": "dashboard",
    "duration_ms": 1500
  }
}
```

### Event Types You Can Track

**Common Event Names:**
- `install` - Application installation
- `uninstall` - Application removal
- `app_started` - Application launch
- `app_closed` - Application shutdown
- `feature_used` - Feature usage
- `error_occurred` - Error tracking
- `user_action` - User interactions
- `performance_metric` - Performance data

### Sending Data Methods

**1. Single Event (Immediate):**
```bash
POST /t
Content-Type: application/json

{
  "schema": "jwc.v1",
  "anon": "user_abc123",
  "evt": "feature_used",
  "t": 1692230400000,
  "m": {"feature": "export", "format": "pdf"}
}
```

**2. Batch Events (Efficient):**
```bash
POST /t
Content-Type: application/json

{
  "schema": "jwc.v1",
  "batch": [
    {"anon": "user_abc123", "evt": "app_started", "t": 1692230400000},
    {"anon": "user_abc123", "evt": "feature_used", "t": 1692230460000, "m": {"feature": "search"}},
    {"anon": "user_abc123", "evt": "app_closed", "t": 1692230500000}
  ]
}
```

### Response Format

All requests return a JSON response:

```json
{
  "ok": true,
  "accepted": 2,    // Number of events successfully processed
  "skipped": 0      // Number of events that failed validation
}
```

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
- Application version adoption rates
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
        ext: '1.2.3', // Your application version
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

// Track application installation
await telemetry.track('install', {
  installMethod: 'download',
  firstInstall: true
});

// Track feature usage
await telemetry.track('feature_used', {
  feature: 'dashboard',
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

### Shared Hosting Deployment (A2Hosting, etc.)

The telemetry system is optimized for shared hosting platforms with Node.js 16+ support:

**Deployment Steps:**
1. Upload built files to your hosting account
2. Configure startup file to use `dist/server-operational.js`
3. Set environment variables for your hosting platform
4. Upload dashboard files to web-accessible directory

**A2Hosting Configuration Example:**
```bash
# Startup file: dist/server-operational.js
# Environment variables (matches your current setup):
STATS_JSON_FILE=/home/mandersj/telemetary.jwc.minigem.uk/data/test3.json
LAUNCH_DATE=2025-07-22
LAUNCH_DURATION=90
LOG_FILE=/home/mandersj/tmp/jwc-telemetry.log
GEO_DB=/home/mandersj/mmdb/GeoLite2-City.mmdb
GEO_MMDB=/home/mandersj/mmdb/GeoLite2-Country.mmdb
YEARLY_SALT=jwc-2025-salt
RATE_LIMIT_MAX=2000
STATS_SECRET=your-admin-secret
NODE_ENV=production
DEBUG_DB=1
DEBUG_STATS_TRACE=1
LOG_LEVEL=debug
STATS_WINDOW_DAYS=7
```

**Key Features for Shared Hosting:**
- **Node.js 16.20.2+ Compatible**: Tested and optimized for shared hosting Node.js versions
- **Pure Node.js Implementation**: No external dependencies that might conflict with hosting
- **File-based Storage**: Uses hosting file system instead of databases
- **Automatic Path Detection**: Switches between production and local paths automatically
- **Graceful Error Handling**: Robust operation in various hosting environments

### Production Environment

**Environment Variables for Shared Hosting:**
```bash
# Core Configuration
PORT=3001                    # Hosting platform manages actual port binding
NODE_ENV=production

# Storage Configuration (Shared Hosting Paths)
STATS_JSON_FILE=/home/username/domain.com/data/test3.json  # Primary telemetry data
LOG_FILE=/home/username/tmp/jwc-telemetry.log             # Application logs

# Launch Window Configuration
LAUNCH_DATE=2025-07-22       # Start date for launch tracking
LAUNCH_DURATION=90           # Days for launch window (90-day tracking)

# Geographic Data (Optional MaxMind Integration)
GEO_DB=/home/username/mmdb/GeoLite2-City.mmdb           # City-level geo data
GEO_MMDB=/home/username/mmdb/GeoLite2-Country.mmdb      # Country-level geo data

# Security Configuration
YEARLY_SALT=jwc-2025-salt                    # Anonymous user hashing salt
STATS_SECRET=your-admin-secret-key           # Admin endpoint protection

# Performance Configuration (Conservative for Shared Hosting)
RATE_LIMIT_MAX=2000                          # Max requests per hour
RATE_LIMIT_TIME_WINDOW="1 hour"              # Rate limiting window

# Analytics Configuration
STATS_WINDOW_DAYS=7                          # Statistics analysis window
DEBUG_STATS_TRACE=1                          # Enable detailed analytics logging
DEBUG_DB=1                                   # Enable database debugging
LOG_LEVEL=debug                              # Logging verbosity level
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

*Built with ❤️ for developers who value privacy and performance*
