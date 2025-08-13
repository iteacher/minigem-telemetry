# Minigem Telemetry Tool

[![MIT License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GitHub](https://img.shields.io/github/license/iteacher/minigem-telemetry)](https://github.com/iteacher/minigem-telemetry/blob/main/LICENSE)

A high-performance telemetry ingestion server that collects, stores, and analyzes application usage data. Built with Node.js/TypeScript and PostgreSQL, it provides real-time analytics and dashboards for monitoring application performance and user behavior.

## Features

- **Fast telemetry ingestion** with rate limiting and validation
- **PostgreSQL storage** with optimized schema and indexes
- **Real-time analytics** with statistics endpoints
- **Geographic data** enrichment using MaxMind GeoIP
- **Multiple output formats** (structured logs + database)
- **Built-in dashboard** for data visualization
- **Health monitoring** and debugging endpoints

## Quick Start

### 1. Prerequisites

- Node.js 18+ 
- PostgreSQL database
- MaxMind GeoLite2 database (optional, for geographic data)

### 2. Installation

```bash
git clone https://github.com/iteacher/minigem-telemetry.git
cd minigem-telemetry/ingest
npm install
npm run build
```

### 3. Database Setup

Create a PostgreSQL database and user:

```sql
-- Create database
CREATE DATABASE minigem_telemetry;

-- Create user (optional)
CREATE USER telemetry_user WITH ENCRYPTED PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE minigem_telemetry TO telemetry_user;
```

The application will automatically create the required tables on first run.

### 4. Configuration

Set the following environment variables:

#### Required Variables

- `DATABASE_URL` or `PG_URL` - PostgreSQL connection string
  - Format: `postgresql://username:password@host:port/database_name`
  - Example: `postgresql://telemetry_user:password@localhost:5432/minigem_telemetry`

#### Optional Variables

- `PORT` - Server port (default: 8088)
- `LOG_DIR` - Directory for log files (default: `/opt/jwc-telemetry/logs/events-transformed`)
- `GEO_DB` - Path to MaxMind GeoLite2-City.mmdb file (default: `/opt/jwc-telemetry/geo/GeoLite2-City.mmdb`)
- `YEARLY_SALT` - Salt for anonymous user hashing (default: `jwc-2025-salt`)
- `RATE_LIMIT_MAX` - Maximum requests per time window (default: 2000)
- `RATE_LIMIT_TIME_WINDOW` - Rate limit time window (default: `1 hour`)
- `STATS_SECRET` - Secret key for accessing debug endpoints (optional)
- `STATS_WINDOW_DAYS` - Number of days for statistics window (default: 7)
- `PG_SSL` - Enable SSL for PostgreSQL connection (default: `false`)

### 5. Running the Server

```bash
# Development
npm run dev

# Production
npm start

# Or directly
node dist/server.js
```

## API Endpoints

### POST /t - Telemetry Ingestion

Send telemetry events to this endpoint:

```bash
curl -X POST https://your-domain.com/t \
  -H "Content-Type: application/json" \
  -d '{
    "schema": "jwc.v1",
    "anon": "user123",
    "evt": "app.started",
    "t": 1640995200000,
    "os": "Windows_NT",
    "ext": "1.0.0",
    "vscode": "1.74.0",
    "m": {
      "custom": "data"
    }
  }'
```

#### Batch Requests

Send multiple events at once:

```json
{
  "schema": "jwc.v1",
  "batch": [
    {
      "anon": "user123",
      "evt": "app.started",
      "t": 1640995200000
    },
    {
      "anon": "user123", 
      "evt": "feature.used",
      "t": 1640995260000
    }
  ]
}
```

#### Event Schema

- `schema` - Must be `"jwc.v1"`
- `anon` - Anonymous user identifier (string)
- `evt` - Event name (string)
- `t` - Timestamp (Unix timestamp in milliseconds or ISO string)
- `os` - Operating system (optional)
- `ext` - Extension/app version (optional)
- `vscode` - VS Code version (optional)
- `m` - Additional metadata (optional object)

### GET /health - Health Check

Returns server health status:

```json
{
  "ok": true,
  "ts": 1640995200000
}
```

### GET /dbhealth - Database Health

Returns database connection status and version.

### GET /stats - Public Statistics

Returns aggregated analytics data including:
- Total events and unique users
- Events by type, OS, extension version
- Daily/hourly activity patterns
- Performance metrics
- Geographic distribution

## Integrating with Your Application

### JavaScript/TypeScript

```javascript
class TelemetryClient {
  constructor(endpoint, userAgent = 'MyApp/1.0') {
    this.endpoint = endpoint;
    this.userAgent = userAgent;
    this.userId = this.generateUserId();
  }

  generateUserId() {
    // Generate a stable anonymous ID
    return 'user_' + Math.random().toString(36).substr(2, 9);
  }

  async track(event, metadata = {}) {
    try {
      const payload = {
        schema: 'jwc.v1',
        anon: this.userId,
        evt: event,
        t: Date.now(),
        os: navigator.platform,
        ext: '1.0.0', // Your app version
        m: metadata
      };

      await fetch(this.endpoint + '/t', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
    } catch (err) {
      console.warn('Telemetry failed:', err);
    }
  }
}

// Usage
const telemetry = new TelemetryClient('https://telemetry.yourdomain.com');
telemetry.track('app.started');
telemetry.track('feature.used', { feature: 'search', query_length: 15 });
```

### Python

```python
import requests
import time
import uuid

class TelemetryClient:
    def __init__(self, endpoint, app_version='1.0.0'):
        self.endpoint = endpoint
        self.app_version = app_version
        self.user_id = f"user_{uuid.uuid4().hex[:12]}"
    
    def track(self, event, metadata=None):
        try:
            payload = {
                'schema': 'jwc.v1',
                'anon': self.user_id,
                'evt': event,
                't': int(time.time() * 1000),
                'ext': self.app_version,
                'm': metadata or {}
            }
            
            requests.post(f"{self.endpoint}/t", 
                         json=payload, 
                         timeout=5)
        except Exception as e:
            print(f"Telemetry failed: {e}")

# Usage
telemetry = TelemetryClient('https://telemetry.yourdomain.com')
telemetry.track('app.started')
telemetry.track('api.called', {'endpoint': '/users', 'duration_ms': 150})
```

## Dashboard

Access the built-in dashboard at `https://your-domain.com/dashboard/` to view:

- Real-time event statistics
- User activity patterns
- Performance metrics
- Geographic distribution
- Error rates and trends

## Deployment

### Docker

```dockerfile
FROM node:18-alpine
WORKDIR /app
COPY ingest/package*.json ./
RUN npm ci --only=production
COPY ingest/dist ./dist
EXPOSE 8088
CMD ["node", "dist/server.js"]
```

### Environment Variables Example

```bash
# Required
DATABASE_URL=postgresql://user:pass@localhost:5432/telemetry

# Optional
PORT=8088
LOG_DIR=/var/log/telemetry
GEO_DB=/opt/geo/GeoLite2-City.mmdb
YEARLY_SALT=my-secret-salt-2025
RATE_LIMIT_MAX=5000
STATS_SECRET=admin-secret-key
```

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request
