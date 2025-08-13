# JWC Telemetry Ingestion (Deploy on A2 Hosting cPanel)

This module is separate from your website. Deploy it under a subdomain (telemetary.jwc.minigem.uk) as a Node.js app.

Steps (cPanel, Shared/VPS with Node.js Selector):

1) Create subdomain
- cPanel > Domains > Create a New Domain (or Subdomains)
- Domain: telemetary.jwc.minigem.uk
- Document Root: telemetary.jwc.minigem.uk (or a chosen folder)

2) Upload app
- Build locally: cd jwc-telemetry/ingest && npm install && npm run build
- Zip the folder and upload to the subdomain document root (or use cPanel Git if available)
- Ensure files exist: package.json, dist/server.js, node_modules/

3) Create Node.js App (cPanel)
- cPanel > Setup Node.js App > Create Application
- Node.js version: 18+
- Application root: the subdomain docroot you uploaded to
- Application URL: https://telemetary.jwc.minigem.uk
- Application startup file: dist/server.js
- Environment variables:
  - PORT=8088 (or leave blank; some cPanel assigns automatically)
  - LOG_DIR=/home/USERNAME/jwc-logs/events-transformed
  - GEO_DB=/home/USERNAME/jwc-geo/GeoLite2-City.mmdb
  - YEARLY_SALT=jwc-2025-salt
- Click Create, then Start app

4) Route /t to the app
- If cPanel app is bound to the subdomain root, POST https://telemetary.jwc.minigem.uk/t will work directly.
- To protect a dashboard, serve static HTML under telemetary.jwc.minigem.uk/dashboard/ and protect with cPanel Directory Privacy.

5) DNS (if your domain uses A2 nameservers)
- cPanel > Zone Editor > Manage
- Add A record: telemetary.jwc.minigem.uk -> your hosting IP
- SSL: cPanel > SSL/TLS Status > Run AutoSSL for the subdomain

6) Logs and dashboard
- App writes flat logs to LOG_DIR. You can download and analyze, or host a dashboard under /dashboard/.
- GoAccess likely requires root packages; on shared hosting use offline analysis or a client-side dashboard that tails files via cron to a JSON endpoint.

Health check: https://telemetary.jwc.minigem.uk/health
API: POST https://telemetary.jwc.minigem.uk/t
