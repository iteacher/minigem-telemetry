#!/usr/bin/env node

// Reset installs.json to default structure with proper initialization
// Usage: node reset-store.js [path-to-installs.json]

const fs = require('fs');
const path = require('path');

const defaultInstallsJsonPath = process.argv[2] || '/Users/Shared/jwc-telemetry/data/installs.json';

function createDefaultStore() {
  const now = new Date().toISOString();
  const currentYear = String(new Date().getUTCFullYear());
  const currentMonth = String(new Date().getUTCMonth() + 1).padStart(2, '0');
  const currentMonthKey = `${currentYear}-${currentMonth}`;
  
  return {
    meta: { createdAt: now, updatedAt: now },
    months: {
      [currentYear]: {
        [currentMonth]: { installs: 0 }
      }
    },
    byExt: {
      "0.0.0": 0
    },
    byOs: {
      "darwin-arm64": 0,
      "darwin-x64": 0,
      "win32-x64": 0,
      "win32-arm64": 0,
      "linux-x64": 0,
      "linux-arm64": 0,
      "Unknown": 0
    },
    byCountry: {
      "US": 0,
      "GB": 0,
      "CA": 0,
      "DE": 0,
      "FR": 0,
      "AU": 0,
      "JP": 0,
      "Unknown": 0
    },
    versionsByMonth: {
      [currentMonthKey]: {
        "0.0.0": 0
      }
    },
    osByMonth: {
      [currentMonthKey]: {
        "darwin-arm64": 0,
        "darwin-x64": 0,
        "win32-x64": 0,
        "win32-arm64": 0,
        "linux-x64": 0,
        "linux-arm64": 0,
        "Unknown": 0
      }
    },
    geoByMonth: {
      [currentMonthKey]: {
        "US": 0,
        "GB": 0,
        "CA": 0,
        "DE": 0,
        "FR": 0,
        "AU": 0,
        "JP": 0,
        "Unknown": 0
      }
    }
  };
}

// Ensure directory exists
const dir = path.dirname(defaultInstallsJsonPath);
if (!fs.existsSync(dir)) {
  fs.mkdirSync(dir, { recursive: true });
  console.log(`Created directory: ${dir}`);
}

// Create default store
const defaultStore = createDefaultStore();

// Write to file
const content = JSON.stringify(defaultStore, null, 2);
fs.writeFileSync(defaultInstallsJsonPath, content, 'utf8');

console.log(`✅ Reset store to default structure: ${defaultInstallsJsonPath}`);
console.log(`📊 Dashboard should now display properly with initialized data structure`);
console.log(`🔄 New installs will increment from these base values`);
