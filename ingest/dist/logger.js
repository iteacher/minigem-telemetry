import fs from 'fs';
import path from 'path';
import { CONFIG } from './config.js';
const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };
const LOG_FILE = process.env.LOG_FILE || CONFIG?.LOG_FILE || '/tmp/jwc-telemetry.log';
const LOG_LEVEL = ((process.env.LOG_LEVEL || CONFIG?.LOG_LEVEL || 'info').toString().toLowerCase());
let stream = null;
function ensureStream() {
    if (stream)
        return;
    try {
        fs.mkdirSync(path.dirname(LOG_FILE), { recursive: true });
        stream = fs.createWriteStream(LOG_FILE, { flags: 'a' });
    }
    catch { }
}
function safe(v) { try {
    return JSON.stringify(v);
}
catch {
    return String(v);
} }
function write(level, msg, data) {
    const ts = new Date().toISOString();
    const line = `[${ts}] [${level}] ${msg}${data !== undefined ? ' ' + safe(data) : ''}`;
    if (LEVELS[level] >= LEVELS[LOG_LEVEL]) {
        try {
            console.log(line);
        }
        catch { }
        try {
            ensureStream();
            stream?.write(line + '\n');
        }
        catch { }
    }
}
export const log = {
    debug: (m, d) => write('debug', m, d),
    info: (m, d) => write('info', m, d),
    warn: (m, d) => write('warn', m, d),
    error: (m, d) => write('error', m, d),
    file: LOG_FILE,
    level: LOG_LEVEL
};
