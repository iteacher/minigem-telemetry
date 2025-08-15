import fs from 'fs';
import path from 'path';
import { CONFIG } from './config.js';

type Level = 'debug' | 'info' | 'warn' | 'error';
const LEVELS: Record<Level, number> = { debug: 10, info: 20, warn: 30, error: 40 };

const LOG_FILE = process.env.LOG_FILE || (CONFIG as any)?.LOG_FILE || '/tmp/jwc-telemetry.log';
const LOG_LEVEL = ((process.env.LOG_LEVEL || (CONFIG as any)?.LOG_LEVEL || 'info').toString().toLowerCase()) as Level;

let stream: fs.WriteStream | null = null;
function ensureStream() {
  if (stream) return;
  try {
    fs.mkdirSync(path.dirname(LOG_FILE), { recursive: true });
    stream = fs.createWriteStream(LOG_FILE, { flags: 'a' });
  } catch {}
}

function safe(v: any): string { try { return JSON.stringify(v); } catch { return String(v); } }

function write(level: Level, msg: string, data?: any) {
  const ts = new Date().toISOString();
  const line = `[${ts}] [${level}] ${msg}${data !== undefined ? ' ' + safe(data) : ''}`;
  if (LEVELS[level] >= LEVELS[LOG_LEVEL]) {
    try { console.log(line); } catch {}
    try { ensureStream(); stream?.write(line + '\n'); } catch {}
  }
}

export const log = {
  debug: (m: string, d?: any) => write('debug', m, d),
  info:  (m: string, d?: any) => write('info',  m, d),
  warn:  (m: string, d?: any) => write('warn',  m, d),
  error: (m: string, d?: any) => write('error', m, d),
  file: LOG_FILE,
  level: LOG_LEVEL
};
