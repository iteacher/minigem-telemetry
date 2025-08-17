import fs from 'fs';
import path from 'path';
import { CONFIG } from './config.js';
function pad(n) { return n < 10 ? '0' + n : '' + n; }
function iso(ts) {
    const d = new Date(ts);
    return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}
export function lineForEvent(e, geo) {
    const dur = e.m?.durationMs ?? '';
    const exit = e.m?.exit ?? '';
    return `${iso(e.t)} ${e.anon} ${e.evt} ${e.os} ${e.ext} ${e.vscode} ${dur} ${exit} ${geo.country}-${geo.region}`;
}
export function appendLine(line) {
    const day = new Date().toISOString().slice(0, 10);
    const file = path.join(CONFIG.LOG_DIR, `${day}.log`);
    fs.mkdirSync(CONFIG.LOG_DIR, { recursive: true });
    fs.appendFileSync(file, line + '\n');
}
export function appendJsonEvent(e, geo) {
    const day = new Date().toISOString().slice(0, 10);
    const dir = path.join(CONFIG.LOG_DIR, 'json');
    const file = path.join(dir, `${day}.jsonl`);
    fs.mkdirSync(dir, { recursive: true });
    const out = geo ? { ...e, country: geo.country, region: geo.region } : e;
    fs.appendFileSync(file, JSON.stringify(out) + '\n');
}
