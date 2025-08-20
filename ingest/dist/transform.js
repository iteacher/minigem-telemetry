"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.lineForEvent = lineForEvent;
exports.appendLine = appendLine;
exports.appendJsonEvent = appendJsonEvent;
const fs_1 = __importDefault(require("fs"));
const path_1 = __importDefault(require("path"));
const config_js_1 = require("./config.js");
function pad(n) { return n < 10 ? '0' + n : '' + n; }
function iso(ts) {
    const d = new Date(ts);
    return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}
function lineForEvent(e, geo) {
    const dur = e.m?.durationMs ?? '';
    const exit = e.m?.exit ?? '';
    return `${iso(e.t)} ${e.anon} ${e.evt} ${e.os} ${e.ext} ${e.vscode} ${dur} ${exit} ${geo.country}-${geo.region}`;
}
function appendLine(line) {
    const day = new Date().toISOString().slice(0, 10);
    const file = path_1.default.join(config_js_1.CONFIG.LOG_DIR, `${day}.log`);
    fs_1.default.mkdirSync(config_js_1.CONFIG.LOG_DIR, { recursive: true });
    fs_1.default.appendFileSync(file, line + '\n');
}
function appendJsonEvent(e, geo) {
    const day = new Date().toISOString().slice(0, 10);
    const dir = path_1.default.join(config_js_1.CONFIG.LOG_DIR, 'json');
    const file = path_1.default.join(dir, `${day}.jsonl`);
    fs_1.default.mkdirSync(dir, { recursive: true });
    const out = geo ? { ...e, country: geo.country, region: geo.region } : e;
    fs_1.default.appendFileSync(file, JSON.stringify(out) + '\n');
}
