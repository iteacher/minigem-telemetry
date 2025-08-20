"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.log = void 0;
const fs_1 = __importDefault(require("fs"));
const path_1 = __importDefault(require("path"));
const config_js_1 = require("./config.js");
const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };
const LOG_FILE = process.env.LOG_FILE || './logs/jwc-telemetry.log';
const LOG_LEVEL = ((process.env.LOG_LEVEL || config_js_1.CONFIG?.LOG_LEVEL || 'info').toString().toLowerCase());
let stream = null;
function ensureStream() {
    if (stream)
        return;
    try {
        fs_1.default.mkdirSync(path_1.default.dirname(LOG_FILE), { recursive: true });
        stream = fs_1.default.createWriteStream(LOG_FILE, { flags: 'a' });
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
exports.log = {
    debug: (m, d) => write('debug', m, d),
    info: (m, d) => write('info', m, d),
    warn: (m, d) => write('warn', m, d),
    error: (m, d) => write('error', m, d),
    file: LOG_FILE,
    level: LOG_LEVEL
};
