"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.initGeo = initGeo;
exports.lookup = lookup;
const maxmind_1 = __importDefault(require("maxmind"));
const logger_js_1 = require("./logger.js");
// Simple MaxMind GeoLite2 Country reader
let reader;
async function initGeo() {
    const path = process.env.GEO_MMDB || process.env.GEO_DB;
    if (!path) {
        logger_js_1.log.warn('geo.init.no_mmdb', 'GEO_MMDB/GEO_DB not set; headers-only geolocation');
        return;
    }
    try {
        reader = await maxmind_1.default.open(path);
        logger_js_1.log.info('geo.init.loaded', { path });
    }
    catch (e) {
        logger_js_1.log.error('geo.init.failed', String(e));
    }
}
function lookup(ip) {
    try {
        if (!reader || !ip)
            return { country: '', region: '' };
        const rec = reader.get(ip);
        const country = rec?.country?.iso_code || rec?.registered_country?.iso_code || '';
        const region = rec?.subdivisions?.[0]?.iso_code || '';
        return { country: country ? country.toUpperCase() : '', region: region ? region.toUpperCase() : '' };
    }
    catch {
        return { country: '', region: '' };
    }
}
