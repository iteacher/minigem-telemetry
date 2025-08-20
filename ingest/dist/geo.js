"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.initGeo = initGeo;
exports.lookup = lookup;
const logger_js_1 = require("./logger.js");
let maxmind;
try {
    maxmind = require('maxmind');
}
catch (e) {
    logger_js_1.log.warn('geo.maxmind.require.failed', String(e));
    maxmind = null;
}
// Simple MaxMind GeoLite2 Country reader
let reader;
async function initGeo() {
    const path = process.env.GEO_MMDB || process.env.GEO_DB;
    if (!path) {
        logger_js_1.log.warn('geo.init.no_mmdb', 'GEO_MMDB/GEO_DB not set; headers-only geolocation');
        return;
    }
    if (!maxmind) {
        logger_js_1.log.warn('geo.init.no_maxmind', 'maxmind module not available; headers-only geolocation');
        return;
    }
    try {
        reader = await maxmind.open(path);
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
