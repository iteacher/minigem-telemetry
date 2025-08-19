import maxmind from 'maxmind';
import { log } from './logger.js';

// Simple MaxMind GeoLite2 Country reader
let reader: any | undefined;

export async function initGeo() {
  const path = process.env.GEO_MMDB;
  if (!path) {
  log.warn('geo.init.no_mmdb', 'GEO_MMDB not set; headers-only geolocation');
    return;
  }
  try {
    reader = await maxmind.open(path);
  log.info('geo.init.loaded', { path });
  } catch (e) {
  log.error('geo.init.failed', String(e));
  }
}

export function lookup(ip: string): { country: string; region: string } {
  try {
    if (!reader || !ip) return { country: '', region: '' };
    const rec: any = reader.get(ip);
    const country: string = rec?.country?.iso_code || rec?.registered_country?.iso_code || '';
    const region: string = rec?.subdivisions?.[0]?.iso_code || '';
    return { country: country ? country.toUpperCase() : '', region: region ? region.toUpperCase() : '' };
  } catch {
    return { country: '', region: '' };
  }
}