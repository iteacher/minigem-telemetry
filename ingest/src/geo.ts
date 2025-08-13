import maxmind from 'maxmind';

// Simple MaxMind GeoLite2 Country reader
let reader: any | undefined;

export async function initGeo() {
  const path = process.env.GEO_MMDB;
  if (!path) {
    console.warn('geo: GEO_MMDB not set; country lookups will be Unknown unless provider headers are present');
    return;
  }
  try {
    reader = await maxmind.open(path);
    console.log('geo: loaded mmdb from', path);
  } catch (e) {
    console.error('geo: failed to open mmdb', e);
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