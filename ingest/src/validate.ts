const EVENTS = new Set([
  'lifecycle.activate','lifecycle.deactivate','extension.upgraded',
  'java.run.started','java.run.completed','java.run.error',
  'feature.webview.open','feature.theme.change',
  'feature.customCss.enable','feature.customCss.disable',
  'settings.changed','error.unhandled','telemetry.optout','telemetry.optin',
  'test.ping','install.created'
]);

export function validateEnvelope(body: any): body is { schema: string; sentAt: number; batch: any[] } {
  return !!body && body.schema === 'jwc.v1' && Array.isArray(body.batch);
}

export function normalizeEvent(e: any): any | null {
  if (!e) return null;
  const out: any = { ...e };
  // Coerce timestamp to ms
  if (typeof out.t === 'string') out.t = Number(out.t);
  if (typeof out.t !== 'number' || !isFinite(out.t)) return null;
  if (out.t < 1e12) out.t = out.t * 1000; // seconds → ms

  // Defaults
  if (typeof out.ext !== 'string') out.ext = '0.0.0';
  if (typeof out.vscode !== 'string') out.vscode = '0.0.0';
  if (typeof out.os !== 'string') out.os = 'unknown';

  // Basic checks
  if (typeof out.anon !== 'string' || !/^[a-f0-9]{32}$/.test(out.anon)) return null;
  if (typeof out.evt !== 'string' || !EVENTS.has(out.evt)) return null;
  if (out.ext.length > 30 || out.vscode.length > 30 || out.os.length > 30) return null;
  return out;
}

export function validateEvent(e: any): boolean {
  if (!e || typeof e.t !== 'number') return false;
  if (typeof e.anon !== 'string' || !/^[a-f0-9]{32}$/.test(e.anon)) return false;
  if (typeof e.evt !== 'string' || !EVENTS.has(e.evt)) return false;
  if (typeof e.ext !== 'string' || e.ext.length > 30) return false;
  if (typeof e.vscode !== 'string' || e.vscode.length > 30) return false;
  if (typeof e.os !== 'string' || e.os.length > 30) return false;
  return true;
}