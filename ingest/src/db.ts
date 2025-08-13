import { Pool } from 'pg';
import { CONFIG } from './config.js';

let pool: Pool | null = null;

function getUrl(): string | undefined {
  return process.env.DATABASE_URL || process.env.PG_URL || (CONFIG as any)?.DATABASE_URL;
}

function buildPool(): Pool {
  const url = getUrl();
  if (!url) throw new Error('DATABASE_URL/PG_URL not set');
  const u = new URL(url);
  const sslmode = u.searchParams.get('sslmode');
  const ssl = sslmode && sslmode !== 'disable' ? { rejectUnauthorized: false } : undefined;
  return new Pool({ connectionString: url, ssl });
}

export function dbEnabled(): boolean { return !!getUrl(); }

export async function dbInit(): Promise<void> {
  if (!dbEnabled()) return;
  if (!pool) pool = buildPool();
  const client = await pool.connect();
  try {
    await client.query('select 1');
    await client.query(`
      create table if not exists telemetry_events (
        id bigserial primary key,
        t timestamptz not null,
        anon text not null,
        evt text not null,
        os text,
        ext text,
        vscode text,
        country text,
        region text,
        -- explicit metric columns (nullable)
        duration_ms bigint,
        wait_ms_total bigint,
        exit_code integer,
        out_bytes_bucket text,
        scanner_usage boolean,
        truncated_output boolean,
        error_phase text,
        exception_hash text,
        m jsonb
      );
      create index if not exists idx_events_t on telemetry_events (t);
      create index if not exists idx_events_evt on telemetry_events (evt);
      create index if not exists idx_events_country on telemetry_events (country);
      create index if not exists idx_events_os on telemetry_events (os);
      create index if not exists idx_events_ext on telemetry_events (ext);
      create index if not exists idx_events_vscode on telemetry_events (vscode);
      create index if not exists idx_events_anon on telemetry_events (anon);
    `);

    // Ensure columns exist if table predated this migration
    await client.query(`
      alter table telemetry_events
        add column if not exists duration_ms bigint,
        add column if not exists wait_ms_total bigint,
        add column if not exists exit_code integer,
        add column if not exists out_bytes_bucket text,
        add column if not exists scanner_usage boolean,
        add column if not exists truncated_output boolean,
        add column if not exists error_phase text,
        add column if not exists exception_hash text;
    `);
  } finally {
    client.release();
  }
}

export async function dbInsertEvent(ev: any, geo?: { country: string; region: string }): Promise<void> {
  if (!dbEnabled()) return;
  if (!pool) pool = buildPool();
  const tnum = typeof ev.t === 'number' ? ev.t : Date.parse(ev.t);
  const ms = tnum < 1e12 ? tnum * 1000 : tnum;
  const ts = new Date(ms);

  const m = ev.m || {};
  const duration_ms = m.durationMs ?? null;
  const wait_ms_total = m.waitMsTotal ?? null;
  const exit_code = m.exit ?? null;
  const out_bytes_bucket = m.cumulativeBytesBucket ?? null;
  const scanner_usage = typeof m.scannerUsage === 'boolean' ? m.scannerUsage : null;
  const truncated_output = typeof m.truncatedOutput === 'boolean' ? m.truncatedOutput : null;
  const error_phase = m.phase ?? null;
  const exception_hash = m.exceptionHash ?? null;

  const text = `
    insert into telemetry_events (
      t, anon, evt, os, ext, vscode, country, region,
      duration_ms, wait_ms_total, exit_code, out_bytes_bucket, scanner_usage, truncated_output, error_phase, exception_hash,
      m
    ) values (
      $1,$2,$3,$4,$5,$6,$7,$8,
      $9,$10,$11,$12,$13,$14,$15,$16,
      $17
    )`;

  const values = [
    ts,
    ev.anon,
    ev.evt,
    ev.os || null,
    ev.ext || null,
    ev.vscode || null,
    geo?.country || null,
    geo?.region || null,
    duration_ms,
    wait_ms_total,
    exit_code,
    out_bytes_bucket,
    scanner_usage,
    truncated_output,
    error_phase,
    exception_hash,
    Object.keys(m).length ? JSON.stringify(m) : null
  ];
  await pool.query(text, values);
}

function arr24(rows: { h: number; c: number }[]): number[] { const a = new Array(24).fill(0); for (const r of rows) a[r.h|0] = Number(r.c)||0; return a; }
function arr7(rows: { d: number; c: number }[]): number[] { const a = new Array(7).fill(0); for (const r of rows) a[r.d|0] = Number(r.c)||0; return a; }

const HIST_EDGES = [0,500,1000,3000,10000,30000,60000,120000];

function continentOf(country?: string): string {
  if (!country) return 'Unknown';
  const cc = country.toUpperCase();
  const C: Record<string,string> = {
    US:'NA', CA:'NA', MX:'NA', BR:'SA', AR:'SA', CL:'SA', CO:'SA',
    GB:'EU', DE:'EU', FR:'EU', ES:'EU', IT:'EU', NL:'EU', SE:'EU', PL:'EU', IE:'EU', PT:'EU', CZ:'EU', RO:'EU', HU:'EU',
    RU:'EU', UA:'EU', TR:'AS',
    CN:'AS', JP:'AS', KR:'AS', IN:'AS', SG:'AS', HK:'AS', TW:'AS', ID:'AS', PH:'AS', TH:'AS', VN:'AS', MY:'AS', PK:'AS', BD:'AS',
    AU:'OC', NZ:'OC', ZA:'AF', NG:'AF', EG:'AF', MA:'AF', KE:'AF', ET:'AF', DZ:'AF', GH:'AF', TZ:'AF', CI:'AF', SN:'AF'
  };
  return C[cc] || 'Unknown';
}

export async function dbReadStats(windowDays: number): Promise<any | null> {
  if (!dbEnabled()) return null;
  if (!pool) pool = buildPool();
  const client = await pool.connect();
  try {
    const to = new Date();
    const from = new Date(to.getTime() - (windowDays-1) * 86400000);

    // Totals and KPIs
    const qTotals = await client.query(`
      select
        count(*)::int as total,
        count(distinct anon)::int as uniques
      from telemetry_events
      where t >= $1 and t < $2
    `, [from, to]);
    const totals = qTotals.rows[0] || { total:0, uniques:0 };

    // Grouped counts
    const [byEvent, byOs, byExt, byVscode] = await Promise.all([
      client.query(`select evt as k, count(*)::int as v from telemetry_events where t >= $1 and t < $2 group by 1 order by 2 desc`, [from,to]),
      client.query(`select coalesce(os,'Unknown') as k, count(*)::int as v from telemetry_events where t >= $1 and t < $2 group by 1 order by 2 desc`, [from,to]),
      client.query(`select coalesce(ext,'Unknown') as k, count(*)::int as v from telemetry_events where t >= $1 and t < $2 group by 1 order by 2 desc`, [from,to]),
      client.query(`select coalesce(vscode,'Unknown') as k, count(*)::int as v from telemetry_events where t >= $1 and t < $2 group by 1 order by 2 desc`, [from,to])
    ]);

    // Daily series
    const qDailyHits = await client.query(`
      with days as (
        select generate_series(date_trunc('day',$1::timestamptz), date_trunc('day',$2::timestamptz), '1 day')::date d
      )
      select to_char(d.d,'YYYY-MM-DD') as date,
             coalesce(x.c,0)::int as hits,
             coalesce(x.u,0)::int as uniques
      from days d
      left join (
        select date(t) as dd,
               count(*) as c,
               count(distinct anon) as u
        from telemetry_events where t >= $1 and t < $2 group by 1
      ) x on x.dd = d.d
      order by d.d
    `, [from,to]);

    // Hourly hits and visitors
    const [qHourlyHits, qHourlyVisitors] = await Promise.all([
      client.query(`select extract(hour from t at time zone 'utc')::int h, count(*)::int c from telemetry_events where t >= $1 and t < $2 group by 1`, [from,to]),
      client.query(`select extract(hour from t at time zone 'utc')::int h, count(distinct anon)::int c from telemetry_events where t >= $1 and t < $2 group by 1`, [from,to])
    ]);

    // Day of week
    const qDow = await client.query(`select extract(dow from t at time zone 'utc')::int d, count(*)::int c from telemetry_events where t >= $1 and t < $2 group by 1`, [from,to]);

    // Runs and errors
    const [qStarted, qCompleted, qCompileErr, qRuntimeErr] = await Promise.all([
      client.query(`select count(*)::int c from telemetry_events where t >= $1 and t < $2 and evt='java.run.started'`, [from,to]),
      client.query(`select count(*)::int c from telemetry_events where t >= $1 and t < $2 and evt='java.run.completed'`, [from,to]),
      client.query(`select count(*)::int c from telemetry_events where t >= $1 and t < $2 and evt='java.run.error' and coalesce(error_phase, m->>'phase')='compile'`, [from,to]),
      client.query(`select count(*)::int c from telemetry_events where t >= $1 and t < $2 and evt='java.run.error' and coalesce(error_phase, m->>'phase')<>'compile'`, [from,to])
    ]);

    // Durations and histogram (completed runs)
    const qDur = await client.query(`
      with d as (
        select coalesce(duration_ms, (m->>'durationMs')::bigint) as dur,
               coalesce(wait_ms_total, (m->>'waitMsTotal')::bigint) as wait
        from telemetry_events
        where t >= $1 and t < $2 and evt='java.run.completed' and coalesce(duration_ms, (m->>'durationMs')::bigint) is not null
      )
      select count(*)::int as count,
             percentile_disc(0.5) within group (order by dur) as median,
             percentile_disc(0.9) within group (order by dur) as p90,
             avg(wait)::float as avgwait
      from d
    `, [from,to]);

    const qDurHist = await client.query(`
      select
        sum(case when dur>=0   and dur<500    then 1 else 0 end)::int as b0,
        sum(case when dur>=500 and dur<1000   then 1 else 0 end)::int as b1,
        sum(case when dur>=1000 and dur<3000  then 1 else 0 end)::int as b2,
        sum(case when dur>=3000 and dur<10000 then 1 else 0 end)::int as b3,
        sum(case when dur>=10000 and dur<30000 then 1 else 0 end)::int as b4,
        sum(case when dur>=30000 and dur<60000 then 1 else 0 end)::int as b5,
        sum(case when dur>=60000 and dur<120000 then 1 else 0 end)::int as b6,
        sum(case when dur>=120000 then 1 else 0 end)::int as b7
      from (
        select coalesce(duration_ms, (m->>'durationMs')::bigint) as dur
        from telemetry_events where t >= $1 and t < $2 and evt='java.run.completed'
      ) x
    `, [from,to]);

    // Exit codes, output buckets, interactive/truncation
    const [qExit, qOut, qInteractive, qTrunc] = await Promise.all([
      client.query(`select coalesce(exit_code::text, m->>'exit','0') as k, count(*)::int v from telemetry_events where t >= $1 and t < $2 and evt='java.run.completed' group by 1 order by 2 desc`, [from,to]),
      client.query(`select coalesce(out_bytes_bucket, m->>'cumulativeBytesBucket','-') as k, count(*)::int v from telemetry_events where t >= $1 and t < $2 and evt='java.run.completed' group by 1 order by 2 desc`, [from,to]),
      client.query(`select count(*)::int c from telemetry_events where t >= $1 and t < $2 and evt='java.run.completed' and coalesce(scanner_usage, (m->>'scannerUsage')::boolean)=true`, [from,to]),
      client.query(`select count(*)::int c from telemetry_events where t >= $1 and t < $2 and evt='java.run.completed' and coalesce(truncated_output, (m->>'truncatedOutput')::boolean)=true`, [from,to])
    ]);

    // Top exceptions
    const qTopEx = await client.query(`
      select coalesce(exception_hash, m->>'exceptionHash','') as k, count(*)::int v
      from telemetry_events where t >= $1 and t < $2 and evt='java.run.error' and coalesce(exception_hash, m->>'exceptionHash') is not null
      group by 1 order by 2 desc limit 50
    `, [from,to]);

    // OS daily series
    const qOsDaily = await client.query(`
      with days as (
        select generate_series(date_trunc('day',$1::timestamptz), date_trunc('day',$2::timestamptz), '1 day')::date d
      ), counts as (
        select date(t) as dd, coalesce(os,'Unknown') as os, count(*) as c
        from telemetry_events where t >= $1 and t < $2 group by 1,2
      )
      select to_char(d.d,'YYYY-MM-DD') as date, c.os, coalesce(c.c,0)::int as v
      from days d
      left join counts c on c.dd = d.d
      order by d.d
    `, [from,to]);

    // Geo by country
    const qCountry = await client.query(`
      select coalesce(country,'Unknown') as k, count(*)::int as hits, count(distinct anon)::int as visitors
      from telemetry_events where t >= $1 and t < $2 group by 1 order by 2 desc
    `, [from,to]);

    // Assemble results
    const dailyDates: string[] = qDailyHits.rows.map(r=>r.date);
    const dailyHits: number[] = qDailyHits.rows.map(r=>Number(r.hits)||0);
    const dailyUniques: number[] = qDailyHits.rows.map(r=>Number(r.uniques)||0);

    const hourly = arr24(qHourlyHits.rows as any);
    const hourlyVisitors = arr24(qHourlyVisitors.rows as any);
    const dow = arr7(qDow.rows as any);

    const started = Number(qStarted.rows[0]?.c||0);
    const completed = Number(qCompleted.rows[0]?.c||0);
    const compileErr = Number(qCompileErr.rows[0]?.c||0);
    const runtimeErr = Number(qRuntimeErr.rows[0]?.c||0);

    const durRow = qDur.rows[0] || { count:0, median:0, p90:0, avgwait:0 };
    const histRow = qDurHist.rows[0] || { b0:0,b1:0,b2:0,b3:0,b4:0,b5:0,b6:0,b7:0 } as any;

    const durations = {
      count: Number(durRow.count||0),
      median: Number(durRow.median||0),
      p90: Number(durRow.p90||0),
      hist: { labels: ['0-500ms','500-1000ms','1000-3000ms','3000-10000ms','10000-30000ms','30000-60000ms','60000-120000ms','≥120000ms'], values: [histRow.b0,histRow.b1,histRow.b2,histRow.b3,histRow.b4,histRow.b5,histRow.b6,histRow.b7].map(Number) },
      avgWaitMs: Number(durRow.avgwait||0)
    };

    const objFrom = (rows: any[]) => Object.fromEntries(rows.map(r=>[r.k, Number(r.v||r.c||0)]));

    // OS daily series assemble
    const osSet = new Set<string>();
    qOsDaily.rows.forEach(r=>{ if (r.os) osSet.add(r.os); });
    const osList = Array.from(osSet);
    const osSeries = osList.map(os=>({ key: os, data: dailyDates.map(d=>{
      const row = qOsDaily.rows.find(r=>r.date===d && r.os===os);
      return Number(row?.v||0);
    }) }));

    const byCountry: Record<string,{hits:number;visitors:number}> = {};
    qCountry.rows.forEach(r=>{ byCountry[r.k] = { hits: Number(r.hits||0), visitors: Number(r.visitors||0) }; });
    const byContinent: Record<string,{hits:number;visitors:number}> = {};
    for (const [cc, v] of Object.entries(byCountry)) {
      const cont = continentOf(cc === 'Unknown' ? undefined : cc);
      if (!byContinent[cont]) byContinent[cont] = { hits:0, visitors:0 };
      byContinent[cont].hits += v.hits;
      byContinent[cont].visitors += v.visitors;
    }

    const total = Number(totals.total||0);
    const uniques = Number(totals.uniques||0);

    const res = {
      from: dailyDates[0] || new Date(from).toISOString().slice(0,10),
      to: dailyDates[dailyDates.length-1] || new Date(to).toISOString().slice(0,10),
      windowDays,
      total,
      uniques,
      osTypes: byOs.rows.length,
      extTypes: byExt.rows.length,
      vscodeTypes: byVscode.rows.length,
      daily: { dates: dailyDates, hits: dailyHits, uniques: dailyUniques },
      dailyOs: { labels: dailyDates, series: osSeries },
      byEvent: objFrom(byEvent.rows),
      byOs: objFrom(byOs.rows),
      byExt: objFrom(byExt.rows),
      byVscode: objFrom(byVscode.rows),
      hourly,
      hourlyVisitors,
      dow,
      runs: { started, completed },
      errors: { compile: compileErr, runtime: runtimeErr, compileRate: started? compileErr/started : 0, runtimeRate: started? runtimeErr/started : 0 },
      durations,
      exitCodes: objFrom(qExit.rows),
      outputBuckets: objFrom(qOut.rows),
      interactiveRate: completed? Number(qInteractive.rows[0]?.c||0)/completed : 0,
      truncationRate: completed? Number(qTrunc.rows[0]?.c||0)/completed : 0,
      topExceptions: objFrom(qTopEx.rows),
      geo: { byContinent, byCountry },
      tables: {
        eventsTop: byEvent.rows.map((r:any)=>{ const v=Number(r.v); const p= total? v/total:0; return { key: r.k, event: r.k, count: v, pct: p, percent: Math.round(p*1000)/10 }; }).slice(0,20),
        extTop: byExt.rows.map((r:any)=>{ const v=Number(r.v); const p= total? v/total:0; return { key: r.k, version: r.k, count: v, pct: p, percent: Math.round(p*1000)/10 }; }).slice(0,20),
        vscodeTop: byVscode.rows.map((r:any)=>{ const v=Number(r.v); const p= total? v/total:0; return { key: r.k, version: r.k, count: v, pct: p, percent: Math.round(p*1000)/10 }; }).slice(0,20),
        exitTop: qExit.rows.map((r:any)=>{ const v=Number(r.v); const p= total? v/total:0; return { key: r.k, code: r.k, count: v, pct: p, percent: Math.round(p*1000)/10 }; }).slice(0,20),
        exceptionsTop: qTopEx.rows.map((r:any)=>{ const v=Number(r.v); const p= total? v/total:0; return { key: r.k, exceptionHash: r.k, count: v, pct: p, percent: Math.round(p*1000)/10 }; }).slice(0,20),
      }
    };

    return res;
  } finally {
    client.release();
  }
}

export async function dbRecent(limit = 50): Promise<any[]> {
  if (!dbEnabled()) return [];
  if (!pool) pool = buildPool();
  const { rows } = await pool.query(`
    select id, t, anon, evt, os, ext, vscode, country, region,
           duration_ms, wait_ms_total, exit_code, out_bytes_bucket, scanner_usage, truncated_output, error_phase, exception_hash
    from telemetry_events
    order by id desc
    limit $1
  `, [Math.max(1, Math.min(500, limit))]);
  return rows;
}

export async function dbCounts(hours = 24): Promise<any> {
  if (!dbEnabled()) return {};
  if (!pool) pool = buildPool();
  const since = new Date(Date.now() - Math.max(1, hours) * 3600_000);
  const q = await pool.query(`
    select evt, count(*)::int c from telemetry_events where t >= $1 group by 1 order by 2 desc
  `, [since]);
  const total = (await pool.query(`select count(*)::int c from telemetry_events where t >= $1`, [since])).rows[0]?.c || 0;
  return { since, total, byEvent: Object.fromEntries(q.rows.map(r=>[r.evt, r.c])) };
}

export async function dbHealth(): Promise<any> {
  const url = getUrl();
  if (!url) return { enabled: false, error: 'DATABASE_URL not set' };
  try {
    if (!pool) pool = buildPool();
    const r = await pool.query('select version()');
    return { enabled: true, ok: true, version: r.rows?.[0]?.version };
  } catch (e: any) {
    return { enabled: true, ok: false, error: String(e?.message || e) };
  }
}
