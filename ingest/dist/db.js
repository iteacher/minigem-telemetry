import { Pool } from 'pg';
import { CONFIG } from './config.js';
import { log } from './logger.js';
let pool = null;
function getUrl() {
    // Prefer environment variables; fall back to config if provided
    return process.env.DATABASE_URL
        || process.env.PG_URL
        || CONFIG?.DATABASE_URL
        || CONFIG?.PG_URL;
}
function buildPool() {
    const url = getUrl();
    if (url) {
        if (CONFIG.DEBUG_DB)
            log.debug('[db] using url');
        const u = new URL(url);
        const sslmode = u.searchParams.get('sslmode');
        const ssl = sslmode && sslmode !== 'disable' ? { rejectUnauthorized: false } : undefined;
        return new Pool({ connectionString: url, ssl });
    }
    // Build from discrete env/config (supports TCP or Unix socket via host path)
    if (CONFIG.DEBUG_DB)
        log.debug('[db] using discrete env vars');
    const host = process.env.PGHOST || CONFIG?.PGHOST;
    const portRaw = process.env.PGPORT || CONFIG?.PGPORT;
    const port = portRaw ? parseInt(String(portRaw), 10) : 5432;
    const user = process.env.PGUSER || CONFIG?.PGUSER;
    const password = process.env.PGPASSWORD || CONFIG?.PGPASSWORD;
    const database = process.env.PGDATABASE || CONFIG?.PGDATABASE;
    const sslEnv = (process.env.PGSSL || CONFIG?.PG_SSL || '').toString().toLowerCase();
    const ssl = sslEnv === 'true' || sslEnv === 'require' ? { rejectUnauthorized: false } : undefined;
    if (!host || !user || !database) {
        throw new Error('DB not configured: set DATABASE_URL/PG_URL or PGHOST, PGUSER, PGDATABASE');
    }
    if (CONFIG.DEBUG_DB)
        log.info('[db] pool conf', { host, port, user, database, ssl: !!ssl });
    return new Pool({ host, port, user, password, database, ssl });
}
export function dbEnabled() {
    if (getUrl())
        return true;
    const host = process.env.PGHOST || CONFIG?.PGHOST;
    const user = process.env.PGUSER || CONFIG?.PGUSER;
    const database = process.env.PGDATABASE || CONFIG?.PGDATABASE;
    return !!(host && user && database);
}
export async function dbInit() {
    if (!dbEnabled())
        return;
    if (!pool)
        pool = buildPool();
    if (CONFIG.DEBUG_DB)
        log.info('[db] init: connecting and ensuring schema');
    const client = await pool.connect();
    try {
        if (CONFIG.DEBUG_DB)
            log.debug('[db] ping');
        await client.query('select 1');
        if (CONFIG.DEBUG_DB)
            log.debug('[db] creating table if not exists');
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
        session_id text,
        out_bytes_bucket text,
        scanner_usage boolean,
        truncated_output boolean,
        error_phase text,
        exception_hash text,
        m jsonb
      );
    `);
        // Ensure columns exist if table predated this migration (must be before creating indexes on them)
        if (CONFIG.DEBUG_DB)
            log.debug('[db] migrating columns if needed');
        await client.query(`
      alter table telemetry_events
        add column if not exists duration_ms bigint,
        add column if not exists wait_ms_total bigint,
        add column if not exists exit_code integer,
        add column if not exists session_id text,
        add column if not exists out_bytes_bucket text,
        add column if not exists scanner_usage boolean,
        add column if not exists truncated_output boolean,
        add column if not exists error_phase text,
        add column if not exists exception_hash text;
    `);
        // Indexes (safe to create after columns are guaranteed to exist)
        if (CONFIG.DEBUG_DB)
            log.debug('[db] creating indexes if not exist');
        await client.query(`
      create index if not exists idx_events_t on telemetry_events (t);
      create index if not exists idx_events_evt on telemetry_events (evt);
      create index if not exists idx_events_country on telemetry_events (country);
      create index if not exists idx_events_os on telemetry_events (os);
      create index if not exists idx_events_ext on telemetry_events (ext);
      create index if not exists idx_events_vscode on telemetry_events (vscode);
      create index if not exists idx_events_anon on telemetry_events (anon);
      create index if not exists idx_events_session on telemetry_events (session_id);
    `);
    }
    finally {
        client.release();
    }
}
export async function dbInsertEvent(ev, geo) {
    if (!dbEnabled())
        return;
    if (!pool)
        pool = buildPool();
    if (CONFIG.DEBUG_DB)
        log.debug('[db] insert: raw event', ev);
    const tnum = typeof ev.t === 'number' ? ev.t : Date.parse(ev.t);
    const ms = tnum < 1e12 ? tnum * 1000 : tnum;
    const ts = new Date(ms);
    const m = ev.m || {};
    const duration_ms = m.durationMs ?? null;
    const wait_ms_total = m.waitMsTotal ?? null;
    const exit_code = m.exit ?? null;
    const session_id = (ev.sessionId ?? m.sessionId) ?? null;
    const out_bytes_bucket = m.cumulativeBytesBucket ?? null;
    const scanner_usage = typeof m.scannerUsage === 'boolean' ? m.scannerUsage : null;
    const truncated_output = typeof m.truncatedOutput === 'boolean' ? m.truncatedOutput : null;
    const error_phase = m.phase ?? null;
    const exception_hash = m.exceptionHash ?? null;
    const text = `
    insert into telemetry_events (
      t, anon, evt, os, ext, vscode, country, region,
      duration_ms, wait_ms_total, exit_code, session_id, out_bytes_bucket, scanner_usage, truncated_output, error_phase, exception_hash,
      m
    ) values (
      $1,$2,$3,$4,$5,$6,$7,$8,
      $9,$10,$11,$12,$13,$14,$15,$16,
      $17,
      $18
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
        session_id,
        out_bytes_bucket,
        scanner_usage,
        truncated_output,
        error_phase,
        exception_hash,
        Object.keys(m).length ? JSON.stringify(m) : null
    ];
    if (CONFIG.DEBUG_DB)
        log.debug('[db] insert sql', text.replace(/\s+/g, ' '));
    if (CONFIG.DEBUG_DB)
        log.debug('[db] insert values', values);
    try {
        await pool.query(text, values);
        if (CONFIG.DEBUG_DB)
            log.info('[db] insert ok');
    }
    catch (e) {
        log.error('[db] insert error', { error: String(e?.message || e) });
        throw e;
    }
}
function arr24(rows) { const a = new Array(24).fill(0); for (const r of rows)
    a[r.h | 0] = Number(r.c) || 0; return a; }
function arr7(rows) { const a = new Array(7).fill(0); for (const r of rows)
    a[r.d | 0] = Number(r.c) || 0; return a; }
const HIST_EDGES = [0, 500, 1000, 3000, 10000, 30000, 60000, 120000];
function continentOf(country) {
    if (!country)
        return 'Unknown';
    const cc = country.toUpperCase();
    const C = {
        US: 'NA', CA: 'NA', MX: 'NA', BR: 'SA', AR: 'SA', CL: 'SA', CO: 'SA',
        GB: 'EU', DE: 'EU', FR: 'EU', ES: 'EU', IT: 'EU', NL: 'EU', SE: 'EU', PL: 'EU', IE: 'EU', PT: 'EU', CZ: 'EU', RO: 'EU', HU: 'EU',
        RU: 'EU', UA: 'EU', TR: 'AS',
        CN: 'AS', JP: 'AS', KR: 'AS', IN: 'AS', SG: 'AS', HK: 'AS', TW: 'AS', ID: 'AS', PH: 'AS', TH: 'AS', VN: 'AS', MY: 'AS', PK: 'AS', BD: 'AS',
        AU: 'OC', NZ: 'OC', ZA: 'AF', NG: 'AF', EG: 'AF', MA: 'AF', KE: 'AF', ET: 'AF', DZ: 'AF', GH: 'AF', TZ: 'AF', CI: 'AF', SN: 'AF'
    };
    return C[cc] || 'Unknown';
}
export async function dbReadStats(windowDays, fromS, toS) {
    if (!dbEnabled())
        return null;
    if (!pool)
        pool = buildPool();
    const trace = [];
    const client = await pool.connect();
    try {
        // Show ALL data - no date restrictions unless specifically requested
        let to = new Date();
        let from = new Date('1970-01-01T00:00:00Z'); // Start from Unix epoch to include all data
        if (fromS && toS) {
            // Parse YYYY-MM-DD inputs; inclusive range for days
            const ft = new Date(fromS + 'T00:00:00Z');
            const tt = new Date(toS + 'T23:59:59Z');
            if (!isNaN(ft.getTime()) && !isNaN(tt.getTime()) && ft <= tt) {
                from = ft;
                to = tt;
            }
        }
        if (CONFIG.DEBUG_DB)
            log.debug('[db] stats date range', { from: from.toISOString(), to: to.toISOString(), windowDays });
        // Totals and KPIs
        const qTotalsSql = `
      select
        count(*)::int as total,
        count(distinct anon)::int as uniques
      from telemetry_events
      where t >= $1 and t < $2
  `;
        trace.push({ sql: qTotalsSql, params: [from, to] });
        if (CONFIG.DEBUG_DB)
            log.debug('[db] stats.qTotals', { sql: qTotalsSql, params: [from, to] });
        const qTotals = await client.query(qTotalsSql, [from, to]);
        const totals = qTotals.rows[0] || { total: 0, uniques: 0 };
        // Grouped counts
        const sqlByEvent = `select evt as k, count(*)::int as v from telemetry_events where t >= $1 and t < $2 group by 1 order by 2 desc`;
        const sqlByOs = `select coalesce(os,'Unknown') as k, count(*)::int as v from telemetry_events where t >= $1 and t < $2 group by 1 order by 2 desc`;
        const sqlByExt = `select coalesce(ext,'Unknown') as k, count(*)::int as v from telemetry_events where t >= $1 and t < $2 group by 1 order by 2 desc`;
        const sqlByVscode = `select coalesce(vscode,'Unknown') as k, count(*)::int as v from telemetry_events where t >= $1 and t < $2 group by 1 order by 2 desc`;
        trace.push({ sql: sqlByEvent, params: [from, to] }, { sql: sqlByOs, params: [from, to] }, { sql: sqlByExt, params: [from, to] }, { sql: sqlByVscode, params: [from, to] });
        const [byEvent, byOs, byExt, byVscode] = await Promise.all([
            client.query(sqlByEvent, [from, to]),
            client.query(sqlByOs, [from, to]),
            client.query(sqlByExt, [from, to]),
            client.query(sqlByVscode, [from, to])
        ]);
        // Daily series
        const qDailyHitsSql = `
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
  `;
        trace.push({ sql: qDailyHitsSql, params: [from, to] });
        if (CONFIG.DEBUG_DB)
            log.debug('[db] stats.qDailyHits', { sql: qDailyHitsSql, params: [from, to] });
        const qDailyHits = await client.query(qDailyHitsSql, [from, to]);
        // Hourly hits and visitors
        const sqlHourlyHits = `select extract(hour from t at time zone 'utc')::int h, count(*)::int c from telemetry_events where t >= $1 and t < $2 group by 1`;
        const sqlHourlyVisitors = `select extract(hour from t at time zone 'utc')::int h, count(distinct anon)::int c from telemetry_events where t >= $1 and t < $2 group by 1`;
        trace.push({ sql: sqlHourlyHits, params: [from, to] }, { sql: sqlHourlyVisitors, params: [from, to] });
        const [qHourlyHits, qHourlyVisitors] = await Promise.all([
            client.query(sqlHourlyHits, [from, to]),
            client.query(sqlHourlyVisitors, [from, to])
        ]);
        // Day of week
        const sqlDow = `select extract(dow from t at time zone 'utc')::int d, count(*)::int c from telemetry_events where t >= $1 and t < $2 group by 1`;
        trace.push({ sql: sqlDow, params: [from, to] });
        if (CONFIG.DEBUG_DB)
            log.debug('[db] stats.qDow', { sql: sqlDow, params: [from, to] });
        const qDow = await client.query(sqlDow, [from, to]);
        // Runs and errors
        const sqlStarted = `select count(*)::int c from telemetry_events where t >= $1 and t < $2 and evt='java.run.started'`;
        const sqlCompleted = `select count(*)::int c from telemetry_events where t >= $1 and t < $2 and evt='java.run.completed'`;
        const sqlCompileErr = `select count(*)::int c from telemetry_events where t >= $1 and t < $2 and evt='java.run.error' and coalesce(error_phase, m->>'phase')='compile'`;
        const sqlRuntimeErr = `select count(*)::int c from telemetry_events where t >= $1 and t < $2 and evt='java.run.error' and coalesce(error_phase, m->>'phase')<>'compile'`;
        trace.push({ sql: sqlStarted, params: [from, to] }, { sql: sqlCompleted, params: [from, to] }, { sql: sqlCompileErr, params: [from, to] }, { sql: sqlRuntimeErr, params: [from, to] });
        const [qStarted, qCompleted, qCompileErr, qRuntimeErr] = await Promise.all([
            client.query(sqlStarted, [from, to]),
            client.query(sqlCompleted, [from, to]),
            client.query(sqlCompileErr, [from, to]),
            client.query(sqlRuntimeErr, [from, to])
        ]);
        // Durations and histogram (completed runs)
        const qDurSql = `
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
  `;
        trace.push({ sql: qDurSql, params: [from, to] });
        if (CONFIG.DEBUG_DB)
            log.debug('[db] stats.qDur', { sql: qDurSql, params: [from, to] });
        const qDur = await client.query(qDurSql, [from, to]);
        const qDurHistSql = `
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
  `;
        trace.push({ sql: qDurHistSql, params: [from, to] });
        if (CONFIG.DEBUG_DB)
            log.debug('[db] stats.qDurHist', { sql: qDurHistSql, params: [from, to] });
        const qDurHist = await client.query(qDurHistSql, [from, to]);
        // Exit codes, output buckets, interactive/truncation
        const sqlExit = `select coalesce(exit_code::text, m->>'exit','0') as k, count(*)::int v from telemetry_events where t >= $1 and t < $2 and evt='java.run.completed' group by 1 order by 2 desc`;
        const sqlOut = `select coalesce(out_bytes_bucket, m->>'cumulativeBytesBucket','-') as k, count(*)::int v from telemetry_events where t >= $1 and t < $2 and evt='java.run.completed' group by 1 order by 2 desc`;
        const sqlInteractive = `select count(*)::int c from telemetry_events where t >= $1 and t < $2 and evt='java.run.completed' and coalesce(scanner_usage, (m->>'scannerUsage')::boolean)=true`;
        const sqlTrunc = `select count(*)::int c from telemetry_events where t >= $1 and t < $2 and evt='java.run.completed' and coalesce(truncated_output, (m->>'truncatedOutput')::boolean)=true`;
        trace.push({ sql: sqlExit, params: [from, to] }, { sql: sqlOut, params: [from, to] }, { sql: sqlInteractive, params: [from, to] }, { sql: sqlTrunc, params: [from, to] });
        const [qExit, qOut, qInteractive, qTrunc] = await Promise.all([
            client.query(sqlExit, [from, to]),
            client.query(sqlOut, [from, to]),
            client.query(sqlInteractive, [from, to]),
            client.query(sqlTrunc, [from, to])
        ]);
        // Active sessions: sessions with activity in last 10 minutes and not yet completed
        const qActiveSql = `
      with recent as (
        select session_id,
               max(t) as tmax,
               bool_or(evt='java.run.completed') as completed
        from telemetry_events
        where session_id is not null and t >= now() - interval '10 minutes'
        group by session_id
      )
      select count(*)::int as c from recent where completed = false
  `;
        trace.push({ sql: qActiveSql, params: [] });
        if (CONFIG.DEBUG_DB)
            log.debug('[db] stats.qActive', { sql: qActiveSql });
        const qActive = await client.query(qActiveSql);
        // Recent sessions summary (last N by last event time)
        const qRecentSessionsSql = `
      with s as (
        select session_id,
               min(t) as started_at,
               max(t) as last_at,
               max(case when evt='java.run.completed' then 1 else 0 end) as completed,
               max(coalesce(exit_code, (m->>'exit')::int)) as exit_code,
               max(coalesce(duration_ms, (m->>'durationMs')::bigint)) as duration_ms,
               max(coalesce(scanner_usage, (m->>'scannerUsage')::boolean)) as interactive,
               any_value(anon) as anon
        from telemetry_events
        where session_id is not null and t >= $1 and t < $2
        group by session_id
      )
      select session_id, anon, started_at, last_at, completed::int as completed,
             coalesce(exit_code, -1) as exit_code,
             coalesce(duration_ms, 0) as duration_ms,
             coalesce(interactive,false) as interactive
      from s
      order by last_at desc
      limit 100
  `;
        trace.push({ sql: qRecentSessionsSql, params: [from, to] });
        if (CONFIG.DEBUG_DB)
            log.debug('[db] stats.qRecentSessions', { sql: qRecentSessionsSql, params: [from, to] });
        const qRecentSessions = await client.query(qRecentSessionsSql, [from, to]);
        // Installs total (lifetime) and window installs
        const sqlInstallsTotal = `select count(*)::int as c from telemetry_events where evt='install.created'`;
        const sqlInstallsWindow = `select count(*)::int as c from telemetry_events where evt='install.created' and t >= $1 and t < $2`;
        trace.push({ sql: sqlInstallsTotal, params: [] }, { sql: sqlInstallsWindow, params: [from, to] });
        if (CONFIG.DEBUG_DB)
            log.debug('[db] stats.qInstallsTotal', { sql: sqlInstallsTotal });
        const qInstallsTotal = await client.query(sqlInstallsTotal);
        if (CONFIG.DEBUG_DB)
            log.debug('[db] stats.qInstallsWindow', { sql: sqlInstallsWindow, params: [from, to] });
        const qInstallsWindow = await client.query(sqlInstallsWindow, [from, to]);
        // Daily learning outcomes by exit code (teacher-focused)
        const qDloSql = `
      select to_char(date(t),'YYYY-MM-DD') as day,
             coalesce(exit_code, (m->>'exit')::int, 0) as exit,
             count(*)::int as c
      from telemetry_events
      where t >= $1 and t < $2 and evt='java.run.completed'
      group by 1,2
      order by 1,2
  `;
        trace.push({ sql: qDloSql, params: [from, to] });
        if (CONFIG.DEBUG_DB)
            log.debug('[db] stats.qDLO', { sql: qDloSql, params: [from, to] });
        const qDLO = await client.query(qDloSql, [from, to]);
        // Ranked sessions: successful (exit=0) and frustrated (Ctrl+C=130)
        const sqlSuccessTop = `
        select anon, t,
               coalesce(duration_ms,(m->>'durationMs')::bigint) as dur,
               coalesce(scanner_usage,(m->>'scannerUsage')::boolean) as inter,
               coalesce(exit_code,(m->>'exit')::int) as exit,
               coalesce(out_bytes_bucket, m->>'cumulativeBytesBucket') as ob
        from telemetry_events
        where t >= $1 and t < $2 and evt='java.run.completed' and coalesce(exit_code,(m->>'exit')::int)=0
        order by inter desc nulls last, dur desc nulls last
        limit 20
      `;
        const sqlFrustratedTop = `
        select anon, t,
               coalesce(duration_ms,(m->>'durationMs')::bigint) as dur,
               coalesce(scanner_usage,(m->>'scannerUsage')::boolean) as inter,
               coalesce(exit_code,(m->>'exit')::int) as exit,
               coalesce(out_bytes_bucket, m->>'cumulativeBytesBucket') as ob
        from telemetry_events
        where t >= $1 and t < $2 and evt='java.run.completed' and coalesce(exit_code,(m->>'exit')::int)=130
        order by inter asc nulls last, dur asc nulls last
        limit 20
      `;
        trace.push({ sql: sqlSuccessTop, params: [from, to] }, { sql: sqlFrustratedTop, params: [from, to] });
        const [qSuccessTop, qFrustratedTop] = await Promise.all([
            client.query(sqlSuccessTop, [from, to]),
            client.query(sqlFrustratedTop, [from, to])
        ]);
        // Top exceptions
        const sqlTopEx = `
      select coalesce(exception_hash, m->>'exceptionHash','') as k, count(*)::int v
      from telemetry_events where t >= $1 and t < $2 and evt='java.run.error' and coalesce(exception_hash, m->>'exceptionHash') is not null
      group by 1 order by 2 desc limit 50
  `;
        trace.push({ sql: sqlTopEx, params: [from, to] });
        if (CONFIG.DEBUG_DB)
            log.debug('[db] stats.qTopEx', { sql: sqlTopEx, params: [from, to] });
        const qTopEx = await client.query(sqlTopEx, [from, to]);
        // OS daily series
        const sqlOsDaily = `
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
  `;
        trace.push({ sql: sqlOsDaily, params: [from, to] });
        if (CONFIG.DEBUG_DB)
            log.debug('[db] stats.qOsDaily', { sql: sqlOsDaily, params: [from, to] });
        const qOsDaily = await client.query(sqlOsDaily, [from, to]);
        // Geo by country
        const sqlCountry = `
      select coalesce(country,'Unknown') as k, count(*)::int as hits, count(distinct anon)::int as visitors
      from telemetry_events where t >= $1 and t < $2 group by 1 order by 2 desc
  `;
        trace.push({ sql: sqlCountry, params: [from, to] });
        if (CONFIG.DEBUG_DB)
            log.debug('[db] stats.qCountry', { sql: sqlCountry, params: [from, to] });
        const qCountry = await client.query(sqlCountry, [from, to]);
        // Assemble results
        const dailyDates = qDailyHits.rows.map(r => r.date);
        const dailyHits = qDailyHits.rows.map(r => Number(r.hits) || 0);
        const dailyUniques = qDailyHits.rows.map(r => Number(r.uniques) || 0);
        const hourly = arr24(qHourlyHits.rows);
        const hourlyVisitors = arr24(qHourlyVisitors.rows);
        const dow = arr7(qDow.rows);
        const started = Number(qStarted.rows[0]?.c || 0);
        const completed = Number(qCompleted.rows[0]?.c || 0);
        const compileErr = Number(qCompileErr.rows[0]?.c || 0);
        const runtimeErr = Number(qRuntimeErr.rows[0]?.c || 0);
        const durRow = qDur.rows[0] || { count: 0, median: 0, p90: 0, avgwait: 0 };
        const histRow = qDurHist.rows[0] || { b0: 0, b1: 0, b2: 0, b3: 0, b4: 0, b5: 0, b6: 0, b7: 0 };
        const durations = {
            count: Number(durRow.count || 0),
            median: Number(durRow.median || 0),
            p90: Number(durRow.p90 || 0),
            hist: { labels: ['0-500ms', '500-1000ms', '1000-3000ms', '3000-10000ms', '10000-30000ms', '30000-60000ms', '60000-120000ms', '≥120000ms'], values: [histRow.b0, histRow.b1, histRow.b2, histRow.b3, histRow.b4, histRow.b5, histRow.b6, histRow.b7].map(Number) },
            avgWaitMs: Number(durRow.avgwait || 0)
        };
        const objFrom = (rows) => Object.fromEntries(rows.map(r => [r.k, Number(r.v || r.c || 0)]));
        // OS daily series assemble
        const osSet = new Set();
        qOsDaily.rows.forEach(r => { if (r.os)
            osSet.add(r.os); });
        const osList = Array.from(osSet);
        const osSeries = osList.map(os => ({ key: os, data: dailyDates.map(d => {
                const row = qOsDaily.rows.find(r => r.date === d && r.os === os);
                return Number(row?.v || 0);
            }) }));
        const byCountry = {};
        qCountry.rows.forEach(r => { byCountry[r.k] = { hits: Number(r.hits || 0), visitors: Number(r.visitors || 0) }; });
        const byContinent = {};
        for (const [cc, v] of Object.entries(byCountry)) {
            const cont = continentOf(cc === 'Unknown' ? undefined : cc);
            if (!byContinent[cont])
                byContinent[cont] = { hits: 0, visitors: 0 };
            byContinent[cont].hits += v.hits;
            byContinent[cont].visitors += v.visitors;
        }
        const total = Number(totals.total || 0);
        const uniques = Number(totals.uniques || 0);
        // Assemble teacher-focused daily learning outcomes
        const dailyLearningOutcomes = {};
        for (const r of qDLO.rows) {
            const day = r.day;
            const code = String((r.exit ?? 0));
            const c = Number(r.c || 0);
            if (!dailyLearningOutcomes[day])
                dailyLearningOutcomes[day] = {};
            dailyLearningOutcomes[day][code] = (dailyLearningOutcomes[day][code] || 0) + c;
        }
        const mask = (anon) => (!anon || anon.length < 6) ? 'Student #—' : `Student #${anon.slice(-6)}`;
        const successTop = qSuccessTop.rows.map(r => ({
            student: mask(r.anon),
            ts: new Date(r.t).toISOString(),
            durationMs: Number(r.dur || 0),
            interactive: r.inter === true,
            exit: Number(r.exit || 0),
            outputBucket: r.ob || undefined
        }));
        const frustratedTop = qFrustratedTop.rows.map(r => ({
            student: mask(r.anon),
            ts: new Date(r.t).toISOString(),
            durationMs: Number(r.dur || 0),
            interactive: r.inter === true,
            exit: Number(r.exit || 130),
            outputBucket: r.ob || undefined
        }));
        const res = {
            from: dailyDates[0] || "All data",
            to: dailyDates[dailyDates.length - 1] || "All data",
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
            errors: { compile: compileErr, runtime: runtimeErr, compileRate: started ? compileErr / started : 0, runtimeRate: started ? runtimeErr / started : 0 },
            durations,
            exitCodes: objFrom(qExit.rows),
            outputBuckets: objFrom(qOut.rows),
            interactiveRate: completed ? Number(qInteractive.rows[0]?.c || 0) / completed : 0,
            truncationRate: completed ? Number(qTrunc.rows[0]?.c || 0) / completed : 0,
            topExceptions: objFrom(qTopEx.rows),
            geo: { byContinent, byCountry },
            activeSessions: Number(qActive.rows[0]?.c || 0),
            installsTotal: Number(qInstallsTotal.rows[0]?.c || 0),
            installsWindow: Number(qInstallsWindow.rows[0]?.c || 0),
            sessionsRecent: qRecentSessions.rows.map(r => ({
                id: r.session_id,
                student: r.anon || '',
                startedAt: r.started_at?.toISOString?.() || String(r.started_at),
                lastAt: r.last_at?.toISOString?.() || String(r.last_at),
                completed: !!r.completed,
                exit: Number(r.exit_code || -1),
                durationMs: Number(r.duration_ms || 0),
                interactive: !!r.interactive
            })),
            dailyLearningOutcomes,
            sessions: { successTop, frustratedTop },
            tables: {
                eventsTop: byEvent.rows.map((r) => { const v = Number(r.v); const p = total ? v / total : 0; return { key: r.k, event: r.k, count: v, pct: p, percent: Math.round(p * 1000) / 10 }; }).slice(0, 20),
                extTop: byExt.rows.map((r) => { const v = Number(r.v); const p = total ? v / total : 0; return { key: r.k, version: r.k, count: v, pct: p, percent: Math.round(p * 1000) / 10 }; }).slice(0, 20),
                vscodeTop: byVscode.rows.map((r) => { const v = Number(r.v); const p = total ? v / total : 0; return { key: r.k, version: r.k, count: v, pct: p, percent: Math.round(p * 1000) / 10 }; }).slice(0, 20),
                exitTop: qExit.rows.map((r) => { const v = Number(r.v); const p = total ? v / total : 0; return { key: r.k, code: r.k, count: v, pct: p, percent: Math.round(p * 1000) / 10 }; }).slice(0, 20),
                exceptionsTop: qTopEx.rows.map((r) => { const v = Number(r.v); const p = total ? v / total : 0; return { key: r.k, exceptionHash: r.k, count: v, pct: p, percent: Math.round(p * 1000) / 10 }; }).slice(0, 20),
            }
        };
        if (CONFIG.DEBUG_STATS_TRACE) {
            res._trace = trace;
            console.log('[stats] trace', JSON.stringify(trace));
        }
        if (CONFIG.DEBUG_DB)
            log.info('[db] stats.ok', { total, uniques, from: res.from, to: res.to });
        return res;
    }
    finally {
        client.release();
    }
}
export async function dbRecent(limit = 50) {
    if (!dbEnabled())
        return [];
    if (!pool)
        pool = buildPool();
    const sql = `
    select id, t, anon, evt, os, ext, vscode, country, region,
           duration_ms, wait_ms_total, exit_code, out_bytes_bucket, scanner_usage, truncated_output, error_phase, exception_hash
    from telemetry_events
    order by id desc
    limit $1
  `;
    if (CONFIG.DEBUG_DB)
        log.debug('[db] recent sql', { sql, limit });
    const { rows } = await pool.query(sql, [Math.max(1, Math.min(500, limit))]);
    return rows;
}
export async function dbCounts(hours = 24) {
    if (!dbEnabled())
        return {};
    if (!pool)
        pool = buildPool();
    const since = new Date(Date.now() - Math.max(1, hours) * 3600_000);
    const sql = `
    select evt, count(*)::int c from telemetry_events where t >= $1 group by 1 order by 2 desc
  `;
    if (CONFIG.DEBUG_DB)
        log.debug('[db] counts sql', { sql, since: since.toISOString() });
    const q = await pool.query(sql, [since]);
    const total = (await pool.query(`select count(*)::int c from telemetry_events where t >= $1`, [since])).rows[0]?.c || 0;
    return { since, total, byEvent: Object.fromEntries(q.rows.map(r => [r.evt, r.c])) };
}
export async function dbHealth() {
    try {
        if (!dbEnabled()) {
            return { enabled: false, ok: false, error: 'DB not configured (set DATABASE_URL or PGHOST/PGUSER/PGDATABASE)' };
        }
        if (!pool)
            pool = buildPool();
        const r = await pool.query('select current_database() as db, version() as version');
        return { enabled: true, ok: true, db: r.rows?.[0]?.db, version: r.rows?.[0]?.version };
    }
    catch (e) {
        log.error('[db] health error', { error: String(e?.message || e) });
        return { enabled: true, ok: false, error: String(e?.message || e) };
    }
}
