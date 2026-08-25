import cors from "cors";
import express from "express";
import pg from "pg";

const { Pool } = pg;

const PORT = Number(process.env.PORT || 8788);
const DATABASE_URL = process.env.DATABASE_URL || "";
const INGEST_API_KEY = process.env.PUMP_RADAR_INGEST_KEY || "";
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";

if (!DATABASE_URL) {
  console.warn("DATABASE_URL is not set. Snapshot storage will fail until Postgres is configured.");
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: DATABASE_URL.includes("localhost") ? false : { rejectUnauthorized: false },
});

const app = express();
app.use(cors({ origin: CORS_ORIGIN === "*" ? true : CORS_ORIGIN.split(",").map((item) => item.trim()) }));
app.use(express.json({ limit: "2mb" }));

let schemaReady = false;

app.get("/health", async (_request, response) => {
  const db = await checkDb();
  response.json({
    ok: Boolean(db.ok),
    service: "pump-radar-learning-api",
    db: db.ok ? "connected" : "unavailable",
    message: db.message,
    at: new Date().toISOString(),
  });
});

app.post("/api/v1/snapshots", requireIngestKey, async (request, response) => {
  await ensureSchema();
  const body = request.body || {};
  const items = Array.isArray(body.items) ? body.items.slice(0, 50) : [];
  if (!items.length) {
    response.status(400).json({ ok: false, error: "No snapshot items supplied." });
    return;
  }

  const source = cleanText(body.source || "extension", 80);
  const batchId = cleanText(body.batchId || crypto.randomUUID(), 80);
  const observedAt = new Date(Number(body.updatedAt || body.at || Date.now()));
  const client = await pool.connect();
  try {
    await client.query("begin");
    for (const item of items) {
      await client.query(
        `insert into pump_radar_snapshots
          (batch_id, observed_at, source, coin, rank, score, verdict, market_cap, price_change_5m, payload)
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
        [
          batchId,
          observedAt,
          source,
          cleanText(item.coin, 128),
          numberOrNull(item.rank),
          numberOrNull(item.score),
          cleanText(item.verdict, 80),
          numberOrNull(item.marketCap ?? item.metrics?.marketCapRaw),
          cleanText(item.priceChange5m ?? item.metrics?.priceChange5m, 40),
          item,
        ],
      );
    }
    await client.query("commit");
  } catch (error) {
    await client.query("rollback");
    throw error;
  } finally {
    client.release();
  }

  response.json({ ok: true, stored: items.length, batchId });
});

app.get("/api/v1/performance", async (_request, response) => {
  await ensureSchema();
  const { rows } = await pool.query(`
    with first_seen as (
      select distinct on (coin)
        coin,
        observed_at as first_seen_at,
        rank as first_rank,
        score as first_score,
        verdict as first_verdict,
        market_cap as entry_market_cap
      from pump_radar_snapshots
      where coin is not null and market_cap is not null and market_cap > 0
      order by coin, observed_at asc
    ),
    outcomes as (
      select
        f.coin,
        f.first_seen_at,
        f.first_rank,
        f.first_score,
        f.first_verdict,
        f.entry_market_cap,
        max(s.market_cap) as max_market_cap,
        min(s.market_cap) as min_market_cap,
        max(s.observed_at) as last_seen_at,
        count(*) as snapshots
      from first_seen f
      join pump_radar_snapshots s on s.coin = f.coin
      group by f.coin, f.first_seen_at, f.first_rank, f.first_score, f.first_verdict, f.entry_market_cap
    )
    select
      count(*)::int as tracked_coins,
      count(*) filter (where snapshots >= 3)::int as coins_with_followup,
      count(*) filter (where first_rank <= 10)::int as first_top10,
      count(*) filter (where entry_market_cap > 0 and ((max_market_cap - entry_market_cap) / entry_market_cap) * 100 >= 20)::int as hit_plus20,
      count(*) filter (where entry_market_cap > 0 and ((max_market_cap - entry_market_cap) / entry_market_cap) * 100 >= 50)::int as hit_plus50,
      count(*) filter (where entry_market_cap > 0 and ((min_market_cap - entry_market_cap) / entry_market_cap) * 100 <= -20)::int as dumped_minus20,
      max(last_seen_at) as latest_snapshot_at
    from outcomes
  `);
  const recent = await pool.query(`
    select observed_at, coin, rank, score, verdict, market_cap, price_change_5m
    from pump_radar_snapshots
    order by observed_at desc, rank asc nulls last
    limit 25
  `);
  response.json({ ok: true, summary: rows[0] || {}, recent: recent.rows });
});

app.use((error, _request, response, _next) => {
  console.error(error);
  response.status(500).json({ ok: false, error: error.message || "Server error." });
});

app.listen(PORT, () => {
  console.log(`Pump Radar learning API listening on ${PORT}`);
});

function requireIngestKey(request, response, next) {
  if (!INGEST_API_KEY) {
    response.status(500).json({ ok: false, error: "PUMP_RADAR_INGEST_KEY is not configured on the server." });
    return;
  }
  const supplied = request.get("x-pump-radar-key") || "";
  if (supplied !== INGEST_API_KEY) {
    response.status(401).json({ ok: false, error: "Invalid ingest key." });
    return;
  }
  next();
}

async function ensureSchema() {
  if (schemaReady) return;
  await pool.query(`
    create table if not exists pump_radar_snapshots (
      id bigserial primary key,
      batch_id text not null,
      observed_at timestamptz not null,
      received_at timestamptz not null default now(),
      source text not null,
      coin text,
      rank integer,
      score numeric,
      verdict text,
      market_cap numeric,
      price_change_5m text,
      payload jsonb not null
    );
  `);
  await pool.query("create index if not exists pump_radar_snapshots_coin_at_idx on pump_radar_snapshots (coin, observed_at desc)");
  await pool.query("create index if not exists pump_radar_snapshots_at_idx on pump_radar_snapshots (observed_at desc)");
  schemaReady = true;
}

async function checkDb() {
  try {
    await pool.query("select 1");
    return { ok: true, message: "Postgres connected." };
  } catch (error) {
    return { ok: false, message: error.message };
  }
}

function numberOrNull(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function cleanText(value, maxLength) {
  const text = String(value || "").trim();
  return text ? text.slice(0, maxLength) : null;
}
