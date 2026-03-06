use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::{anyhow, Result};
use redis::AsyncCommands;
use serde::Serialize;
use serde_json::json;
use tokio::sync::mpsc;

use crate::models::{RelayTickRow, RoundRow, SnapshotRow};

#[derive(Debug, Clone)]
pub struct DbSinkConfig {
    pub clickhouse_url: Option<String>,
    pub clickhouse_database: String,
    pub clickhouse_snapshot_table: String,
    pub clickhouse_processed_table: String,
    pub clickhouse_round_table: String,
    pub clickhouse_snapshot_ttl_days: u16,
    pub clickhouse_processed_ttl_days: u16,
    pub clickhouse_round_ttl_days: u16,
    pub redis_url: Option<String>,
    pub redis_prefix: String,
    pub redis_ttl_sec: u64,
    pub batch_size: usize,
    pub flush_ms: u64,
    pub queue_cap: usize,
}

#[derive(Debug, Clone)]
pub enum DbEvent {
    Snapshot(Box<SnapshotRow>),
    RelayTick(Box<RelayTickRow>),
    Round(RoundRow),
}

pub fn normalize_opt_url(raw: &str) -> Option<String> {
    let v = raw.trim();
    if v.is_empty() {
        None
    } else {
        Some(v.to_string())
    }
}

// ------------------------------------------------------------------ //
// 统一 Redis payload 结构 — 消灭 RedisSnapshotPayload / RedisSnapshotPayloadOwned 的冗余二元
// ------------------------------------------------------------------ //
#[derive(Debug, Clone, Serialize)]
struct RedisSnapshot {
    schema_version: String,
    ts_ireland_sample_ms: i64,
    ts_tokyo_recv_ms: Option<i64>,
    ts_tokyo_send_ms: Option<i64>,
    ts_exchange_ms: Option<i64>,
    ts_ireland_recv_ms: Option<i64>,
    tokyo_relay_proc_lag_ms: Option<f64>,
    cross_region_net_lag_ms: Option<f64>,
    path_lag_ms: Option<f64>,
    symbol: String,
    timeframe: String,
    market_id: String,
    round_id: String,
    title: String,
    target_price: Option<f64>,
    binance_price: Option<f64>,
    pm_live_btc_price: Option<f64>,
    chainlink_price: Option<f64>,
    mid_yes: f64,
    mid_no: f64,
    mid_yes_smooth: f64,
    mid_no_smooth: f64,
    bid_yes: f64,
    ask_yes: f64,
    bid_no: f64,
    ask_no: f64,
    delta_price: Option<f64>,
    delta_pct: Option<f64>,
    delta_pct_smooth: Option<f64>,
    remaining_ms: i64,
    velocity_bps_per_sec: Option<f64>,
    acceleration: Option<f64>,
}

impl RedisSnapshot {
    fn from_row(row: &SnapshotRow) -> Self {
        Self {
            schema_version: row.schema_version.to_string(),
            ts_ireland_sample_ms: row.ts_ireland_sample_ms,
            ts_tokyo_recv_ms: row.ts_tokyo_recv_ms,
            ts_tokyo_send_ms: row.ts_tokyo_send_ms,
            ts_exchange_ms: row.ts_exchange_ms,
            ts_ireland_recv_ms: row.ts_ireland_recv_ms,
            tokyo_relay_proc_lag_ms: row.tokyo_relay_proc_lag_ms,
            cross_region_net_lag_ms: row.cross_region_net_lag_ms,
            path_lag_ms: row.path_lag_ms,
            symbol: row.symbol.clone(),
            timeframe: row.timeframe.clone(),
            market_id: row.market_id.clone(),
            round_id: row.round_id.clone(),
            title: row.title.clone(),
            target_price: row.target_price,
            binance_price: row.binance_price,
            pm_live_btc_price: row.pm_live_btc_price,
            chainlink_price: row.chainlink_price,
            mid_yes: row.mid_yes,
            mid_no: row.mid_no,
            mid_yes_smooth: row.mid_yes_smooth,
            mid_no_smooth: row.mid_no_smooth,
            bid_yes: row.bid_yes,
            ask_yes: row.ask_yes,
            bid_no: row.bid_no,
            ask_no: row.ask_no,
            delta_price: row.delta_price,
            delta_pct: row.delta_pct,
            delta_pct_smooth: row.delta_pct_smooth,
            remaining_ms: row.remaining_ms,
            velocity_bps_per_sec: row.velocity_bps_per_sec,
            acceleration: row.acceleration,
        }
    }
}

// ------------------------------------------------------------------ //
// Redis 最新快照聚合状态 — by_symbol_tf 和 by_market 共用同一结构
// ------------------------------------------------------------------ //
#[derive(Debug, Default)]
struct RedisLatestState {
    by_symbol_tf: HashMap<(String, String), RedisSnapshot>,
    by_market: HashMap<(String, String, String), RedisSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
struct RedisSnapshotEvent {
    event: &'static str,
    symbol: String,
    timeframe: String,
    market_id: String,
    round_id: String,
    ts_ireland_sample_ms: i64,
    remaining_ms: i64,
    mid_yes: f64,
    mid_yes_smooth: f64,
    bid_yes: f64,
    ask_yes: f64,
    bid_no: f64,
    ask_no: f64,
    delta_pct: Option<f64>,
    delta_pct_smooth: Option<f64>,
    velocity_bps_per_sec: Option<f64>,
    acceleration: Option<f64>,
    source: &'static str,
}

pub async fn run_db_sink(cfg: DbSinkConfig, mut rx: mpsc::Receiver<DbEvent>) {
    if let Some(ch_url) = cfg.clickhouse_url.as_deref() {
        if let Err(err) = init_clickhouse(ch_url, &cfg).await {
            tracing::warn!(?err, "clickhouse init failed; continue");
        }
    }

    // Redis 连接在此提升为持久连接，整个 sink 生命周期内复用
    let redis_conn = if let Some(redis_url) = cfg.redis_url.as_deref() {
        match redis::Client::open(redis_url).map_err(|e| anyhow!("{e}")) {
            Ok(client) => match client.get_multiplexed_tokio_connection().await {
                Ok(conn) => {
                    tracing::info!("redis persistent connection established");
                    Some(conn)
                }
                Err(err) => {
                    tracing::warn!(?err, "redis connect failed; redis flush disabled");
                    None
                }
            },
            Err(err) => {
                tracing::warn!(?err, "redis client open failed");
                None
            }
        }
    } else {
        None
    };

    let mut flush_tick = tokio::time::interval(Duration::from_millis(cfg.flush_ms.max(100)));
    let mut snapshot_batch = Vec::<SnapshotRow>::with_capacity(cfg.batch_size.saturating_mul(2));
    let mut relay_tick_batch = Vec::<RelayTickRow>::with_capacity(cfg.batch_size.saturating_mul(2));
    let mut round_batch = Vec::<RoundRow>::with_capacity(cfg.batch_size.saturating_mul(2));
    let mut redis_state = RedisLatestState::default();
    let mut conn = redis_conn;

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(DbEvent::Snapshot(row)) => {
                        if let Some(clean) = sanitize_snapshot_row(*row) {
                            snapshot_batch.push(clean);
                        }
                    }
                    Some(DbEvent::RelayTick(row)) => {
                        relay_tick_batch.push(*row);
                    }
                    Some(DbEvent::Round(row)) => {
                        if sanitize_round_row(&row) {
                            round_batch.push(row);
                        }
                    }
                    None => {
                        flush_batches(&cfg, &mut snapshot_batch, &mut relay_tick_batch, &mut round_batch, &mut redis_state, &mut conn).await;
                        break;
                    }
                }

                if snapshot_batch.len() >= cfg.batch_size.max(10)
                    || relay_tick_batch.len() >= cfg.batch_size.max(10)
                    || round_batch.len() >= cfg.batch_size.max(10)
                {
                    flush_batches(&cfg, &mut snapshot_batch, &mut relay_tick_batch, &mut round_batch, &mut redis_state, &mut conn).await;
                }
            }
            _ = flush_tick.tick() => {
                if !snapshot_batch.is_empty() || !relay_tick_batch.is_empty() || !round_batch.is_empty() {
                    flush_batches(&cfg, &mut snapshot_batch, &mut relay_tick_batch, &mut round_batch, &mut redis_state, &mut conn).await;
                }
            }
        }
    }
}

fn redis_snapshot_ring_max() -> i64 {
    std::env::var("FORGE_SNAPSHOT_REDIS_RING_MAX")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(20_000)
        .clamp(2_000, 500_000)
}

async fn flush_batches(
    cfg: &DbSinkConfig,
    snapshot_batch: &mut Vec<SnapshotRow>,
    relay_tick_batch: &mut Vec<RelayTickRow>,
    round_batch: &mut Vec<RoundRow>,
    redis_state: &mut RedisLatestState,
    redis_conn: &mut Option<redis::aio::MultiplexedConnection>,
) {
    if snapshot_batch.is_empty() && relay_tick_batch.is_empty() && round_batch.is_empty() {
        return;
    }

    let mut snapshots = Vec::new();
    std::mem::swap(&mut snapshots, snapshot_batch);

    let mut relay_ticks = Vec::new();
    std::mem::swap(&mut relay_ticks, relay_tick_batch);

    let mut rounds = Vec::new();
    std::mem::swap(&mut rounds, round_batch);

    if let Some(ch_url) = cfg.clickhouse_url.as_deref() {
        if !relay_ticks.is_empty() {
            if let Err(err) = flush_clickhouse_relay_ticks(ch_url, cfg, &relay_ticks).await {
                tracing::warn!(
                    ?err,
                    rows = relay_ticks.len(),
                    "clickhouse relay tick flush failed"
                );
            }
        }
        if !snapshots.is_empty() {
            if let Err(err) = flush_clickhouse_snapshots(ch_url, cfg, &snapshots).await {
                tracing::warn!(
                    ?err,
                    rows = snapshots.len(),
                    "clickhouse snapshot flush failed"
                );
            }
        }
        if !rounds.is_empty() {
            if let Err(err) = flush_clickhouse_rounds(ch_url, cfg, &rounds).await {
                tracing::warn!(?err, rows = rounds.len(), "clickhouse round flush failed");
            }
        }
    }

    if let Some(conn) = redis_conn {
        if !snapshots.is_empty() {
            if let Err(err) = flush_redis_latest(cfg, &snapshots, redis_state, conn).await {
                tracing::warn!(?err, rows = snapshots.len(), "redis flush failed");
            }
        }
    }
}

fn sanitize_optional_f64(v: Option<f64>) -> Option<f64> {
    v.filter(|x| x.is_finite())
}

fn sanitize_snapshot_row(mut row: SnapshotRow) -> Option<SnapshotRow> {
    if !(row.mid_yes.is_finite()
        && row.mid_no.is_finite()
        && row.mid_yes_smooth.is_finite()
        && row.mid_no_smooth.is_finite()
        && row.bid_yes.is_finite()
        && row.ask_yes.is_finite()
        && row.bid_no.is_finite()
        && row.ask_no.is_finite()
        && row.bid_size_yes.is_finite()
        && row.ask_size_yes.is_finite()
        && row.bid_size_no.is_finite()
        && row.ask_size_no.is_finite())
    {
        return None;
    }
    row.tokyo_relay_proc_lag_ms = sanitize_optional_f64(row.tokyo_relay_proc_lag_ms);
    row.cross_region_net_lag_ms = sanitize_optional_f64(row.cross_region_net_lag_ms);
    row.path_lag_ms = sanitize_optional_f64(row.path_lag_ms);
    row.target_price = sanitize_optional_f64(row.target_price);
    row.binance_price = sanitize_optional_f64(row.binance_price);
    row.pm_live_btc_price = sanitize_optional_f64(row.pm_live_btc_price);
    row.chainlink_price = sanitize_optional_f64(row.chainlink_price);
    row.delta_price = sanitize_optional_f64(row.delta_price);
    row.delta_pct = sanitize_optional_f64(row.delta_pct);
    row.delta_pct_smooth = sanitize_optional_f64(row.delta_pct_smooth);
    row.velocity_bps_per_sec = sanitize_optional_f64(row.velocity_bps_per_sec);
    row.acceleration = sanitize_optional_f64(row.acceleration);
    Some(row)
}

fn sanitize_round_row(row: &RoundRow) -> bool {
    row.target_price.is_finite()
        && row.target_price > 0.0
        && row.settle_price.is_finite()
        && row.settle_price > 0.0
}

async fn execute_clickhouse_query(ch_url: &str, sql: &str) -> Result<()> {
    let resp = reqwest::Client::new()
        .post(ch_url)
        .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(sql.to_string())
        .send()
        .await?;
    if resp.status().is_success() {
        return Ok(());
    }
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    Err(anyhow!(
        "clickhouse query failed status={} body={}",
        status,
        body
    ))
}

async fn init_clickhouse(ch_url: &str, cfg: &DbSinkConfig) -> Result<()> {
    let db = &cfg.clickhouse_database;
    let snapshot_tbl = &cfg.clickhouse_snapshot_table;
    let processed_tbl = &cfg.clickhouse_processed_table;
    let round_tbl = &cfg.clickhouse_round_table;
    let relay_tick_tbl = "relay_tick_raw";
    let snapshot_ttl_days = cfg.clickhouse_snapshot_ttl_days.max(1);
    let processed_ttl_days = cfg.clickhouse_processed_ttl_days.max(1);
    let round_ttl_days = cfg.clickhouse_round_ttl_days.max(1);
    let snapshot_ttl_expr = format!(
        "fromUnixTimestamp64Milli(ts_ireland_sample_ms) + INTERVAL {} DAY",
        snapshot_ttl_days
    );
    let processed_ttl_expr = format!(
        "fromUnixTimestamp64Milli(ts_ireland_sample_ms) + INTERVAL {} DAY",
        processed_ttl_days
    );
    let round_ttl_expr = format!(
        "fromUnixTimestamp64Milli(ts_recorded_ms) + INTERVAL {} DAY",
        round_ttl_days
    );
    let relay_tick_ttl_expr = format!(
        "fromUnixTimestamp64Milli(ts_ireland_recv_ms) + INTERVAL {} DAY",
        snapshot_ttl_days
    );

    execute_clickhouse_query(ch_url, &format!("CREATE DATABASE IF NOT EXISTS {}", db)).await?;

    let create_snapshot_tbl = format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
            schema_version String,
            ingest_seq UInt64,
            ts_ireland_sample_ms Int64,
            ts_tokyo_recv_ms Nullable(Int64),
            ts_tokyo_send_ms Nullable(Int64),
            ts_exchange_ms Nullable(Int64),
            ts_ireland_recv_ms Nullable(Int64),
            tokyo_relay_proc_lag_ms Nullable(Float64),
            cross_region_net_lag_ms Nullable(Float64),
            path_lag_ms Nullable(Float64),
            symbol LowCardinality(String),
            ts_pm_recv_ms Int64,
            market_id String,
            timeframe LowCardinality(String),
            title String,
            target_price Nullable(Float64),
            mid_yes Float64,
            mid_no Float64,
            mid_yes_smooth Float64,
            mid_no_smooth Float64,
            bid_yes Float64,
            ask_yes Float64,
            bid_no Float64,
            ask_no Float64,
            bid_size_yes Float64,
            ask_size_yes Float64,
            bid_size_no Float64,
            ask_size_no Float64,
            binance_price Nullable(Float64),
            pm_live_btc_price Nullable(Float64),
            ts_pm_live_exchange_ms Nullable(Int64),
            ts_pm_live_recv_ms Nullable(Int64),
            chainlink_price Nullable(Float64),
            ts_chainlink_exchange_ms Nullable(Int64),
            ts_chainlink_recv_ms Nullable(Int64),
            delta_price Nullable(Float64),
            delta_pct Nullable(Float64),
            delta_pct_smooth Nullable(Float64),
            remaining_ms Int64,
            velocity_bps_per_sec Nullable(Float64),
            acceleration Nullable(Float64),
            round_id String,
            ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
        )
        ENGINE = MergeTree
        PARTITION BY toDate(fromUnixTimestamp64Milli(ts_ireland_sample_ms))
        ORDER BY (symbol, timeframe, market_id, ts_ireland_sample_ms)
        TTL {} DELETE
        SETTINGS index_granularity = 8192",
        db, snapshot_tbl, snapshot_ttl_expr
    );
    execute_clickhouse_query(ch_url, &create_snapshot_tbl).await?;

    let alter_snapshot_queries = [
        format!(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS ts_tokyo_send_ms Nullable(Int64) AFTER ts_tokyo_recv_ms",
            db, snapshot_tbl
        ),
        format!(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS tokyo_relay_proc_lag_ms Nullable(Float64) AFTER ts_ireland_recv_ms",
            db, snapshot_tbl
        ),
        format!(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS cross_region_net_lag_ms Nullable(Float64) AFTER tokyo_relay_proc_lag_ms",
            db, snapshot_tbl
        ),
        format!(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS pm_live_btc_price Nullable(Float64) AFTER binance_price",
            db, snapshot_tbl
        ),
        format!(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS ts_pm_live_exchange_ms Nullable(Int64) AFTER pm_live_btc_price",
            db, snapshot_tbl
        ),
        format!(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS ts_pm_live_recv_ms Nullable(Int64) AFTER ts_pm_live_exchange_ms",
            db, snapshot_tbl
        ),
        format!(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS mid_yes_smooth Float64 DEFAULT mid_yes AFTER mid_no",
            db, snapshot_tbl
        ),
        format!(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS mid_no_smooth Float64 DEFAULT mid_no AFTER mid_yes_smooth",
            db, snapshot_tbl
        ),
        format!(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS delta_pct_smooth Nullable(Float64) AFTER delta_pct",
            db, snapshot_tbl
        ),
        format!(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS bid_size_yes Float64 DEFAULT 0.0 AFTER ask_no",
            db, snapshot_tbl
        ),
        format!(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS ask_size_yes Float64 DEFAULT 0.0 AFTER bid_size_yes",
            db, snapshot_tbl
        ),
        format!(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS bid_size_no Float64 DEFAULT 0.0 AFTER ask_size_yes",
            db, snapshot_tbl
        ),
        format!(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS ask_size_no Float64 DEFAULT 0.0 AFTER bid_size_no",
            db, snapshot_tbl
        ),
        format!(
            "ALTER TABLE {}.{} MODIFY TTL {}",
            db, snapshot_tbl, snapshot_ttl_expr
        ),
    ];
    for q in alter_snapshot_queries {
        execute_clickhouse_query(ch_url, &q).await?;
    }

    let create_processed_tbl = format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
            schema_version String,
            ingest_seq UInt64,
            ts_ireland_sample_ms Int64,
            symbol LowCardinality(String),
            timeframe LowCardinality(String),
            market_id String,
            round_id String,
            title String,
            target_price Nullable(Float64),
            binance_price Nullable(Float64),
            pm_live_btc_price Nullable(Float64),
            chainlink_price Nullable(Float64),
            remaining_ms Int64,
            raw_up_prob Float64,
            raw_down_prob Float64,
            smooth_up_prob Float64,
            smooth_down_prob Float64,
            display_up_cents Float64,
            display_down_cents Float64,
            raw_delta_pct Nullable(Float64),
            smooth_delta_pct Nullable(Float64),
            velocity_bps_per_sec Nullable(Float64),
            acceleration Nullable(Float64),
            ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
        )
        ENGINE = MergeTree
        PARTITION BY toDate(fromUnixTimestamp64Milli(ts_ireland_sample_ms))
        ORDER BY (symbol, timeframe, round_id, ts_ireland_sample_ms)
        TTL {} DELETE
        SETTINGS index_granularity = 8192",
        db, processed_tbl, processed_ttl_expr
    );
    execute_clickhouse_query(ch_url, &create_processed_tbl).await?;
    execute_clickhouse_query(
        ch_url,
        &format!(
            "ALTER TABLE {}.{} MODIFY TTL {}",
            db, processed_tbl, processed_ttl_expr
        ),
    )
    .await?;

    let mv_name = format!("mv_{}_to_{}", snapshot_tbl, processed_tbl);
    let create_processed_mv = format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {}.{}
        TO {}.{}
        AS
        SELECT
            schema_version,
            ingest_seq,
            ts_ireland_sample_ms,
            symbol,
            timeframe,
            market_id,
            round_id,
            title,
            target_price,
            binance_price,
            pm_live_btc_price,
            chainlink_price,
            remaining_ms,
            mid_yes AS raw_up_prob,
            mid_no AS raw_down_prob,
            mid_yes_smooth AS smooth_up_prob,
            mid_no_smooth AS smooth_down_prob,
            mid_yes_smooth * 100.0 AS display_up_cents,
            (1.0 - mid_yes_smooth) * 100.0 AS display_down_cents,
            delta_pct AS raw_delta_pct,
            delta_pct_smooth AS smooth_delta_pct,
            velocity_bps_per_sec,
            acceleration
        FROM {}.{}",
        db, mv_name, db, processed_tbl, db, snapshot_tbl
    );
    execute_clickhouse_query(ch_url, &create_processed_mv).await?;

    let create_relay_tick_tbl = format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
            ts_ireland_recv_ms Int64,
            symbol LowCardinality(String),
            source LowCardinality(String),
            relay_seq UInt64,
            source_seq UInt64,
            ts_tokyo_recv_ms Int64,
            ts_tokyo_send_ms Int64,
            ts_exchange_ms Int64,
            binance_price Float64,
            checksum String,
            checksum_ok UInt8,
            ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
        )
        ENGINE = MergeTree
        PARTITION BY toDate(fromUnixTimestamp64Milli(ts_ireland_recv_ms))
        ORDER BY (symbol, relay_seq, ts_ireland_recv_ms)
        TTL {} DELETE
        SETTINGS index_granularity = 8192",
        db, relay_tick_tbl, relay_tick_ttl_expr
    );
    execute_clickhouse_query(ch_url, &create_relay_tick_tbl).await?;
    execute_clickhouse_query(
        ch_url,
        &format!(
            "ALTER TABLE {}.{} MODIFY TTL {}",
            db, relay_tick_tbl, relay_tick_ttl_expr
        ),
    )
    .await?;

    let create_round_tbl = format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
            round_id String,
            market_id String,
            symbol LowCardinality(String),
            timeframe LowCardinality(String),
            title String,
            start_ts_ms Int64,
            end_ts_ms Int64,
            target_price Float64,
            settle_price Float64,
            label_up UInt8,
            ts_recorded_ms Int64,
            ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
        )
        ENGINE = MergeTree
        PARTITION BY toDate(fromUnixTimestamp64Milli(end_ts_ms))
        ORDER BY (symbol, timeframe, end_ts_ms, market_id)
        TTL {} DELETE
        SETTINGS index_granularity = 8192",
        db, round_tbl, round_ttl_expr
    );
    execute_clickhouse_query(ch_url, &create_round_tbl).await?;
    execute_clickhouse_query(
        ch_url,
        &format!(
            "ALTER TABLE {}.{} MODIFY TTL {}",
            db, round_tbl, round_ttl_expr
        ),
    )
    .await?;

    Ok(())
}

async fn flush_clickhouse_snapshots(
    ch_url: &str,
    cfg: &DbSinkConfig,
    rows: &[SnapshotRow],
) -> Result<()> {
    let db = &cfg.clickhouse_database;
    let tbl = &cfg.clickhouse_snapshot_table;
    let query = format!("INSERT INTO {}.{} FORMAT JSONEachRow", db, tbl);

    let mut body = String::with_capacity(rows.len().saturating_mul(1400));
    for row in rows {
        let line = serde_json::to_string(row)?;
        body.push_str(&line);
        body.push('\n');
    }

    let resp = reqwest::Client::new()
        .post(ch_url)
        .query(&[
            ("query", query.as_str()),
            ("async_insert", "1"),
            ("wait_for_async_insert", "1"),
        ])
        .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
        .body(body.clone())
        .send()
        .await?;

    if resp.status().is_success() {
        return Ok(());
    }

    let status = resp.status();
    let body_err = resp.text().await.unwrap_or_default();
    if body_err.contains("UNKNOWN_DATABASE")
        || body_err.contains("UNKNOWN_TABLE")
        || body_err.contains("UNKNOWN_IDENTIFIER")
    {
        init_clickhouse(ch_url, cfg).await?;
        let resp2 = reqwest::Client::new()
            .post(ch_url)
            .query(&[
                ("query", query.as_str()),
                ("async_insert", "1"),
                ("wait_for_async_insert", "1"),
            ])
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .body(body)
            .send()
            .await?;
        if resp2.status().is_success() {
            return Ok(());
        }
        let st2 = resp2.status();
        let b2 = resp2.text().await.unwrap_or_default();
        return Err(anyhow!(
            "clickhouse insert retry failed status={} body={}",
            st2,
            b2
        ));
    }

    Err(anyhow!(
        "clickhouse insert failed status={} body={}",
        status,
        body_err
    ))
}

#[derive(Debug, Clone, Serialize)]
struct RoundInsertRow<'a> {
    round_id: &'a str,
    market_id: &'a str,
    symbol: &'a str,
    timeframe: &'a str,
    title: &'a str,
    start_ts_ms: i64,
    end_ts_ms: i64,
    target_price: f64,
    settle_price: f64,
    label_up: u8,
    ts_recorded_ms: i64,
}

async fn flush_clickhouse_rounds(
    ch_url: &str,
    cfg: &DbSinkConfig,
    rows: &[RoundRow],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let db = &cfg.clickhouse_database;
    let tbl = &cfg.clickhouse_round_table;

    let mut unique_rows = Vec::<&RoundRow>::with_capacity(rows.len());
    let mut seen = HashSet::<&str>::with_capacity(rows.len());
    for row in rows {
        if seen.insert(row.round_id.as_str()) {
            unique_rows.push(row);
        }
    }
    if unique_rows.is_empty() {
        return Ok(());
    }

    let mut existing_round_ids = HashSet::<String>::new();
    let id_sql = unique_rows
        .iter()
        .map(|r| format!("'{}'", r.round_id.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(",");
    let existing_query = format!(
        "SELECT round_id FROM {}.{} WHERE round_id IN ({}) FORMAT JSON",
        db, tbl, id_sql
    );
    let existing_resp = reqwest::Client::new()
        .post(ch_url)
        .query(&[("query", existing_query.as_str())])
        .send()
        .await?;
    if existing_resp.status().is_success() {
        let payload: serde_json::Value = existing_resp.json().await?;
        if let Some(arr) = payload.get("data").and_then(|v| v.as_array()) {
            for row in arr {
                if let Some(id) = row.get("round_id").and_then(|v| v.as_str()) {
                    existing_round_ids.insert(id.to_string());
                }
            }
        }
    }
    unique_rows.retain(|r| !existing_round_ids.contains(&r.round_id));
    if unique_rows.is_empty() {
        return Ok(());
    }

    let query = format!("INSERT INTO {}.{} FORMAT JSONEachRow", db, tbl);

    let mut body = String::with_capacity(unique_rows.len().saturating_mul(400));
    for row in unique_rows {
        let line = serde_json::to_string(&RoundInsertRow {
            round_id: &row.round_id,
            market_id: &row.market_id,
            symbol: &row.symbol,
            timeframe: &row.timeframe,
            title: &row.title,
            start_ts_ms: row.start_ts_ms,
            end_ts_ms: row.end_ts_ms,
            target_price: row.target_price,
            settle_price: row.settle_price,
            label_up: u8::from(row.label_up),
            ts_recorded_ms: row.ts_recorded_ms,
        })?;
        body.push_str(&line);
        body.push('\n');
    }

    let resp = reqwest::Client::new()
        .post(ch_url)
        .query(&[
            ("query", query.as_str()),
            ("async_insert", "1"),
            ("wait_for_async_insert", "1"),
        ])
        .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
        .body(body.clone())
        .send()
        .await?;

    if resp.status().is_success() {
        return Ok(());
    }

    let status = resp.status();
    let body_err = resp.text().await.unwrap_or_default();
    if body_err.contains("UNKNOWN_DATABASE")
        || body_err.contains("UNKNOWN_TABLE")
        || body_err.contains("UNKNOWN_IDENTIFIER")
    {
        init_clickhouse(ch_url, cfg).await?;
        let resp2 = reqwest::Client::new()
            .post(ch_url)
            .query(&[
                ("query", query.as_str()),
                ("async_insert", "1"),
                ("wait_for_async_insert", "1"),
            ])
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .body(body)
            .send()
            .await?;
        if resp2.status().is_success() {
            return Ok(());
        }
        let st2 = resp2.status();
        let b2 = resp2.text().await.unwrap_or_default();
        return Err(anyhow!(
            "clickhouse insert retry failed status={} body={}",
            st2,
            b2
        ));
    }

    Err(anyhow!(
        "clickhouse insert failed status={} body={}",
        status,
        body_err
    ))
}

async fn flush_clickhouse_relay_ticks(
    ch_url: &str,
    cfg: &DbSinkConfig,
    rows: &[RelayTickRow],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let db = &cfg.clickhouse_database;
    let query = format!("INSERT INTO {}.relay_tick_raw FORMAT JSONEachRow", db);

    let mut body = String::with_capacity(rows.len().saturating_mul(256));
    for row in rows {
        let line = serde_json::to_string(row)?;
        body.push_str(&line);
        body.push('\n');
    }

    let resp = reqwest::Client::new()
        .post(ch_url)
        .query(&[
            ("query", query.as_str()),
            ("async_insert", "1"),
            ("wait_for_async_insert", "1"),
        ])
        .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
        .body(body)
        .send()
        .await?;

    if resp.status().is_success() {
        return Ok(());
    }

    let status = resp.status();
    let body_err = resp.text().await.unwrap_or_default();
    Err(anyhow!(
        "clickhouse relay tick insert failed status={} body={}",
        status,
        body_err
    ))
}

async fn flush_redis_latest(
    cfg: &DbSinkConfig,
    rows: &[SnapshotRow],
    state: &mut RedisLatestState,
    conn: &mut redis::aio::MultiplexedConnection,
) -> Result<()> {
    // 更新聚合索引
    let mut changed_markets = HashSet::<(String, String, String)>::new();
    for row in rows {
        let snap = RedisSnapshot::from_row(row);
        changed_markets.insert((
            snap.symbol.clone(),
            snap.timeframe.clone(),
            snap.market_id.clone(),
        ));
        state
            .by_symbol_tf
            .insert((snap.symbol.clone(), snap.timeframe.clone()), snap.clone());
        state.by_market.insert(
            (
                snap.symbol.clone(),
                snap.timeframe.clone(),
                snap.market_id.clone(),
            ),
            snap,
        );
    }

    // 过期清理
    let ttl_ms = (cfg.redis_ttl_sec.min(i64::MAX as u64) as i64).saturating_mul(1000);
    let cutoff_ms = chrono::Utc::now().timestamp_millis().saturating_sub(ttl_ms);
    state
        .by_symbol_tf
        .retain(|_, s| s.ts_ireland_sample_ms >= cutoff_ms);
    state
        .by_market
        .retain(|_, s| s.ts_ireland_sample_ms >= cutoff_ms);

    // symbol+tf 级别最新快照
    for ((symbol, timeframe), snap) in &state.by_symbol_tf {
        let key = format!(
            "{}:snapshot:latest:{}:{}",
            cfg.redis_prefix, symbol, timeframe
        );
        let payload = serde_json::to_string(snap)?;
        let _: () = conn.set_ex(key, payload, cfg.redis_ttl_sec).await?;
    }

    // market 级别最新快照
    for ((symbol, timeframe, market_id), snap) in &state.by_market {
        let key = format!(
            "{}:snapshot:latest:{}:{}:{}",
            cfg.redis_prefix, symbol, timeframe, market_id
        );
        let payload = serde_json::to_string(snap)?;
        let _: () = conn.set_ex(key, payload, cfg.redis_ttl_sec).await?;
    }

    // 全量 latest:all — 按时间倒序
    let mut latest_all: Vec<&RedisSnapshot> = state.by_market.values().collect();
    latest_all.sort_by(|a, b| {
        b.ts_ireland_sample_ms
            .cmp(&a.ts_ireland_sample_ms)
            .then(a.symbol.cmp(&b.symbol))
            .then(a.timeframe.cmp(&b.timeframe))
            .then(a.market_id.cmp(&b.market_id))
    });

    let all_key = format!("{}:snapshot:latest:all", cfg.redis_prefix);
    let all_payload = serde_json::to_string(&latest_all)?;
    let _: () = conn.set_ex(all_key, all_payload, cfg.redis_ttl_sec).await?;

    // 按 timeframe 分组
    let mut by_tf: HashMap<&str, Vec<&RedisSnapshot>> = HashMap::new();
    for snap in &latest_all {
        by_tf.entry(snap.timeframe.as_str()).or_default().push(snap);
    }
    for (tf, payload_rows) in by_tf {
        let key = format!("{}:snapshot:latest:tf:{}", cfg.redis_prefix, tf);
        let payload = serde_json::to_string(&payload_rows)?;
        let _: () = conn.set_ex(key, payload, cfg.redis_ttl_sec).await?;
    }

    let updated_key = format!("{}:snapshot:latest:updated_ms", cfg.redis_prefix);
    let _: () = conn
        .set_ex(
            updated_key,
            chrono::Utc::now().timestamp_millis().to_string(),
            cfg.redis_ttl_sec,
        )
        .await?;

    let ring_max = redis_snapshot_ring_max();
    let mut ring_batch: HashMap<(String, String), Vec<String>> = HashMap::new();
    for row in rows {
        let snap = RedisSnapshot::from_row(row);
        let payload = serde_json::to_string(&snap)?;
        ring_batch
            .entry((snap.symbol, snap.timeframe))
            .or_default()
            .push(payload);
    }
    for ((symbol, timeframe), payloads) in ring_batch {
        if payloads.is_empty() {
            continue;
        }
        let ring_key = format!(
            "{}:snapshot:ring:{}:{}",
            cfg.redis_prefix, symbol, timeframe
        );
        let mut pipe = redis::pipe();
        for payload in payloads {
            pipe.cmd("RPUSH").arg(&ring_key).arg(payload).ignore();
        }
        pipe.cmd("LTRIM")
            .arg(&ring_key)
            .arg(-ring_max)
            .arg(-1_i64)
            .ignore();
        pipe.cmd("EXPIRE")
            .arg(&ring_key)
            .arg(cfg.redis_ttl_sec)
            .ignore();
        pipe.query_async::<()>(conn).await?;
    }

    let event_channel = format!("{}:snapshot:events", cfg.redis_prefix);
    for (symbol, timeframe, market_id) in changed_markets {
        let Some(snap) =
            state
                .by_market
                .get(&(symbol.clone(), timeframe.clone(), market_id.clone()))
        else {
            continue;
        };
        let payload = serde_json::to_string(&RedisSnapshotEvent {
            event: "snapshot_updated",
            symbol,
            timeframe,
            market_id,
            round_id: snap.round_id.clone(),
            ts_ireland_sample_ms: snap.ts_ireland_sample_ms,
            remaining_ms: snap.remaining_ms,
            mid_yes: snap.mid_yes,
            mid_yes_smooth: snap.mid_yes_smooth,
            bid_yes: snap.bid_yes,
            ask_yes: snap.ask_yes,
            bid_no: snap.bid_no,
            ask_no: snap.ask_no,
            delta_pct: snap.delta_pct,
            delta_pct_smooth: snap.delta_pct_smooth,
            velocity_bps_per_sec: snap.velocity_bps_per_sec,
            acceleration: snap.acceleration,
            source: "db_sink",
        })?;
        if let Err(err) = conn.publish::<_, _, i64>(&event_channel, payload).await {
            tracing::warn!(
                ?err,
                channel = %event_channel,
                "redis snapshot event publish failed"
            );
        }
    }

    let heartbeat_channel = format!("{}:snapshot:events:heartbeat", cfg.redis_prefix);
    let heartbeat = json!({
        "event": "snapshot_flush",
        "source": "db_sink",
        "updated_count": rows.len(),
        "ts_ms": chrono::Utc::now().timestamp_millis()
    });
    if let Err(err) = conn
        .publish::<_, _, i64>(&heartbeat_channel, heartbeat.to_string())
        .await
    {
        tracing::debug!(
            ?err,
            channel = %heartbeat_channel,
            "redis snapshot heartbeat publish failed"
        );
    }

    Ok(())
}
