use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Result};
use redis::AsyncCommands;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::models::{RoundRow, SnapshotRow};

#[derive(Debug, Clone)]
pub struct DbSinkConfig {
    pub clickhouse_url: Option<String>,
    pub clickhouse_database: String,
    pub clickhouse_snapshot_table: String,
    pub clickhouse_processed_table: String,
    pub clickhouse_round_table: String,
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
                    Some(DbEvent::Round(row)) => {
                        if sanitize_round_row(&row) {
                            round_batch.push(row);
                        }
                    }
                    None => {
                        flush_batches(&cfg, &mut snapshot_batch, &mut round_batch, &mut redis_state, &mut conn).await;
                        break;
                    }
                }

                if snapshot_batch.len() >= cfg.batch_size.max(10)
                    || round_batch.len() >= cfg.batch_size.max(10)
                {
                    flush_batches(&cfg, &mut snapshot_batch, &mut round_batch, &mut redis_state, &mut conn).await;
                }
            }
            _ = flush_tick.tick() => {
                if !snapshot_batch.is_empty() || !round_batch.is_empty() {
                    flush_batches(&cfg, &mut snapshot_batch, &mut round_batch, &mut redis_state, &mut conn).await;
                }
            }
        }
    }
}

async fn flush_batches(
    cfg: &DbSinkConfig,
    snapshot_batch: &mut Vec<SnapshotRow>,
    round_batch: &mut Vec<RoundRow>,
    redis_state: &mut RedisLatestState,
    redis_conn: &mut Option<redis::aio::MultiplexedConnection>,
) {
    if snapshot_batch.is_empty() && round_batch.is_empty() {
        return;
    }

    let mut snapshots = Vec::new();
    std::mem::swap(&mut snapshots, snapshot_batch);

    let mut rounds = Vec::new();
    std::mem::swap(&mut rounds, round_batch);

    if let Some(ch_url) = cfg.clickhouse_url.as_deref() {
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
        && row.ask_no.is_finite())
    {
        return None;
    }
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

    execute_clickhouse_query(ch_url, &format!("CREATE DATABASE IF NOT EXISTS {}", db)).await?;

    let create_snapshot_tbl = format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
            schema_version String,
            ingest_seq UInt64,
            ts_ireland_sample_ms Int64,
            ts_tokyo_recv_ms Nullable(Int64),
            ts_exchange_ms Nullable(Int64),
            ts_ireland_recv_ms Nullable(Int64),
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
        SETTINGS index_granularity = 8192",
        db, snapshot_tbl
    );
    execute_clickhouse_query(ch_url, &create_snapshot_tbl).await?;

    let alter_snapshot_queries = [
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
        SETTINGS index_granularity = 8192",
        db, processed_tbl
    );
    execute_clickhouse_query(ch_url, &create_processed_tbl).await?;

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
        SETTINGS index_granularity = 8192",
        db, round_tbl
    );
    execute_clickhouse_query(ch_url, &create_round_tbl).await?;

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
        .query(&[("query", query.as_str())])
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
            .query(&[("query", query.as_str())])
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
    let db = &cfg.clickhouse_database;
    let tbl = &cfg.clickhouse_round_table;
    let query = format!("INSERT INTO {}.{} FORMAT JSONEachRow", db, tbl);

    let mut body = String::with_capacity(rows.len().saturating_mul(400));
    for row in rows {
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
        .query(&[("query", query.as_str())])
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
            .query(&[("query", query.as_str())])
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

async fn flush_redis_latest(
    cfg: &DbSinkConfig,
    rows: &[SnapshotRow],
    state: &mut RedisLatestState,
    conn: &mut redis::aio::MultiplexedConnection,
) -> Result<()> {
    // 更新聚合索引
    for row in rows {
        let snap = RedisSnapshot::from_row(row);
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

    Ok(())
}
