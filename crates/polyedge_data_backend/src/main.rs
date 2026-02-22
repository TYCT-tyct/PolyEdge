use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use chrono_tz::America::New_York;
use clap::{Parser, Subcommand};
use core_types::{BookTop, MarketFeed, RefPriceWsFeed};
use feed_polymarket::PolymarketFeed;
use feed_reference::MultiSourceRefFeed;
use futures::StreamExt;
use market_discovery::{DiscoveryConfig, MarketDiscovery, MarketDescriptor};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, RwLock};

#[derive(Parser, Debug)]
#[command(name = "polyedge-data-backend", version, about = "Independent high-resolution data backend")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Tokyo-side Binance collector + forwarder (default BTCUSDT only)
    TokyoCollector(TokyoCollectorArgs),
    /// Ireland-side ingestion (Tokyo ticks + PM books + Chainlink + snapshots)
    IrelandIngest(IrelandIngestArgs),
    /// HTTP API for frontend
    Api(ApiArgs),
}

#[derive(clap::Args, Debug, Clone)]
struct TokyoCollectorArgs {
    #[arg(long, env = "POLYEDGE_DATA_ROOT", default_value = "datasets/research_backend")]
    dataset_root: String,
    #[arg(long, env = "POLYEDGE_SYMBOLS", default_value = "BTCUSDT")]
    symbols: String,
    #[arg(long, env = "POLYEDGE_TOKYO_BIND", default_value = "0.0.0.0:0")]
    bind: String,
    #[arg(long, env = "POLYEDGE_IRELAND_UDP", default_value = "10.0.3.123:9801")]
    ireland_udp: String,
}

#[derive(clap::Args, Debug, Clone)]
struct IrelandIngestArgs {
    #[arg(long, env = "POLYEDGE_DATA_ROOT", default_value = "/data/polyedge-data")]
    dataset_root: String,
    #[arg(long, env = "POLYEDGE_SYMBOLS", default_value = "BTCUSDT")]
    symbols: String,
    #[arg(long, env = "POLYEDGE_TIMEFRAMES", default_value = "5m")]
    timeframes: String,
    #[arg(long, env = "POLYEDGE_IRELAND_UDP_BIND", default_value = "0.0.0.0:9801")]
    udp_bind: String,
    #[arg(long, env = "POLYEDGE_SNAPSHOT_MS", default_value_t = 100)]
    snapshot_ms: u64,
    #[arg(long, env = "POLYEDGE_DISCOVERY_REFRESH_SEC", default_value_t = 10)]
    discovery_refresh_sec: u64,
    #[arg(long, env = "POLYEDGE_CH_URL", default_value = "")]
    clickhouse_url: String,
    #[arg(long, env = "POLYEDGE_CH_DATABASE", default_value = "polyedge")]
    clickhouse_database: String,
    #[arg(long, env = "POLYEDGE_CH_SNAPSHOT_TABLE", default_value = "snapshot_1s")]
    clickhouse_snapshot_table: String,
    #[arg(long, env = "POLYEDGE_CH_TTL_DAYS", default_value_t = 30)]
    clickhouse_ttl_days: u32,
    #[arg(long, env = "POLYEDGE_REDIS_URL", default_value = "")]
    redis_url: String,
    #[arg(long, env = "POLYEDGE_REDIS_PREFIX", default_value = "polyedge")]
    redis_prefix: String,
    #[arg(long, env = "POLYEDGE_REDIS_TTL_SEC", default_value_t = 7200)]
    redis_ttl_sec: u64,
    #[arg(long, env = "POLYEDGE_SINK_BATCH_SIZE", default_value_t = 200)]
    sink_batch_size: usize,
    #[arg(long, env = "POLYEDGE_SINK_FLUSH_MS", default_value_t = 1000)]
    sink_flush_ms: u64,
    #[arg(long, env = "POLYEDGE_SINK_QUEUE_CAP", default_value_t = 20000)]
    sink_queue_cap: usize,
}

#[derive(clap::Args, Debug, Clone)]
struct ApiArgs {
    #[arg(long, env = "POLYEDGE_DATA_ROOT", default_value = "/data/polyedge-data")]
    dataset_root: String,
    #[arg(long, env = "POLYEDGE_API_BIND", default_value = "0.0.0.0:8095")]
    bind: String,
    #[arg(long, env = "POLYEDGE_REDIS_URL", default_value = "")]
    redis_url: String,
    #[arg(long, env = "POLYEDGE_REDIS_PREFIX", default_value = "polyedge")]
    redis_prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TokyoTick {
    ts_tokyo_recv_ms: i64,
    ts_exchange_ms: i64,
    symbol: String,
    binance_price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChainlinkTick {
    ts_chainlink_recv_ms: i64,
    ts_exchange_ms: i64,
    symbol: String,
    chainlink_price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MarketMeta {
    market_id: String,
    symbol: String,
    timeframe: String,
    title: String,
    end_date: Option<String>,
    event_start_time: Option<String>,
    start_ts_utc_ms: Option<i64>,
    target_price: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PmBookRow {
    ts_pm_recv_ms: i64,
    ts_exchange_ms: i64,
    market_id: String,
    token_id_yes: String,
    token_id_no: String,
    symbol: String,
    timeframe: String,
    title: String,
    bid_yes: f64,
    ask_yes: f64,
    mid_yes: f64,
    bid_no: f64,
    ask_no: f64,
    mid_no: f64,
    spread_yes: f64,
    spread_no: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotRow {
    ts_ms_utc: i64,
    market_id: String,
    symbol: String,
    timeframe: String,
    title: String,
    target_price: Option<f64>,
    binance_price_tokyo: Option<f64>,
    chainlink_price: Option<f64>,
    chainlink_age_ms: Option<i64>,
    chainlink_stale: Option<bool>,
    bid_yes: f64,
    ask_yes: f64,
    mid_yes: f64,
    bid_no: f64,
    ask_no: f64,
    mid_no: f64,
    delta_price: Option<f64>,
    delta_pct: Option<f64>,
    velocity_bps_per_sec: Option<f64>,
    acceleration: Option<f64>,
    remaining_ms: Option<i64>,
    predicted_win_rate: Option<f64>,
    historical_win_rate: Option<f64>,
    net_ev_bps: Option<f64>,
    time_edge_ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TargetAnchor {
    price: f64,
    source: String,
}

#[derive(Default)]
struct IngestState {
    tokyo_last: HashMap<String, TokyoTick>,
    tokyo_prev: HashMap<String, TokyoTick>,
    vel_prev: HashMap<String, (i64, f64)>,
    chainlink_last: HashMap<String, ChainlinkTick>,
    tokyo_hist: HashMap<String, VecDeque<TokyoTick>>,
    chainlink_hist: HashMap<String, VecDeque<ChainlinkTick>>,
    books: HashMap<String, PmBookRow>,
    meta: HashMap<String, MarketMeta>,
    target_anchor: HashMap<String, TargetAnchor>,
    settled_markets: HashSet<String>,
    historical_up_stats: HashMap<(String, String), (u64, u64)>,
}

#[derive(Clone, Debug)]
struct AnalyticsSinkConfig {
    clickhouse_url: Option<String>,
    clickhouse_database: String,
    clickhouse_snapshot_table: String,
    clickhouse_ttl_days: u32,
    redis_url: Option<String>,
    redis_prefix: String,
    redis_ttl_sec: u64,
    batch_size: usize,
    flush_ms: u64,
    queue_cap: usize,
}

#[derive(Debug, Clone)]
enum SinkEvent {
    Snapshot(SnapshotRow),
}

#[derive(Clone)]
struct ApiState {
    root: PathBuf,
    redis_url: Option<String>,
    redis_prefix: String,
}

#[derive(Debug, Deserialize)]
struct SnapshotQuery {
    market_id: Option<String>,
    symbol: Option<String>,
    timeframe: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct MarketsQuery {
    date: Option<String>,
    date_tz: Option<String>,
    symbol: Option<String>,
    timeframe: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HistorySnapshotQuery {
    date: Option<String>,
    date_tz: Option<String>,
    symbol: Option<String>,
    timeframe: Option<String>,
    market_id: Option<String>,
    from_ms: Option<i64>,
    to_ms: Option<i64>,
    limit: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    install_tracing();
    install_rustls_provider();
    let cli = Cli::parse();
    match cli.command {
        Command::TokyoCollector(args) => run_tokyo_collector(args).await,
        Command::IrelandIngest(args) => run_ireland_ingest(args).await,
        Command::Api(args) => run_api(args).await,
    }
}

fn install_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info,polyedge_data_backend=debug".into()),
        )
        .try_init();
}

fn install_rustls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

async fn run_tokyo_collector(args: TokyoCollectorArgs) -> Result<()> {
    ensure_non_legacy_root(&args.dataset_root)?;
    let symbols = parse_symbols(&args.symbols);
    let out_root = PathBuf::from(args.dataset_root);
    let dst: SocketAddr = args
        .ireland_udp
        .parse()
        .with_context(|| format!("invalid ireland udp addr: {}", args.ireland_udp))?;
    let sock = UdpSocket::bind(&args.bind)
        .await
        .with_context(|| format!("bind udp {}", args.bind))?;

    tracing::info!(?symbols, bind = %args.bind, ireland_udp = %args.ireland_udp, "tokyo collector started");

    let feed = MultiSourceRefFeed::new(Duration::from_millis(25));
    let mut stream = feed.stream_ticks_ws(symbols.clone()).await?;

    loop {
        let Some(next) = stream.next().await else {
            return Err(anyhow!("reference stream ended"));
        };
        let tick = match next {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(?err, "tokyo collector stream error");
                continue;
            }
        };
        if tick.source.as_str() != "binance_ws" {
            continue;
        }
        if !symbols.iter().any(|s| s == &tick.symbol) {
            continue;
        }

        let row = TokyoTick {
            ts_tokyo_recv_ms: tick.recv_ts_ms,
            ts_exchange_ms: tick.event_ts_exchange_ms,
            symbol: tick.symbol,
            binance_price: tick.price,
        };

        let payload = serde_json::to_vec(&row)?;
        let _ = sock.send_to(&payload, dst).await;
        append_jsonl(&tokyo_tick_path(&out_root, &row), &row)?;
    }
}

async fn run_ireland_ingest(args: IrelandIngestArgs) -> Result<()> {
    ensure_non_legacy_root(&args.dataset_root)?;
    let symbols = parse_symbols(&args.symbols);
    let timeframes = parse_lower_csv(&args.timeframes);
    let root = PathBuf::from(&args.dataset_root);
    fs::create_dir_all(root.join("raw")).ok();
    fs::create_dir_all(root.join("normalized")).ok();
    fs::create_dir_all(root.join("reports")).ok();

    tracing::info!(?symbols, ?timeframes, udp_bind = %args.udp_bind, root = %args.dataset_root, "ireland ingest started");

    let shared = Arc::new(RwLock::new(IngestState::default()));
    let sink_cfg = AnalyticsSinkConfig {
        clickhouse_url: normalize_opt_url(&args.clickhouse_url),
        clickhouse_database: args.clickhouse_database.clone(),
        clickhouse_snapshot_table: args.clickhouse_snapshot_table.clone(),
        clickhouse_ttl_days: args.clickhouse_ttl_days.max(1),
        redis_url: normalize_opt_url(&args.redis_url),
        redis_prefix: args.redis_prefix.clone(),
        redis_ttl_sec: args.redis_ttl_sec.max(60),
        batch_size: args.sink_batch_size.clamp(10, 5_000),
        flush_ms: args.sink_flush_ms.clamp(100, 5_000),
        queue_cap: args.sink_queue_cap.clamp(1_000, 200_000),
    };
    let sink_tx = if sink_cfg.clickhouse_url.is_some() || sink_cfg.redis_url.is_some() {
        let (tx, rx) = mpsc::channel::<SinkEvent>(sink_cfg.queue_cap);
        tokio::spawn(run_analytics_sink(sink_cfg.clone(), rx));
        Some(tx)
    } else {
        None
    };

    let discover_state = shared.clone();
    let discover_root = root.clone();
    let discover_symbols = symbols.clone();
    let discover_tfs = timeframes.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(args.discovery_refresh_sec.max(3)));
        loop {
            tick.tick().await;
            match discover_once(&discover_symbols, &discover_tfs).await {
                Ok(meta_rows) => {
                    let mut w = discover_state.write().await;
                    w.meta = meta_rows
                        .into_iter()
                        .map(|m| (m.market_id.clone(), m))
                        .collect::<HashMap<_, _>>();
                    for (market_id, anchor) in w.target_anchor.clone() {
                        if let Some(meta) = w.meta.get_mut(&market_id) {
                            if meta.target_price.is_none() {
                                meta.target_price = Some(anchor.price);
                            }
                        }
                    }
                    let markets = w
                        .meta
                        .values()
                        .map(|m| {
                            let start_price = w
                                .target_anchor
                                .get(&m.market_id)
                                .map(|a| a.price)
                                .or(m.target_price);
                            serde_json::json!({
                                "market_id": m.market_id,
                                "symbol": m.symbol,
                                "timeframe": m.timeframe,
                                "title": m.title,
                                "end_date": m.end_date,
                                "event_start_time": m.event_start_time,
                                "start_ts_utc_ms": m.start_ts_utc_ms,
                                "target_price": m.target_price,
                                "start_price": start_price
                            })
                        })
                        .collect::<Vec<_>>();
                    let report = serde_json::json!({
                        "ts_ms": now_ms(),
                        "market_count": markets.len(),
                        "markets": markets
                    });
                    let _ = write_json_pretty(discover_root.join("reports").join("latest_markets.json"), &report);
                    let day = Utc::now().format("%Y-%m-%d").to_string();
                    let _ = write_json_pretty(
                        discover_root
                            .join("reports")
                            .join("catalog")
                            .join(day)
                            .join("markets.json"),
                        &report,
                    );
                }
                Err(err) => {
                    tracing::warn!(?err, "market discovery refresh failed");
                }
            }
        }
    });

    let udp_state = shared.clone();
    let udp_root = root.clone();
    let udp_bind = args.udp_bind.clone();
    tokio::spawn(async move {
        let sock = match UdpSocket::bind(&udp_bind).await {
            Ok(v) => v,
            Err(err) => {
                tracing::error!(?err, bind = %udp_bind, "udp bind failed");
                return;
            }
        };
        let mut buf = vec![0_u8; 4096];
        loop {
            let recv = sock.recv_from(&mut buf).await;
            let (n, _addr) = match recv {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(?err, "udp recv failed");
                    continue;
                }
            };
            let parsed = serde_json::from_slice::<TokyoTick>(&buf[..n]);
            let row = match parsed {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(?err, "udp tick parse failed");
                    continue;
                }
            };
            {
                let mut w = udp_state.write().await;
                if let Some(prev) = w.tokyo_last.insert(row.symbol.clone(), row.clone()) {
                    w.tokyo_prev.insert(row.symbol.clone(), prev);
                }
                push_tokyo_hist(&mut w.tokyo_hist, &row);
            }
            let _ = append_jsonl(&tokyo_tick_path(&udp_root, &row), &row);
        }
    });

    let pm_state = shared.clone();
    let pm_root = root.clone();
    let pm_symbols = symbols.clone();
    let pm_timeframes = timeframes.clone();
    tokio::spawn(async move {
        loop {
            let feed = PolymarketFeed::new_with_universe(
                Duration::from_millis(50),
                pm_symbols.clone(),
                vec!["updown".to_string()],
                pm_timeframes.clone(),
            );
            let mut stream = match feed.stream_books().await {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(?err, "pm stream start failed; retry");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            while let Some(next) = stream.next().await {
                let book = match next {
                    Ok(v) => v,
                    Err(err) => {
                        tracing::warn!(?err, "pm stream record error");
                        continue;
                    }
                };
                let row = map_book_to_row(&pm_state, &book).await;
                {
                    let mut w = pm_state.write().await;
                    w.books.insert(row.market_id.clone(), row.clone());
                }
                let _ = append_jsonl(&pm_books_symbol_tf_path(&pm_root, &row), &row);
                let _ = append_jsonl(&pm_books_market_path(&pm_root, &row), &row);
            }

            tracing::warn!("pm stream ended; restarting");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    let ch_state = shared.clone();
    let ch_root = root.clone();
    let ch_symbols = symbols.clone();
    tokio::spawn(async move {
        loop {
            let feed = MultiSourceRefFeed::new(Duration::from_millis(25));
            let mut stream = match feed.stream_ticks_ws(ch_symbols.clone()).await {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(?err, "chainlink stream start failed; retry");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };
            while let Some(next) = stream.next().await {
                let tick = match next {
                    Ok(v) => v,
                    Err(err) => {
                        tracing::warn!(?err, "chainlink stream tick error");
                        continue;
                    }
                };
                if tick.source.as_str() != "chainlink_rtds" {
                    continue;
                }
                let row = ChainlinkTick {
                    ts_chainlink_recv_ms: tick.recv_ts_ms,
                    ts_exchange_ms: tick.event_ts_exchange_ms,
                    symbol: tick.symbol,
                    chainlink_price: tick.price,
                };
                {
                    let mut w = ch_state.write().await;
                    w.chainlink_last.insert(row.symbol.clone(), row.clone());
                    push_chainlink_hist(&mut w.chainlink_hist, &row);
                }
                let _ = append_jsonl(&chainlink_tick_path(&ch_root, &row), &row);
            }
            tracing::warn!("chainlink stream ended; restarting");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    let mut snap_tick = tokio::time::interval(Duration::from_millis(args.snapshot_ms.max(20)));
    let mut sec_cache: HashMap<(String, String, String), SnapshotRow> = HashMap::new();
    loop {
        snap_tick.tick().await;
        let rows = build_snapshots(shared.clone()).await;
        if rows.is_empty() {
            continue;
        }
        let mut seen_keys = std::collections::HashSet::new();
        for row in rows {
            let key = (row.symbol.clone(), row.timeframe.clone(), row.market_id.clone());
            seen_keys.insert(key.clone());
            let sec = row.ts_ms_utc / 1000;
            if let Some(prev) = sec_cache.get(&key).cloned() {
                let prev_sec = prev.ts_ms_utc / 1000;
                if prev_sec == sec {
                    sec_cache.insert(key, row);
                    continue;
                }
                append_jsonl(&snapshot_symbol_tf_1s_path(&root, &prev), &prev)?;
                append_jsonl(&snapshot_market_1s_path(&root, &prev), &prev)?;
                if let Some(tx) = sink_tx.as_ref() {
                    let _ = tx.try_send(SinkEvent::Snapshot(prev));
                }
            }
            sec_cache.insert(key, row);
        }
        let stale_keys = sec_cache
            .keys()
            .filter(|k| !seen_keys.contains(*k))
            .cloned()
            .collect::<Vec<_>>();
        for key in stale_keys {
            if let Some(last) = sec_cache.remove(&key) {
                append_jsonl(&snapshot_symbol_tf_1s_path(&root, &last), &last)?;
                append_jsonl(&snapshot_market_1s_path(&root, &last), &last)?;
                if let Some(tx) = sink_tx.as_ref() {
                    let _ = tx.try_send(SinkEvent::Snapshot(last));
                }
            }
        }
    }
}

async fn map_book_to_row(state: &Arc<RwLock<IngestState>>, book: &BookTop) -> PmBookRow {
    let meta = {
        let r = state.read().await;
        r.meta.get(&book.market_id).cloned()
    };

    PmBookRow {
        ts_pm_recv_ms: now_ms(),
        ts_exchange_ms: book.ts_ms,
        market_id: book.market_id.clone(),
        token_id_yes: book.token_id_yes.clone(),
        token_id_no: book.token_id_no.clone(),
        symbol: meta
            .as_ref()
            .map(|m| m.symbol.clone())
            .unwrap_or_else(|| "UNKNOWN".to_string()),
        timeframe: meta
            .as_ref()
            .map(|m| m.timeframe.clone())
            .unwrap_or_else(|| "unknown".to_string()),
        title: meta.as_ref().map(|m| m.title.clone()).unwrap_or_default(),
        bid_yes: book.bid_yes,
        ask_yes: book.ask_yes,
        mid_yes: (book.bid_yes + book.ask_yes) * 0.5,
        bid_no: book.bid_no,
        ask_no: book.ask_no,
        mid_no: (book.bid_no + book.ask_no) * 0.5,
        spread_yes: (book.ask_yes - book.bid_yes).max(0.0),
        spread_no: (book.ask_no - book.bid_no).max(0.0),
    }
}

async fn build_snapshots(state: Arc<RwLock<IngestState>>) -> Vec<SnapshotRow> {
    let now = now_ms();
    let mut out = Vec::new();

    let mut w = state.write().await;
    for (market_id, book) in w.books.clone() {
        let meta = match w.meta.get(&market_id).cloned() {
            Some(v) => v,
            None => continue,
        };
        let symbol = meta.symbol.clone();
        let tokyo = w.tokyo_last.get(&symbol).cloned();
        let prev = w.tokyo_prev.get(&symbol).cloned();
        let chainlink = w.chainlink_last.get(&symbol).cloned();

        let target = resolve_target_anchor(&mut w, &meta, now);
        let target_price = target.as_ref().map(|x| x.price);

        let delta_price = target_price.and_then(|t| tokyo.as_ref().map(|x| x.binance_price - t));
        let delta_pct = match (delta_price, target_price) {
            (Some(d), Some(t)) if t > 0.0 => Some(d / t),
            _ => None,
        };

        let velocity = calc_velocity(prev.as_ref(), tokyo.as_ref());
        let acceleration = if let Some(v) = velocity {
            let prev_v = w.vel_prev.get(&symbol).copied();
            w.vel_prev.insert(symbol.clone(), (now, v));
            prev_v.and_then(|(t0, v0)| {
                let dt = (now - t0) as f64 / 1000.0;
                if dt > 0.0 { Some((v - v0) / dt) } else { None }
            })
        } else {
            None
        };

        let remaining_ms = meta
            .end_date
            .as_deref()
            .and_then(parse_end_ms)
            .map(|end| end.saturating_sub(now));

        let time_edge_ms = tokyo
            .as_ref()
            .map(|t| book.ts_exchange_ms as f64 - t.ts_exchange_ms as f64);

        let net_ev_bps = estimate_net_ev_bps(delta_pct, book.spread_yes, velocity);
        let predicted_win_rate = estimate_predicted_win_rate(book.mid_yes, delta_pct, velocity, acceleration);
        if let (Some(rem), Some(target), Some(ch)) = (remaining_ms, target_price, chainlink.as_ref()) {
            if rem <= 0 && !w.settled_markets.contains(&market_id) {
                let final_up = ch.chainlink_price >= target;
                let k = (meta.symbol.clone(), meta.timeframe.clone());
                let entry = w.historical_up_stats.entry(k).or_insert((0, 0));
                if final_up {
                    entry.0 = entry.0.saturating_add(1);
                }
                entry.1 = entry.1.saturating_add(1);
                w.settled_markets.insert(market_id.clone());
            }
        }
        let historical_win_rate = w
            .historical_up_stats
            .get(&(meta.symbol.clone(), meta.timeframe.clone()))
            .and_then(|(wins, total)| {
                if *total == 0 {
                    None
                } else {
                    Some(*wins as f64 / *total as f64)
                }
            });

        let chainlink_age_ms = chainlink
            .as_ref()
            .map(|x| now.saturating_sub(x.ts_chainlink_recv_ms));
        let chainlink_stale = chainlink_age_ms.map(|age| age > 5000);

        out.push(SnapshotRow {
            ts_ms_utc: now,
            market_id,
            symbol: meta.symbol,
            timeframe: meta.timeframe,
            title: meta.title,
            target_price,
            binance_price_tokyo: tokyo.map(|x| x.binance_price),
            chainlink_price: chainlink.map(|x| x.chainlink_price),
            chainlink_age_ms,
            chainlink_stale,
            bid_yes: book.bid_yes,
            ask_yes: book.ask_yes,
            mid_yes: book.mid_yes,
            bid_no: book.bid_no,
            ask_no: book.ask_no,
            mid_no: book.mid_no,
            delta_price,
            delta_pct,
            velocity_bps_per_sec: velocity,
            acceleration,
            remaining_ms,
            predicted_win_rate,
            historical_win_rate,
            net_ev_bps,
            time_edge_ms,
        });
    }

    out
}

fn calc_velocity(prev: Option<&TokyoTick>, now: Option<&TokyoTick>) -> Option<f64> {
    let (prev, now) = (prev?, now?);
    if prev.binance_price <= 0.0 {
        return None;
    }
    let dt_s = (now.ts_tokyo_recv_ms - prev.ts_tokyo_recv_ms) as f64 / 1000.0;
    if dt_s <= 0.0 {
        return None;
    }
    let ret = (now.binance_price - prev.binance_price) / prev.binance_price;
    Some((ret * 10_000.0) / dt_s)
}

fn estimate_net_ev_bps(delta_pct: Option<f64>, spread_yes: f64, velocity: Option<f64>) -> Option<f64> {
    let d = delta_pct?;
    let v = velocity.unwrap_or(0.0).abs();
    let signal_bps = d * 10_000.0 + v * 0.15;
    let cost_bps = (spread_yes * 10_000.0) + 44.0; // include upper taker fee envelope
    Some(signal_bps - cost_bps)
}

fn estimate_predicted_win_rate(
    mid_yes: f64,
    delta_pct: Option<f64>,
    velocity: Option<f64>,
    acceleration: Option<f64>,
) -> Option<f64> {
    if !mid_yes.is_finite() {
        return None;
    }
    let d = delta_pct.unwrap_or(0.0);
    let v = velocity.unwrap_or(0.0);
    let a = acceleration.unwrap_or(0.0);
    let raw = mid_yes + d * 40.0 + v * 0.001 + a * 0.00005;
    Some(raw.clamp(0.0, 1.0))
}

fn normalize_opt_url(v: &str) -> Option<String> {
    let s = v.trim();
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

#[derive(Debug, Clone, Serialize)]
struct ClickHouseSnapshotRow {
    ts_ms_utc: i64,
    market_id: String,
    symbol: String,
    timeframe: String,
    title: String,
    target_price: Option<f64>,
    binance_price_tokyo: Option<f64>,
    chainlink_price: Option<f64>,
    chainlink_age_ms: Option<i64>,
    chainlink_stale: Option<u8>,
    bid_yes: f64,
    ask_yes: f64,
    mid_yes: f64,
    bid_no: f64,
    ask_no: f64,
    mid_no: f64,
    delta_price: Option<f64>,
    delta_pct: Option<f64>,
    velocity_bps_per_sec: Option<f64>,
    acceleration: Option<f64>,
    remaining_ms: Option<i64>,
    predicted_win_rate: Option<f64>,
    historical_win_rate: Option<f64>,
    net_ev_bps: Option<f64>,
    time_edge_ms: Option<f64>,
}

impl From<SnapshotRow> for ClickHouseSnapshotRow {
    fn from(v: SnapshotRow) -> Self {
        Self {
            ts_ms_utc: v.ts_ms_utc,
            market_id: v.market_id,
            symbol: v.symbol,
            timeframe: v.timeframe,
            title: v.title,
            target_price: v.target_price,
            binance_price_tokyo: v.binance_price_tokyo,
            chainlink_price: v.chainlink_price,
            chainlink_age_ms: v.chainlink_age_ms,
            chainlink_stale: v.chainlink_stale.map(|b| if b { 1 } else { 0 }),
            bid_yes: v.bid_yes,
            ask_yes: v.ask_yes,
            mid_yes: v.mid_yes,
            bid_no: v.bid_no,
            ask_no: v.ask_no,
            mid_no: v.mid_no,
            delta_price: v.delta_price,
            delta_pct: v.delta_pct,
            velocity_bps_per_sec: v.velocity_bps_per_sec,
            acceleration: v.acceleration,
            remaining_ms: v.remaining_ms,
            predicted_win_rate: v.predicted_win_rate,
            historical_win_rate: v.historical_win_rate,
            net_ev_bps: v.net_ev_bps,
            time_edge_ms: v.time_edge_ms,
        }
    }
}

async fn run_analytics_sink(cfg: AnalyticsSinkConfig, mut rx: mpsc::Receiver<SinkEvent>) {
    if let Some(ch_url) = cfg.clickhouse_url.as_deref() {
        if let Err(err) = init_clickhouse(ch_url, &cfg).await {
            tracing::warn!(?err, "clickhouse init failed; continue without hard fail");
        }
    }

    let mut flush_tick = tokio::time::interval(Duration::from_millis(cfg.flush_ms));
    let mut batch = Vec::<SnapshotRow>::with_capacity(cfg.batch_size.saturating_mul(2));
    loop {
        tokio::select! {
            maybe_evt = rx.recv() => {
                match maybe_evt {
                    Some(SinkEvent::Snapshot(row)) => {
                        batch.push(row);
                        if batch.len() >= cfg.batch_size {
                            flush_sink_batch(&cfg, &mut batch).await;
                        }
                    }
                    None => {
                        flush_sink_batch(&cfg, &mut batch).await;
                        break;
                    }
                }
            }
            _ = flush_tick.tick() => {
                if !batch.is_empty() {
                    flush_sink_batch(&cfg, &mut batch).await;
                }
            }
        }
    }
}

async fn flush_sink_batch(cfg: &AnalyticsSinkConfig, batch: &mut Vec<SnapshotRow>) {
    if batch.is_empty() {
        return;
    }
    let mut to_flush = Vec::new();
    std::mem::swap(batch, &mut to_flush);

    if let Some(ch_url) = cfg.clickhouse_url.as_deref() {
        if let Err(err) = flush_clickhouse_snapshots(ch_url, cfg, &to_flush).await {
            tracing::warn!(?err, rows = to_flush.len(), "clickhouse flush failed");
        }
    }
    if let Some(redis_url) = cfg.redis_url.as_deref() {
        if let Err(err) = flush_redis_latest(redis_url, cfg, &to_flush).await {
            tracing::warn!(?err, rows = to_flush.len(), "redis latest cache flush failed");
        }
    }
}

async fn init_clickhouse(ch_url: &str, cfg: &AnalyticsSinkConfig) -> Result<()> {
    let client = reqwest::Client::new();
    let db = &cfg.clickhouse_database;
    let tbl = &cfg.clickhouse_snapshot_table;
    let ttl_days = cfg.clickhouse_ttl_days.max(1);
    let create_db = format!("CREATE DATABASE IF NOT EXISTS {}", db);
    let create_tbl = format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
            ts_ms_utc Int64,
            market_id String,
            symbol LowCardinality(String),
            timeframe LowCardinality(String),
            title String,
            target_price Nullable(Float64),
            binance_price_tokyo Nullable(Float64),
            chainlink_price Nullable(Float64),
            chainlink_age_ms Nullable(Int64),
            chainlink_stale Nullable(UInt8),
            bid_yes Float64,
            ask_yes Float64,
            mid_yes Float64,
            bid_no Float64,
            ask_no Float64,
            mid_no Float64,
            delta_price Nullable(Float64),
            delta_pct Nullable(Float64),
            velocity_bps_per_sec Nullable(Float64),
            acceleration Nullable(Float64),
            remaining_ms Nullable(Int64),
            predicted_win_rate Nullable(Float64),
            historical_win_rate Nullable(Float64),
            net_ev_bps Nullable(Float64),
            time_edge_ms Nullable(Float64),
            ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
        )
        ENGINE = MergeTree
        PARTITION BY toDate(fromUnixTimestamp64Milli(ts_ms_utc))
        ORDER BY (symbol, timeframe, market_id, ts_ms_utc)
        TTL toDate(fromUnixTimestamp64Milli(ts_ms_utc)) + toIntervalDay({})
        SETTINGS index_granularity = 8192",
        db, tbl, ttl_days
    );
    for q in [&create_db, &create_tbl] {
        let resp = client
            .get(ch_url)
            .query(&[("query", q)])
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("clickhouse init failed status={} body={}", status, body));
        }
    }
    Ok(())
}

async fn flush_clickhouse_snapshots(
    ch_url: &str,
    cfg: &AnalyticsSinkConfig,
    rows: &[SnapshotRow],
) -> Result<()> {
    let db = &cfg.clickhouse_database;
    let tbl = &cfg.clickhouse_snapshot_table;
    let query = format!("INSERT INTO {}.{} FORMAT JSONEachRow", db, tbl);
    let mut body = String::with_capacity(rows.len().saturating_mul(512));
    for row in rows.iter().cloned() {
        let ch_row: ClickHouseSnapshotRow = row.into();
        let line = serde_json::to_string(&ch_row)?;
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
    if body_err.contains("UNKNOWN_DATABASE") || body_err.contains("UNKNOWN_TABLE") {
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
        return Err(anyhow!("clickhouse insert retry failed status={} body={}", st2, b2));
    }
    Err(anyhow!(
        "clickhouse insert failed status={} body={}",
        status, body_err
    ))
}

async fn flush_redis_latest(redis_url: &str, cfg: &AnalyticsSinkConfig, rows: &[SnapshotRow]) -> Result<()> {
    let client = redis::Client::open(redis_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let mut latest_symbol_tf: HashMap<(String, String), &SnapshotRow> = HashMap::new();
    let mut latest_market: HashMap<(String, String, String), &SnapshotRow> = HashMap::new();
    for row in rows {
        latest_symbol_tf.insert((row.symbol.clone(), row.timeframe.clone()), row);
        latest_market.insert(
            (row.symbol.clone(), row.timeframe.clone(), row.market_id.clone()),
            row,
        );
    }

    for ((symbol, timeframe), row) in latest_symbol_tf {
        let key = format!(
            "{}:snapshot:latest:{}:{}",
            cfg.redis_prefix, symbol, timeframe
        );
        let payload = serde_json::to_string(row)?;
        let _: () = conn.set_ex(key, payload, cfg.redis_ttl_sec).await?;
    }
    for ((symbol, timeframe, market_id), row) in latest_market {
        let key = format!(
            "{}:snapshot:latest:{}:{}:{}",
            cfg.redis_prefix, symbol, timeframe, market_id
        );
        let payload = serde_json::to_string(row)?;
        let _: () = conn.set_ex(key, payload, cfg.redis_ttl_sec).await?;
    }
    Ok(())
}

async fn read_cached_row(redis_url: &str, key: &str) -> Result<serde_json::Value> {
    let client = redis::Client::open(redis_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let raw: Option<String> = conn.get(key).await?;
    let Some(raw) = raw else {
        return Ok(serde_json::Value::Null);
    };
    serde_json::from_str::<serde_json::Value>(&raw).map_err(|e| anyhow!(e))
}

async fn discover_once(symbols: &[String], timeframes: &[String]) -> Result<Vec<MarketMeta>> {
    let discovery = MarketDiscovery::new(DiscoveryConfig {
        symbols: symbols.to_vec(),
        market_types: vec!["updown".to_string()],
        timeframes: timeframes.to_vec(),
        ..DiscoveryConfig::default()
    });
    let markets = discovery.discover().await?;
    Ok(markets.into_iter().map(map_market_meta).collect())
}

fn map_market_meta(m: MarketDescriptor) -> MarketMeta {
    let target = extract_price_from_title(&m.question);
    let timeframe = m.timeframe.unwrap_or_else(|| "unknown".to_string());
    let start_ts_utc_ms = m
        .event_start_time
        .as_deref()
        .and_then(parse_end_ms)
        .or_else(|| {
            m.end_date
                .as_deref()
                .and_then(parse_end_ms)
                .and_then(|end| parse_timeframe_ms(&timeframe).map(|dur| end.saturating_sub(dur)))
        });
    MarketMeta {
        market_id: m.market_id,
        symbol: m.symbol,
        timeframe,
        title: m.question,
        end_date: m.end_date,
        event_start_time: m.event_start_time,
        start_ts_utc_ms,
        target_price: target,
    }
}

fn extract_price_from_title(title: &str) -> Option<f64> {
    let lower = title.to_ascii_lowercase();
    if !lower.contains("target") && !lower.contains("at ") && !lower.contains("above") {
        return None;
    }
    let mut buf = String::new();
    let mut best: Option<f64> = None;
    for ch in title.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            buf.push(ch);
        } else if !buf.is_empty() {
            if let Ok(v) = buf.parse::<f64>() {
                if v > 100.0 {
                    best = Some(v);
                    break;
                }
            }
            buf.clear();
        }
    }
    if best.is_none() && !buf.is_empty() {
        if let Ok(v) = buf.parse::<f64>() {
            if v > 100.0 {
                best = Some(v);
            }
        }
    }
    best
}

fn parse_timeframe_ms(tf: &str) -> Option<i64> {
    match tf.trim().to_ascii_lowercase().as_str() {
        "5m" => Some(5 * 60 * 1000),
        "15m" => Some(15 * 60 * 1000),
        "1h" => Some(60 * 60 * 1000),
        _ => None,
    }
}

fn push_tokyo_hist(hist: &mut HashMap<String, VecDeque<TokyoTick>>, row: &TokyoTick) {
    let q = hist.entry(row.symbol.clone()).or_default();
    q.push_back(row.clone());
    let cutoff = row.ts_exchange_ms.saturating_sub(6 * 60 * 60 * 1000);
    while q.front().map(|x| x.ts_exchange_ms < cutoff).unwrap_or(false) {
        q.pop_front();
    }
    if q.len() > 500_000 {
        q.pop_front();
    }
}

fn push_chainlink_hist(hist: &mut HashMap<String, VecDeque<ChainlinkTick>>, row: &ChainlinkTick) {
    let q = hist.entry(row.symbol.clone()).or_default();
    q.push_back(row.clone());
    let cutoff = row.ts_exchange_ms.saturating_sub(6 * 60 * 60 * 1000);
    while q.front().map(|x| x.ts_exchange_ms < cutoff).unwrap_or(false) {
        q.pop_front();
    }
    if q.len() > 500_000 {
        q.pop_front();
    }
}

fn find_closest_price_tokyo(
    hist: &HashMap<String, VecDeque<TokyoTick>>,
    symbol: &str,
    ts_ms: i64,
    tolerance_ms: i64,
) -> Option<f64> {
    let q = hist.get(symbol)?;
    q.iter()
        .filter_map(|x| {
            let d = (x.ts_exchange_ms - ts_ms).abs();
            if d <= tolerance_ms {
                Some((d, x.binance_price))
            } else {
                None
            }
        })
        .min_by_key(|(d, _)| *d)
        .map(|(_, p)| p)
}

fn find_closest_price_chainlink(
    hist: &HashMap<String, VecDeque<ChainlinkTick>>,
    symbol: &str,
    ts_ms: i64,
    tolerance_ms: i64,
) -> Option<f64> {
    let q = hist.get(symbol)?;
    q.iter()
        .filter_map(|x| {
            let d = (x.ts_exchange_ms - ts_ms).abs();
            if d <= tolerance_ms {
                Some((d, x.chainlink_price))
            } else {
                None
            }
        })
        .min_by_key(|(d, _)| *d)
        .map(|(_, p)| p)
}

fn resolve_target_anchor(
    state: &mut IngestState,
    meta: &MarketMeta,
    now_ms: i64,
) -> Option<TargetAnchor> {
    if let Some(existing) = state.target_anchor.get(&meta.market_id).cloned() {
        return Some(existing);
    }
    if let Some(tp) = meta.target_price {
        let a = TargetAnchor {
            price: tp,
            source: "metadata_target".to_string(),
        };
        state.target_anchor.insert(meta.market_id.clone(), a.clone());
        return Some(a);
    }
    let start = meta.start_ts_utc_ms?;
    if now_ms < start.saturating_sub(5_000) {
        return None;
    }
    let by_chainlink = find_closest_price_chainlink(&state.chainlink_hist, &meta.symbol, start, 120_000);
    let by_tokyo = find_closest_price_tokyo(&state.tokyo_hist, &meta.symbol, start, 120_000);
    let resolved = by_chainlink
        .map(|p| TargetAnchor {
            price: p,
            source: "chainlink_market_open".to_string(),
        })
        .or_else(|| {
            by_tokyo.map(|p| TargetAnchor {
                price: p,
                source: "tokyo_market_open".to_string(),
            })
        })
        .or_else(|| {
            if now_ms >= start {
                state.tokyo_last.get(&meta.symbol).map(|x| TargetAnchor {
                    price: x.binance_price,
                    source: "tokyo_first_seen_after_open".to_string(),
                })
            } else {
                None
            }
        });
    if let Some(anchor) = resolved.clone() {
        state.target_anchor.insert(meta.market_id.clone(), anchor);
    }
    resolved
}

async fn run_api(args: ApiArgs) -> Result<()> {
    ensure_non_legacy_root(&args.dataset_root)?;
    let root = PathBuf::from(&args.dataset_root);
    let state = ApiState {
        root,
        redis_url: normalize_opt_url(&args.redis_url),
        redis_prefix: args.redis_prefix,
    };

    let app = Router::new()
        .route("/health", get(api_health))
        .route("/api/live/markets", get(api_markets))
        .route("/api/live/snapshot", get(api_snapshot))
        .route("/api/live/latest", get(api_latest))
        .route("/api/live/latest_cached", get(api_latest_cached))
        .route("/api/live/trades", get(api_trades))
        .route("/api/live/heatmap", get(api_heatmap))
        .route("/api/history/dates", get(api_history_dates))
        .route("/api/history/markets", get(api_history_markets))
        .route("/api/history/snapshot", get(api_history_snapshot))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&args.bind)
        .await
        .with_context(|| format!("bind {}", args.bind))?;
    tracing::info!(bind = %args.bind, "api started");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn api_health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"ok": true, "ts_ms": now_ms()}))
}

async fn api_markets(
    State(st): State<ApiState>,
    Query(q): Query<MarketsQuery>,
) -> Json<serde_json::Value> {
    let path = if let Some(date) = q.date.as_deref() {
        st.root
            .join("reports")
            .join("catalog")
            .join(date)
            .join("markets.json")
    } else {
        st.root.join("reports").join("latest_markets.json")
    };
    let mut payload =
        read_json_or(path, serde_json::json!({"markets": [], "market_count": 0, "ts_ms": now_ms()}));
    if let Some(markets) = payload.get_mut("markets").and_then(|v| v.as_array_mut()) {
        if let Some(sym) = q.symbol.as_deref() {
            markets.retain(|m| m.get("symbol").and_then(|x| x.as_str()) == Some(sym));
        }
        if let Some(tf) = q.timeframe.as_deref() {
            markets.retain(|m| m.get("timeframe").and_then(|x| x.as_str()) == Some(tf));
        }
        payload["market_count"] = serde_json::json!(markets.len());
    }
    Json(payload)
}

async fn api_snapshot(
    State(st): State<ApiState>,
    Query(q): Query<SnapshotQuery>,
) -> Json<serde_json::Value> {
    let symbol = q.symbol.clone().unwrap_or_else(|| "BTCUSDT".to_string());
    let timeframe = q.timeframe.clone().unwrap_or_else(|| "5m".to_string());
    let path = latest_symbol_tf_snapshot_file(&st.root, &symbol, &timeframe);
    let limit = q.limit.unwrap_or(300).clamp(1, 5000);
    let mut rows = path
        .as_ref()
        .map(|p| read_last_jsonl_rows(p, limit))
        .unwrap_or_default();
    if let Some(mid) = q.market_id.as_deref() {
        rows.retain(|r| r.get("market_id").and_then(|v| v.as_str()) == Some(mid));
    }
    Json(serde_json::json!({"ts_ms": now_ms(), "count": rows.len(), "rows": rows}))
}

async fn api_latest(
    State(st): State<ApiState>,
    Query(q): Query<SnapshotQuery>,
) -> Json<serde_json::Value> {
    let symbol = q.symbol.clone().unwrap_or_else(|| "BTCUSDT".to_string());
    let timeframe = q.timeframe.clone().unwrap_or_else(|| "5m".to_string());
    let path = latest_symbol_tf_snapshot_file(&st.root, &symbol, &timeframe);
    let row = path
        .as_ref()
        .and_then(|p| read_last_jsonl_rows(p, 1).into_iter().next())
        .unwrap_or_else(|| serde_json::json!({}));
    Json(serde_json::json!({"ts_ms": now_ms(), "row": row}))
}

async fn api_latest_cached(
    State(st): State<ApiState>,
    Query(q): Query<SnapshotQuery>,
) -> Json<serde_json::Value> {
    let symbol = q.symbol.unwrap_or_else(|| "BTCUSDT".to_string());
    let timeframe = q.timeframe.unwrap_or_else(|| "5m".to_string());
    let market_id = q.market_id;
    let Some(redis_url) = st.redis_url.clone() else {
        return Json(serde_json::json!({
            "ts_ms": now_ms(),
            "cache_enabled": false,
            "row": serde_json::Value::Null
        }));
    };
    let key = if let Some(mid) = market_id {
        format!(
            "{}:snapshot:latest:{}:{}:{}",
            st.redis_prefix, symbol, timeframe, mid
        )
    } else {
        format!("{}:snapshot:latest:{}:{}", st.redis_prefix, symbol, timeframe)
    };
    let row = match read_cached_row(&redis_url, &key).await {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!(?err, key = %key, "redis latest_cached read failed");
            serde_json::Value::Null
        }
    };
    Json(serde_json::json!({
        "ts_ms": now_ms(),
        "cache_enabled": true,
        "key": key,
        "row": row
    }))
}

async fn api_trades(State(st): State<ApiState>) -> Json<serde_json::Value> {
    let path = latest_dated_file(&st.root, "normalized", "order_fill_events.jsonl");
    let rows = path
        .as_ref()
        .map(|p| read_last_jsonl_rows(p, 500))
        .unwrap_or_default();
    Json(serde_json::json!({"ts_ms": now_ms(), "count": rows.len(), "rows": rows}))
}

async fn api_heatmap(
    State(st): State<ApiState>,
    Query(q): Query<SnapshotQuery>,
) -> Json<serde_json::Value> {
    let symbol = q.symbol.clone().unwrap_or_else(|| "BTCUSDT".to_string());
    let timeframe = q.timeframe.clone().unwrap_or_else(|| "5m".to_string());
    let path = latest_symbol_tf_snapshot_file(&st.root, &symbol, &timeframe);
    let rows = path
        .as_ref()
        .map(|p| read_last_jsonl_rows(p, 20_000))
        .unwrap_or_default();

    let mut bins: HashMap<(i32, i32), (u64, f64)> = HashMap::new();
    for r in rows {
        let Some(delta) = r.get("delta_pct").and_then(|v| v.as_f64()) else { continue; };
        let Some(rem) = r.get("remaining_ms").and_then(|v| v.as_i64()) else { continue; };
        let key = (((delta * 1000.0) as i32).clamp(-500, 500), ((rem / 10000) as i32).clamp(0, 60));
        let score = r.get("mid_yes").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let e = bins.entry(key).or_insert((0, 0.0));
        e.0 = e.0.saturating_add(1);
        e.1 += score;
    }

    let mut out = Vec::new();
    for ((delta_bp10, rem_bin), (count, score_sum)) in bins {
        out.push(serde_json::json!({
            "delta_bin": delta_bp10,
            "remaining_bin": rem_bin,
            "count": count,
            "avg_mid_yes": if count > 0 { score_sum / count as f64 } else { 0.0 }
        }));
    }
    Json(serde_json::json!({"ts_ms": now_ms(), "cells": out}))
}

async fn api_history_dates(State(st): State<ApiState>) -> Json<serde_json::Value> {
    let utc_dates = list_date_dirs(&st.root.join("normalized"));
    let mut et_dates = utc_dates
        .iter()
        .filter_map(|d| naive_date(d))
        .flat_map(|d| [d.pred_opt().unwrap_or(d), d])
        .map(|d| d.format("%Y-%m-%d").to_string())
        .collect::<Vec<_>>();
    et_dates.sort();
    et_dates.dedup();
    et_dates.reverse();
    Json(serde_json::json!({
        "ts_ms": now_ms(),
        "count_utc": utc_dates.len(),
        "count_et": et_dates.len(),
        "dates_utc": utc_dates,
        "dates_et": et_dates
    }))
}

async fn api_history_markets(
    State(st): State<ApiState>,
    Query(q): Query<MarketsQuery>,
) -> Json<serde_json::Value> {
    let req_tz = q.date_tz.clone().unwrap_or_else(|| "et".to_string());
    let date = q
        .date
        .clone()
        .or_else(|| list_date_dirs(&st.root.join("reports").join("catalog")).into_iter().next());
    let Some(date) = date else {
        return Json(serde_json::json!({"ts_ms": now_ms(), "market_count": 0, "markets": []}));
    };
    let storage_dates = resolve_storage_dates_from_query(&date, &req_tz);
    let mut all = Vec::<serde_json::Value>::new();
    let mut seen = std::collections::HashSet::<String>::new();
    for storage_date in &storage_dates {
        let path = st
            .root
            .join("reports")
            .join("catalog")
            .join(storage_date)
            .join("markets.json");
        let payload = read_json_or(path, serde_json::json!({"markets": []}));
        if let Some(markets) = payload.get("markets").and_then(|v| v.as_array()) {
            for m in markets {
                let market_id = m
                    .get("market_id")
                    .and_then(|x| x.as_str())
                    .unwrap_or_default()
                    .to_string();
                if market_id.is_empty() || seen.contains(&market_id) {
                    continue;
                }
                seen.insert(market_id);
                all.push(m.clone());
            }
        }
    }
    if let Some(sym) = q.symbol.as_deref() {
        all.retain(|m| m.get("symbol").and_then(|x| x.as_str()) == Some(sym));
    }
    if let Some(tf) = q.timeframe.as_deref() {
        all.retain(|m| m.get("timeframe").and_then(|x| x.as_str()) == Some(tf));
    }
    Json(serde_json::json!({
        "ts_ms": now_ms(),
        "date": date,
        "date_tz": req_tz,
        "storage_dates": storage_dates,
        "market_count": all.len(),
        "markets": all
    }))
}

async fn api_history_snapshot(
    State(st): State<ApiState>,
    Query(q): Query<HistorySnapshotQuery>,
) -> Json<serde_json::Value> {
    let req_tz = q.date_tz.clone().unwrap_or_else(|| "et".to_string());
    let date = q
        .date
        .clone();
    let date = date
        .or_else(|| {
            if req_tz.eq_ignore_ascii_case("utc") {
                list_date_dirs(&st.root.join("normalized")).into_iter().next()
            } else {
                Some(ts_to_et(now_ms()).chars().take(10).collect::<String>())
            }
        });
    let Some(date) = date else {
        return Json(serde_json::json!({"ts_ms": now_ms(), "count": 0, "rows": []}));
    };
    let symbol = q.symbol.clone().unwrap_or_else(|| "BTCUSDT".to_string());
    let timeframe = q.timeframe.clone().unwrap_or_else(|| "5m".to_string());
    let limit = q.limit.unwrap_or(10_000).clamp(1, 200_000);
    let storage_dates = resolve_storage_dates_from_query(&date, &req_tz);
    let paths = storage_dates
        .iter()
        .map(|d| history_snapshot_path(&st.root, d, &symbol, &timeframe, q.market_id.as_deref()))
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    for p in &paths {
        let mut part = read_jsonl_filtered_rows(p, q.from_ms, q.to_ms, limit);
        rows.append(&mut part);
    }
    if req_tz.eq_ignore_ascii_case("et") {
        rows.retain(|v| {
            v.get("ts_ms_utc")
                .and_then(|x| x.as_i64())
                .map(|ms| et_date_from_ms(ms) == date)
                .unwrap_or(false)
        });
    }
    rows.sort_by_key(|v| v.get("ts_ms_utc").and_then(|x| x.as_i64()).unwrap_or(0));
    if rows.len() > limit {
        rows.drain(0..(rows.len() - limit));
    }
    Json(serde_json::json!({
        "ts_ms": now_ms(),
        "date": date,
        "date_tz": req_tz,
        "storage_dates": storage_dates,
        "symbol": symbol,
        "timeframe": timeframe,
        "market_id": q.market_id,
        "count": rows.len(),
        "rows": rows
    }))
}

fn parse_symbols(s: &str) -> Vec<String> {
    s.split(',')
        .map(|x| x.trim().to_ascii_uppercase())
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>()
}

fn parse_lower_csv(s: &str) -> Vec<String> {
    s.split(',')
        .map(|x| x.trim().to_ascii_lowercase())
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>()
}

fn ensure_non_legacy_root(root: &str) -> Result<()> {
    let normalized = root.replace('\\', "/").to_ascii_lowercase();
    if normalized.contains("/home/ubuntu/polyedge/datasets") {
        return Err(anyhow!(
            "refuse to use legacy data root {} ; use /data/polyedge-data",
            root
        ));
    }
    Ok(())
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn ts_to_et(ms: i64) -> String {
    if let Some(dt) = DateTime::<Utc>::from_timestamp_millis(ms) {
        dt.with_timezone(&New_York)
            .format("%Y-%m-%d %H:%M:%S ET")
            .to_string()
    } else {
        "invalid".to_string()
    }
}

fn et_date_from_ms(ms: i64) -> String {
    if let Some(dt) = DateTime::<Utc>::from_timestamp_millis(ms) {
        dt.with_timezone(&New_York)
            .format("%Y-%m-%d")
            .to_string()
    } else {
        Utc::now()
            .with_timezone(&New_York)
            .format("%Y-%m-%d")
            .to_string()
    }
}

fn parse_end_ms(v: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(v)
        .ok()
        .map(|d| d.timestamp_millis())
}

fn naive_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

fn resolve_storage_dates_from_query(date: &str, date_tz: &str) -> Vec<String> {
    if date_tz.eq_ignore_ascii_case("utc") {
        return vec![date.to_string()];
    }
    let Some(d) = naive_date(date) else {
        return vec![date.to_string()];
    };
    let start_local = New_York
        .with_ymd_and_hms(d.year(), d.month(), d.day(), 0, 0, 0)
        .single();
    let end_local = New_York
        .with_ymd_and_hms(d.year(), d.month(), d.day(), 23, 59, 59)
        .single();
    let mut out = Vec::<String>::new();
    if let Some(v) = start_local {
        out.push(v.with_timezone(&Utc).format("%Y-%m-%d").to_string());
    }
    if let Some(v) = end_local {
        out.push(v.with_timezone(&Utc).format("%Y-%m-%d").to_string());
    }
    out.sort();
    out.dedup();
    out
}

fn utc_date_from_ms(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| Utc::now().format("%Y-%m-%d").to_string())
}

fn path_safe_segment(v: &str) -> String {
    v.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn tokyo_tick_path(root: &Path, row: &TokyoTick) -> PathBuf {
    let d = utc_date_from_ms(row.ts_tokyo_recv_ms);
    root.join("raw")
        .join(d)
        .join("symbol")
        .join(path_safe_segment(&row.symbol))
        .join("tokyo_binance_ticks.jsonl")
}

fn chainlink_tick_path(root: &Path, row: &ChainlinkTick) -> PathBuf {
    let d = utc_date_from_ms(row.ts_chainlink_recv_ms);
    root.join("raw")
        .join(d)
        .join("symbol")
        .join(path_safe_segment(&row.symbol))
        .join("chainlink_ticks.jsonl")
}

fn pm_books_symbol_tf_path(root: &Path, row: &PmBookRow) -> PathBuf {
    let d = utc_date_from_ms(row.ts_pm_recv_ms);
    root.join("raw")
        .join(d)
        .join("symbol")
        .join(path_safe_segment(&row.symbol))
        .join("timeframe")
        .join(path_safe_segment(&row.timeframe))
        .join("pm_books.jsonl")
}

fn pm_books_market_path(root: &Path, row: &PmBookRow) -> PathBuf {
    let d = utc_date_from_ms(row.ts_pm_recv_ms);
    root.join("raw")
        .join(d)
        .join("symbol")
        .join(path_safe_segment(&row.symbol))
        .join("timeframe")
        .join(path_safe_segment(&row.timeframe))
        .join("market")
        .join(path_safe_segment(&row.market_id))
        .join("pm_books.jsonl")
}

fn snapshot_symbol_tf_1s_path(root: &Path, row: &SnapshotRow) -> PathBuf {
    let d = utc_date_from_ms(row.ts_ms_utc);
    root.join("normalized")
        .join(d)
        .join("symbol")
        .join(path_safe_segment(&row.symbol))
        .join("timeframe")
        .join(path_safe_segment(&row.timeframe))
        .join("snapshot_1s.jsonl")
}

fn snapshot_market_1s_path(root: &Path, row: &SnapshotRow) -> PathBuf {
    let d = utc_date_from_ms(row.ts_ms_utc);
    root.join("normalized")
        .join(d)
        .join("symbol")
        .join(path_safe_segment(&row.symbol))
        .join("timeframe")
        .join(path_safe_segment(&row.timeframe))
        .join("market")
        .join(path_safe_segment(&row.market_id))
        .join("snapshot_1s.jsonl")
}

fn append_jsonl<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create dir {}", parent.display()))?;
    }
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    serde_json::to_writer(&mut f, value)?;
    f.write_all(b"\n")?;
    Ok(())
}

fn write_json_pretty(path: PathBuf, value: &serde_json::Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).ok();
    }
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(path, bytes)?;
    Ok(())
}

fn read_json_or(path: PathBuf, fallback: serde_json::Value) -> serde_json::Value {
    match fs::read(&path) {
        Ok(buf) => serde_json::from_slice::<serde_json::Value>(&buf).unwrap_or(fallback),
        Err(_) => fallback,
    }
}

fn latest_dated_file(root: &Path, bucket: &str, file: &str) -> Option<PathBuf> {
    let base = root.join(bucket);
    let mut dirs = fs::read_dir(&base)
        .ok()?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .map(|e| e.path())
        .collect::<Vec<_>>();
    dirs.sort();
    dirs.reverse();
    for d in dirs {
        let candidate = d.join(file);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

fn list_date_dirs(base: &Path) -> Vec<String> {
    let mut out = fs::read_dir(base)
        .ok()
        .into_iter()
        .flat_map(|it| it.filter_map(|e| e.ok()))
        .filter(|e| e.path().is_dir())
        .filter_map(|e| e.file_name().into_string().ok())
        .collect::<Vec<_>>();
    out.sort();
    out.reverse();
    out
}

fn latest_symbol_tf_snapshot_file(root: &Path, symbol: &str, timeframe: &str) -> Option<PathBuf> {
    let base = root.join("normalized");
    for date in list_date_dirs(&base) {
        let p1 = base
            .join(date)
            .join("symbol")
            .join(path_safe_segment(symbol))
            .join("timeframe")
            .join(path_safe_segment(timeframe))
            .join("snapshot_1s.jsonl");
        if p1.exists() {
            return Some(p1);
        }
    }
    None
}

fn history_snapshot_path(
    root: &Path,
    date: &str,
    symbol: &str,
    timeframe: &str,
    market_id: Option<&str>,
) -> PathBuf {
    let mut p = root
        .join("normalized")
        .join(date)
        .join("symbol")
        .join(path_safe_segment(symbol))
        .join("timeframe")
        .join(path_safe_segment(timeframe));
    if let Some(mid) = market_id {
        p = p.join("market").join(path_safe_segment(mid));
    }
    p.join("snapshot_1s.jsonl")
}

fn read_last_jsonl_rows(path: &Path, limit: usize) -> Vec<serde_json::Value> {
    let file = match OpenOptions::new().read(true).open(path) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };
    let reader = BufReader::new(file);
    let mut rows = reader
        .lines()
        .map_while(Result::ok)
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(&line).ok())
        .collect::<Vec<_>>();
    if rows.len() > limit {
        rows.drain(0..(rows.len() - limit));
    }
    rows
}

fn read_jsonl_filtered_rows(
    path: &Path,
    from_ms: Option<i64>,
    to_ms: Option<i64>,
    limit: usize,
) -> Vec<serde_json::Value> {
    let file = match OpenOptions::new().read(true).open(path) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };
    let reader = BufReader::new(file);
    let mut out = Vec::new();
    for line in reader.lines().map_while(Result::ok) {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        let ts = v.get("ts_ms_utc").and_then(|x| x.as_i64());
        if let Some(fm) = from_ms {
            if ts.map(|t| t < fm).unwrap_or(false) {
                continue;
            }
        }
        if let Some(tm) = to_ms {
            if ts.map(|t| t > tm).unwrap_or(false) {
                continue;
            }
        }
        out.push(v);
        if out.len() > limit {
            out.drain(0..(out.len() - limit));
        }
    }
    out
}
