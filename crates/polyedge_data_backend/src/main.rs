use std::collections::HashMap;
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
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use core_types::{BookTop, MarketFeed, RefPriceWsFeed};
use feed_polymarket::PolymarketFeed;
use feed_reference::MultiSourceRefFeed;
use futures::StreamExt;
use market_discovery::{DiscoveryConfig, MarketDiscovery, MarketDescriptor};
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::RwLock;

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
    #[arg(long, env = "POLYEDGE_DATA_ROOT", default_value = "/dev/xvdbb/polyedge-data")]
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
}

#[derive(clap::Args, Debug, Clone)]
struct ApiArgs {
    #[arg(long, env = "POLYEDGE_DATA_ROOT", default_value = "/dev/xvdbb/polyedge-data")]
    dataset_root: String,
    #[arg(long, env = "POLYEDGE_API_BIND", default_value = "0.0.0.0:8095")]
    bind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TokyoTick {
    ts_tokyo_recv_ms: i64,
    ts_exchange_ms: i64,
    symbol: String,
    binance_price: f64,
    source: String,
    source_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChainlinkTick {
    ts_chainlink_recv_ms: i64,
    ts_exchange_ms: i64,
    symbol: String,
    chainlink_price: f64,
    source_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MarketMeta {
    market_id: String,
    symbol: String,
    timeframe: String,
    title: String,
    end_date: Option<String>,
    target_price: Option<f64>,
    target_source: String,
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
    ts_et: String,
    market_id: String,
    symbol: String,
    timeframe: String,
    title: String,
    target_price: Option<f64>,
    target_source: String,
    binance_price_tokyo: Option<f64>,
    chainlink_price: Option<f64>,
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
    net_ev_bps: Option<f64>,
    time_edge_ms: Option<f64>,
}

#[derive(Default)]
struct IngestState {
    tokyo_last: HashMap<String, TokyoTick>,
    tokyo_prev: HashMap<String, TokyoTick>,
    vel_prev: HashMap<String, (i64, f64)>,
    chainlink_last: HashMap<String, ChainlinkTick>,
    books: HashMap<String, PmBookRow>,
    meta: HashMap<String, MarketMeta>,
    target_fallback: HashMap<String, f64>,
}

#[derive(Clone)]
struct ApiState {
    root: PathBuf,
}

#[derive(Debug, Deserialize)]
struct SnapshotQuery {
    market_id: Option<String>,
    symbol: Option<String>,
    timeframe: Option<String>,
    limit: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    install_tracing();
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

async fn run_tokyo_collector(args: TokyoCollectorArgs) -> Result<()> {
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
            source: tick.source.to_string(),
            source_seq: tick.source_seq,
        };

        let payload = serde_json::to_vec(&row)?;
        let _ = sock.send_to(&payload, dst).await;
        append_jsonl(&dated_path(&out_root, "raw", "tokyo_binance_ticks.jsonl"), &row)?;
    }
}

async fn run_ireland_ingest(args: IrelandIngestArgs) -> Result<()> {
    let symbols = parse_symbols(&args.symbols);
    let timeframes = parse_lower_csv(&args.timeframes);
    let root = PathBuf::from(&args.dataset_root);
    fs::create_dir_all(root.join("raw")).ok();
    fs::create_dir_all(root.join("normalized")).ok();
    fs::create_dir_all(root.join("reports")).ok();

    tracing::info!(?symbols, ?timeframes, udp_bind = %args.udp_bind, root = %args.dataset_root, "ireland ingest started");

    let shared = Arc::new(RwLock::new(IngestState::default()));

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
                    let report = serde_json::json!({
                        "ts_ms": now_ms(),
                        "market_count": w.meta.len(),
                        "markets": w.meta.values().collect::<Vec<_>>()
                    });
                    let _ = write_json_pretty(discover_root.join("reports").join("latest_markets.json"), &report);
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
            }
            let _ = append_jsonl(&dated_path(&udp_root, "raw", "tokyo_binance_ticks.jsonl"), &row);
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
                let _ = append_jsonl(&dated_path(&pm_root, "raw", "pm_books.jsonl"), &row);
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
                    source_seq: tick.source_seq,
                };
                {
                    let mut w = ch_state.write().await;
                    w.chainlink_last.insert(row.symbol.clone(), row.clone());
                }
                let _ = append_jsonl(&dated_path(&ch_root, "raw", "chainlink_ticks.jsonl"), &row);
            }
            tracing::warn!("chainlink stream ended; restarting");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    let mut snap_tick = tokio::time::interval(Duration::from_millis(args.snapshot_ms.max(20)));
    loop {
        snap_tick.tick().await;
        let rows = build_snapshots(shared.clone()).await;
        if rows.is_empty() {
            continue;
        }
        for row in rows {
            append_jsonl(
                &dated_path(&root, "normalized", "snapshot_100ms.jsonl"),
                &row,
            )?;
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
    let et = ts_to_et(now);

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

        let target_price = if let Some(tp) = meta.target_price {
            Some(tp)
        } else if let Some(existing) = w.target_fallback.get(&market_id).copied() {
            Some(existing)
        } else if let Some(tk) = tokyo.as_ref() {
            w.target_fallback.insert(market_id.clone(), tk.binance_price);
            Some(tk.binance_price)
        } else {
            None
        };

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
            .map(|t| book.ts_pm_recv_ms as f64 - t.ts_tokyo_recv_ms as f64);

        let net_ev_bps = estimate_net_ev_bps(delta_pct, book.spread_yes, velocity);

        out.push(SnapshotRow {
            ts_ms_utc: now,
            ts_et: et.clone(),
            market_id,
            symbol: meta.symbol,
            timeframe: meta.timeframe,
            title: meta.title,
            target_price,
            target_source: if meta.target_price.is_some() {
                meta.target_source
            } else {
                "fallback_first_tokyo_price".to_string()
            },
            binance_price_tokyo: tokyo.map(|x| x.binance_price),
            chainlink_price: chainlink.map(|x| x.chainlink_price),
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
    MarketMeta {
        market_id: m.market_id,
        symbol: m.symbol,
        timeframe: m.timeframe.unwrap_or_else(|| "unknown".to_string()),
        title: m.question,
        end_date: m.end_date,
        target_price: target,
        target_source: if target.is_some() {
            "title_parse".to_string()
        } else {
            "none".to_string()
        },
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

async fn run_api(args: ApiArgs) -> Result<()> {
    let root = PathBuf::from(&args.dataset_root);
    let state = ApiState { root };

    let app = Router::new()
        .route("/health", get(api_health))
        .route("/api/live/markets", get(api_markets))
        .route("/api/live/snapshot", get(api_snapshot))
        .route("/api/live/latest", get(api_latest))
        .route("/api/live/trades", get(api_trades))
        .route("/api/live/heatmap", get(api_heatmap))
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

async fn api_markets(State(st): State<ApiState>) -> Json<serde_json::Value> {
    let path = st.root.join("reports").join("latest_markets.json");
    let payload = read_json_or(path, serde_json::json!({"markets": [], "market_count": 0, "ts_ms": now_ms()}));
    Json(payload)
}

async fn api_snapshot(
    State(st): State<ApiState>,
    Query(q): Query<SnapshotQuery>,
) -> Json<serde_json::Value> {
    let path = latest_dated_file(&st.root, "normalized", "snapshot_100ms.jsonl");
    let limit = q.limit.unwrap_or(300).clamp(1, 5000);
    let mut rows = path
        .as_ref()
        .map(|p| read_last_jsonl_rows(p, limit))
        .unwrap_or_default();
    if let Some(mid) = q.market_id.as_deref() {
        rows.retain(|r| r.get("market_id").and_then(|v| v.as_str()) == Some(mid));
    }
    if let Some(sym) = q.symbol.as_deref() {
        rows.retain(|r| r.get("symbol").and_then(|v| v.as_str()) == Some(sym));
    }
    if let Some(tf) = q.timeframe.as_deref() {
        rows.retain(|r| r.get("timeframe").and_then(|v| v.as_str()) == Some(tf));
    }
    Json(serde_json::json!({"ts_ms": now_ms(), "count": rows.len(), "rows": rows}))
}

async fn api_latest(State(st): State<ApiState>) -> Json<serde_json::Value> {
    let path = latest_dated_file(&st.root, "normalized", "snapshot_100ms.jsonl");
    let row = path
        .as_ref()
        .and_then(|p| read_last_jsonl_rows(p, 1).into_iter().next())
        .unwrap_or_else(|| serde_json::json!({}));
    Json(serde_json::json!({"ts_ms": now_ms(), "row": row}))
}

async fn api_trades(State(st): State<ApiState>) -> Json<serde_json::Value> {
    let path = latest_dated_file(&st.root, "normalized", "order_fill_events.jsonl");
    let rows = path
        .as_ref()
        .map(|p| read_last_jsonl_rows(p, 500))
        .unwrap_or_default();
    Json(serde_json::json!({"ts_ms": now_ms(), "count": rows.len(), "rows": rows}))
}

async fn api_heatmap(State(st): State<ApiState>) -> Json<serde_json::Value> {
    let path = latest_dated_file(&st.root, "normalized", "snapshot_100ms.jsonl");
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

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn ts_to_et(ms: i64) -> String {
    if let Some(dt) = DateTime::<Utc>::from_timestamp_millis(ms) {
        dt.with_timezone(&chrono::FixedOffset::west_opt(5 * 3600).unwrap())
            .format("%Y-%m-%d %H:%M:%S ET")
            .to_string()
    } else {
        "invalid".to_string()
    }
}

fn parse_end_ms(v: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(v)
        .ok()
        .map(|d| d.timestamp_millis())
}

fn dated_path(root: &Path, bucket: &str, file: &str) -> PathBuf {
    let date = Utc::now().format("%Y-%m-%d").to_string();
    root.join(bucket).join(date).join(file)
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
