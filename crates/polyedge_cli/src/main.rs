use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
#[cfg(target_family = "unix")]
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use chrono::{Duration as ChronoDuration, NaiveDate, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use core_types::MarketFeed;
use feed_polymarket::PolymarketFeed;
use futures::StreamExt;
use market_discovery::{DiscoveryConfig, MarketDiscovery};
use reqwest::Client;
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(name = "polyedge", about = "PolyEdge operations CLI", version)]
struct Cli {
    #[arg(long, global = true, default_value_t = false)]
    verbose: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Strategy(StrategyArgs),
    Markets(MarketsArgs),
    Paper(PaperArgs),
    Report(ReportArgs),
    Recorder(RecorderArgs),
    Storage(StorageArgs),
    Git(GitArgs),
    Remote(RemoteArgs),
}

#[derive(Debug, Clone, ValueEnum)]
enum EngineModeArg {
    LegacyV52,
    RollV1,
}

impl EngineModeArg {
    fn as_mode_str(&self) -> &'static str {
        match self {
            Self::LegacyV52 => "legacy_v52",
            Self::RollV1 => "roll_v1",
        }
    }
}

#[derive(Debug, Clone, ValueEnum)]
enum PaperProfile {
    Smoke10,
    Stage20,
    Stage30,
}

#[derive(Args, Debug)]
struct StrategyArgs {
    #[command(subcommand)]
    command: StrategyCommand,
}

#[derive(Subcommand, Debug)]
enum StrategyCommand {
    ModeSet(StrategyModeSet),
}

#[derive(Args, Debug, Clone)]
struct StrategyModeSet {
    #[arg(long, value_enum)]
    engine: EngineModeArg,
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    base_url: String,
}

#[derive(Args, Debug)]
struct MarketsArgs {
    #[command(subcommand)]
    command: MarketsCommand,
}

#[derive(Subcommand, Debug)]
enum MarketsCommand {
    Active(MarketsActive),
}

#[derive(Args, Debug, Clone)]
struct MarketsActive {
    #[arg(long, default_value = "BTC,ETH,SOL,XRP")]
    symbols: String,
    #[arg(long, default_value = "updown")]
    market_types: String,
    #[arg(long, default_value = "5m,15m")]
    tfs: String,
    #[arg(long, default_value_t = false)]
    json: bool,
}

#[derive(Args, Debug)]
struct PaperArgs {
    #[command(subcommand)]
    command: PaperCommand,
}

#[derive(Subcommand, Debug)]
enum PaperCommand {
    Run(PaperRun),
}

#[derive(Args, Debug, Clone)]
struct PaperRun {
    #[arg(long, value_enum, default_value_t = PaperProfile::Smoke10)]
    profile: PaperProfile,
    #[arg(long, default_value_t = 1)]
    instances: u32,
    #[arg(long, default_value = "datasets/paper_runs/roll_v1")]
    dataset_root: String,
    #[arg(long, default_value_t = false)]
    background: bool,
}

#[derive(Args, Debug)]
struct ReportArgs {
    #[command(subcommand)]
    command: ReportCommand,
}

#[derive(Subcommand, Debug)]
enum ReportCommand {
    Gates(ReportGates),
    Pnl(ReportPnl),
}

#[derive(Args, Debug, Clone)]
struct ReportGates {
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    base_url: String,
}

#[derive(Args, Debug, Clone)]
struct ReportPnl {
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    base_url: String,
}

#[derive(Args, Debug)]
struct RecorderArgs {
    #[command(subcommand)]
    command: RecorderCommand,
}

#[derive(Subcommand, Debug)]
enum RecorderCommand {
    Run(RecorderRun),
    Once(RecorderOnce),
}

#[derive(Args, Debug, Clone)]
struct RecorderRun {
    #[arg(
        long,
        env = "POLYEDGE_RECORDER_ROOT",
        default_value = "datasets/recorder"
    )]
    dataset_root: String,
    #[arg(
        long,
        env = "POLYEDGE_RECORDER_SYMBOLS",
        default_value = "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT"
    )]
    symbols: String,
    #[arg(long, env = "POLYEDGE_RECORDER_MARKET_TYPES", default_value = "updown")]
    market_types: String,
    #[arg(long, env = "POLYEDGE_RECORDER_TIMEFRAMES", default_value = "5m,15m")]
    timeframes: String,
    #[arg(
        long,
        env = "POLYEDGE_RECORDER_WRITE_THROTTLE_MS",
        default_value_t = 40
    )]
    write_throttle_ms: i64,
    #[arg(long, env = "POLYEDGE_RECORDER_META_REFRESH_SEC", default_value_t = 20)]
    meta_refresh_sec: u64,
    #[arg(
        long,
        env = "POLYEDGE_RECORDER_STATUS_INTERVAL_SEC",
        default_value_t = 5
    )]
    status_interval_sec: u64,
    #[arg(long, env = "POLYEDGE_STORAGE_MAX_USED_PCT", default_value_t = 90.0)]
    storage_max_used_pct: f64,
    #[arg(long, env = "POLYEDGE_STORAGE_KEEP_RAW_DAYS", default_value_t = 2)]
    keep_raw_days: i64,
    #[arg(long, env = "POLYEDGE_STORAGE_KEEP_REPORTS_DAYS", default_value_t = 14)]
    keep_reports_days: i64,
    #[arg(long, env = "POLYEDGE_STORAGE_GC_INTERVAL_SEC", default_value_t = 300)]
    storage_gc_interval_sec: u64,
}

#[derive(Args, Debug, Clone)]
struct RecorderOnce {
    #[arg(
        long,
        env = "POLYEDGE_RECORDER_SYMBOLS",
        default_value = "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT"
    )]
    symbols: String,
    #[arg(long, env = "POLYEDGE_RECORDER_MARKET_TYPES", default_value = "updown")]
    market_types: String,
    #[arg(long, env = "POLYEDGE_RECORDER_TIMEFRAMES", default_value = "5m,15m")]
    timeframes: String,
}

#[derive(Args, Debug)]
struct StorageArgs {
    #[command(subcommand)]
    command: StorageCommand,
}

#[derive(Subcommand, Debug)]
enum StorageCommand {
    Gc(StorageGc),
}

#[derive(Args, Debug, Clone)]
struct StorageGc {
    #[arg(long, default_value = "datasets/recorder")]
    dataset_root: String,
    #[arg(long, default_value_t = 90.0)]
    max_used_pct: f64,
    #[arg(long, default_value_t = 2)]
    keep_raw_days: i64,
    #[arg(long, default_value_t = 14)]
    keep_reports_days: i64,
}

#[derive(Args, Debug)]
struct GitArgs {
    #[command(subcommand)]
    command: GitCommand,
}

#[derive(Subcommand, Debug)]
enum GitCommand {
    AuditDirty {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Args, Debug)]
struct RemoteArgs {
    #[command(subcommand)]
    command: RemoteCommand,
}

#[derive(Subcommand, Debug)]
enum RemoteCommand {
    DeployIreland(RemoteBase),
    StatusIreland(RemoteBase),
    LogsIreland(RemoteLogs),
}

#[derive(Args, Debug, Clone)]
struct RemoteBase {
    #[arg(long, default_value = "54.77.232.166")]
    host: String,
    #[arg(long, default_value = "ubuntu")]
    user: String,
    #[arg(long, default_value = "C:\\Users\\Shini\\Documents\\PolyEdge.pem")]
    key: String,
    #[arg(long, default_value = "/home/ubuntu/PolyEdge")]
    repo: String,
    #[arg(long, default_value = "feat/p1-wire-dual")]
    branch: String,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

#[derive(Args, Debug, Clone)]
struct RemoteLogs {
    #[command(flatten)]
    base: RemoteBase,
    #[arg(long, default_value_t = 200)]
    lines: u64,
}

#[derive(Debug, Clone)]
struct MetaRow {
    symbol: String,
    timeframe: String,
    market_type: String,
    title: String,
}

#[derive(Debug, Serialize)]
struct RecorderStatus {
    ts_ms: i64,
    uptime_sec: u64,
    seen_books: u64,
    written_rows: u64,
    throttled_rows: u64,
    discovered_markets: usize,
    template_expected_count: usize,
    template_discovered_count: usize,
    template_missing: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    install_rustls_provider();
    let cli = Cli::parse();
    let level = if cli.verbose { "debug" } else { "info" };
    let _ = tracing_subscriber::fmt().with_env_filter(level).try_init();
    match cli.command {
        Commands::Strategy(args) => match args.command {
            StrategyCommand::ModeSet(req) => run_strategy_mode_set(req).await,
        },
        Commands::Markets(args) => match args.command {
            MarketsCommand::Active(req) => run_markets_active(req).await,
        },
        Commands::Paper(args) => match args.command {
            PaperCommand::Run(req) => run_paper_profile(req),
        },
        Commands::Report(args) => match args.command {
            ReportCommand::Gates(req) => run_report_gates(req).await,
            ReportCommand::Pnl(req) => run_report_pnl(req).await,
        },
        Commands::Recorder(args) => match args.command {
            RecorderCommand::Run(run) => run_recorder(run).await,
            RecorderCommand::Once(once) => run_recorder_once(once).await,
        },
        Commands::Storage(args) => match args.command {
            StorageCommand::Gc(gc) => {
                let summary = gc_once(
                    &PathBuf::from(gc.dataset_root),
                    gc.max_used_pct,
                    gc.keep_raw_days,
                    gc.keep_reports_days,
                );
                println!("{}", serde_json::to_string_pretty(&summary)?);
                Ok(())
            }
        },
        Commands::Git(args) => match args.command {
            GitCommand::AuditDirty { json } => run_git_audit(json),
        },
        Commands::Remote(args) => match args.command {
            RemoteCommand::DeployIreland(base) => run_deploy_ireland(base),
            RemoteCommand::StatusIreland(base) => run_status_ireland(base),
            RemoteCommand::LogsIreland(args) => run_logs_ireland(args),
        },
    }
}

fn install_rustls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn repo_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."))
}

fn http_base(base_url: &str) -> String {
    base_url.trim_end_matches('/').to_string()
}

async fn http_get_json(base_url: &str, path: &str) -> Result<serde_json::Value> {
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let url = format!("{}{}", http_base(base_url), path);
    let resp = client.get(url).send().await?;
    let resp = resp.error_for_status()?;
    Ok(resp.json::<serde_json::Value>().await?)
}

async fn run_strategy_mode_set(req: StrategyModeSet) -> Result<()> {
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let url = format!("{}/control/switch_engine_mode", http_base(&req.base_url));
    let payload = serde_json::json!({ "engine_mode": req.engine.as_mode_str() });
    let resp = client.post(url).json(&payload).send().await?;
    let resp = resp.error_for_status()?;
    let body = resp.json::<serde_json::Value>().await?;
    println!("{}", serde_json::to_string_pretty(&body)?);
    Ok(())
}

async fn run_markets_active(req: MarketsActive) -> Result<()> {
    let symbols = normalize_symbols(&req.symbols);
    let market_types = parse_csv_lower(&req.market_types);
    let timeframes = parse_csv_lower(&req.tfs);
    let discovery = MarketDiscovery::new(DiscoveryConfig {
        symbols: symbols.clone(),
        market_types: market_types.clone(),
        timeframes: timeframes.clone(),
        ..DiscoveryConfig::default()
    });
    let markets = discovery.discover().await?;
    let mut rows = markets
        .into_iter()
        .map(|m| {
            serde_json::json!({
                "market_id": m.market_id,
                "symbol": m.symbol.trim_end_matches("USDT"),
                "timeframe": m.timeframe.unwrap_or_else(|| "unknown".to_string()),
                "market_type": m.market_type.unwrap_or_else(|| "unknown".to_string()),
                "title": m.question,
                "end_date": m.end_date.unwrap_or_default(),
                "best_bid": m.best_bid,
                "best_ask": m.best_ask
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        a["end_date"]
            .as_str()
            .unwrap_or_default()
            .cmp(b["end_date"].as_str().unwrap_or_default())
    });
    let mut by_tpl = BTreeMap::<String, usize>::new();
    for row in &rows {
        let tpl = format!(
            "{}|{}",
            row["symbol"].as_str().unwrap_or("UNKNOWN"),
            row["timeframe"].as_str().unwrap_or("unknown")
        );
        *by_tpl.entry(tpl).or_default() += 1;
    }
    if req.json {
        let payload = serde_json::json!({
            "ts_ms": Utc::now().timestamp_millis(),
            "symbols": symbols.iter().map(|s| s.trim_end_matches("USDT")).collect::<Vec<_>>(),
            "timeframes": timeframes,
            "total_markets": rows.len(),
            "by_symbol_timeframe": by_tpl,
            "markets": rows
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        println!("ts_ms={}", Utc::now().timestamp_millis());
        println!("total_markets={}", rows.len());
        println!("by_symbol_timeframe={}", serde_json::to_string(&by_tpl)?);
        for row in rows {
            println!(
                "{} | {} {} | {}",
                row["market_id"].as_str().unwrap_or(""),
                row["symbol"].as_str().unwrap_or(""),
                row["timeframe"].as_str().unwrap_or(""),
                row["title"].as_str().unwrap_or("")
            );
        }
    }
    Ok(())
}

fn paper_profile_config(profile: &PaperProfile) -> (&'static str, u64) {
    match profile {
        PaperProfile::Smoke10 => ("configs/paper-fast/stage20_coverage.toml", 10 * 60),
        PaperProfile::Stage20 => ("configs/paper-fast/stage20_balanced.toml", 20 * 60),
        PaperProfile::Stage30 => ("configs/paper-fast/stage30_diagnostic.toml", 30 * 60),
    }
}

fn python_program() -> String {
    for cand in ["python", "python3"] {
        let ok = Command::new(cand)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if ok {
            return cand.to_string();
        }
    }
    "python".to_string()
}

fn run_paper_profile(req: PaperRun) -> Result<()> {
    let (config1, duration) = paper_profile_config(&req.profile);
    let root = repo_root();
    let script = root.join("scripts").join("paper_runner.py");
    if !script.exists() {
        return Err(anyhow!(
            "paper runner script not found: {}",
            script.display()
        ));
    }
    let mut args = vec![
        script.to_string_lossy().to_string(),
        "--duration".to_string(),
        duration.to_string(),
        "--instances".to_string(),
        req.instances.max(1).to_string(),
        "--dataset-root".to_string(),
        req.dataset_root.clone(),
        "--config1".to_string(),
        config1.to_string(),
    ];
    if req.instances >= 2 {
        args.push("--config2".to_string());
        args.push("configs/paper-fast/stage20_quality.toml".to_string());
    }
    if req.instances >= 3 {
        args.push("--config3".to_string());
        args.push("configs/paper-fast/stage20_coverage.toml".to_string());
    }
    let python = python_program();
    if req.background {
        let log_root = PathBuf::from(&req.dataset_root).join("background");
        fs::create_dir_all(&log_root)?;
        let log_path = log_root.join(format!(
            "paper_{}_{}.log",
            match req.profile {
                PaperProfile::Smoke10 => "smoke10",
                PaperProfile::Stage20 => "stage20",
                PaperProfile::Stage30 => "stage30",
            },
            Utc::now().format("%Y%m%d_%H%M%S")
        ));
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;
        let stderr_file = log_file.try_clone()?;
        let mut cmd = Command::new(&python);
        cmd.args(&args)
            .current_dir(root)
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(stderr_file));
        let child = cmd
            .spawn()
            .with_context(|| "spawn paper runner in background")?;
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": "started",
                "pid": child.id(),
                "profile": format!("{:?}", req.profile),
                "duration_sec": duration,
                "log_path": log_path
            }))?
        );
        return Ok(());
    }
    run_cmd(&python, &args, false)
}

async fn run_report_gates(req: ReportGates) -> Result<()> {
    let payload = http_get_json(&req.base_url, "/report/gates/drop_reasons").await?;
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

async fn run_report_pnl(req: ReportPnl) -> Result<()> {
    let summary = http_get_json(&req.base_url, "/report/paper/summary")
        .await
        .unwrap_or_else(|_| serde_json::json!({"error":"unavailable"}));
    let live = http_get_json(&req.base_url, "/report/paper/live")
        .await
        .unwrap_or_else(|_| serde_json::json!({"error":"unavailable"}));
    let by_engine = http_get_json(&req.base_url, "/report/pnl/by_engine")
        .await
        .unwrap_or_else(|_| serde_json::json!({"error":"unavailable"}));
    let payload = serde_json::json!({
        "ts_ms": Utc::now().timestamp_millis(),
        "paper_live": live,
        "paper_summary": summary,
        "pnl_by_engine": by_engine
    });
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

async fn run_recorder_once(args: RecorderOnce) -> Result<()> {
    let symbols = normalize_symbols(&args.symbols);
    let market_types = parse_csv_lower(&args.market_types);
    let timeframes = parse_csv_lower(&args.timeframes);
    let metas = discover_meta(&symbols, &market_types, &timeframes).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "ts_ms": Utc::now().timestamp_millis(),
            "symbol_count": symbols.len(),
            "market_count": metas.len()
        }))?
    );
    Ok(())
}

async fn run_recorder(args: RecorderRun) -> Result<()> {
    let root = PathBuf::from(&args.dataset_root);
    fs::create_dir_all(root.join("raw")).ok();
    fs::create_dir_all(root.join("reports")).ok();

    let symbols = normalize_symbols(&args.symbols);
    let market_types = parse_csv_lower(&args.market_types);
    let timeframes = parse_csv_lower(&args.timeframes);
    let expected = expected_templates(&symbols, &market_types, &timeframes);
    let mut meta = discover_meta(&symbols, &market_types, &timeframes).await?;
    let mut last_emit_ms = HashMap::<String, i64>::new();

    let mut seen_books = 0_u64;
    let mut written_rows = 0_u64;
    let mut throttled_rows = 0_u64;
    let started = Instant::now();

    let mut meta_tick = tokio::time::interval(Duration::from_secs(args.meta_refresh_sec.max(5)));
    let mut status_tick =
        tokio::time::interval(Duration::from_secs(args.status_interval_sec.max(2)));
    let mut gc_tick =
        tokio::time::interval(Duration::from_secs(args.storage_gc_interval_sec.max(30)));

    loop {
        let feed = PolymarketFeed::new_with_universe(
            Duration::from_millis(50),
            symbols.clone(),
            market_types.clone(),
            timeframes.clone(),
        );
        let mut stream = feed.stream_books().await.context("start market stream")?;
        loop {
            tokio::select! {
                _ = meta_tick.tick() => {
                    if let Ok(next) = discover_meta(&symbols, &market_types, &timeframes).await {
                        meta = next;
                    }
                }
                _ = status_tick.tick() => {
                    let discovered = discovered_templates(&meta);
                    let mut missing = expected.difference(&discovered).cloned().collect::<Vec<_>>();
                    missing.sort();
                    let status = RecorderStatus {
                        ts_ms: Utc::now().timestamp_millis(),
                        uptime_sec: started.elapsed().as_secs(),
                        seen_books,
                        written_rows,
                        throttled_rows,
                        discovered_markets: meta.len(),
                        template_expected_count: expected.len(),
                        template_discovered_count: discovered.len(),
                        template_missing: missing,
                    };
                    let s = serde_json::to_vec_pretty(&status)?;
                    let p1 = dated_path(&root, "reports", "recorder_status_latest.json");
                    if let Some(parent) = p1.parent() { fs::create_dir_all(parent).ok(); }
                    fs::write(&p1, &s).ok();
                    fs::write(root.join("reports").join("recorder_status_latest.json"), &s).ok();
                }
                _ = gc_tick.tick() => {
                    let summary = gc_once(&root, args.storage_max_used_pct, args.keep_raw_days, args.keep_reports_days);
                    append_jsonl(&dated_path(&root, "reports", "storage_gc.jsonl"), &serde_json::json!({
                        "ts_ms": Utc::now().timestamp_millis(),
                        "summary": summary
                    }).to_string());
                }
                next = stream.next() => {
                    let Some(next) = next else { break; };
                    let book = match next { Ok(v) => v, Err(_) => continue };
                    seen_books = seen_books.saturating_add(1);
                    let now_ms = Utc::now().timestamp_millis();
                    if let Some(prev) = last_emit_ms.get(&book.market_id) {
                        if now_ms.saturating_sub(*prev) < args.write_throttle_ms {
                            throttled_rows = throttled_rows.saturating_add(1);
                            continue;
                        }
                    }
                    last_emit_ms.insert(book.market_id.clone(), now_ms);
                    let m = meta.get(&book.market_id);
                    let row = serde_json::json!({
                        "ts_ms": now_ms,
                        "market_id": book.market_id,
                        "token_id_yes": book.token_id_yes,
                        "token_id_no": book.token_id_no,
                        "symbol": m.map(|x| x.symbol.clone()).unwrap_or_else(|| "UNKNOWN".to_string()),
                        "market_type": m.map(|x| x.market_type.clone()).unwrap_or_else(|| "unknown".to_string()),
                        "timeframe": m.map(|x| x.timeframe.clone()).unwrap_or_else(|| "unknown".to_string()),
                        "title": m.map(|x| x.title.clone()).unwrap_or_default(),
                        "bid_yes": book.bid_yes, "ask_yes": book.ask_yes,
                        "bid_no": book.bid_no, "ask_no": book.ask_no,
                        "mid_yes": ((book.bid_yes + book.ask_yes) * 0.5),
                        "mid_no": ((book.bid_no + book.ask_no) * 0.5),
                        "spread_yes": (book.ask_yes - book.bid_yes),
                        "spread_no": (book.ask_no - book.bid_no),
                        "bid_size_yes": book.bid_size_yes, "ask_size_yes": book.ask_size_yes,
                        "bid_size_no": book.bid_size_no, "ask_size_no": book.ask_size_no,
                        "ts_exchange_ms": book.ts_ms, "recv_ts_local_ns": book.recv_ts_local_ns
                    });
                    append_jsonl(&dated_path(&root, "raw", "market_tape.jsonl"), &row.to_string());
                    written_rows = written_rows.saturating_add(1);
                }
            }
        }
    }
}

async fn discover_meta(
    symbols: &[String],
    market_types: &[String],
    timeframes: &[String],
) -> Result<HashMap<String, MetaRow>> {
    let mut last_err = None;
    for attempt in 0..4 {
        let discovery = MarketDiscovery::new(DiscoveryConfig {
            symbols: symbols.to_vec(),
            market_types: market_types.to_vec(),
            timeframes: timeframes.to_vec(),
            ..DiscoveryConfig::default()
        });
        match discovery.discover().await {
            Ok(markets) => {
                let mut out = HashMap::new();
                for m in markets {
                    out.insert(
                        m.market_id,
                        MetaRow {
                            symbol: m.symbol.trim_end_matches("USDT").to_string(),
                            timeframe: m.timeframe.unwrap_or_else(|| "unknown".to_string()),
                            market_type: m.market_type.unwrap_or_else(|| "unknown".to_string()),
                            title: m.question,
                        },
                    );
                }
                return Ok(out);
            }
            Err(err) => {
                last_err = Some(err);
                if attempt < 3 {
                    tokio::time::sleep(Duration::from_millis(300 * (attempt + 1) as u64)).await;
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("market discovery failed")))
}

fn expected_templates(
    symbols: &[String],
    market_types: &[String],
    timeframes: &[String],
) -> HashSet<String> {
    let mut out = HashSet::new();
    for s in symbols {
        for mt in market_types {
            for tf in timeframes {
                out.insert(format!("{}|{}|{}", s.trim_end_matches("USDT"), mt, tf));
            }
        }
    }
    out
}

fn discovered_templates(meta: &HashMap<String, MetaRow>) -> HashSet<String> {
    meta.values()
        .map(|m| format!("{}|{}|{}", m.symbol, m.market_type, m.timeframe))
        .collect()
}

fn run_git_audit(json: bool) -> Result<()> {
    let out = Command::new("git")
        .args(["status", "--short"])
        .output()
        .context("git status --short")?;
    if !out.status.success() {
        return Err(anyhow!("git status failed"));
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let mut grouped = BTreeMap::<String, usize>::new();
    let mut rows = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let status = line.chars().take(2).collect::<String>().trim().to_string();
        let path = line.get(3..).unwrap_or_default().trim().to_string();
        let cat = classify_path(&path).to_string();
        *grouped.entry(cat.clone()).or_default() += 1;
        rows.push((status, cat, path));
    }
    let payload = serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "total": rows.len(), "groups": grouped, "rows": rows});
    if json {
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        println!("Dirty files: {}", payload["total"]);
        println!("{}", serde_json::to_string_pretty(&payload["groups"])?);
    }
    Ok(())
}

fn run_deploy_ireland(base: RemoteBase) -> Result<()> {
    ensure_key(&base.key)?;
    let target = format!("{}@{}", base.user, base.host);
    run_ssh(&base, &target, &format!("set -euo pipefail; cd {}; git fetch origin; git checkout {}; git pull --ff-only origin {}; ~/.cargo/bin/cargo build -p app_runner -p polyedge_cli --release", base.repo, base.branch, base.branch))?;
    run_scp(
        &base,
        "scripts/setup_recorder_systemd.sh",
        &format!("{target}:/tmp/setup_recorder_systemd.sh"),
    )?;
    run_scp(
        &base,
        "ops/systemd/polyedge-recorder.service",
        &format!("{target}:/tmp/polyedge-recorder.service"),
    )?;
    run_ssh(&base, &target, &format!("chmod +x /tmp/setup_recorder_systemd.sh; POLYEDGE_REPO_DIR={} POLYEDGE_BIN_PATH={}/target/release/polyedge POLYEDGE_USER={} bash /tmp/setup_recorder_systemd.sh", base.repo, base.repo, base.user))?;
    run_status_ireland(base)
}

fn run_status_ireland(base: RemoteBase) -> Result<()> {
    ensure_key(&base.key)?;
    let target = format!("{}@{}", base.user, base.host);
    run_ssh(&base, &target, &format!("set -e; echo '== app =='; sudo systemctl --no-pager --full status polyedge.service | sed -n '1,16p'; echo; echo '== recorder =='; sudo systemctl --no-pager --full status polyedge-recorder.service | sed -n '1,20p'; echo; df -h {} {}/datasets || true", base.repo, base.repo))
}

fn run_logs_ireland(args: RemoteLogs) -> Result<()> {
    let base = args.base;
    ensure_key(&base.key)?;
    let target = format!("{}@{}", base.user, base.host);
    run_ssh(
        &base,
        &target,
        &format!(
            "sudo journalctl -u polyedge-recorder.service -n {} --no-pager",
            args.lines.clamp(20, 5000)
        ),
    )
}

fn run_ssh(base: &RemoteBase, target: &str, remote_cmd: &str) -> Result<()> {
    run_cmd(
        "ssh",
        &[
            "-i".into(),
            base.key.clone(),
            "-o".into(),
            "StrictHostKeyChecking=accept-new".into(),
            "-o".into(),
            "ServerAliveInterval=20".into(),
            target.into(),
            remote_cmd.into(),
        ],
        base.dry_run,
    )
}

fn run_scp(base: &RemoteBase, src: &str, dst: &str) -> Result<()> {
    run_cmd(
        "scp",
        &[
            "-i".into(),
            base.key.clone(),
            "-o".into(),
            "StrictHostKeyChecking=accept-new".into(),
            src.into(),
            dst.into(),
        ],
        base.dry_run,
    )
}

fn run_cmd(program: &str, args: &[String], dry_run: bool) -> Result<()> {
    let line = format!("{program} {}", args.join(" "));
    println!("{line}");
    if dry_run {
        return Ok(());
    }
    let st = Command::new(program)
        .args(args)
        .status()
        .with_context(|| line.clone())?;
    if !st.success() {
        return Err(anyhow!("command failed: {line}"));
    }
    Ok(())
}

fn ensure_key(path: &str) -> Result<()> {
    if !PathBuf::from(path).exists() {
        return Err(anyhow!("ssh key not found: {path}"));
    }
    Ok(())
}

fn normalize_symbols(s: &str) -> Vec<String> {
    s.split(',')
        .map(|v| v.trim().to_ascii_uppercase())
        .filter(|v| !v.is_empty())
        .map(|v| {
            if v.ends_with("USDT") {
                v
            } else {
                format!("{v}USDT")
            }
        })
        .collect()
}

fn parse_csv_lower(s: &str) -> Vec<String> {
    s.split(',')
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty())
        .collect()
}

fn classify_path(path: &str) -> &'static str {
    if path.starts_with("configs/") {
        return "config";
    }
    if path.starts_with("crates/app_runner/") {
        return "app_runner";
    }
    if path.starts_with("crates/") {
        return "rust_core";
    }
    if path.starts_with("scripts/") || path.ends_with(".ps1") || path.ends_with(".sh") {
        return "ops_script";
    }
    if path.contains("ai_slop") || path.ends_with(".txt") {
        return "artifact";
    }
    "other"
}

fn dated_path(root: &Path, bucket: &str, file: &str) -> PathBuf {
    root.join(bucket)
        .join(Utc::now().format("%Y-%m-%d").to_string())
        .join(file)
}

fn append_jsonl(path: &Path, line: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).ok();
    }
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(f, "{line}");
    }
}

#[derive(Debug, Serialize)]
struct GcSummary {
    before_used_pct: Option<f64>,
    after_used_pct: Option<f64>,
    removed_dirs: u64,
    removed_bytes: u64,
}

fn gc_once(
    root: &Path,
    max_used_pct: f64,
    keep_raw_days: i64,
    keep_reports_days: i64,
) -> GcSummary {
    let mut out = GcSummary {
        before_used_pct: fs_used_pct(root),
        after_used_pct: None,
        removed_dirs: 0,
        removed_bytes: 0,
    };
    let today = Utc::now().date_naive();
    clean_older_than(
        &root.join("raw"),
        today - ChronoDuration::days(keep_raw_days.max(0)),
        &mut out,
    );
    clean_older_than(
        &root.join("reports"),
        today - ChronoDuration::days(keep_reports_days.max(0)),
        &mut out,
    );
    for _ in 0..24 {
        let Some(used) = fs_used_pct(root) else { break };
        if used <= max_used_pct {
            break;
        }
        if !remove_oldest(&root.join("raw"), today, &mut out)
            && !remove_oldest(&root.join("reports"), today, &mut out)
        {
            break;
        }
    }
    out.after_used_pct = fs_used_pct(root);
    out
}

fn clean_older_than(root: &Path, threshold: NaiveDate, out: &mut GcSummary) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for e in entries.flatten() {
        let p = e.path();
        if !p.is_dir() {
            continue;
        }
        let Some(name) = p.file_name().and_then(|v| v.to_str()) else {
            continue;
        };
        let Ok(date) = NaiveDate::parse_from_str(name, "%Y-%m-%d") else {
            continue;
        };
        if date < threshold {
            let bytes = remove_dir_with_size(&p);
            if bytes > 0 {
                out.removed_dirs += 1;
                out.removed_bytes += bytes;
            }
        }
    }
}

fn remove_oldest(root: &Path, today: NaiveDate, out: &mut GcSummary) -> bool {
    let Ok(entries) = fs::read_dir(root) else {
        return false;
    };
    let mut dirs = Vec::<(NaiveDate, PathBuf)>::new();
    for e in entries.flatten() {
        let p = e.path();
        if !p.is_dir() {
            continue;
        }
        let Some(name) = p.file_name().and_then(|v| v.to_str()) else {
            continue;
        };
        let Ok(date) = NaiveDate::parse_from_str(name, "%Y-%m-%d") else {
            continue;
        };
        if date < today {
            dirs.push((date, p));
        }
    }
    dirs.sort_by_key(|x| x.0);
    if let Some((_, p)) = dirs.into_iter().next() {
        let bytes = remove_dir_with_size(&p);
        if bytes > 0 {
            out.removed_dirs += 1;
            out.removed_bytes += bytes;
            return true;
        }
    }
    false
}

fn remove_dir_with_size(path: &Path) -> u64 {
    let bytes = dir_size(path);
    if fs::remove_dir_all(path).is_ok() {
        bytes
    } else {
        0
    }
}

fn dir_size(path: &Path) -> u64 {
    let Ok(meta) = fs::symlink_metadata(path) else {
        return 0;
    };
    if meta.is_file() {
        return meta.len();
    }
    let mut total = 0_u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = fs::read_dir(d) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            let Ok(m) = fs::symlink_metadata(&p) else {
                continue;
            };
            if m.is_file() {
                total += m.len();
            } else if m.is_dir() {
                stack.push(p);
            }
        }
    }
    total
}

#[cfg(target_family = "unix")]
fn fs_used_pct(path: &Path) -> Option<f64> {
    use std::ffi::CString;
    let cpath = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(cpath.as_ptr(), &mut st as *mut libc::statvfs) } != 0
        || st.f_blocks == 0
    {
        return None;
    }
    let frsize = if st.f_frsize > 0 {
        st.f_frsize
    } else {
        st.f_bsize
    } as f64;
    let total = st.f_blocks as f64 * frsize;
    let avail = st.f_bavail as f64 * frsize;
    if total <= 0.0 {
        return None;
    }
    Some(((total - avail) / total * 100.0).clamp(0.0, 100.0))
}

#[cfg(not(target_family = "unix"))]
fn fs_used_pct(_path: &Path) -> Option<f64> {
    None
}
