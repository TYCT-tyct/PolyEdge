use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
#[cfg(target_family = "unix")]
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use chrono::{Duration as ChronoDuration, NaiveDate, Utc};
use clap::{Args, Parser, Subcommand};
use core_types::MarketFeed;
use feed_polymarket::PolymarketFeed;
use futures::StreamExt;
use market_discovery::{DiscoveryConfig, MarketDiscovery};
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
    Recorder(RecorderArgs),
    Storage(StorageArgs),
    Git(GitArgs),
    Remote(RemoteArgs),
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
    #[arg(long, env = "POLYEDGE_RECORDER_ROOT", default_value = "datasets/recorder")]
    dataset_root: String,
    #[arg(long, env = "POLYEDGE_RECORDER_SYMBOLS", default_value = "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT")]
    symbols: String,
    #[arg(long, env = "POLYEDGE_RECORDER_MARKET_TYPES", default_value = "updown")]
    market_types: String,
    #[arg(long, env = "POLYEDGE_RECORDER_TIMEFRAMES", default_value = "5m,15m")]
    timeframes: String,
    #[arg(long, env = "POLYEDGE_RECORDER_WRITE_THROTTLE_MS", default_value_t = 40)]
    write_throttle_ms: i64,
    #[arg(long, env = "POLYEDGE_RECORDER_META_REFRESH_SEC", default_value_t = 20)]
    meta_refresh_sec: u64,
    #[arg(long, env = "POLYEDGE_RECORDER_STATUS_INTERVAL_SEC", default_value_t = 5)]
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
    #[arg(long, env = "POLYEDGE_RECORDER_SYMBOLS", default_value = "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT")]
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
    let cli = Cli::parse();
    let level = if cli.verbose { "debug" } else { "info" };
    let _ = tracing_subscriber::fmt().with_env_filter(level).try_init();
    match cli.command {
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
    let mut status_tick = tokio::time::interval(Duration::from_secs(args.status_interval_sec.max(2)));
    let mut gc_tick = tokio::time::interval(Duration::from_secs(args.storage_gc_interval_sec.max(30)));

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
                        "symbol": m.map(|x| x.symbol.clone()).unwrap_or_else(|| "UNKNOWN".to_string()),
                        "market_type": m.map(|x| x.market_type.clone()).unwrap_or_else(|| "unknown".to_string()),
                        "timeframe": m.map(|x| x.timeframe.clone()).unwrap_or_else(|| "unknown".to_string()),
                        "title": m.map(|x| x.title.clone()).unwrap_or_default(),
                        "bid_yes": book.bid_yes, "ask_yes": book.ask_yes,
                        "bid_no": book.bid_no, "ask_no": book.ask_no,
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

async fn discover_meta(symbols: &[String], market_types: &[String], timeframes: &[String]) -> Result<HashMap<String, MetaRow>> {
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
                    out.insert(m.market_id, MetaRow {
                        symbol: m.symbol.trim_end_matches("USDT").to_string(),
                        timeframe: m.timeframe.unwrap_or_else(|| "unknown".to_string()),
                        market_type: m.market_type.unwrap_or_else(|| "unknown".to_string()),
                        title: m.question,
                    });
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

fn expected_templates(symbols: &[String], market_types: &[String], timeframes: &[String]) -> HashSet<String> {
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
    meta.values().map(|m| format!("{}|{}|{}", m.symbol, m.market_type, m.timeframe)).collect()
}

fn run_git_audit(json: bool) -> Result<()> {
    let out = Command::new("git").args(["status", "--short"]).output().context("git status --short")?;
    if !out.status.success() { return Err(anyhow!("git status failed")); }
    let text = String::from_utf8_lossy(&out.stdout);
    let mut grouped = BTreeMap::<String, usize>::new();
    let mut rows = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() { continue; }
        let status = line.chars().take(2).collect::<String>().trim().to_string();
        let path = line.get(3..).unwrap_or_default().trim().to_string();
        let cat = classify_path(&path).to_string();
        *grouped.entry(cat.clone()).or_default() += 1;
        rows.push((status, cat, path));
    }
    let payload = serde_json::json!({"ts_ms": Utc::now().timestamp_millis(), "total": rows.len(), "groups": grouped, "rows": rows});
    if json { println!("{}", serde_json::to_string_pretty(&payload)?); }
    else {
        println!("Dirty files: {}", payload["total"]);
        println!("{}", serde_json::to_string_pretty(&payload["groups"])?);
    }
    Ok(())
}

fn run_deploy_ireland(base: RemoteBase) -> Result<()> {
    ensure_key(&base.key)?;
    let target = format!("{}@{}", base.user, base.host);
    run_ssh(&base, &target, &format!("set -euo pipefail; export PATH=\\\"$HOME/.cargo/bin:$PATH\\\"; cd {}; git fetch origin; git checkout {}; git pull --ff-only origin {}; cargo build -p app_runner -p polyedge_cli --release", base.repo, base.branch, base.branch))?;
    run_scp(&base, "scripts/setup_recorder_systemd.sh", &format!("{target}:/tmp/setup_recorder_systemd.sh"))?;
    run_scp(&base, "ops/systemd/polyedge-recorder.service", &format!("{target}:/tmp/polyedge-recorder.service"))?;
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
    run_ssh(&base, &target, &format!("sudo journalctl -u polyedge-recorder.service -n {} --no-pager", args.lines.clamp(20, 5000)))
}

fn run_ssh(base: &RemoteBase, target: &str, remote_cmd: &str) -> Result<()> {
    run_cmd("ssh", &[
        "-i".into(), base.key.clone(),
        "-o".into(), "StrictHostKeyChecking=accept-new".into(),
        "-o".into(), "ServerAliveInterval=20".into(),
        target.into(), remote_cmd.into()
    ], base.dry_run)
}

fn run_scp(base: &RemoteBase, src: &str, dst: &str) -> Result<()> {
    run_cmd("scp", &[
        "-i".into(), base.key.clone(),
        "-o".into(), "StrictHostKeyChecking=accept-new".into(),
        src.into(), dst.into()
    ], base.dry_run)
}

fn run_cmd(program: &str, args: &[String], dry_run: bool) -> Result<()> {
    let line = format!("{program} {}", args.join(" "));
    println!("{line}");
    if dry_run { return Ok(()); }
    let st = Command::new(program).args(args).status().with_context(|| line.clone())?;
    if !st.success() { return Err(anyhow!("command failed: {line}")); }
    Ok(())
}

fn ensure_key(path: &str) -> Result<()> {
    if !PathBuf::from(path).exists() { return Err(anyhow!("ssh key not found: {path}")); }
    Ok(())
}

fn normalize_symbols(s: &str) -> Vec<String> {
    s.split(',').map(|v| v.trim().to_ascii_uppercase()).filter(|v| !v.is_empty()).map(|v| if v.ends_with("USDT") { v } else { format!("{v}USDT") }).collect()
}

fn parse_csv_lower(s: &str) -> Vec<String> {
    s.split(',').map(|v| v.trim().to_ascii_lowercase()).filter(|v| !v.is_empty()).collect()
}

fn classify_path(path: &str) -> &'static str {
    if path.starts_with("configs/") { return "config"; }
    if path.starts_with("crates/app_runner/") { return "app_runner"; }
    if path.starts_with("crates/") { return "rust_core"; }
    if path.starts_with("scripts/") || path.ends_with(".ps1") || path.ends_with(".sh") { return "ops_script"; }
    if path.contains("ai_slop") || path.ends_with(".txt") { return "artifact"; }
    "other"
}

fn dated_path(root: &Path, bucket: &str, file: &str) -> PathBuf {
    root.join(bucket).join(Utc::now().format("%Y-%m-%d").to_string()).join(file)
}

fn append_jsonl(path: &Path, line: &str) {
    if let Some(parent) = path.parent() { fs::create_dir_all(parent).ok(); }
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(f, "{line}");
    }
}

#[derive(Debug, Serialize)]
struct GcSummary { before_used_pct: Option<f64>, after_used_pct: Option<f64>, removed_dirs: u64, removed_bytes: u64 }

fn gc_once(root: &Path, max_used_pct: f64, keep_raw_days: i64, keep_reports_days: i64) -> GcSummary {
    let mut out = GcSummary { before_used_pct: fs_used_pct(root), after_used_pct: None, removed_dirs: 0, removed_bytes: 0 };
    let today = Utc::now().date_naive();
    clean_older_than(&root.join("raw"), today - ChronoDuration::days(keep_raw_days.max(0)), &mut out);
    clean_older_than(&root.join("reports"), today - ChronoDuration::days(keep_reports_days.max(0)), &mut out);
    for _ in 0..24 {
        let Some(used) = fs_used_pct(root) else { break };
        if used <= max_used_pct { break; }
        if !remove_oldest(&root.join("raw"), today, &mut out) && !remove_oldest(&root.join("reports"), today, &mut out) { break; }
    }
    out.after_used_pct = fs_used_pct(root);
    out
}

fn clean_older_than(root: &Path, threshold: NaiveDate, out: &mut GcSummary) {
    let Ok(entries) = fs::read_dir(root) else { return; };
    for e in entries.flatten() {
        let p = e.path();
        if !p.is_dir() { continue; }
        let Some(name) = p.file_name().and_then(|v| v.to_str()) else { continue; };
        let Ok(date) = NaiveDate::parse_from_str(name, "%Y-%m-%d") else { continue; };
        if date < threshold {
            let bytes = remove_dir_with_size(&p);
            if bytes > 0 { out.removed_dirs += 1; out.removed_bytes += bytes; }
        }
    }
}

fn remove_oldest(root: &Path, today: NaiveDate, out: &mut GcSummary) -> bool {
    let Ok(entries) = fs::read_dir(root) else { return false; };
    let mut dirs = Vec::<(NaiveDate, PathBuf)>::new();
    for e in entries.flatten() {
        let p = e.path();
        if !p.is_dir() { continue; }
        let Some(name) = p.file_name().and_then(|v| v.to_str()) else { continue; };
        let Ok(date) = NaiveDate::parse_from_str(name, "%Y-%m-%d") else { continue; };
        if date < today { dirs.push((date, p)); }
    }
    dirs.sort_by_key(|x| x.0);
    if let Some((_, p)) = dirs.into_iter().next() {
        let bytes = remove_dir_with_size(&p);
        if bytes > 0 { out.removed_dirs += 1; out.removed_bytes += bytes; return true; }
    }
    false
}

fn remove_dir_with_size(path: &Path) -> u64 {
    let bytes = dir_size(path);
    if fs::remove_dir_all(path).is_ok() { bytes } else { 0 }
}

fn dir_size(path: &Path) -> u64 {
    let Ok(meta) = fs::symlink_metadata(path) else { return 0; };
    if meta.is_file() { return meta.len(); }
    let mut total = 0_u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = fs::read_dir(d) else { continue; };
        for e in entries.flatten() {
            let p = e.path();
            let Ok(m) = fs::symlink_metadata(&p) else { continue; };
            if m.is_file() { total += m.len(); } else if m.is_dir() { stack.push(p); }
        }
    }
    total
}

#[cfg(target_family = "unix")]
fn fs_used_pct(path: &Path) -> Option<f64> {
    use std::ffi::CString;
    let cpath = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(cpath.as_ptr(), &mut st as *mut libc::statvfs) } != 0 || st.f_blocks == 0 { return None; }
    let frsize = if st.f_frsize > 0 { st.f_frsize } else { st.f_bsize } as f64;
    let total = st.f_blocks as f64 * frsize;
    let avail = st.f_bavail as f64 * frsize;
    if total <= 0.0 { return None; }
    Some(((total - avail) / total * 100.0).clamp(0.0, 100.0))
}

#[cfg(not(target_family = "unix"))]
fn fs_used_pct(_path: &Path) -> Option<f64> { None }
