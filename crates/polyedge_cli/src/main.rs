use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
#[cfg(target_family = "unix")]
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::{Duration as ChronoDuration, NaiveDate, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
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
    #[arg(long, default_value = "BTC")]
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
    run_ssh(&base, &target, &format!("set -euo pipefail; cd {}; git fetch origin; git checkout {}; git pull --ff-only origin {}; ~/.cargo/bin/cargo build -p app_runner -p polyedge_cli -p polyedge_data_backend --release", base.repo, base.branch, base.branch))?;
    run_scp(
        &base,
        "scripts/setup_data_backend_systemd.sh",
        &format!("{target}:/tmp/setup_data_backend_systemd.sh"),
    )?;
    run_ssh(&base, &target, &format!("chmod +x /tmp/setup_data_backend_systemd.sh; POLYEDGE_REPO_DIR={} POLYEDGE_BIN_PATH={}/target/release/polyedge_data_backend POLYEDGE_USER={} bash /tmp/setup_data_backend_systemd.sh ireland", base.repo, base.repo, base.user))?;
    run_status_ireland(base)
}

fn run_status_ireland(base: RemoteBase) -> Result<()> {
    ensure_key(&base.key)?;
    let target = format!("{}@{}", base.user, base.host);
    run_ssh(&base, &target, &format!("set -e; echo '== app_runner =='; sudo systemctl --no-pager --full status polyedge.service | sed -n '1,16p'; echo; echo '== data_backend_ingest =='; sudo systemctl --no-pager --full status polyedge-data-backend-ireland.service | sed -n '1,20p'; echo; echo '== data_backend_api =='; sudo systemctl --no-pager --full status polyedge-data-backend-api.service | sed -n '1,20p'; echo; echo '== legacy_recorder (should be inactive)=='; sudo systemctl --no-pager --full status polyedge-recorder.service | sed -n '1,12p' || true; echo; df -h {} /dev/xvdbb || true", base.repo))
}

fn run_logs_ireland(args: RemoteLogs) -> Result<()> {
    let base = args.base;
    ensure_key(&base.key)?;
    let target = format!("{}@{}", base.user, base.host);
    run_ssh(
        &base,
        &target,
        &format!(
            "echo '== data_backend_ingest =='; sudo journalctl -u polyedge-data-backend-ireland.service -n {} --no-pager; echo; echo '== data_backend_api =='; sudo journalctl -u polyedge-data-backend-api.service -n {} --no-pager",
            args.lines.clamp(20, 5000),
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
