use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{anyhow, bail, Context, Result};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(
    name = "polyedge",
    version,
    about = "PolyEdge ops CLI",
    long_about = "Unified command entry for Paper/Live/Forge/Data control and health checks."
)]
struct Cli {
    /// Optional config path (default: configs/polyedge_cli.toml)
    #[arg(long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Control paper simulation runtime
    Paper {
        #[arg(value_enum)]
        action: ServiceAction,
    },
    /// Control live execution gate
    Live {
        #[arg(value_enum)]
        action: ServiceAction,
    },
    /// Control forge service (dashboard/API)
    Forge {
        #[arg(value_enum)]
        action: ServiceAction,
    },
    /// Control data collection (persistent + self-check timer)
    Data {
        #[arg(value_enum)]
        action: ServiceAction,
    },
    /// Health checks: local deps + SSH + remote service + port
    Docker,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ServiceAction {
    Start,
    Stop,
    Restart,
}

impl Display for ServiceAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ServiceAction::Start => "start",
            ServiceAction::Stop => "stop",
            ServiceAction::Restart => "restart",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug, Clone, Deserialize)]
struct PolyedgeConfig {
    ssh_user: String,
    ireland_host: String,
    ireland_key: String,
    tokyo_host: String,
    tokyo_key: String,
    forge_service_ireland: String,
    forge_proxy_service_ireland: String,
    data_service_tokyo: String,
    data_health_timer_ireland: String,
    api_bind_port_ireland: u16,
}

impl Default for PolyedgeConfig {
    fn default() -> Self {
        Self {
            ssh_user: "ubuntu".to_string(),
            ireland_host: "54.77.232.166".to_string(),
            ireland_key: r"C:\Users\Shini\Documents\PolyEdge.pem".to_string(),
            tokyo_host: "57.180.89.145".to_string(),
            tokyo_key: r"C:\Users\Shini\Documents\dongjing.pem".to_string(),
            forge_service_ireland: "polyedge-forge-ireland.service".to_string(),
            forge_proxy_service_ireland: "polyedge-9180-proxy.service".to_string(),
            data_service_tokyo: "polyedge-forge-tokyo.service".to_string(),
            data_health_timer_ireland: "polyedge-forge-healthcheck.timer".to_string(),
            api_bind_port_ireland: 9810,
        }
    }
}

#[derive(Clone, Copy)]
enum HostTarget {
    Ireland,
    Tokyo,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg_path = cli
        .config
        .unwrap_or_else(|| PathBuf::from("configs/polyedge_cli.toml"));
    let cfg = load_config(&cfg_path)?;

    match cli.command {
        None => {
            Cli::command().print_long_help()?;
            println!();
            Ok(())
        }
        Some(Commands::Paper { action }) => cmd_paper(&cfg, action),
        Some(Commands::Live { action }) => cmd_live(&cfg, action),
        Some(Commands::Forge { action }) => cmd_forge(&cfg, action),
        Some(Commands::Data { action }) => cmd_data(&cfg, action),
        Some(Commands::Docker) => cmd_docker(&cfg),
    }
}

fn load_config(path: &Path) -> Result<PolyedgeConfig> {
    if !path.exists() {
        return Ok(PolyedgeConfig::default());
    }
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read config: {}", path.display()))?;
    let cfg: PolyedgeConfig = toml::from_str(&raw)
        .with_context(|| format!("failed to parse config: {}", path.display()))?;
    Ok(cfg)
}

fn cmd_paper(cfg: &PolyedgeConfig, action: ServiceAction) -> Result<()> {
    print_header("paper", action);
    match action {
        ServiceAction::Start => {
            set_ireland_env(
                cfg,
                "30-polyedge-paper.conf",
                &[("FORGE_FEV1_RUNTIME_ENABLED", "true")],
                true,
            )?;
            ok("Paper simulation enabled (runtime loop ON).");
        }
        ServiceAction::Stop => {
            set_ireland_env(
                cfg,
                "30-polyedge-paper.conf",
                &[("FORGE_FEV1_RUNTIME_ENABLED", "false")],
                true,
            )?;
            ok("Paper simulation disabled (runtime loop OFF).");
        }
        ServiceAction::Restart => {
            restart_ireland_forge(cfg)?;
            ok("Paper runtime restarted.");
        }
    }
    Ok(())
}

fn cmd_live(cfg: &PolyedgeConfig, action: ServiceAction) -> Result<()> {
    print_header("live", action);
    match action {
        ServiceAction::Start => {
            set_ireland_env(
                cfg,
                "40-polyedge-live.conf",
                &[
                    ("FORGE_FEV1_LIVE_ENABLED", "true"),
                    ("FORGE_FEV1_LIVE_EXECUTE", "true"),
                    ("FORGE_FEV1_RUNTIME_DRAIN_ONLY", "false"),
                ],
                true,
            )?;
            ok("Live trading enabled.");
        }
        ServiceAction::Stop => {
            set_ireland_env(
                cfg,
                "40-polyedge-live.conf",
                &[
                    ("FORGE_FEV1_LIVE_ENABLED", "true"),
                    ("FORGE_FEV1_LIVE_EXECUTE", "true"),
                    ("FORGE_FEV1_RUNTIME_DRAIN_ONLY", "true"),
                ],
                true,
            )?;
            warn("Live switched to DRAIN mode: no new entries, existing positions can exit.");
        }
        ServiceAction::Restart => {
            restart_ireland_forge(cfg)?;
            ok("Live path restarted.");
        }
    }
    Ok(())
}

fn cmd_forge(cfg: &PolyedgeConfig, action: ServiceAction) -> Result<()> {
    print_header("forge", action);
    match action {
        ServiceAction::Start => {
            ssh_systemctl(
                cfg,
                HostTarget::Ireland,
                "start",
                &[&cfg.forge_service_ireland, &cfg.forge_proxy_service_ireland],
            )?;
            ok("Forge + dashboard proxy started.");
        }
        ServiceAction::Stop => {
            ssh_systemctl(
                cfg,
                HostTarget::Ireland,
                "stop",
                &[&cfg.forge_proxy_service_ireland, &cfg.forge_service_ireland],
            )?;
            ok("Forge + dashboard proxy stopped.");
        }
        ServiceAction::Restart => {
            ssh_systemctl(
                cfg,
                HostTarget::Ireland,
                "restart",
                &[&cfg.forge_service_ireland, &cfg.forge_proxy_service_ireland],
            )?;
            ok("Forge + dashboard proxy restarted.");
        }
    }
    Ok(())
}

fn cmd_data(cfg: &PolyedgeConfig, action: ServiceAction) -> Result<()> {
    print_header("data", action);
    match action {
        ServiceAction::Start => {
            ssh_systemctl(
                cfg,
                HostTarget::Tokyo,
                "enable --now",
                &[&cfg.data_service_tokyo],
            )?;
            ssh_systemctl(
                cfg,
                HostTarget::Ireland,
                "enable --now",
                &[&cfg.data_health_timer_ireland],
            )?;
            ok("Data collection started. Health timer enabled for self-heal.");
        }
        ServiceAction::Stop => {
            ssh_systemctl(
                cfg,
                HostTarget::Tokyo,
                "disable --now",
                &[&cfg.data_service_tokyo],
            )?;
            ssh_systemctl(
                cfg,
                HostTarget::Ireland,
                "disable --now",
                &[&cfg.data_health_timer_ireland],
            )?;
            warn("Data collection stopped and health timer disabled.");
        }
        ServiceAction::Restart => {
            ssh_systemctl(
                cfg,
                HostTarget::Tokyo,
                "restart",
                &[&cfg.data_service_tokyo],
            )?;
            ssh_systemctl(
                cfg,
                HostTarget::Ireland,
                "restart",
                &[&cfg.data_health_timer_ireland],
            )?;
            ok("Data services restarted.");
        }
    }
    Ok(())
}

fn cmd_docker(cfg: &PolyedgeConfig) -> Result<()> {
    print_header("docker", ServiceAction::Start);
    let mut failures = 0_u32;

    check_local_cmd("ssh", &["-V"], &mut failures);
    check_local_cmd("git", &["--version"], &mut failures);
    check_local_cmd("cargo", &["-V"], &mut failures);

    check_path("ireland key", &cfg.ireland_key, &mut failures);
    check_path("tokyo key", &cfg.tokyo_key, &mut failures);

    check_remote_echo(cfg, HostTarget::Ireland, &mut failures);
    check_remote_echo(cfg, HostTarget::Tokyo, &mut failures);

    check_remote_service(
        cfg,
        HostTarget::Ireland,
        &cfg.forge_service_ireland,
        &mut failures,
    );
    check_remote_service(
        cfg,
        HostTarget::Ireland,
        &cfg.forge_proxy_service_ireland,
        &mut failures,
    );
    check_remote_service(
        cfg,
        HostTarget::Tokyo,
        &cfg.data_service_tokyo,
        &mut failures,
    );

    let health_cmd = format!(
        "curl -s -S -m 8 http://127.0.0.1:{}/health/live >/dev/null",
        cfg.api_bind_port_ireland
    );
    match ssh_exec(cfg, HostTarget::Ireland, &health_cmd) {
        Ok(_) => ok("ireland health endpoint OK"),
        Err(err) => {
            failures += 1;
            err_line(&format!("ireland health endpoint failed: {err}"));
        }
    }

    if failures == 0 {
        ok("All checks passed.");
        Ok(())
    } else {
        bail!("{failures} checks failed");
    }
}

fn restart_ireland_forge(cfg: &PolyedgeConfig) -> Result<()> {
    ssh_systemctl(
        cfg,
        HostTarget::Ireland,
        "restart",
        &[&cfg.forge_service_ireland],
    )
}

fn set_ireland_env(
    cfg: &PolyedgeConfig,
    dropin_file: &str,
    envs: &[(&str, &str)],
    restart_service: bool,
) -> Result<()> {
    let mut body = String::from("[Service]\n");
    for (k, v) in envs {
        body.push_str(&format!("Environment={k}={v}\n"));
    }
    let dst = format!(
        "/etc/systemd/system/{}.d/{}",
        cfg.forge_service_ireland, dropin_file
    );

    let script = format!(
        "set -euo pipefail; \
         sudo mkdir -p /etc/systemd/system/{svc}.d; \
         cat <<EOF | sudo tee {dst} >/dev/null\n{body}EOF\n\
         sudo systemctl daemon-reload; \
         {restart}",
        svc = cfg.forge_service_ireland,
        dst = dst,
        body = body,
        restart = if restart_service {
            format!("sudo systemctl restart {}", cfg.forge_service_ireland)
        } else {
            "true".to_string()
        }
    );
    ssh_exec(cfg, HostTarget::Ireland, &script)?;
    Ok(())
}

fn ssh_systemctl(
    cfg: &PolyedgeConfig,
    target: HostTarget,
    action: &str,
    services: &[&str],
) -> Result<()> {
    let joined = services
        .iter()
        .map(|v| format!("{v} || true"))
        .collect::<Vec<_>>()
        .join(" ; ");
    let script = format!(
        "set -euo pipefail; \
         for svc in {svcs}; do sudo systemctl {action} \"$svc\" || true; done; \
         for svc in {svcs}; do systemctl is-enabled \"$svc\" >/dev/null 2>&1 || true; done; \
         true",
        svcs = services
            .iter()
            .map(|s| format!("\"{s}\""))
            .collect::<Vec<_>>()
            .join(" "),
        action = action
    );
    let _ = joined;
    ssh_exec(cfg, target, &script)?;
    Ok(())
}

fn check_local_cmd(name: &str, args: &[&str], failures: &mut u32) {
    let output = Command::new(name).args(args).output();
    match output {
        Ok(out) if out.status.success() => ok(&format!("local dependency `{name}` OK")),
        Ok(out) => {
            *failures += 1;
            err_line(&format!(
                "local dependency `{name}` failed: exit={}",
                out.status.code().unwrap_or(-1)
            ));
        }
        Err(err) => {
            *failures += 1;
            err_line(&format!("local dependency `{name}` missing: {err}"));
        }
    }
}

fn check_path(label: &str, path: &str, failures: &mut u32) {
    if Path::new(path).exists() {
        ok(&format!("{label} found: {path}"));
    } else {
        *failures += 1;
        err_line(&format!("{label} missing: {path}"));
    }
}

fn check_remote_echo(cfg: &PolyedgeConfig, target: HostTarget, failures: &mut u32) {
    let name = match target {
        HostTarget::Ireland => "ireland ssh",
        HostTarget::Tokyo => "tokyo ssh",
    };
    match ssh_exec(cfg, target, "echo ok") {
        Ok(_) => ok(&format!("{name} OK")),
        Err(err) => {
            *failures += 1;
            err_line(&format!("{name} failed: {err}"));
        }
    }
}

fn check_remote_service(
    cfg: &PolyedgeConfig,
    target: HostTarget,
    service: &str,
    failures: &mut u32,
) {
    let cmd = format!("systemctl is-active {service}");
    match ssh_exec(cfg, target, &cmd) {
        Ok(out) => {
            let state = out.trim();
            if state == "active" {
                ok(&format!("{service} active"));
            } else {
                *failures += 1;
                err_line(&format!("{service} not active: {state}"));
            }
        }
        Err(err) => {
            *failures += 1;
            err_line(&format!("{service} check failed: {err}"));
        }
    }
}

fn ssh_exec(cfg: &PolyedgeConfig, target: HostTarget, remote_cmd: &str) -> Result<String> {
    let (host, key) = match target {
        HostTarget::Ireland => (&cfg.ireland_host, &cfg.ireland_key),
        HostTarget::Tokyo => (&cfg.tokyo_host, &cfg.tokyo_key),
    };
    let remote = format!("{}@{}", cfg.ssh_user, host);
    let quoted = shell_single_quote(remote_cmd);
    let full = format!("bash -lc {quoted}");
    let output = Command::new("ssh")
        .arg("-o")
        .arg("BatchMode=yes")
        .arg("-o")
        .arg("ConnectTimeout=10")
        .arg("-i")
        .arg(key)
        .arg(remote)
        .arg(full)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("failed to execute ssh to {host}"))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        Err(anyhow!(
            "ssh command failed on {host}: {}",
            if detail.is_empty() {
                "unknown error".to_string()
            } else {
                detail
            }
        ))
    }
}

fn shell_single_quote(input: &str) -> String {
    let escaped = input.replace('\'', r#"'"'"'"#);
    format!("'{escaped}'")
}

fn print_header(scope: &str, action: ServiceAction) {
    println!("----------------------------------------");
    println!("polyedge {scope} {action}");
    println!("----------------------------------------");
}

fn ok(msg: &str) {
    println!("[OK ] {msg}");
}

fn warn(msg: &str) {
    println!("[WARN] {msg}");
}

fn err_line(msg: &str) {
    eprintln!("[ERR] {msg}");
}
