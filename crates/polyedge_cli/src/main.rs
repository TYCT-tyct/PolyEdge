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
    about = "PolyEdge 运维命令行",
    long_about = "统一入口：Paper/Live/Forge/Data 控制与健康检查。"
)]
struct Cli {
    /// 配置文件路径（默认：configs/polyedge_cli.toml）
    #[arg(long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// 控制 Paper 模拟运行
    Paper {
        #[arg(value_enum)]
        action: ServiceAction,
    },
    /// 控制 Live 实盘执行
    Live {
        #[arg(value_enum)]
        action: ServiceAction,
    },
    /// 控制 Forge 服务（Dashboard/API）
    Forge {
        #[arg(value_enum)]
        action: ServiceAction,
    },
    /// 控制数据采集（持久化 + 自检定时器）
    Data {
        #[arg(value_enum)]
        action: ServiceAction,
    },
    /// 健康检查：本地依赖 + SSH + 远程服务 + 端口
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
            ServiceAction::Start => "启动",
            ServiceAction::Stop => "停止",
            ServiceAction::Restart => "重启",
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
            ok("Paper 模拟已开启（运行循环 ON）。");
        }
        ServiceAction::Stop => {
            set_ireland_env(
                cfg,
                "30-polyedge-paper.conf",
                &[("FORGE_FEV1_RUNTIME_ENABLED", "false")],
                true,
            )?;
            ok("Paper 模拟已关闭（运行循环 OFF）。");
        }
        ServiceAction::Restart => {
            restart_ireland_forge(cfg)?;
            ok("Paper 运行已重启。");
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
            ok("Live 实盘已开启。");
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
            warn("Live 已切换为排空模式：不再开新仓，仅允许已有仓位退出。");
        }
        ServiceAction::Restart => {
            restart_ireland_forge(cfg)?;
            ok("Live 链路已重启。");
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
            ok("Forge + Dashboard 代理已启动。");
        }
        ServiceAction::Stop => {
            ssh_systemctl(
                cfg,
                HostTarget::Ireland,
                "stop",
                &[&cfg.forge_proxy_service_ireland, &cfg.forge_service_ireland],
            )?;
            ok("Forge + Dashboard 代理已停止。");
        }
        ServiceAction::Restart => {
            ssh_systemctl(
                cfg,
                HostTarget::Ireland,
                "restart",
                &[&cfg.forge_service_ireland, &cfg.forge_proxy_service_ireland],
            )?;
            ok("Forge + Dashboard 代理已重启。");
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
            ok("数据采集已启动，自检定时器已开启。");
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
            warn("数据采集已停止，自检定时器已关闭。");
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
            ok("数据相关服务已重启。");
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

    check_path("爱尔兰密钥", &cfg.ireland_key, &mut failures);
    check_path("东京密钥", &cfg.tokyo_key, &mut failures);

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
        Ok(_) => ok("爱尔兰健康接口正常"),
        Err(err) => {
            failures += 1;
            err_line(&format!("爱尔兰健康接口检查失败: {err}"));
        }
    }

    if failures == 0 {
        ok("全部检查通过。");
        Ok(())
    } else {
        bail!("共有 {failures} 项检查失败");
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
        Ok(out) if out.status.success() => ok(&format!("本地依赖 `{name}` 正常")),
        Ok(out) => {
            *failures += 1;
            err_line(&format!(
                "本地依赖 `{name}` 检查失败: exit={}",
                out.status.code().unwrap_or(-1)
            ));
        }
        Err(err) => {
            *failures += 1;
            err_line(&format!("本地依赖 `{name}` 缺失: {err}"));
        }
    }
}

fn check_path(label: &str, path: &str, failures: &mut u32) {
    if Path::new(path).exists() {
        ok(&format!("{label} 存在: {path}"));
    } else {
        *failures += 1;
        err_line(&format!("{label} 不存在: {path}"));
    }
}

fn check_remote_echo(cfg: &PolyedgeConfig, target: HostTarget, failures: &mut u32) {
    let name = match target {
        HostTarget::Ireland => "爱尔兰 SSH",
        HostTarget::Tokyo => "东京 SSH",
    };
    match ssh_exec(cfg, target, "echo ok") {
        Ok(_) => ok(&format!("{name} 正常")),
        Err(err) => {
            *failures += 1;
            err_line(&format!("{name} 失败: {err}"));
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
                ok(&format!("{service} 运行中"));
            } else {
                *failures += 1;
                err_line(&format!("{service} 非运行状态: {state}"));
            }
        }
        Err(err) => {
            *failures += 1;
            err_line(&format!("{service} 检查失败: {err}"));
        }
    }
}

fn ssh_exec(cfg: &PolyedgeConfig, target: HostTarget, remote_cmd: &str) -> Result<String> {
    let (host, key) = match target {
        HostTarget::Ireland => (&cfg.ireland_host, &cfg.ireland_key),
        HostTarget::Tokyo => (&cfg.tokyo_host, &cfg.tokyo_key),
    };
    if should_use_local_exec(target, key) {
        return local_exec(remote_cmd);
    }
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

fn should_use_local_exec(target: HostTarget, key_path: &str) -> bool {
    if std::env::var("POLYEDGE_CLI_LOCAL")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        return true;
    }
    matches!(target, HostTarget::Ireland) && !Path::new(key_path).exists()
}

fn local_exec(cmd: &str) -> Result<String> {
    let output = Command::new("bash")
        .arg("-lc")
        .arg(cmd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("failed to execute local command")?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        Err(anyhow!(
            "local command failed: {}",
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
    let scope_zh = match scope {
        "paper" => "模拟交易",
        "live" => "实盘交易",
        "forge" => "Forge 服务",
        "data" => "数据采集",
        "docker" => "环境体检",
        _ => scope,
    };
    println!("----------------------------------------");
    println!("PolyEdge · {scope_zh} · {action}");
    println!("----------------------------------------");
}

fn ok(msg: &str) {
    println!("[成功] {msg}");
}

fn warn(msg: &str) {
    println!("[警告] {msg}");
}

fn err_line(msg: &str) {
    eprintln!("[失败] {msg}");
}
