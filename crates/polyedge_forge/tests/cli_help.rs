use std::process::Command;

fn run_help(args: &[&str]) -> String {
    let exe = env!("CARGO_BIN_EXE_polyedge_forge");
    let out = Command::new(exe)
        .args(args)
        .output()
        .expect("run help command");
    assert!(
        out.status.success(),
        "help command failed: status={:?}, stderr={}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).to_string()
}

#[test]
fn top_level_help_lists_subcommands() {
    let stdout = run_help(&["--help"]);
    assert!(stdout.contains("tokyo-relay"));
    assert!(stdout.contains("ireland-recorder"));
    assert!(stdout.contains("ireland-api"));
}

#[test]
fn ireland_api_help_exposes_bind_flag() {
    let stdout = run_help(&["ireland-api", "--help"]);
    assert!(stdout.contains("--bind"));
    assert!(stdout.contains("FORGE_API_BIND"));
}

#[test]
fn tokyo_relay_help_exposes_link_flags() {
    let stdout = run_help(&["tokyo-relay", "--help"]);
    assert!(stdout.contains("--ireland-udp"));
    assert!(stdout.contains("FORGE_IRELAND_UDP"));
}
