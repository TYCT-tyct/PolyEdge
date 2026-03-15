use std::process::Command;

fn run_help(args: &[&str]) -> String {
    let exe = env!("CARGO_BIN_EXE_forge_tokyo_relay");
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
fn top_level_help_exposes_link_flags() {
    let stdout = run_help(&["--help"]);
    assert!(stdout.contains("--ireland-udp"));
    assert!(stdout.contains("FORGE_IRELAND_UDP"));
}
