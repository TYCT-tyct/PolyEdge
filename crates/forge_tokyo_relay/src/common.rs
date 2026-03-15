pub fn install_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "info,forge_tokyo_relay=debug".to_string()),
        )
        .try_init();
}

pub fn parse_upper_csv(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|v| v.trim().to_ascii_uppercase())
        .filter(|v| !v.is_empty())
        .collect()
}
