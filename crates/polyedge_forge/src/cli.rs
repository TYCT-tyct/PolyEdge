use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "polyedge-forge", version, about = "PolyEdge Forge backend")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    TokyoRelay(TokyoRelayArgs),
    IrelandRecorder(Box<IrelandRecorderArgs>),
    IrelandApi(Box<IrelandApiArgs>),
}

#[derive(clap::Args, Debug, Clone)]
pub struct TokyoRelayArgs {
    #[arg(
        long,
        env = "FORGE_SYMBOLS",
        default_value = "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT"
    )]
    pub symbols: String,
    #[arg(long, env = "FORGE_TOKYO_BIND", default_value = "0.0.0.0:0")]
    pub bind: String,
    #[arg(long, env = "FORGE_IRELAND_UDP", default_value = "10.0.3.123:9801")]
    pub ireland_udp: String,
    #[arg(long, env = "FORGE_UDP_REDUNDANCY", default_value_t = 2)]
    pub redundancy: u8,
}

#[derive(clap::Args, Debug, Clone)]
pub struct IrelandRecorderArgs {
    #[arg(long, env = "FORGE_DATA_ROOT", default_value = "/data/polyedge-forge")]
    pub data_root: String,
    #[arg(long, env = "FORGE_IRELAND_UDP_BIND", default_value = "0.0.0.0:9801")]
    pub udp_bind: String,
    #[arg(long, env = "FORGE_SAMPLE_MS", default_value_t = 100)]
    pub sample_ms: u64,
    #[arg(
        long,
        env = "FORGE_SUPPORTED_SYMBOLS",
        default_value = "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT"
    )]
    pub supported_symbols: String,
    #[arg(
        long,
        env = "FORGE_ACTIVE_SYMBOLS",
        default_value = "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT"
    )]
    pub active_symbols: String,
    #[arg(long, env = "FORGE_ACTIVE_TFS", default_value = "5m,15m")]
    pub active_timeframes: String,
    #[arg(long, env = "FORGE_ACTIVE_SYMBOL_TIMEFRAMES", default_value = "")]
    pub active_symbol_timeframes: String,
    #[arg(long, env = "FORGE_DISCOVERY_REFRESH_SEC", default_value_t = 5)]
    pub discovery_refresh_sec: u64,
    #[arg(long, env = "FORGE_CH_URL", default_value = "http://127.0.0.1:8123")]
    pub clickhouse_url: String,
    #[arg(long, env = "FORGE_CH_DATABASE", default_value = "polyedge_forge")]
    pub clickhouse_database: String,
    #[arg(
        long,
        env = "FORGE_CH_SNAPSHOT_TABLE",
        default_value = "snapshot_100ms"
    )]
    pub clickhouse_snapshot_table: String,
    #[arg(
        long,
        env = "FORGE_CH_PROCESSED_TABLE",
        default_value = "snapshot_100ms_processed"
    )]
    pub clickhouse_processed_table: String,
    #[arg(long, env = "FORGE_CH_ROUND_TABLE", default_value = "rounds")]
    pub clickhouse_round_table: String,
    #[arg(long, env = "FORGE_CH_SNAPSHOT_TTL_DAYS", default_value_t = 14)]
    pub clickhouse_snapshot_ttl_days: u16,
    #[arg(long, env = "FORGE_CH_PROCESSED_TTL_DAYS", default_value_t = 14)]
    pub clickhouse_processed_ttl_days: u16,
    #[arg(long, env = "FORGE_CH_ROUND_TTL_DAYS", default_value_t = 180)]
    pub clickhouse_round_ttl_days: u16,
    #[arg(
        long,
        env = "FORGE_REDIS_URL",
        default_value = "redis://127.0.0.1:6379/0"
    )]
    pub redis_url: String,
    #[arg(long, env = "FORGE_REDIS_PREFIX", default_value = "forge")]
    pub redis_prefix: String,
    #[arg(long, env = "FORGE_REDIS_TTL_SEC", default_value_t = 7200)]
    pub redis_ttl_sec: u64,
    #[arg(long, env = "FORGE_SINK_BATCH_SIZE", default_value_t = 200)]
    pub sink_batch_size: usize,
    #[arg(long, env = "FORGE_SINK_FLUSH_MS", default_value_t = 1000)]
    pub sink_flush_ms: u64,
    #[arg(long, env = "FORGE_SINK_QUEUE_CAP", default_value_t = 20000)]
    pub sink_queue_cap: usize,
    #[arg(long, env = "FORGE_ROUND_MIN_COVERAGE_RATIO", default_value_t = 0.90)]
    pub round_min_coverage_ratio: f64,
    #[arg(long, env = "FORGE_ROUND_MIN_SAMPLE_RATIO", default_value_t = 0.85)]
    pub round_min_sample_ratio: f64,
    #[arg(long, env = "FORGE_ROUND_MAX_GAP_MS", default_value_t = 2500)]
    pub round_max_gap_ms: i64,
    #[arg(long, env = "FORGE_ROUND_START_TOLERANCE_MS", default_value_t = 35_000)]
    pub round_start_tolerance_ms: i64,
    #[arg(long, env = "FORGE_ROUND_END_TOLERANCE_MS", default_value_t = 10_000)]
    pub round_end_tolerance_ms: i64,
    #[arg(long, env = "FORGE_API_BIND", default_value = "0.0.0.0:9810")]
    pub api_bind: String,
    #[arg(long, env = "FORGE_DISABLE_API", default_value_t = false)]
    pub disable_api: bool,
    #[arg(
        long,
        env = "FORGE_DASHBOARD_DIST",
        default_value = "/home/ubuntu/PolyEdge/heatmap_dashboard/dist"
    )]
    pub dashboard_dist: String,
}

#[derive(clap::Args, Debug, Clone)]
pub struct IrelandApiArgs {
    #[arg(long, env = "FORGE_API_BIND", default_value = "0.0.0.0:9810")]
    pub bind: String,
    #[arg(long, env = "FORGE_CH_URL", default_value = "http://127.0.0.1:8123")]
    pub clickhouse_url: String,
    #[arg(
        long,
        env = "FORGE_REDIS_URL",
        default_value = "redis://127.0.0.1:6379/0"
    )]
    pub redis_url: String,
    #[arg(long, env = "FORGE_REDIS_PREFIX", default_value = "forge")]
    pub redis_prefix: String,
    #[arg(
        long,
        env = "FORGE_DASHBOARD_DIST",
        default_value = "/home/ubuntu/PolyEdge/heatmap_dashboard/dist"
    )]
    pub dashboard_dist: String,
}
