use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "forge_tokyo_relay", version, about = "PolyEdge Tokyo relay")]
pub struct Cli {
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
