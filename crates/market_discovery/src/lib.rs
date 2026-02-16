use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketDescriptor {
    pub market_id: String,
    pub question: String,
    pub symbol: String,
    pub token_id_yes: Option<String>,
    pub token_id_no: Option<String>,
    pub event_slug: Option<String>,
    pub end_date: Option<String>,
    pub timeframe: Option<String>,    // "5m" / "15m" / "1h" / "1d"
    pub market_type: Option<String>,  // "updown" / "above_below" / "range"
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    pub symbols: Vec<String>,
    pub market_types: Vec<String>,
    pub timeframes: Vec<String>,
    pub endpoint: String,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            symbols: vec![
                "BTCUSDT".to_string(),
                "ETHUSDT".to_string(),
                "SOLUSDT".to_string(),
                "XRPUSDT".to_string(),
                "BNBUSDT".to_string(),
                "DOGEUSDT".to_string(),
                "ADAUSDT".to_string(),
                "AVAXUSDT".to_string(),
                "LINKUSDT".to_string(),
                "MATICUSDT".to_string(),
                "LTCUSDT".to_string(),
                "DOTUSDT".to_string(),
                "TRXUSDT".to_string(),
                "TONUSDT".to_string(),
                "NEARUSDT".to_string(),
            ],
            market_types: vec![
                "updown".to_string(),
                "above_below".to_string(),
                "range".to_string(),
            ],
            timeframes: vec![
                "5m".to_string(),
                "15m".to_string(),
                "1h".to_string(),
                "1d".to_string(),
            ],
            endpoint: "https://gamma-api.polymarket.com/markets".to_string(),
        }
    }
}

pub struct MarketDiscovery {
    http: Client,
    cfg: DiscoveryConfig,
}

impl MarketDiscovery {
    pub fn new(cfg: DiscoveryConfig) -> Self {
        Self {
            http: Client::new(),
            cfg,
        }
    }

    pub async fn discover(&self) -> Result<Vec<MarketDescriptor>> {
        let mut out = Vec::new();
        let mut seen = std::collections::HashSet::<String>::new();

        // Gamma is ordered by volume and paginated by offset; crypto up/down markets can fall
        // outside the top 1k depending on the global market mix. Scan a few pages deterministically.
        let limit: i64 = 1000;
        for offset in [0_i64, 1000, 2000, 3000] {
            let limit_s = limit.to_string();
            let offset_s = offset.to_string();
            let markets: Vec<GammaMarket> = self
                .http
                .get(&self.cfg.endpoint)
                .query(&[
                    ("closed", "false"),
                    ("archived", "false"),
                    ("active", "true"),
                    ("limit", limit_s.as_str()),
                    ("offset", offset_s.as_str()),
                    ("order", "volume"),
                    ("ascending", "false"),
                ])
                .send()
                .await
                .context("discovery request")?
                .error_for_status()
                .context("discovery status")?
                .json()
                .await
                .context("discovery json")?;

            if markets.is_empty() {
                break;
            }

            for market in markets {
                if !market.active || market.closed || !market.accepting_orders {
                    continue;
                }
                if !seen.insert(market.id.clone()) {
                    continue;
                }

                let text = format!(
                    "{} {}",
                    market.question.to_ascii_uppercase(),
                    market.slug.clone().unwrap_or_default().to_ascii_uppercase()
                );
                let market_type = classify_market_type(&text);
                if !self.cfg.market_types.is_empty()
                    && !self
                        .cfg
                        .market_types
                        .iter()
                        .any(|t| t.eq_ignore_ascii_case(market_type))
                {
                    continue;
                }
                let timeframe = classify_timeframe(&text);
                if !self.cfg.timeframes.is_empty() {
                    let Some(tf) = timeframe else {
                        continue;
                    };
                    if !self.cfg.timeframes.iter().any(|t| t.eq_ignore_ascii_case(tf)) {
                        continue;
                    }
                }
                let Some(symbol) = detect_symbol(&text, &self.cfg.symbols) else {
                    continue;
                };

                out.push(MarketDescriptor {
                    market_id: market.id,
                    question: market.question,
                    symbol,
                    token_id_yes: parse_token_pair(market.clob_token_ids.as_deref()).map(|x| x.0),
                    token_id_no: parse_token_pair(market.clob_token_ids.as_deref()).map(|x| x.1),
                    event_slug: market.event_slug,
                    end_date: market.end_date,
                    timeframe: timeframe.map(|v| v.to_string()),
                    market_type: Some(market_type.to_string()),
                    best_bid: market.best_bid,
                    best_ask: market.best_ask,
                });
            }
        }

        Ok(out)
    }
}

fn detect_symbol(text: &str, allowed_symbols: &[String]) -> Option<String> {
    let aliases: [(&str, [&str; 3]); 15] = [
        ("BTCUSDT", ["BITCOIN", "BTC", "XBT"]),
        ("ETHUSDT", ["ETHEREUM", "ETH", "ETHER"]),
        ("SOLUSDT", ["SOLANA", "SOL", "SOLAN"]),
        ("XRPUSDT", ["RIPPLE", "XRP", "XRP"]),
        ("BNBUSDT", ["BINANCE", "BNB", "BNB"]),
        ("DOGEUSDT", ["DOGECOIN", "DOGE", "DOGE"]),
        ("ADAUSDT", ["CARDANO", "ADA", "ADA"]),
        ("AVAXUSDT", ["AVALANCHE", "AVAX", "AVAX"]),
        ("LINKUSDT", ["CHAINLINK", "LINK", "LINK"]),
        ("MATICUSDT", ["POLYGON", "MATIC", "POL"]),
        ("LTCUSDT", ["LITECOIN", "LTC", "LTC"]),
        ("DOTUSDT", ["POLKADOT", "DOT", "DOT"]),
        ("TRXUSDT", ["TRON", "TRX", "TRX"]),
        ("TONUSDT", ["TONCOIN", "TON", "TON"]),
        ("NEARUSDT", ["NEAR", "NEAR", "NEAR"]),
    ];

    for (symbol, keys) in aliases {
        if !allowed_symbols.iter().any(|s| s == symbol) {
            continue;
        }
        if keys.iter().any(|k| text.contains(k)) {
            return Some(symbol.to_string());
        }
    }

    for symbol in allowed_symbols {
        let needle = symbol.trim_end_matches("USDT");
        if text.contains(needle) {
            return Some(symbol.clone());
        }
    }
    None
}

fn classify_market_type(text: &str) -> &'static str {
    if text.contains("UP OR DOWN") || text.contains("UP/DOWN") {
        return "updown";
    }
    if text.contains("ABOVE") || text.contains("BELOW") {
        return "above_below";
    }
    if text.contains("BETWEEN") || text.contains("RANGE") {
        return "range";
    }
    "other"
}

fn classify_timeframe(text: &str) -> Option<&'static str> {
    if text.contains("15 MIN") || text.contains("15M") || text.contains("15 MINUTE") {
        return Some("15m");
    }
    if text.contains("5 MIN") || text.contains("5M") || text.contains("5 MINUTE") {
        return Some("5m");
    }
    if text.contains("1 HOUR") || text.contains("1H") || text.contains("60 MIN") {
        return Some("1h");
    }
    if text.contains("1 DAY") || text.contains("1D") || text.contains("24 HOUR") {
        return Some("1d");
    }
    None
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    id: String,
    question: String,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    event_slug: Option<String>,
    #[serde(default)]
    end_date: Option<String>,
    #[serde(default)]
    best_bid: Option<f64>,
    #[serde(default)]
    best_ask: Option<f64>,
    #[serde(default)]
    clob_token_ids: Option<String>,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    closed: bool,
    #[serde(default)]
    accepting_orders: bool,
}

fn parse_token_pair(input: Option<&str>) -> Option<(String, String)> {
    let raw = input?;
    let parsed: Vec<String> = serde_json::from_str(raw).ok()?;
    if parsed.len() < 2 {
        return None;
    }
    Some((parsed[0].clone(), parsed[1].clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_symbols_include_all() {
        let cfg = DiscoveryConfig::default();
        assert!(cfg.symbols.contains(&"BTCUSDT".to_string()));
        assert!(cfg.symbols.contains(&"ETHUSDT".to_string()));
        assert!(cfg.symbols.contains(&"SOLUSDT".to_string()));
        assert!(cfg.symbols.contains(&"XRPUSDT".to_string()));
    }

    #[test]
    fn detect_symbol_handles_name_aliases() {
        let symbols = DiscoveryConfig::default().symbols;
        assert_eq!(
            detect_symbol("WILL BITCOIN RISE IN 1 HOUR", &symbols),
            Some("BTCUSDT".to_string())
        );
        assert_eq!(
            detect_symbol("ETHEREUM 5M PRICE MOVE", &symbols),
            Some("ETHUSDT".to_string())
        );
        assert_eq!(
            detect_symbol("SOLANA SHORT WINDOW", &symbols),
            Some("SOLUSDT".to_string())
        );
        assert_eq!(
            detect_symbol("RIPPLE MOMENTUM", &symbols),
            Some("XRPUSDT".to_string())
        );
    }

    #[test]
    fn classify_timeframe_and_type() {
        assert_eq!(
            classify_market_type("BITCOIN UP OR DOWN - 15 MINUTES"),
            "updown"
        );
        assert_eq!(
            classify_timeframe("BITCOIN UP OR DOWN - 5 MINUTES"),
            Some("5m")
        );
        assert_eq!(
            classify_timeframe("ETHEREUM UP OR DOWN - 15 MINUTES"),
            Some("15m")
        );
        assert_eq!(classify_timeframe("SOLANA UP OR DOWN - 1 HOUR"), Some("1h"));
        assert_eq!(classify_timeframe("XRP UP OR DOWN - 1 DAY"), Some("1d"));
    }
}
