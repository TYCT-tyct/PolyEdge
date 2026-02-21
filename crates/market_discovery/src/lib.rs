use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
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
    pub timeframe: Option<String>,   // "5m" / "15m" / "1h" / "1d"
    pub market_type: Option<String>, // "updown" / "above_below" / "range"
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    pub symbols: Vec<String>,
    pub market_types: Vec<String>,
    pub timeframes: Vec<String>,
    pub endpoint: String,
    pub near_expiry_only: bool,
    pub max_future_ms_5m: i64,
    pub max_future_ms_15m: i64,
    pub max_future_ms_1h: i64,
    pub max_future_ms_1d: i64,
    pub max_past_ms: i64,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        fn env_bool(key: &str, default: bool) -> bool {
            std::env::var(key)
                .ok()
                .map(|v| {
                    !matches!(
                        v.trim().to_ascii_lowercase().as_str(),
                        "0" | "false" | "off" | "no"
                    )
                })
                .unwrap_or(default)
        }
        fn env_i64_ms(key: &str, default: i64, min: i64, max: i64) -> i64 {
            std::env::var(key)
                .ok()
                .and_then(|v| v.trim().parse::<i64>().ok())
                .unwrap_or(default)
                .clamp(min, max)
        }
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
            // Keep discovery focused on currently tradable windows instead of scanning far-future
            // active listings. This reduces stale/untracked churn in strategy loop.
            near_expiry_only: env_bool("POLYEDGE_DISCOVERY_NEAR_EXPIRY_ONLY", true),
            max_future_ms_5m: env_i64_ms(
                "POLYEDGE_DISCOVERY_MAX_FUTURE_MS_5M",
                25 * 60 * 1_000,
                30_000,
                24 * 60 * 60 * 1_000,
            ),
            max_future_ms_15m: env_i64_ms(
                "POLYEDGE_DISCOVERY_MAX_FUTURE_MS_15M",
                70 * 60 * 1_000,
                30_000,
                24 * 60 * 60 * 1_000,
            ),
            max_future_ms_1h: env_i64_ms(
                "POLYEDGE_DISCOVERY_MAX_FUTURE_MS_1H",
                4 * 60 * 60 * 1_000,
                60_000,
                48 * 60 * 60 * 1_000,
            ),
            max_future_ms_1d: env_i64_ms(
                "POLYEDGE_DISCOVERY_MAX_FUTURE_MS_1D",
                36 * 60 * 60 * 1_000,
                60_000,
                14 * 24 * 60 * 60 * 1_000,
            ),
            max_past_ms: env_i64_ms(
                "POLYEDGE_DISCOVERY_MAX_PAST_MS",
                2 * 60 * 1_000,
                0,
                30 * 60 * 1_000,
            ),
        }
    }
}

pub struct MarketDiscovery {
    http: Client,
    cfg: DiscoveryConfig,
}

#[derive(Clone, Copy)]
struct ScanPlan {
    order: &'static str,
    ascending: &'static str,
    limit: i64,
    max_pages: usize,
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
        let now_ms = Utc::now().timestamp_millis();

        // Prefer time-window ordering first to avoid missing low-volume but currently tradable
        // crypto windows (e.g. SOL 15m), then do a volume sweep as fallback enrichment.
        let plans = [
            ScanPlan {
                order: "endDate",
                ascending: "true",
                limit: 200,
                max_pages: 16,
            },
            ScanPlan {
                order: "volume",
                ascending: "false",
                limit: 1000,
                max_pages: 4,
            },
        ];

        for plan in plans {
            for page_idx in 0..plan.max_pages {
                let offset = (page_idx as i64) * plan.limit;
                let limit_s = plan.limit.to_string();
                let offset_s = offset.to_string();
                let response = self
                    .http
                    .get(&self.cfg.endpoint)
                    .query(&[
                        ("closed", "false"),
                        ("archived", "false"),
                        ("active", "true"),
                        ("limit", limit_s.as_str()),
                        ("offset", offset_s.as_str()),
                        ("order", plan.order),
                        ("ascending", plan.ascending),
                    ])
                    .send()
                    .await;
                let response = match response {
                    Ok(v) => v,
                    Err(err) => {
                        if out.is_empty() {
                            return Err(err).context("discovery request");
                        }
                        break;
                    }
                };
                let response = match response.error_for_status() {
                    Ok(v) => v,
                    Err(err) => {
                        if out.is_empty() {
                            return Err(err).context("discovery status");
                        }
                        break;
                    }
                };
                let markets: Vec<GammaMarket> = match response.json().await {
                    Ok(v) => v,
                    Err(err) => {
                        if out.is_empty() {
                            return Err(err).context("discovery json");
                        }
                        break;
                    }
                };

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
                        if !self
                            .cfg
                            .timeframes
                            .iter()
                            .any(|t| t.eq_ignore_ascii_case(tf))
                        {
                            continue;
                        }
                    }
                    if self.cfg.near_expiry_only
                        && !is_within_discovery_window(
                            market.end_date.as_deref(),
                            timeframe,
                            now_ms,
                            &self.cfg,
                        )
                    {
                        continue;
                    }
                    let Some(symbol) = detect_symbol(&text, &self.cfg.symbols) else {
                        continue;
                    };

                    out.push(MarketDescriptor {
                        market_id: market.id,
                        question: market.question,
                        symbol,
                        token_id_yes: parse_token_pair(market.clob_token_ids.as_deref())
                            .map(|x| x.0),
                        token_id_no: parse_token_pair(market.clob_token_ids.as_deref())
                            .map(|x| x.1),
                        event_slug: market.event_slug,
                        end_date: market.end_date,
                        timeframe: timeframe.map(|v| v.to_string()),
                        market_type: Some(market_type.to_string()),
                        best_bid: market.best_bid,
                        best_ask: market.best_ask,
                    });
                }
            }
        }

        if out.is_empty() {
            return Err(anyhow!("no markets discovered from gamma"));
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

fn parse_end_date_ms(raw: Option<&str>) -> Option<i64> {
    let value = raw?.trim();
    if value.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

fn is_within_discovery_window(
    end_date: Option<&str>,
    timeframe: Option<&str>,
    now_ms: i64,
    cfg: &DiscoveryConfig,
) -> bool {
    let Some(end_ms) = parse_end_date_ms(end_date) else {
        return false;
    };
    let delta_ms = end_ms.saturating_sub(now_ms);
    if delta_ms < -cfg.max_past_ms {
        return false;
    }
    let max_future_ms = match timeframe {
        Some("5m") => cfg.max_future_ms_5m,
        Some("15m") => cfg.max_future_ms_15m,
        Some("1h") => cfg.max_future_ms_1h,
        Some("1d") => cfg.max_future_ms_1d,
        _ => cfg.max_future_ms_15m.max(cfg.max_future_ms_5m),
    };
    delta_ms <= max_future_ms
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
    use chrono::TimeZone;

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

    #[test]
    fn discovery_window_filters_far_future_markets() {
        let cfg = DiscoveryConfig::default();
        let now_ms = Utc::now().timestamp_millis();
        let near_end = Utc
            .timestamp_millis_opt(now_ms + 10 * 60 * 1_000)
            .single()
            .expect("valid ts")
            .to_rfc3339();
        let far_end = Utc
            .timestamp_millis_opt(now_ms + 3 * 60 * 60 * 1_000)
            .single()
            .expect("valid ts")
            .to_rfc3339();

        assert!(is_within_discovery_window(
            Some(near_end.as_str()),
            Some("5m"),
            now_ms,
            &cfg
        ));
        assert!(!is_within_discovery_window(
            Some(far_end.as_str()),
            Some("5m"),
            now_ms,
            &cfg
        ));
    }
}
