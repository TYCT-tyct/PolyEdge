use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketDescriptor {
    pub market_id: String,
    pub question: String,
    pub symbol: String,
    pub market_slug: Option<String>,
    pub token_id_yes: Option<String>,
    pub token_id_no: Option<String>,
    pub event_slug: Option<String>,
    pub end_date: Option<String>,
    pub event_start_time: Option<String>,
    pub timeframe: Option<String>,   // "5m" / "15m" / "1h" / "1d"
    pub market_type: Option<String>, // "updown" / "above_below" / "range"
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub price_to_beat: Option<f64>,
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
    pub one_market_per_template: bool,
    pub markets_per_template: usize,
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
        fn env_usize(key: &str, default: usize, min: usize, max: usize) -> usize {
            std::env::var(key)
                .ok()
                .and_then(|v| v.trim().parse::<usize>().ok())
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
                20 * 1_000,
                0,
                30 * 60 * 1_000,
            ),
            // Keep one active market per (symbol, market_type, timeframe) template
            // to prevent stale/future window fanout from diluting strategy decisions.
            one_market_per_template: env_bool("POLYEDGE_DISCOVERY_ONE_PER_TEMPLATE", true),
            // Keep more than one candidate to allow immediate round switch at boundaries.
            markets_per_template: env_usize("POLYEDGE_DISCOVERY_MARKETS_PER_TEMPLATE", 2, 1, 8),
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
        let user_agent = std::env::var("POLYEDGE_DISCOVERY_USER_AGENT")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| {
                "Mozilla/5.0 (compatible; PolyEdgeBot/1.0; +https://github.com/TYCT-tyct/PolyEdge)"
                    .to_string()
            });
        let http = Client::builder()
            .user_agent(user_agent)
            .connect_timeout(Duration::from_secs(6))
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap_or_else(|_| Client::new());
        Self { http, cfg }
    }

    pub async fn discover(&self) -> Result<Vec<MarketDescriptor>> {
        fn env_usize(key: &str, default: usize, min: usize, max: usize) -> usize {
            std::env::var(key)
                .ok()
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(default)
                .clamp(min, max)
        }
        fn env_i64(key: &str, default: i64, min: i64, max: i64) -> i64 {
            std::env::var(key)
                .ok()
                .and_then(|v| v.trim().parse::<i64>().ok())
                .unwrap_or(default)
                .clamp(min, max)
        }

        let mut out = Vec::new();
        let mut seen = std::collections::HashSet::<String>::new();
        let now_ms = Utc::now().timestamp_millis();
        let can_early_stop = !self.cfg.symbols.is_empty()
            && !self.cfg.market_types.is_empty()
            && !self.cfg.timeframes.is_empty();
        let mut target_template_keys = std::collections::HashSet::<String>::new();
        let mut matched_template_keys = std::collections::HashSet::<String>::new();
        if can_early_stop {
            for symbol in &self.cfg.symbols {
                for market_type in &self.cfg.market_types {
                    for timeframe in &self.cfg.timeframes {
                        target_template_keys.insert(format!(
                            "{}|{}|{}",
                            symbol,
                            market_type.to_ascii_lowercase(),
                            timeframe.to_ascii_lowercase()
                        ));
                    }
                }
            }
        }

        // Keep discovery query volume bounded by default to avoid Gamma rate-limits (429),
        // while still allowing opt-in wider scans via env variables.
        let enddate_limit = env_i64("POLYEDGE_DISCOVERY_ENDDATE_LIMIT", 200, 50, 1000);
        let enddate_pages = env_usize("POLYEDGE_DISCOVERY_ENDDATE_MAX_PAGES", 20, 1, 64);
        let volume_limit = env_i64("POLYEDGE_DISCOVERY_VOLUME_LIMIT", 200, 50, 1000);
        let volume_pages_cfg = env_usize("POLYEDGE_DISCOVERY_VOLUME_MAX_PAGES", 0, 0, 32);
        let volume_pages_fallback = env_usize("POLYEDGE_DISCOVERY_VOLUME_FALLBACK_PAGES", 1, 0, 8);
        let volume_pages = if volume_pages_cfg > 0 {
            volume_pages_cfg
        } else {
            volume_pages_fallback
        };

        let mut plans = Vec::with_capacity(2);
        plans.push(ScanPlan {
            order: "endDate",
            ascending: "true",
            limit: enddate_limit,
            max_pages: enddate_pages,
        });
        if volume_pages > 0 {
            plans.push(ScanPlan {
                order: "volume",
                ascending: "false",
                limit: volume_limit,
                max_pages: volume_pages,
            });
        }

        let mut last_err: Option<anyhow::Error> = None;
        'scan_plans: for plan in plans {
            for page_idx in 0..plan.max_pages {
                let offset = (page_idx as i64) * plan.limit;
                let limit_s = plan.limit.to_string();
                let offset_s = offset.to_string();
                // Do not set `active=true` query param here: Gamma may omit near-expiry
                // markets under that server-side filter. We filter by `market.active` below.
                let response = self
                    .http
                    .get(&self.cfg.endpoint)
                    .query(&[
                        ("closed", "false"),
                        ("archived", "false"),
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
                        last_err = Some(anyhow!("discovery request failed: {err}"));
                        break;
                    }
                };
                let response = match response.error_for_status() {
                    Ok(v) => v,
                    Err(err) => {
                        last_err = Some(anyhow!("discovery status failed: {err}"));
                        break;
                    }
                };
                let markets: Vec<GammaMarket> = match response.json().await {
                    Ok(v) => v,
                    Err(err) => {
                        last_err = Some(anyhow!("discovery json parse failed: {err}"));
                        break;
                    }
                };

                if markets.is_empty() {
                    break;
                }

                for market in markets {
                    // Gamma can omit `acceptingOrders` for still-tradable markets.
                    // Treat only explicit `false` as non-tradable.
                    // Do not hard-reject inactive markets here. Near-expiry filtering and
                    // timeframe/type checks already keep scope tight, and keeping inactive
                    // next-round listings reduces switch lag at window boundaries.
                    if market.closed || matches!(market.accepting_orders, Some(false)) {
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
                    let timeframe = classify_timeframe_with_bounds(
                        &text,
                        market.event_start_time.as_deref(),
                        market.end_date.as_deref(),
                    );
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

                    let price_to_beat = market
                        .events
                        .as_ref()
                        .and_then(|evs| evs.first())
                        .and_then(|ev| ev.event_metadata.as_ref())
                        .and_then(|m| m.price_to_beat);
                    out.push(MarketDescriptor {
                        market_id: market.id.clone(),
                        question: market.question.clone(),
                        symbol: symbol.clone(),
                        market_slug: market.slug.clone(),
                        token_id_yes: parse_token_pair(market.clob_token_ids.as_deref())
                            .map(|x| x.0),
                        token_id_no: parse_token_pair(market.clob_token_ids.as_deref())
                            .map(|x| x.1),
                        event_slug: market.event_slug.clone(),
                        end_date: market.end_date.clone(),
                        event_start_time: market.event_start_time.clone(),
                        timeframe: timeframe.map(|v| v.to_string()),
                        market_type: Some(market_type.to_string()),
                        best_bid: market.best_bid,
                        best_ask: market.best_ask,
                        price_to_beat,
                    });

                    if can_early_stop {
                        if let Some(tf) = timeframe {
                            let discovered_key = format!(
                                "{}|{}|{}",
                                symbol,
                                market_type.to_ascii_lowercase(),
                                tf.to_ascii_lowercase()
                            );
                            if target_template_keys.contains(&discovered_key) {
                                matched_template_keys.insert(discovered_key);
                            }
                        }
                        if !target_template_keys.is_empty()
                            && matched_template_keys.len() >= target_template_keys.len()
                        {
                            break 'scan_plans;
                        }
                    }
                }
            }
        }

        if out.is_empty() {
            if let Some(err) = last_err {
                return Err(err).context("no markets discovered from gamma");
            }
            return Err(anyhow!("no markets discovered from gamma"));
        }

        if self.cfg.one_market_per_template {
            out = collapse_to_markets_per_template(out, now_ms, self.cfg.markets_per_template);
        }

        Ok(out)
    }
}

fn collapse_to_markets_per_template(
    markets: Vec<MarketDescriptor>,
    now_ms: i64,
    keep_per_template: usize,
) -> Vec<MarketDescriptor> {
    let keep_per_template = keep_per_template.max(1);
    let mut grouped = std::collections::HashMap::<String, Vec<MarketDescriptor>>::new();
    for m in markets {
        let key = format!(
            "{}|{}|{}",
            m.symbol,
            m.market_type
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
            m.timeframe.clone().unwrap_or_else(|| "unknown".to_string())
        );
        grouped.entry(key).or_default().push(m);
    }
    let mut out = Vec::<MarketDescriptor>::new();
    for mut entries in grouped.into_values() {
        entries.sort_by_key(|m| {
            let end_ms = parse_end_date_ms(m.end_date.as_deref()).unwrap_or(i64::MAX / 4);
            // Prefer windows closest to "now". Keep only a short grace for just-ended
            // rounds so discovery quickly advances to the current window.
            let recent_grace_ms = recent_end_grace_ms(m.timeframe.as_deref());
            let ended_penalty = if end_ms < now_ms.saturating_sub(recent_grace_ms) {
                1_i64
            } else {
                0_i64
            };
            let distance = end_ms.saturating_sub(now_ms).abs();
            (
                ended_penalty,
                distance,
                std::cmp::Reverse(end_ms),
                m.market_id.clone(),
            )
        });
        out.extend(entries.into_iter().take(keep_per_template));
    }
    out
}

fn recent_end_grace_ms(timeframe: Option<&str>) -> i64 {
    match timeframe {
        Some("5m") => 30 * 1_000,
        Some("15m") => 60 * 1_000,
        Some("1h") => 120 * 1_000,
        Some("1d") => 300 * 1_000,
        _ => 30 * 1_000,
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

fn classify_timeframe_with_bounds(
    text: &str,
    event_start_time: Option<&str>,
    end_date: Option<&str>,
) -> Option<&'static str> {
    if let Some(tf) = classify_timeframe(text) {
        return Some(tf);
    }
    infer_timeframe_from_bounds(event_start_time, end_date)
}

fn infer_timeframe_from_bounds(
    event_start_time: Option<&str>,
    end_date: Option<&str>,
) -> Option<&'static str> {
    let start_ms = parse_end_date_ms(event_start_time)?;
    let end_ms = parse_end_date_ms(end_date)?;
    let span_ms = end_ms.saturating_sub(start_ms).abs();

    if (3 * 60 * 1_000..=7 * 60 * 1_000).contains(&span_ms) {
        return Some("5m");
    }
    if (10 * 60 * 1_000..=20 * 60 * 1_000).contains(&span_ms) {
        return Some("15m");
    }
    if (45 * 60 * 1_000..=90 * 60 * 1_000).contains(&span_ms) {
        return Some("1h");
    }
    if (18 * 60 * 60 * 1_000..=36 * 60 * 60 * 1_000).contains(&span_ms) {
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
    event_start_time: Option<String>,
    #[serde(default)]
    best_bid: Option<f64>,
    #[serde(default)]
    best_ask: Option<f64>,
    #[serde(default)]
    clob_token_ids: Option<String>,
    #[serde(default)]
    closed: bool,
    #[serde(default)]
    accepting_orders: Option<bool>,
    #[serde(default)]
    events: Option<Vec<GammaEvent>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaEvent {
    #[serde(default)]
    event_metadata: Option<GammaEventMetadata>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaEventMetadata {
    #[serde(default)]
    price_to_beat: Option<f64>,
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
        assert_eq!(
            classify_timeframe_with_bounds(
                "BITCOIN UP OR DOWN",
                Some("2026-02-28T09:35:00Z"),
                Some("2026-02-28T09:40:00Z")
            ),
            Some("5m")
        );
        assert_eq!(
            classify_timeframe_with_bounds(
                "BITCOIN UP OR DOWN",
                Some("2026-02-28T09:30:00Z"),
                Some("2026-02-28T09:45:00Z")
            ),
            Some("15m")
        );
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

    #[test]
    fn collapse_keeps_nearest_non_ended_window() {
        let mk = |id: &str, end_date: &str| MarketDescriptor {
            market_id: id.to_string(),
            question: "Bitcoin Up or Down - 5 Minutes".to_string(),
            symbol: "BTCUSDT".to_string(),
            market_slug: None,
            token_id_yes: Some(format!("y-{id}")),
            token_id_no: Some(format!("n-{id}")),
            event_slug: None,
            end_date: Some(end_date.to_string()),
            event_start_time: None,
            timeframe: Some("5m".to_string()),
            market_type: Some("updown".to_string()),
            best_bid: None,
            best_ask: None,
            price_to_beat: None,
        };

        let now = chrono::Utc
            .with_ymd_and_hms(2026, 2, 22, 16, 0, 0)
            .unwrap()
            .timestamp_millis();
        let input = vec![
            mk("past", "2026-02-22T15:55:00Z"),
            mk("future_far", "2026-02-22T16:20:00Z"),
            mk("future_near", "2026-02-22T16:05:00Z"),
        ];
        let out = collapse_to_markets_per_template(input, now, 1);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].market_id, "future_near");
    }

    #[test]
    fn collapse_keeps_current_and_next_when_keep_is_two() {
        let mk = |id: &str, end_date: &str| MarketDescriptor {
            market_id: id.to_string(),
            question: "Bitcoin Up or Down - 5 Minutes".to_string(),
            symbol: "BTCUSDT".to_string(),
            market_slug: None,
            token_id_yes: Some(format!("y-{id}")),
            token_id_no: Some(format!("n-{id}")),
            event_slug: None,
            end_date: Some(end_date.to_string()),
            event_start_time: None,
            timeframe: Some("5m".to_string()),
            market_type: Some("updown".to_string()),
            best_bid: None,
            best_ask: None,
            price_to_beat: None,
        };

        let now = chrono::Utc
            .with_ymd_and_hms(2026, 2, 22, 16, 0, 0)
            .unwrap()
            .timestamp_millis();
        let input = vec![
            mk("past", "2026-02-22T15:54:00Z"),
            mk("current", "2026-02-22T16:05:00Z"),
            mk("next", "2026-02-22T16:10:00Z"),
            mk("far", "2026-02-22T16:25:00Z"),
        ];

        let mut out = collapse_to_markets_per_template(input, now, 2);
        out.sort_by(|a, b| a.market_id.cmp(&b.market_id));
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].market_id, "current");
        assert_eq!(out[1].market_id, "next");
    }
}
