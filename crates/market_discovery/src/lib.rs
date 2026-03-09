use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

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
                30 * 60 * 1_000,
                30_000,
                24 * 60 * 60 * 1_000,
            ),
            max_future_ms_15m: env_i64_ms(
                "POLYEDGE_DISCOVERY_MAX_FUTURE_MS_15M",
                90 * 60 * 1_000,
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
            // Keep at least 3 candidates (current + next + next-next) for fast round switching.
            markets_per_template: env_usize("POLYEDGE_DISCOVERY_MARKETS_PER_TEMPLATE", 3, 1, 8),
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

fn discovery_retry_limit() -> usize {
    std::env::var("POLYEDGE_DISCOVERY_REQUEST_RETRIES")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(2)
        .clamp(0, 6)
}

fn discovery_retry_base_ms() -> u64 {
    std::env::var("POLYEDGE_DISCOVERY_RETRY_BASE_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(400)
        .clamp(100, 5_000)
}

fn discovery_retry_max_ms() -> u64 {
    std::env::var("POLYEDGE_DISCOVERY_RETRY_MAX_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(5_000)
        .clamp(250, 30_000)
}

fn retry_after_delay_ms(headers: &reqwest::header::HeaderMap, fallback_ms: u64) -> u64 {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(|secs| secs.saturating_mul(1_000))
        .filter(|delay_ms| *delay_ms > 0)
        .unwrap_or(fallback_ms)
}

fn discovery_rate_limit_window_ms() -> u64 {
    std::env::var("POLYEDGE_DISCOVERY_RATE_LIMIT_WINDOW_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(10_000)
        .clamp(1_000, 60_000)
}

fn discovery_rate_limit_max_requests() -> usize {
    std::env::var("POLYEDGE_DISCOVERY_MAX_REQUESTS_PER_WINDOW")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(80)
        .clamp(1, 500)
}

fn discovery_rate_limit_state() -> &'static tokio::sync::Mutex<VecDeque<Instant>> {
    static STATE: OnceLock<tokio::sync::Mutex<VecDeque<Instant>>> = OnceLock::new();
    STATE.get_or_init(|| tokio::sync::Mutex::new(VecDeque::new()))
}

async fn wait_for_discovery_rate_limit(request_kind: &str) {
    let max_requests = discovery_rate_limit_max_requests();
    let window = Duration::from_millis(discovery_rate_limit_window_ms());
    loop {
        let maybe_wait = {
            let mut state = discovery_rate_limit_state().lock().await;
            let now = Instant::now();
            while let Some(front) = state.front().copied() {
                if now.duration_since(front) >= window {
                    state.pop_front();
                } else {
                    break;
                }
            }
            if state.len() < max_requests {
                state.push_back(now);
                None
            } else {
                state
                    .front()
                    .copied()
                    .map(|front| window.saturating_sub(now.duration_since(front)))
            }
        };
        let Some(wait) = maybe_wait else {
            return;
        };
        tracing::warn!(
            request_kind,
            wait_ms = wait.as_millis() as u64,
            max_requests = max_requests,
            window_ms = window.as_millis() as u64,
            "gamma discovery client-side throttle"
        );
        tokio::time::sleep(wait.max(Duration::from_millis(1))).await;
    }
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

    async fn get_json_with_retry<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        query: &[(&str, &str)],
        request_kind: &str,
    ) -> Result<T> {
        let max_retries = discovery_retry_limit();
        let max_backoff_ms = discovery_retry_max_ms();
        let mut backoff_ms = discovery_retry_base_ms();

        for attempt in 0..=max_retries {
            wait_for_discovery_rate_limit(request_kind).await;
            let response = self.http.get(endpoint).query(query).send().await;
            match response {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return resp.json::<T>().await.with_context(|| {
                            format!("discovery json parse failed ({request_kind})")
                        });
                    }

                    let wait_ms =
                        retry_after_delay_ms(resp.headers(), backoff_ms).min(max_backoff_ms);
                    let body = resp.text().await.unwrap_or_default();
                    let retriable = status == StatusCode::TOO_MANY_REQUESTS
                        || status == StatusCode::REQUEST_TIMEOUT
                        || status.is_server_error();
                    if retriable && attempt < max_retries {
                        tracing::warn!(
                            request_kind,
                            status = %status,
                            attempt,
                            retry_in_ms = wait_ms,
                            "gamma discovery request throttled; backing off"
                        );
                        tokio::time::sleep(Duration::from_millis(wait_ms.max(1))).await;
                        backoff_ms = backoff_ms.saturating_mul(2).min(max_backoff_ms);
                        continue;
                    }
                    return Err(anyhow!(
                        "discovery status failed ({request_kind}, status={}): {body}",
                        status
                    ));
                }
                Err(err) => {
                    let retriable = err.is_timeout() || err.is_connect() || err.is_request();
                    if retriable && attempt < max_retries {
                        let wait_ms = backoff_ms.min(max_backoff_ms);
                        tracing::warn!(
                            request_kind,
                            ?err,
                            attempt,
                            retry_in_ms = wait_ms,
                            "gamma discovery request failed; retrying"
                        );
                        tokio::time::sleep(Duration::from_millis(wait_ms.max(1))).await;
                        backoff_ms = backoff_ms.saturating_mul(2).min(max_backoff_ms);
                        continue;
                    }
                    return Err(anyhow!("discovery request failed ({request_kind}): {err}"));
                }
            }
        }

        Err(anyhow!(
            "discovery request exhausted retries ({request_kind})"
        ))
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
        let enddate_desc_limit = env_i64("POLYEDGE_DISCOVERY_ENDDATE_DESC_LIMIT", 300, 50, 1000);
        let enddate_desc_pages = env_usize("POLYEDGE_DISCOVERY_ENDDATE_DESC_MAX_PAGES", 8, 0, 64);
        let volume_limit = env_i64("POLYEDGE_DISCOVERY_VOLUME_LIMIT", 200, 50, 1000);
        let volume_pages_cfg = env_usize("POLYEDGE_DISCOVERY_VOLUME_MAX_PAGES", 0, 0, 32);
        let volume_pages_fallback = env_usize("POLYEDGE_DISCOVERY_VOLUME_FALLBACK_PAGES", 1, 0, 8);
        let volume_pages = if volume_pages_cfg > 0 {
            volume_pages_cfg
        } else {
            volume_pages_fallback
        };

        let mut plans = Vec::with_capacity(3);
        // Near-now windows should be preferred for recorder continuity.
        // Run ascending endDate first so template early-stop does not terminate on far-future markets.
        plans.push(ScanPlan {
            order: "endDate",
            ascending: "true",
            limit: enddate_limit,
            max_pages: enddate_pages,
        });
        if enddate_desc_pages > 0 {
            plans.push(ScanPlan {
                order: "endDate",
                ascending: "false",
                limit: enddate_desc_limit,
                max_pages: enddate_desc_pages,
            });
        }
        if volume_pages > 0 {
            plans.push(ScanPlan {
                order: "volume",
                ascending: "false",
                limit: volume_limit,
                max_pages: volume_pages,
            });
        }

        let events_limit = env_i64("POLYEDGE_DISCOVERY_EVENTS_LIMIT", 100, 20, 500);
        let events_pages = env_usize("POLYEDGE_DISCOVERY_EVENTS_MAX_PAGES", 20, 1, 64);
        let events_endpoint = self
            .cfg
            .endpoint
            .strip_suffix("/markets")
            .map(|prefix| format!("{prefix}/events"))
            .unwrap_or_else(|| self.cfg.endpoint.replace("/markets", "/events"));

        let mut last_err: Option<anyhow::Error> = None;
        'events_scan: for page_idx in 0..events_pages {
            let offset = (page_idx as i64) * events_limit;
            let limit_s = events_limit.to_string();
            let offset_s = offset.to_string();
            let request_kind = format!("events limit={} offset={}", events_limit, offset);
            let events: Vec<GammaEventItem> = match self
                .get_json_with_retry(
                    &events_endpoint,
                    &[
                        ("active", "true"),
                        ("closed", "false"),
                        ("limit", limit_s.as_str()),
                        ("offset", offset_s.as_str()),
                    ],
                    &request_kind,
                )
                .await
            {
                Ok(v) => v,
                Err(err) => {
                    last_err = Some(err);
                    break 'events_scan;
                }
            };

            if events.is_empty() {
                break;
            }

            for event in events {
                if event.closed {
                    continue;
                }
                for market in event.markets {
                    absorb_discovered_market(
                        market,
                        &self.cfg,
                        now_ms,
                        &mut seen,
                        &mut out,
                        &mut matched_template_keys,
                        &target_template_keys,
                        can_early_stop,
                    );
                    if can_early_stop
                        && !target_template_keys.is_empty()
                        && matched_template_keys.len() >= target_template_keys.len()
                    {
                        break 'events_scan;
                    }
                }
            }
        }

        if can_early_stop
            && !target_template_keys.is_empty()
            && matched_template_keys.len() >= target_template_keys.len()
        {
            if self.cfg.one_market_per_template {
                out = collapse_to_markets_per_template(out, now_ms, self.cfg.markets_per_template);
            }
            return Ok(out);
        }

        'scan_plans: for plan in plans {
            for page_idx in 0..plan.max_pages {
                let offset = (page_idx as i64) * plan.limit;
                let limit_s = plan.limit.to_string();
                let offset_s = offset.to_string();
                // Do not set `active=true` query param here: Gamma may omit near-expiry
                // markets under that server-side filter. We filter by `market.active` below.
                let request_kind = format!(
                    "markets order={} asc={} limit={} offset={}",
                    plan.order, plan.ascending, plan.limit, offset
                );
                let markets: Vec<GammaMarket> = match self
                    .get_json_with_retry(
                        &self.cfg.endpoint,
                        &[
                            ("closed", "false"),
                            ("archived", "false"),
                            ("limit", limit_s.as_str()),
                            ("offset", offset_s.as_str()),
                            ("order", plan.order),
                            ("ascending", plan.ascending),
                        ],
                        &request_kind,
                    )
                    .await
                {
                    Ok(v) => v,
                    Err(err) => {
                        last_err = Some(err);
                        break;
                    }
                };

                if markets.is_empty() {
                    break;
                }

                for market in markets {
                    absorb_discovered_market(
                        market,
                        &self.cfg,
                        now_ms,
                        &mut seen,
                        &mut out,
                        &mut matched_template_keys,
                        &target_template_keys,
                        can_early_stop,
                    );
                    if can_early_stop
                        && !target_template_keys.is_empty()
                        && matched_template_keys.len() >= target_template_keys.len()
                    {
                        break 'scan_plans;
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

fn absorb_discovered_market(
    market: GammaMarket,
    cfg: &DiscoveryConfig,
    now_ms: i64,
    seen: &mut std::collections::HashSet<String>,
    out: &mut Vec<MarketDescriptor>,
    matched_template_keys: &mut std::collections::HashSet<String>,
    target_template_keys: &std::collections::HashSet<String>,
    can_early_stop: bool,
) {
    if market.closed {
        return;
    }
    if !seen.insert(market.id.clone()) {
        return;
    }

    let text = format!(
        "{} {}",
        market.question.to_ascii_uppercase(),
        market.slug.clone().unwrap_or_default().to_ascii_uppercase()
    );
    let market_type = classify_market_type(&text);
    if !cfg.market_types.is_empty()
        && !cfg
            .market_types
            .iter()
            .any(|t| t.eq_ignore_ascii_case(market_type))
    {
        return;
    }
    let timeframe = classify_timeframe_with_bounds(
        &text,
        market.event_start_time.as_deref(),
        market.end_date.as_deref(),
    );
    if !cfg.timeframes.is_empty() {
        let Some(tf) = timeframe else {
            return;
        };
        if !cfg.timeframes.iter().any(|t| t.eq_ignore_ascii_case(tf)) {
            return;
        }
    }
    if cfg.near_expiry_only
        && !is_within_discovery_window(market.end_date.as_deref(), timeframe, now_ms, cfg)
    {
        return;
    }
    let Some(symbol) = detect_symbol(&text, &cfg.symbols) else {
        return;
    };
    let end_ms = parse_end_date_ms(market.end_date.as_deref());
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
        token_id_yes: parse_token_pair(market.clob_token_ids.as_deref()).map(|x| x.0),
        token_id_no: parse_token_pair(market.clob_token_ids.as_deref()).map(|x| x.1),
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
            if end_ms.map(|v| v >= now_ms).unwrap_or(false) {
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
        }
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
            if end_ms >= now_ms {
                // Keep upcoming windows first (current -> next -> next-next).
                return (
                    0_u8,
                    0_u8,
                    end_ms.saturating_sub(now_ms),
                    m.market_id.clone(),
                );
            }
            let overdue_ms = now_ms.saturating_sub(end_ms);
            // Keep just-ended rounds only as a short emergency fallback.
            let ended_bucket = if overdue_ms <= recent_end_grace_ms(m.timeframe.as_deref()) {
                0_u8
            } else {
                1_u8
            };
            (1_u8, ended_bucket, overdue_ms, m.market_id.clone())
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaEventItem {
    #[serde(default)]
    closed: bool,
    #[serde(default)]
    markets: Vec<GammaMarket>,
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

    #[test]
    fn collapse_prefers_current_next_next2_when_keep_is_three() {
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
            mk("past", "2026-02-22T15:59:40Z"),
            mk("current", "2026-02-22T16:05:00Z"),
            mk("next", "2026-02-22T16:10:00Z"),
            mk("next2", "2026-02-22T16:15:00Z"),
            mk("far", "2026-02-22T16:30:00Z"),
        ];

        let mut out = collapse_to_markets_per_template(input, now, 3);
        out.sort_by(|a, b| a.market_id.cmp(&b.market_id));
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].market_id, "current");
        assert_eq!(out[1].market_id, "next");
        assert_eq!(out[2].market_id, "next2");
    }
}
