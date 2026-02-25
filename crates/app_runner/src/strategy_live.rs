use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use chrono::Utc;
use core_types::{ExecutionStyle, OrderIntentV2, OrderSide, PrebuiltAuth};
use execution_clob::ClobExecution;
use reqwest::Client;
use serde::Deserialize;

use crate::execution_eval::prebuild_order_payload;
use crate::state::{CachedPrebuild, EngineShared};
use crate::spawn_detached;

#[derive(Debug, Clone)]
struct LiveFastPathConfig {
    enabled: bool,
    gateway_url: String,
    interval_ms: u64,
    top_markets: usize,
    prebuild_ttl_ms: u64,
    prebuild_size: f64,
    prebuild_slippage_bps: f64,
    prebuild_time_window_sec: u64,
    cache_ttl_sec: u64,
    max_price_mismatch_bps: f64,
    max_size_mismatch_ratio: f64,
}

static LIVE_FASTPATH_CFG: OnceLock<LiveFastPathConfig> = OnceLock::new();

fn read_env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            let normalized = v.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(default)
}

fn read_env_u64(name: &str, default: u64, min: u64, max: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default)
        .clamp(min, max)
}

fn read_env_usize(name: &str, default: usize, min: usize, max: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(default)
        .clamp(min, max)
}

fn read_env_f64(name: &str, default: f64, min: f64, max: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(default)
        .clamp(min, max)
}

fn live_fastpath_config() -> &'static LiveFastPathConfig {
    LIVE_FASTPATH_CFG.get_or_init(|| LiveFastPathConfig {
        enabled: read_env_bool("POLYEDGE_LIVE_FASTPATH_ENABLED", true),
        gateway_url: std::env::var("POLYEDGE_PRESIGN_GATEWAY_URL")
            .ok()
            .map(|v| v.trim().trim_end_matches('/').to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "http://127.0.0.1:9001".to_string()),
        interval_ms: read_env_u64("POLYEDGE_PRESIGN_INTERVAL_MS", 300, 100, 3_000),
        top_markets: read_env_usize("POLYEDGE_PRESIGN_TOP_MARKETS", 10, 1, 64),
        prebuild_ttl_ms: read_env_u64("POLYEDGE_PRESIGN_TTL_MS", 30_000, 1_000, 120_000),
        prebuild_size: read_env_f64("POLYEDGE_PRESIGN_SIZE", 1.0, 0.01, 1_000.0),
        prebuild_slippage_bps: read_env_f64("POLYEDGE_PRESIGN_SLIPPAGE_BPS", 20.0, 0.0, 300.0),
        prebuild_time_window_sec: read_env_u64(
            "POLYEDGE_PRESIGN_TIME_WINDOW_SEC",
            90,
            10,
            300,
        ),
        cache_ttl_sec: read_env_u64("POLYEDGE_PRESIGN_CACHE_TTL_SEC", 45, 5, 180),
        max_price_mismatch_bps: read_env_f64(
            "POLYEDGE_PRESIGN_MAX_PRICE_MISMATCH_BPS",
            120.0,
            1.0,
            2_000.0,
        ),
        max_size_mismatch_ratio: read_env_f64(
            "POLYEDGE_PRESIGN_MAX_SIZE_MISMATCH_RATIO",
            0.5,
            0.0,
            1.0,
        ),
    })
}

fn side_cache_key(side: &OrderSide) -> &'static str {
    match side {
        OrderSide::BuyYes => "buy_yes",
        OrderSide::BuyNo => "buy_no",
        OrderSide::SellYes => "sell_yes",
        OrderSide::SellNo => "sell_no",
    }
}

fn cache_key(market_id: &str, side: &OrderSide) -> String {
    format!("{}:{}", market_id, side_cache_key(side))
}

fn price_delta_bps(a: f64, b: f64) -> f64 {
    let base = b.abs().max(1e-6);
    ((a - b).abs() / base) * 10_000.0
}

fn take_hmac_for_now(hmac_signatures: &mut HashMap<String, String>) -> Option<(String, String)> {
    let now = Utc::now().timestamp();
    for ts in [now, now + 1, now - 1, now + 2, now - 2] {
        let key = ts.to_string();
        if let Some(sig) = hmac_signatures.remove(&key) {
            return Some((key, sig));
        }
    }
    let fallback_key = hmac_signatures
        .keys()
        .min_by_key(|k| {
            k.parse::<i64>()
                .map(|ts| (ts - now).abs())
                .unwrap_or(i64::MAX)
        })?
        .clone();
    let sig = hmac_signatures.remove(&fallback_key)?;
    Some((fallback_key, sig))
}

#[derive(Debug, Deserialize)]
struct GatewayPrebuildResponse {
    ok: bool,
    body: String,
    api_key: String,
    api_passphrase: String,
    address: String,
    hmac_signatures: HashMap<String, String>,
}

#[derive(Debug, Clone)]
struct PrebuildRequest {
    key: String,
    token_id: String,
    side: OrderSide,
    price: f64,
}

fn build_prebuild_requests(
    market_id: &str,
    token_id_yes: &str,
    token_id_no: &str,
    ask_yes: f64,
    ask_no: f64,
    slip: f64,
) -> Vec<PrebuildRequest> {
    let yes = (ask_yes * (1.0 + slip)).clamp(0.001, 0.999);
    let no = (ask_no * (1.0 + slip)).clamp(0.001, 0.999);
    vec![
        PrebuildRequest {
            key: cache_key(market_id, &OrderSide::BuyYes),
            token_id: token_id_yes.to_string(),
            side: OrderSide::BuyYes,
            price: yes,
        },
        PrebuildRequest {
            key: cache_key(market_id, &OrderSide::BuyNo),
            token_id: token_id_no.to_string(),
            side: OrderSide::BuyNo,
            price: no,
        },
    ]
}

async fn request_prebuild(
    client: &Client,
    cfg: &LiveFastPathConfig,
    req: &PrebuildRequest,
) -> Option<CachedPrebuild> {
    let body = serde_json::json!({
        "token_id": req.token_id,
        "side": side_cache_key(&req.side),
        "price": req.price,
        "size": cfg.prebuild_size,
        "ttl_ms": cfg.prebuild_ttl_ms,
        "tif": "FAK",
        "fee_rate_bps": 0.0,
        "max_slippage_bps": 0.0,
        "time_window_sec": cfg.prebuild_time_window_sec,
    });

    let resp = client
        .post(format!("{}/prebuild_order", cfg.gateway_url))
        .json(&body)
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let parsed: GatewayPrebuildResponse = resp.json().await.ok()?;
    if !parsed.ok || parsed.body.is_empty() || parsed.hmac_signatures.is_empty() {
        return None;
    }
    Some(CachedPrebuild {
        price: req.price,
        size: cfg.prebuild_size,
        payload_bytes: parsed.body.into_bytes(),
        auth: PrebuiltAuth {
            api_key: parsed.api_key,
            passphrase: parsed.api_passphrase,
            address: parsed.address,
            timestamp_sec: String::new(),
            hmac_signature: String::new(),
        },
        hmac_signatures: parsed.hmac_signatures,
        fetched_at: Instant::now(),
    })
}

pub(crate) fn spawn_presign_worker(shared: Arc<EngineShared>) {
    let cfg = live_fastpath_config().clone();
    if !cfg.enabled {
        tracing::info!("live fastpath presign worker disabled");
        return;
    }
    spawn_detached("live_fastpath_presign_worker", false, async move {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_millis(450))
            .pool_idle_timeout(Duration::from_secs(20))
            .pool_max_idle_per_host(8)
            .build()
        {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "failed to build pre-sign client");
                return;
            }
        };
        let mut interval = tokio::time::interval(Duration::from_millis(cfg.interval_ms));
        loop {
            interval.tick().await;
            let mut markets = {
                let books = shared.latest_books.read().await;
                books
                    .iter()
                    .filter_map(|(market_id, b)| {
                        let spread_yes = (b.ask_yes - b.bid_yes).max(0.0);
                        let spread_no = (b.ask_no - b.bid_no).max(0.0);
                        let spread = spread_yes.min(spread_no);
                        if b.ask_yes > 0.0
                            && b.ask_no > 0.0
                            && b.bid_yes > 0.0
                            && b.bid_no > 0.0
                            && !b.token_id_yes.is_empty()
                            && !b.token_id_no.is_empty()
                            && spread.is_finite()
                        {
                            Some((market_id.clone(), b.clone(), spread))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            };
            markets.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));
            let slip = cfg.prebuild_slippage_bps / 10_000.0;
            for (market_id, book, _) in markets.into_iter().take(cfg.top_markets) {
                let reqs = build_prebuild_requests(
                    &market_id,
                    &book.token_id_yes,
                    &book.token_id_no,
                    book.ask_yes,
                    book.ask_no,
                    slip,
                );
                for req in reqs {
                    if let Some(prebuilt) = request_prebuild(&client, &cfg, &req).await {
                        shared.presign_cache.write().await.insert(req.key, prebuilt);
                    }
                }
            }
        }
    });
}

pub(crate) async fn prepare_live_intent_fastpath(
    shared: &Arc<EngineShared>,
    execution: &Arc<ClobExecution>,
    intent: &mut OrderIntentV2,
) -> bool {
    if !execution.is_live() {
        return false;
    }
    let cfg = live_fastpath_config();
    let mut cache_hit = false;
    if cfg.enabled && matches!(intent.style, ExecutionStyle::Taker) {
        let key = cache_key(&intent.market_id, &intent.side);
        if let Some(mut cached) = shared.presign_cache.write().await.remove(&key) {
            let age_ok = cached.fetched_at.elapsed() <= Duration::from_secs(cfg.cache_ttl_sec);
            let price_ok =
                price_delta_bps(cached.price, intent.price) <= cfg.max_price_mismatch_bps;
            let size_ok = ((cached.size - intent.size).abs() / intent.size.max(1e-6))
                <= cfg.max_size_mismatch_ratio;
            if age_ok && price_ok && size_ok {
                if let Some((timestamp_sec, hmac_signature)) =
                    take_hmac_for_now(&mut cached.hmac_signatures)
                {
                    intent.price = cached.price;
                    intent.size = cached.size;
                    intent.prebuilt_payload = Some(cached.payload_bytes.clone());
                    cached.auth.timestamp_sec = timestamp_sec;
                    cached.auth.hmac_signature = hmac_signature;
                    intent.prebuilt_auth = Some(cached.auth);
                    cache_hit = true;
                }
            }
        }
    }
    if !cache_hit {
        intent.prebuilt_auth = None;
        intent.prebuilt_payload = prebuild_order_payload(intent);
    }
    cache_hit
}
