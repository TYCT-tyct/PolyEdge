use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use core_types::{DynStream, RefPriceFeed, RefPriceWsFeed, RefTick};
use futures::{SinkExt, StreamExt};
use rand::Rng;
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

/// WebSocket connection timeout
const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// WebSocket read timeout - prevents hanging on stale connections
const WS_READ_TIMEOUT: Duration = Duration::from_secs(30);
const REF_TICK_QUEUE_DEFAULT: usize = 16_384;

/// Validates that a price value is finite and positive
fn validate_price(price: f64) -> bool {
    price.is_finite() && price > 0.0
}

fn env_flag(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(default)
}

#[derive(Debug, Clone)]
pub struct MultiSourceRefFeed {
    _http: Client,
    reconnect_backoff: Duration,
}

impl MultiSourceRefFeed {
    pub fn new(_poll_interval: Duration) -> Self {
        Self {
            _http: Client::new(),
            reconnect_backoff: Duration::from_secs(1),
        }
    }
}

#[async_trait::async_trait]
impl RefPriceFeed for MultiSourceRefFeed {
    async fn stream_ticks(&self, symbols: Vec<String>) -> Result<DynStream<RefTick>> {
        self.stream_ticks_ws(symbols).await
    }
}

#[async_trait::async_trait]
impl RefPriceWsFeed for MultiSourceRefFeed {
    async fn stream_ticks_ws(&self, symbols: Vec<String>) -> Result<DynStream<RefTick>> {
        let queue_cap = std::env::var("POLYEDGE_REF_TICK_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(REF_TICK_QUEUE_DEFAULT)
            .clamp(1_024, 65_536);
        let (tx, rx) = mpsc::channel::<RefTick>(queue_cap);

        let binance_symbols = symbols.clone();
        let tx_binance = tx.clone();
        let backoff = self.reconnect_backoff;
        if env_flag("POLYEDGE_ENABLE_BINANCE_WS", true) {
            tokio::spawn(async move {
                loop {
                    if let Err(err) = run_binance_stream(&binance_symbols, &tx_binance).await {
                        tracing::warn!(?err, "binance ws stream failed; reconnecting");
                    }
                    sleep_with_jitter(backoff).await;
                }
            });
        }

        let enable_chainlink_anchor = env_flag("POLYEDGE_ENABLE_CHAINLINK_ANCHOR", true);
        if enable_chainlink_anchor {
            let anchor_symbols = symbols.clone();
            let tx_anchor = tx.clone();
            tokio::spawn(async move {
                loop {
                    if let Err(err) = run_chainlink_rtds_stream(&anchor_symbols, &tx_anchor).await {
                        tracing::warn!(?err, "chainlink rtds stream failed; reconnecting");
                    }
                    sleep_with_jitter(backoff).await;
                }
            });
        }

        drop(tx);

        let stream = ReceiverStream::new(rx).map(Ok);
        Ok(Box::pin(stream))
    }
}

async fn sleep_with_jitter(base: Duration) {
    let base_ms = base.as_millis() as u64;
    let jitter_ms = rand::rng().random_range(0..=300);
    tokio::time::sleep(Duration::from_millis(base_ms.saturating_add(jitter_ms))).await;
}

enum TickDispatch {
    Sent,
    Dropped,
    Closed,
}

fn dispatch_ref_tick(tx: &mpsc::Sender<RefTick>, tick: RefTick, source: &'static str) -> TickDispatch {
    static DROP_COUNTER: AtomicU64 = AtomicU64::new(0);

    match tx.try_send(tick) {
        Ok(()) => TickDispatch::Sent,
        Err(mpsc::error::TrySendError::Full(_)) => {
            let dropped = DROP_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped.is_multiple_of(1024) {
                tracing::warn!(
                    source,
                    dropped,
                    "ref tick queue full, dropping stale ticks"
                );
            }
            TickDispatch::Dropped
        }
        Err(mpsc::error::TrySendError::Closed(_)) => TickDispatch::Closed,
    }
}

async fn run_binance_stream(symbols: &[String], tx: &mpsc::Sender<RefTick>) -> Result<()> {
    if symbols.is_empty() {
        anyhow::bail!("binance symbols list is empty");
    }
    let stream_kind = std::env::var("POLYEDGE_BINANCE_STREAM_KIND")
        .unwrap_or_else(|_| "bookTicker".to_string())
        .to_ascii_lowercase();
    let stream_suffix = if stream_kind == "trade" {
        "trade"
    } else {
        "bookTicker"
    };

    let streams = symbols
        .iter()
        .map(|s| format!("{}@{stream_suffix}", s.to_lowercase()))
        .collect::<Vec<_>>()
        .join("/");
    let endpoint_candidates = binance_ws_endpoints(&streams);
    let endpoint_candidates = pick_best_ws_endpoint(endpoint_candidates).await;
    let mut last_err: Option<anyhow::Error> = None;
    let mut ws = None;

    for endpoint in endpoint_candidates {
        match timeout(WS_CONNECT_TIMEOUT, connect_async(&endpoint)).await {
            Ok(Ok((socket, _))) => {
                ws = Some(socket);
                break;
            }
            Ok(Err(err)) => {
                tracing::warn!(
                    ?err,
                    endpoint,
                    "connect binance ws failed; trying next endpoint"
                );
                last_err = Some(anyhow::Error::new(err));
            }
            Err(_) => {
                tracing::warn!(endpoint = %endpoint, "connect binance ws timeout; trying next endpoint");
                last_err = Some(anyhow::anyhow!("connection timeout"));
            }
        }
    }
    let mut ws = ws.ok_or_else(|| {
        last_err
            .unwrap_or_else(|| anyhow::anyhow!("connect binance ws failed: no endpoint available"))
    })?;

    loop {
        match timeout(WS_READ_TIMEOUT, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                // Capture local receive timestamp as close as possible to socket delivery.
                let recv_ns = now_ns();
                let recv_ms = recv_ns / 1_000_000;
                let text = match msg {
                    Message::Text(t) => t.to_string(),
                    Message::Binary(b) => String::from_utf8_lossy(&b).to_string(),
                    Message::Ping(v) => {
                        let _ = ws.send(Message::Pong(v)).await;
                        continue;
                    }
                    Message::Pong(_) => continue,
                    Message::Close(_) => break,
                    Message::Frame(_) => continue,
                };

                let Ok(payload) = serde_json::from_str::<BinanceWsMessage>(&text) else {
                    continue;
                };
                let market = payload.into_market();
                let symbol = market.symbol;
                let price = market
                    .bid
                    .zip(market.ask)
                    .map(|(b, a)| (b + a) * 0.5)
                    .or(market.trade_price)
                    .unwrap_or(0.0);

                // Validate price before creating tick
                if !validate_price(price) {
                    tracing::warn!(price = price, "invalid binance price, skipping");
                    continue;
                }

                let event_ts = market.event_ts.unwrap_or_else(now_ms);
                let ingest_ns = now_ns();

                let tick = RefTick {
                    source: "binance_ws".to_string(),
                    symbol,
                    event_ts_ms: event_ts,
                    recv_ts_ms: recv_ms,
                    source_seq: event_ts.max(0) as u64,
                    event_ts_exchange_ms: event_ts,
                    recv_ts_local_ns: recv_ns,
                    ingest_ts_local_ns: ingest_ns,
                    price,
                };

                match dispatch_ref_tick(tx, tick, "binance_ws") {
                    TickDispatch::Sent | TickDispatch::Dropped => {}
                    TickDispatch::Closed => break,
                }
            }
            Ok(None) => break, // Stream ended
            Ok(Some(Err(e))) => {
                tracing::warn!(error = %e, "binance ws read error");
                break;
            }
            Err(_) => {
                tracing::warn!("binance ws read timeout, reconnecting");
                break;
            }
        }
    }

    Ok(())
}

async fn pick_best_ws_endpoint(endpoints: Vec<String>) -> Vec<String> {
    if endpoints.len() <= 1 {
        return endpoints;
    }

    // Probe all candidates concurrently at startup and prefer the fastest handshake.
    // This matters because some Binance hosts/ports resolve to different regions and DNS can
    // change over time; we want a deterministic "fastest-first" order.
    let timeout = Duration::from_secs(
        std::env::var("POLYEDGE_WS_PROBE_TIMEOUT_SEC")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(3)
            .max(1),
    );

    let mut join_set = tokio::task::JoinSet::new();
    for ep in endpoints.iter().cloned() {
        join_set.spawn(async move {
            let started = Instant::now();
            let ok = tokio::time::timeout(timeout, connect_async(&ep)).await;
            match ok {
                Ok(Ok((ws, _resp))) => {
                    drop(ws);
                    Some((ep, started.elapsed().as_secs_f64() * 1_000.0))
                }
                _ => None,
            }
        });
    }

    let mut results: Vec<(String, f64)> = Vec::new();
    while let Some(res) = join_set.join_next().await {
        if let Ok(Some(v)) = res {
            results.push(v);
        }
    }

    if results.is_empty() {
        return endpoints;
    }

    results.sort_by(|a, b| a.1.total_cmp(&b.1));
    for (ep, ms) in results.iter().take(8) {
        tracing::info!(
            endpoint = ep.as_str(),
            handshake_ms = *ms,
            "ws endpoint probe result"
        );
    }
    let best = results[0].0.clone();
    tracing::info!(
        endpoint = best.as_str(),
        handshake_ms = results[0].1,
        candidates = endpoints.len(),
        "selected best ws endpoint by handshake latency"
    );

    // Return endpoints reordered: best first, then the rest in original order.
    let mut out = Vec::with_capacity(endpoints.len());
    out.push(best.clone());
    for ep in endpoints {
        if ep != best {
            out.push(ep);
        }
    }
    out
}

fn binance_ws_endpoints(streams: &str) -> Vec<String> {
    if let Ok(raw) = std::env::var("POLYEDGE_BINANCE_WS_BASES") {
        let endpoints = raw
            .split(',')
            .map(str::trim)
            .filter(|v| v.starts_with("ws://") || v.starts_with("wss://"))
            .map(|base| format!("{}/stream?streams={streams}", base.trim_end_matches('/')))
            .collect::<Vec<_>>();
        if !endpoints.is_empty() {
            return endpoints;
        }
    }

    if let Ok(base) = std::env::var("POLYEDGE_BINANCE_WS_BASE") {
        if base.starts_with("ws://") || base.starts_with("wss://") {
            return vec![format!(
                "{}/stream?streams={streams}",
                base.trim_end_matches('/')
            )];
        }
    }

    vec![
        format!("wss://stream.binance.com:9443/stream?streams={streams}"),
        format!("wss://data-stream.binance.vision/stream?streams={streams}"),
        // Keep the default host as a fallback, but do not prefer it.
        format!("wss://stream.binance.com/stream?streams={streams}"),
    ]
}

#[derive(Debug, Deserialize)]
struct RtdsEnvelope {
    #[serde(default)]
    topic: Option<String>,
    #[serde(default)]
    timestamp: Option<i64>,
    #[serde(default)]
    payload: Option<RtdsPayload>,
}

#[derive(Debug, Deserialize)]
struct RtdsPayload {
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    timestamp: Option<i64>,
    #[serde(default)]
    value: Option<f64>,
}

fn chainlink_to_internal_symbol(symbol: &str) -> Option<String> {
    let sym = symbol.trim().to_ascii_uppercase();
    let base = sym
        .split(|c: char| c == '/' || c == '-' || c == '_' || c.is_whitespace())
        .next()?;
    if base.is_empty() {
        return None;
    }
    Some(format!("{base}USDT"))
}

async fn run_chainlink_rtds_stream(symbols: &[String], tx: &mpsc::Sender<RefTick>) -> Result<()> {
    let allowed = symbols
        .iter()
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect::<HashSet<_>>();
    if allowed.is_empty() {
        anyhow::bail!("chainlink rtds requires non-empty symbols");
    }

    let endpoint = std::env::var("POLYEDGE_RTDS_WS")
        .unwrap_or_else(|_| "wss://ws-live-data.polymarket.com".to_string());
    let (mut ws, _) = connect_async(&endpoint)
        .await
        .with_context(|| format!("connect polymarket rtds ws: {endpoint}"))?;

    let sub = serde_json::json!({
        "action": "subscribe",
        "subscriptions": [
            {"topic": "crypto_prices_chainlink", "type": "*", "filters": ""}
        ]
    });
    ws.send(Message::Text(sub.to_string().into()))
        .await
        .context("send chainlink rtds subscribe")?;

    let mut ping = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = ping.tick() => {
                // RTDS docs recommend sending a ping periodically.
                let _ = ws.send(Message::Ping(Vec::new().into())).await;
            }
             msg = ws.next() => {
                 let Some(msg) = msg else { break; };
                 let msg = msg.context("chainlink rtds read")?;
                 let recv_ns = now_ns();
                 let recv_ms = recv_ns / 1_000_000;
                 let text = match msg {
                     Message::Text(t) => t.to_string(),
                     Message::Binary(b) => String::from_utf8_lossy(&b).to_string(),
                     Message::Ping(v) => {
                        let _ = ws.send(Message::Pong(v)).await;
                        continue;
                    }
                    Message::Pong(_) => continue,
                    Message::Close(_) => break,
                    Message::Frame(_) => continue,
                };

                let Ok(env) = serde_json::from_str::<RtdsEnvelope>(&text) else {
                    continue;
                };
                if env.topic.as_deref() != Some("crypto_prices_chainlink") {
                    continue;
                }
                let payload = env.payload.unwrap_or(RtdsPayload {
                    symbol: None,
                    timestamp: None,
                    value: None,
                });
                let Some(raw_symbol) = payload.symbol.as_deref() else {
                    continue;
                };
                let Some(symbol) = chainlink_to_internal_symbol(raw_symbol) else {
                    continue;
                };
                if !allowed.contains(symbol.as_str()) {
                    continue;
                }
                let Some(price) = payload.value else {
                    continue;
                };
                let event_ts = payload.timestamp.or(env.timestamp).unwrap_or_else(now_ms);
                let ingest_ns = now_ns();

                let tick = RefTick {
                    source: "chainlink_rtds".to_string(),
                    symbol,
                    event_ts_ms: event_ts,
                    recv_ts_ms: recv_ms,
                    source_seq: event_ts.max(0) as u64,
                    event_ts_exchange_ms: event_ts,
                    recv_ts_local_ns: recv_ns,
                    ingest_ts_local_ns: ingest_ns,
                    price,
                };

                match dispatch_ref_tick(tx, tick, "chainlink_rtds") {
                    TickDispatch::Sent | TickDispatch::Dropped => {}
                    TickDispatch::Closed => break,
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BinanceWsMessage {
    Envelope { data: BinanceMarket },
    Market(BinanceMarket),
}

impl BinanceWsMessage {
    fn into_market(self) -> BinanceMarket {
        match self {
            Self::Envelope { data } => data,
            Self::Market(data) => data,
        }
    }
}

#[derive(Debug, Deserialize)]
struct BinanceMarket {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p", default, deserialize_with = "de_opt_f64_from_str")]
    trade_price: Option<f64>,
    #[serde(rename = "b", default, deserialize_with = "de_opt_f64_from_str")]
    bid: Option<f64>,
    #[serde(rename = "a", default, deserialize_with = "de_opt_f64_from_str")]
    ask: Option<f64>,
    #[serde(rename = "E")]
    event_ts: Option<i64>,
}

fn de_opt_f64_from_str<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(f64),
        Str(String),
    }
    let v = Option::<NumOrStr>::deserialize(deserializer)?;
    match v {
        Some(NumOrStr::Num(n)) => Ok(Some(n)),
        Some(NumOrStr::Str(s)) => s
            .parse::<f64>()
            .map(Some)
            .map_err(|e| serde::de::Error::custom(format!("invalid f64 string: {e}"))),
        None => Ok(None),
    }
}

/// Fast timestamp using SystemTime (more efficient than chrono::Utc::now())
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or_else(|_| now_ms() * 1_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chainlink_symbol_conversion() {
        assert_eq!(
            chainlink_to_internal_symbol("btc/usd").as_deref(),
            Some("BTCUSDT")
        );
        assert_eq!(
            chainlink_to_internal_symbol("eth-usd").as_deref(),
            Some("ETHUSDT")
        );
    }
}
