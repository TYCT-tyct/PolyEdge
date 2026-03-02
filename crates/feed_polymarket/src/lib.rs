use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use core_types::{
    BookDelta, BookLevel, BookSide, BookSnapshot, BookTop, BookUpdate, DynStream, MarketFeed,
    OrderbookStateDigest, PolymarketBookWsFeed,
};
use futures::{SinkExt, StreamExt};
use market_discovery::{DiscoveryConfig, MarketDiscovery};
use rand::Rng;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

/// WebSocket connection timeout
const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// WebSocket read timeout - prevents hanging on stale connections
const WS_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Validates that a price value is finite and within valid range [0, 1]
fn validate_price(price: f64) -> bool {
    price.is_finite() && (0.0..=1.0).contains(&price)
}

#[derive(Debug, Clone)]
pub struct PolymarketEndpoints {
    pub gamma_markets: String,
    pub clob_ws_market: String,
}

impl Default for PolymarketEndpoints {
    fn default() -> Self {
        Self {
            gamma_markets: "https://gamma-api.polymarket.com/markets".to_string(),
            clob_ws_market: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PolymarketFeed {
    pub endpoints: PolymarketEndpoints,
    reconnect_backoff: Duration,
    symbols: Vec<String>,
    market_types: Vec<String>,
    timeframes: Vec<String>,
}

impl PolymarketFeed {
    pub fn new(_poll_interval: Duration) -> Self {
        Self::new_with_symbols(_poll_interval, Vec::new())
    }

    pub fn new_with_symbols(_poll_interval: Duration, symbols: Vec<String>) -> Self {
        Self::new_with_universe(
            _poll_interval,
            symbols,
            vec!["updown".to_string()],
            vec![
                "5m".to_string(),
                "15m".to_string(),
                "1h".to_string(),
                "1d".to_string(),
            ],
        )
    }

    pub fn new_with_universe(
        _poll_interval: Duration,
        symbols: Vec<String>,
        market_types: Vec<String>,
        timeframes: Vec<String>,
    ) -> Self {
        Self {
            endpoints: PolymarketEndpoints::default(),
            reconnect_backoff: Duration::from_secs(1),
            symbols: symbols
                .into_iter()
                .map(|s| s.trim().to_ascii_uppercase())
                .filter(|s| !s.is_empty())
                .collect(),
            market_types: market_types
                .into_iter()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            timeframes: timeframes
                .into_iter()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
        }
    }

    fn cache_key(&self) -> String {
        let mut symbols = self.symbols.clone();
        symbols.sort();
        symbols.dedup();
        let mut market_types = self.market_types.clone();
        market_types.sort();
        market_types.dedup();
        let mut timeframes = self.timeframes.clone();
        timeframes.sort();
        timeframes.dedup();
        format!(
            "symbols={}|types={}|tfs={}",
            symbols.join(","),
            market_types.join(","),
            timeframes.join(",")
        )
    }

    async fn read_cached_markets(&self) -> Option<HashMap<String, MarketState>> {
        let key = self.cache_key();
        {
            let cache = target_market_cache().read().await;
            if let Some(markets) = cache.get(&key).cloned() {
                return Some(markets);
            }
        }

        let disk_cache = read_target_market_cache_file().await?;
        let markets = disk_cache.get(&key).cloned()?;
        let mut cache = target_market_cache().write().await;
        cache.insert(key, markets.clone());
        Some(markets)
    }

    async fn write_cached_markets(&self, markets: &HashMap<String, MarketState>) {
        let key = self.cache_key();
        let snapshot = {
            let mut cache = target_market_cache().write().await;
            let merged = cache
                .get(&key)
                .map(|existing| merge_market_state_maps(existing, markets))
                .unwrap_or_else(|| markets.clone());
            cache.insert(key, merged);
            cache.clone()
        };
        if let Err(err) = write_target_market_cache_file(&snapshot).await {
            tracing::warn!(?err, "persist target market cache failed");
        }
    }

    pub async fn fetch_active_books(&self) -> Result<Vec<BookTop>> {
        let markets = self.discover_target_markets().await?;
        let mut out = Vec::new();
        for market in markets.values() {
            out.push(market.to_book_top());
        }
        Ok(out)
    }

    async fn discover_target_markets(&self) -> Result<HashMap<String, MarketState>> {
        let discovery = MarketDiscovery::new(DiscoveryConfig {
            symbols: self.symbols.clone(),
            market_types: self.market_types.clone(),
            timeframes: self.timeframes.clone(),
            endpoint: self.endpoints.gamma_markets.clone(),
            ..DiscoveryConfig::default()
        });
        let markets = discovery.discover().await?;

        let mut out = HashMap::<String, MarketState>::new();
        let ts_ms = chrono::Utc::now().timestamp_millis();

        for market in markets {
            let (Some(yes), Some(no)) = (market.token_id_yes, market.token_id_no) else {
                continue;
            };
            if out.contains_key(&market.market_id) {
                continue;
            }
            let bid_yes = market.best_bid.unwrap_or(0.0).clamp(0.0, 1.0);
            let ask_yes = market.best_ask.unwrap_or(1.0).clamp(0.0, 1.0);
            let bid_no = (1.0 - ask_yes).max(0.0);
            let ask_no = (1.0 - bid_yes).min(1.0);

            out.insert(
                market.market_id.clone(),
                MarketState {
                    market_id: market.market_id,
                    timeframe: market.timeframe,
                    yes_token: yes,
                    no_token: no,
                    yes: AssetTop {
                        bid: bid_yes,
                        ask: ask_yes,
                        bid_size: 0.0,
                        ask_size: 0.0,
                        ts_exchange_ms: ts_ms,
                        recv_ts_local_ns: now_ns(),
                    },
                    no: AssetTop {
                        bid: bid_no,
                        ask: ask_no,
                        bid_size: 0.0,
                        ask_size: 0.0,
                        ts_exchange_ms: ts_ms,
                        recv_ts_local_ns: now_ns(),
                    },
                },
            );
        }

        if out.is_empty() {
            return Err(anyhow!("no active target markets discovered"));
        }

        self.write_cached_markets(&out).await;
        Ok(out)
    }

    async fn stream_books_ws(&self) -> Result<DynStream<BookTop>> {
        let (tx, rx) = mpsc::channel::<BookTop>(16_384);
        let this = self.clone();

        tokio::spawn(async move {
            // Reconnect lifecycle is owned by the active runtime service.
            // Keep this worker single-shot to avoid nested reconnect loops that
            // can fan out into discovery storms (and trigger Gamma 429s).
            if let Err(err) = this.run_market_loop(&tx).await {
                tracing::warn!(?err, "polymarket market ws loop failed");
            }
        });

        let stream = ReceiverStream::new(rx).map(Ok);
        Ok(Box::pin(stream))
    }

    async fn run_market_loop(&self, tx: &mpsc::Sender<BookTop>) -> Result<()> {
        let mut markets = match self.discover_target_markets().await {
            Ok(v) => v,
            Err(err) => {
                if let Some(cached) = self.read_cached_markets().await {
                    tracing::warn!(
                        ?err,
                        cached_markets = cached.len(),
                        "discover target markets failed, using cached markets"
                    );
                    cached
                } else {
                    return Err(err);
                }
            }
        };

        for state in markets.values() {
            if tx.send(state.to_book_top()).await.is_err() {
                return Ok(());
            }
        }

        let idle_break_sec = std::env::var("POLYEDGE_MARKET_WS_IDLE_BREAK_SEC")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(25)
            .max(10);
        let default_idle_break = Duration::from_secs(idle_break_sec);
        let mut market_idle_break = HashMap::<String, Duration>::new();
        let now = Instant::now();
        let mut market_last_update_at = HashMap::<String, Instant>::new();
        for (market_id, state) in &markets {
            market_idle_break.insert(
                market_id.clone(),
                market_stale_timeout(state, default_idle_break),
            );
            market_last_update_at.insert(market_id.clone(), now);
        }

        let asset_map = build_asset_to_market_map(&markets);
        let assets = asset_map.keys().cloned().collect::<Vec<_>>();
        tracing::info!(
            market_count = markets.len(),
            asset_count = assets.len(),
            endpoint = %self.endpoints.clob_ws_market,
            "polymarket market ws subscribing"
        );

        let (mut ws, _) = timeout(
            WS_CONNECT_TIMEOUT,
            connect_async(&self.endpoints.clob_ws_market),
        )
        .await
        .context("connect polymarket market ws timeout")?
        .context("connect polymarket market ws")?;
        tracing::info!(endpoint = %self.endpoints.clob_ws_market, "polymarket market ws connected");

        let sub = serde_json::json!({
            "type": "market",
            "assets_ids": assets,
        });
        ws.send(Message::Text(sub.to_string().into()))
            .await
            .context("send polymarket subscribe")?;
        tracing::info!("polymarket market ws subscribed");

        let mut ping = tokio::time::interval(Duration::from_secs(15));
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut idle_tick = tokio::time::interval(Duration::from_secs(5));
        idle_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Re-discover market set periodically and reconnect immediately when IDs change.
        // This prevents waiting for coarse refresh windows at round boundaries.
        let rediscover_every = std::env::var("POLYEDGE_MARKET_REDISCOVER_SEC")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(5)
            .clamp(2, 60);
        let mut rediscover_tick = tokio::time::interval(Duration::from_secs(rediscover_every));
        rediscover_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_update_at = Instant::now();
        // Many Polymarket markets (especially 5m/15m contracts) expire quickly. If the WS
        // connection stays up, we would otherwise keep subscribing to stale/closed assets and
        // stop seeing updates. Force a periodic re-discovery + resubscribe.
        let refresh_every = std::env::var("POLYEDGE_MARKET_REFRESH_SEC")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(180));
        let stale_reconnect_ratio = std::env::var("POLYEDGE_MARKET_STALE_RECONNECT_RATIO")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(1.0)
            .clamp(0.25, 1.0);
        let refresh_deadline = tokio::time::sleep(refresh_every);
        tokio::pin!(refresh_deadline);
        let mut parse_failures = 0_u64;
        let mut no_update_msgs = 0_u64;
        let mut seen_msgs = 0_u64;

        loop {
            tokio::select! {
                _ = idle_tick.tick() => {
                    let now = Instant::now();
                    let mut stale_market_count = 0_usize;
                    let mut most_stale_market: Option<(String, u64, u64, Option<String>)> = None;
                    for (market_id, updated_at) in &market_last_update_at {
                        let timeout = market_idle_break
                            .get(market_id)
                            .copied()
                            .unwrap_or(default_idle_break);
                        let elapsed = now.saturating_duration_since(*updated_at);
                        if elapsed >= timeout {
                            stale_market_count = stale_market_count.saturating_add(1);
                            let elapsed_sec = elapsed.as_secs();
                            if most_stale_market
                                .as_ref()
                                .map(|(_, current_elapsed_sec, _, _)| elapsed_sec > *current_elapsed_sec)
                                .unwrap_or(true)
                            {
                                let timeframe = markets.get(market_id).and_then(|m| m.timeframe.clone());
                                most_stale_market = Some((
                                    market_id.clone(),
                                    elapsed_sec,
                                    timeout.as_secs(),
                                    timeframe,
                                ));
                            }
                        }
                    }
                    let market_count = market_last_update_at.len().max(1);
                    let stale_ratio = stale_market_count as f64 / market_count as f64;
                    if stale_market_count > 0 && stale_ratio >= stale_reconnect_ratio {
                        if let Some((market_id, idle_sec, idle_break_sec_market, timeframe)) =
                            most_stale_market
                        {
                            tracing::warn!(
                                %market_id,
                                timeframe = timeframe.as_deref().unwrap_or("unknown"),
                                stale_market_count,
                                market_count,
                                stale_ratio,
                                stale_reconnect_ratio,
                                idle_sec,
                                idle_break_sec_market,
                                "polymarket market ws stale market detected, reconnecting"
                            );
                            break;
                        }
                    }
                    if last_update_at.elapsed().as_secs() >= idle_break_sec {
                        tracing::warn!(
                            idle_sec = last_update_at.elapsed().as_secs(),
                            idle_break_sec,
                            "polymarket market ws idle too long, reconnecting"
                        );
                        break;
                    }
                }
                _ = rediscover_tick.tick() => {
                    match self.discover_target_markets().await {
                        Ok(latest_markets) => {
                            let current_ids = markets.keys().cloned().collect::<HashSet<_>>();
                            let latest_ids = latest_markets.keys().cloned().collect::<HashSet<_>>();
                            if current_ids != latest_ids {
                                let added = latest_ids.difference(&current_ids).count();
                                let removed = current_ids.difference(&latest_ids).count();
                                tracing::info!(
                                    current_market_count = markets.len(),
                                    discovered_market_count = latest_markets.len(),
                                    added,
                                    removed,
                                    rediscover_sec = rediscover_every,
                                    "polymarket market set changed, reconnecting"
                                );
                                break;
                            }
                        }
                        Err(err) => {
                            tracing::warn!(
                                error = %err,
                                rediscover_sec = rediscover_every,
                                "polymarket market rediscover failed"
                            );
                        }
                    }
                }
                _ = &mut refresh_deadline => {
                    tracing::info!(
                        refresh_sec = refresh_every.as_secs(),
                        "polymarket market ws refresh triggered; resubscribing"
                    );
                    break;
                }
                _ = ping.tick() => {
                    // The Polymarket WS docs recommend an application-level "PING".
                    ws.send(Message::Text("PING".to_string().into()))
                        .await
                        .context("send polymarket ping")?;
                }
                msg = timeout(WS_READ_TIMEOUT, ws.next()) => {
                    let msg = match msg {
                        Ok(m) => m,
                        Err(_) => {
                            tracing::warn!("polymarket ws read timeout, reconnecting");
                            break;
                        }
                    };
                    let msg = match msg {
                        Some(Ok(m)) => m,
                        None => break, // Stream ended
                        Some(Err(e)) => {
                            tracing::warn!(error = %e, "polymarket ws read error");
                            break;
                        }
                    };
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
                    if text == "PONG" {
                        continue;
                    }
                    seen_msgs = seen_msgs.saturating_add(1);
                    if seen_msgs <= 2 {
                        let preview = text
                            .chars()
                            .take(240)
                            .collect::<String>()
                            .replace('\n', "\\n");
                        tracing::info!(preview = %preview, "polymarket market ws first messages");
                    }

                    let payload = match serde_json::from_str::<WsEnvelope>(&text) {
                        Ok(v) => v,
                        Err(err) => {
                            parse_failures = parse_failures.saturating_add(1);
                            if parse_failures <= 5 {
                                let preview = text
                                    .chars()
                                    .take(240)
                                    .collect::<String>()
                                    .replace('\n', "\\n");
                                tracing::warn!(
                                    ?err,
                                    failures = parse_failures,
                                    preview = %preview,
                                    "polymarket market ws json parse failed"
                                );
                            }
                            continue;
                        }
                    };

                    for event in payload.into_events() {
                        let updates = parse_asset_updates(&event);
                        if updates.is_empty() {
                            no_update_msgs = no_update_msgs.saturating_add(1);
                            if no_update_msgs <= 3 {
                                tracing::info!(
                                    kind = event.kind.as_deref().unwrap_or(""),
                                    event_type = event.event_type.as_deref().unwrap_or(""),
                                    "polymarket market ws parsed message without updates"
                                );
                            }
                        }
                        for update in updates {
                            let Some((market_id, is_yes)) = asset_map.get(&update.asset_id).cloned() else {
                                continue;
                            };
                            let Some(state) = markets.get_mut(&market_id) else {
                                continue;
                            };

                            let target = if is_yes {
                                &mut state.yes
                            } else {
                                &mut state.no
                            };
                            // Validate price data before assignment
                            if let Some(v) = update.best_bid {
                                if validate_price(v) {
                                    target.bid = v;
                                } else {
                                    tracing::warn!(price = v, "invalid bid price, skipping");
                                }
                            }
                            if let Some(v) = update.best_ask {
                                if validate_price(v) {
                                    target.ask = v;
                                } else {
                                    tracing::warn!(price = v, "invalid ask price, skipping");
                                }
                            }
                            if let Some(v) = update.best_bid_size {
                                target.bid_size = v;
                            }
                            if let Some(v) = update.best_ask_size {
                                target.ask_size = v;
                            }
                            target.ts_exchange_ms = update.ts_exchange_ms;
                            target.recv_ts_local_ns = update.recv_ts_local_ns;

                            if tx.send(state.to_book_top()).await.is_err() {
                                return Ok(());
                            }
                            let now = Instant::now();
                            last_update_at = now;
                            market_last_update_at.insert(market_id, now);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

fn target_market_cache(
) -> &'static tokio::sync::RwLock<HashMap<String, HashMap<String, MarketState>>> {
    static CACHE: OnceLock<tokio::sync::RwLock<HashMap<String, HashMap<String, MarketState>>>> =
        OnceLock::new();
    CACHE.get_or_init(|| tokio::sync::RwLock::new(HashMap::new()))
}

fn market_state_recency_ms(state: &MarketState) -> i64 {
    state.yes.ts_exchange_ms.max(state.no.ts_exchange_ms)
}

fn market_state_timeframe_key(state: &MarketState) -> String {
    state
        .timeframe
        .as_deref()
        .unwrap_or("unknown")
        .trim()
        .to_ascii_lowercase()
}

fn target_market_cache_max_per_timeframe() -> usize {
    std::env::var("POLYEDGE_TARGET_MARKET_CACHE_MAX_PER_TF")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(32)
        .clamp(1, 256)
}

fn merge_market_state_maps(
    existing: &HashMap<String, MarketState>,
    latest: &HashMap<String, MarketState>,
) -> HashMap<String, MarketState> {
    let mut merged = existing.clone();
    for (market_id, state) in latest {
        match merged.get(market_id) {
            Some(prev) if market_state_recency_ms(prev) > market_state_recency_ms(state) => {}
            _ => {
                merged.insert(market_id.clone(), state.clone());
            }
        }
    }

    let max_per_tf = target_market_cache_max_per_timeframe();
    let mut grouped: HashMap<String, Vec<(String, MarketState)>> = HashMap::new();
    for (market_id, state) in merged {
        grouped
            .entry(market_state_timeframe_key(&state))
            .or_default()
            .push((market_id, state));
    }

    let mut pruned = HashMap::<String, MarketState>::new();
    for mut entries in grouped.into_values() {
        entries.sort_by_key(|(_, state)| std::cmp::Reverse(market_state_recency_ms(state)));
        for (idx, (market_id, state)) in entries.into_iter().enumerate() {
            if idx >= max_per_tf {
                break;
            }
            pruned.insert(market_id, state);
        }
    }
    pruned
}

fn target_market_cache_file_path() -> PathBuf {
    if let Ok(raw) = std::env::var("POLYEDGE_TARGET_MARKET_CACHE_FILE") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    std::env::temp_dir().join("polyedge_target_market_cache.json")
}

async fn read_target_market_cache_file() -> Option<HashMap<String, HashMap<String, MarketState>>> {
    let path = target_market_cache_file_path();
    let raw = tokio::fs::read_to_string(&path).await.ok()?;
    match serde_json::from_str::<HashMap<String, HashMap<String, MarketState>>>(&raw) {
        Ok(v) if !v.is_empty() => Some(v),
        Ok(_) => None,
        Err(err) => {
            tracing::warn!(?err, path = %path.display(), "parse target market cache file failed");
            None
        }
    }
}

async fn write_target_market_cache_file(
    cache: &HashMap<String, HashMap<String, MarketState>>,
) -> Result<()> {
    let path = target_market_cache_file_path();
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create cache dir: {}", parent.display()))?;
    }
    let payload =
        serde_json::to_vec(cache).context("serialize target market cache for persistence")?;
    tokio::fs::write(&path, payload)
        .await
        .with_context(|| format!("write target market cache: {}", path.display()))?;
    Ok(())
}

#[async_trait::async_trait]
impl MarketFeed for PolymarketFeed {
    async fn stream_books(&self) -> Result<DynStream<BookTop>> {
        self.stream_books_ws().await
    }
}

#[async_trait::async_trait]
impl PolymarketBookWsFeed for PolymarketFeed {
    async fn stream_book(&self, token_ids: Vec<String>) -> Result<DynStream<BookUpdate>> {
        if token_ids.is_empty() {
            return Err(anyhow!("token_ids cannot be empty"));
        }

        let endpoint = self.endpoints.clob_ws_market.clone();
        let gamma_endpoint = self.endpoints.gamma_markets.clone();
        let backoff = self.reconnect_backoff;

        let (tx, rx) = mpsc::channel::<BookUpdate>(16_384);
        tokio::spawn(async move {
            loop {
                if let Err(err) =
                    run_book_update_loop(&endpoint, &gamma_endpoint, &token_ids, &tx).await
                {
                    tracing::warn!(?err, "book update ws loop failed; reconnecting");
                }
                sleep_with_jitter(backoff).await;
            }
        });

        let stream = ReceiverStream::new(rx).map(Ok);
        Ok(Box::pin(stream))
    }
}

async fn run_book_update_loop(
    endpoint: &str,
    gamma_endpoint: &str,
    token_ids: &[String],
    tx: &mpsc::Sender<BookUpdate>,
) -> Result<()> {
    let token_market_map = fetch_token_market_map(gamma_endpoint, token_ids).await?;

    let (mut ws, _) = timeout(WS_CONNECT_TIMEOUT, connect_async(endpoint))
        .await
        .with_context(|| format!("connect polymarket ws timeout: {endpoint}"))?
        .with_context(|| format!("connect polymarket ws: {endpoint}"))?;

    let sub = serde_json::json!({
        "type": "market",
        "assets_ids": token_ids,
    });
    ws.send(Message::Text(sub.to_string().into()))
        .await
        .context("send polymarket subscribe")?;

    let mut ping = tokio::time::interval(Duration::from_secs(15));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut parse_failures = 0_u64;

    loop {
        tokio::select! {
            _ = ping.tick() => {
                ws.send(Message::Text("PING".to_string().into()))
                    .await
                    .context("send polymarket book ping")?;
            }
            msg = timeout(WS_READ_TIMEOUT, ws.next()) => {
                let msg = match msg {
                    Ok(m) => m,
                    Err(_) => {
                        tracing::warn!("polymarket book ws read timeout, reconnecting");
                        break;
                    }
                };
                let msg = match msg {
                    Some(Ok(m)) => m,
                    None => break,
                    Some(Err(e)) => {
                        tracing::warn!(error = %e, "polymarket book ws read error");
                        break;
                    }
                };
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
                if text == "PONG" {
                    continue;
                }

                let payload = match serde_json::from_str::<WsEnvelope>(&text) {
                    Ok(v) => v,
                    Err(err) => {
                        parse_failures = parse_failures.saturating_add(1);
                        if parse_failures <= 5 {
                            let preview = text
                                .chars()
                                .take(240)
                                .collect::<String>()
                                .replace('\n', "\\n");
                            tracing::warn!(
                                ?err,
                                failures = parse_failures,
                                preview = %preview,
                                "polymarket book ws json parse failed"
                            );
                        }
                        continue;
                    }
                };

                for event in payload.into_events() {
                    if let Some(mut snapshot) = parse_snapshot(&event) {
                        if let Some(market_id) = token_market_map.get(&snapshot.asset_id) {
                            snapshot.market_id = market_id.clone();
                        }
                        // Validate snapshot price levels
                        let mut valid_snapshot = true;
                        for level in snapshot.bids.iter().chain(snapshot.asks.iter()) {
                            if !validate_price(level.price) {
                                tracing::warn!(price = level.price, "invalid snapshot price level");
                                valid_snapshot = false;
                                break;
                            }
                        }
                        if valid_snapshot && tx.send(BookUpdate::Snapshot(snapshot)).await.is_err() {
                            return Ok(());
                        }
                    }

                    for mut delta in parse_deltas(&event) {
                        if let Some(market_id) = token_market_map.get(&delta.asset_id) {
                            delta.market_id = market_id.clone();
                        }
                        // Validate delta prices
                        let valid_bid = delta.best_bid.map(validate_price).unwrap_or(true);
                        let valid_ask = delta.best_ask.map(validate_price).unwrap_or(true);
                        if !valid_bid || !valid_ask {
                            tracing::warn!("invalid delta prices, skipping");
                            continue;
                        }
                        let digest = OrderbookStateDigest {
                            market_id: delta.market_id.clone(),
                            asset_id: delta.asset_id.clone(),
                            best_bid: delta.best_bid.unwrap_or(0.0),
                            best_ask: delta.best_ask.unwrap_or(1.0),
                            spread: (delta.best_ask.unwrap_or(1.0) - delta.best_bid.unwrap_or(0.0))
                                .max(0.0),
                            ts_exchange_ms: delta.ts_exchange_ms,
                            recv_ts_local_ns: delta.recv_ts_local_ns,
                        };
                        if tx.send(BookUpdate::Delta(delta)).await.is_err() {
                            return Ok(());
                        }
                        if tx.send(BookUpdate::Digest(digest)).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn build_asset_to_market_map(
    markets: &HashMap<String, MarketState>,
) -> HashMap<String, (String, bool)> {
    let mut out = HashMap::new();
    for (market_id, state) in markets {
        out.insert(state.yes_token.clone(), (market_id.clone(), true));
        out.insert(state.no_token.clone(), (market_id.clone(), false));
    }
    out
}

fn market_stale_timeout(state: &MarketState, fallback: Duration) -> Duration {
    fn env_secs(key: &str, default: u64, min: u64, max: u64) -> u64 {
        std::env::var(key)
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(default)
            .clamp(min, max)
    }
    let timeframe = state
        .timeframe
        .as_deref()
        .map(|v| v.trim().to_ascii_lowercase());
    match timeframe.as_deref() {
        Some("5m") => {
            Duration::from_secs(env_secs("POLYEDGE_MARKET_IDLE_BREAK_SEC_5M", 20, 5, 300))
        }
        Some("15m") => {
            Duration::from_secs(env_secs("POLYEDGE_MARKET_IDLE_BREAK_SEC_15M", 35, 5, 300))
        }
        Some("1h") => {
            Duration::from_secs(env_secs("POLYEDGE_MARKET_IDLE_BREAK_SEC_1H", 45, 5, 600))
        }
        Some("1d") => {
            Duration::from_secs(env_secs("POLYEDGE_MARKET_IDLE_BREAK_SEC_1D", 60, 5, 900))
        }
        _ => fallback,
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarketTokens {
    id: String,
    #[serde(default)]
    clob_token_ids: Option<String>,
    #[serde(default)]
    closed: bool,
    #[serde(default)]
    accepting_orders: Option<bool>,
}

async fn fetch_token_market_map(
    gamma_endpoint: &str,
    token_ids: &[String],
) -> Result<HashMap<String, String>> {
    let mut wanted = HashSet::<String>::new();
    for t in token_ids {
        let t = t.trim();
        if !t.is_empty() {
            wanted.insert(t.to_string());
        }
    }
    if wanted.is_empty() {
        return Ok(HashMap::new());
    }

    let http = Client::new();
    let mut out = HashMap::<String, String>::new();

    let limit: i64 = 1000;
    for offset in [0_i64, 1000, 2000, 3000] {
        if wanted.is_empty() {
            break;
        }
        let limit_s = limit.to_string();
        let offset_s = offset.to_string();
        let markets: Vec<GammaMarketTokens> = http
            .get(gamma_endpoint)
            .query(&[
                ("closed", "false"),
                ("archived", "false"),
                ("limit", limit_s.as_str()),
                ("offset", offset_s.as_str()),
                ("order", "volume"),
                ("ascending", "false"),
            ])
            .send()
            .await
            .context("gamma request")?
            .error_for_status()
            .context("gamma status")?
            .json()
            .await
            .context("gamma json")?;

        if markets.is_empty() {
            break;
        }

        for market in markets {
            // Gamma may omit `acceptingOrders`; only explicit false should be rejected.
            if market.closed || matches!(market.accepting_orders, Some(false)) {
                continue;
            }
            let Some((yes, no)) = parse_token_pair(market.clob_token_ids.as_deref()) else {
                continue;
            };
            if wanted.remove(&yes) {
                out.insert(yes, market.id.clone());
            }
            if wanted.remove(&no) {
                out.insert(no, market.id.clone());
            }
            if wanted.is_empty() {
                break;
            }
        }
    }

    Ok(out)
}

fn parse_token_pair(input: Option<&str>) -> Option<(String, String)> {
    let raw = input?;
    let parsed: Vec<String> = serde_json::from_str(raw).ok()?;
    if parsed.len() < 2 {
        return None;
    }
    Some((parsed[0].clone(), parsed[1].clone()))
}

fn parse_asset_updates(payload: &WsEvent) -> Vec<AssetUpdate> {
    let mut out = Vec::new();
    collect_asset_updates(payload, &mut out);
    merge_asset_updates(out)
}

fn collect_asset_updates(payload: &WsEvent, out: &mut Vec<AssetUpdate>) {
    if let Some(update) = parse_single_asset_update(payload) {
        out.push(update);
    }
    if let Some(update) = parse_change_based_update(payload) {
        out.push(update);
    }
    if let Some(update) = parse_trade_based_update(payload) {
        out.push(update);
    }
    for item in &payload.price_changes {
        collect_asset_updates(item, out);
    }
}

fn merge_asset_updates(items: Vec<AssetUpdate>) -> Vec<AssetUpdate> {
    let mut merged = HashMap::<String, AssetUpdate>::new();
    for item in items {
        let key = item.asset_id.clone();
        let entry = merged.entry(key).or_insert_with(|| AssetUpdate {
            asset_id: item.asset_id.clone(),
            best_bid: None,
            best_bid_size: None,
            best_ask: None,
            best_ask_size: None,
            ts_exchange_ms: item.ts_exchange_ms,
            recv_ts_local_ns: item.recv_ts_local_ns,
        });
        if item.best_bid.is_some() {
            entry.best_bid = item.best_bid;
        }
        if item.best_bid_size.is_some() {
            entry.best_bid_size = item.best_bid_size;
        }
        if item.best_ask.is_some() {
            entry.best_ask = item.best_ask;
        }
        if item.best_ask_size.is_some() {
            entry.best_ask_size = item.best_ask_size;
        }
        if item.ts_exchange_ms > entry.ts_exchange_ms {
            entry.ts_exchange_ms = item.ts_exchange_ms;
        }
        if item.recv_ts_local_ns > entry.recv_ts_local_ns {
            entry.recv_ts_local_ns = item.recv_ts_local_ns;
        }
    }
    merged.into_values().collect()
}

fn parse_single_asset_update(payload: &WsEvent) -> Option<AssetUpdate> {
    let asset_id = payload.asset_id.clone()?;

    let (bid_top_price, bid_top_size) =
        top_level_price_and_size(payload.bids.as_ref().or(payload.buys.as_ref()));
    let best_bid = payload.best_bid.or(bid_top_price);
    let best_bid_size = bid_top_size;

    let (ask_top_price, ask_top_size) =
        top_level_price_and_size(payload.asks.as_ref().or(payload.sells.as_ref()));
    let best_ask = payload.best_ask.or(ask_top_price);
    let best_ask_size = ask_top_size;
    if best_bid.is_none() || best_ask.is_none() {
        return None;
    }

    Some(AssetUpdate {
        asset_id,
        best_bid,
        best_bid_size,
        best_ask,
        best_ask_size,
        ts_exchange_ms: payload.timestamp.unwrap_or_else(now_ms),
        recv_ts_local_ns: now_ns(),
    })
}

fn parse_change_based_update(payload: &WsEvent) -> Option<AssetUpdate> {
    let asset_id = payload.asset_id.clone()?;
    if payload.changes.is_empty() {
        return None;
    }

    let mut best_bid: Option<f64> = None;
    let mut best_ask: Option<f64> = None;
    let mut bid_size: Option<f64> = None;
    let mut ask_size: Option<f64> = None;

    for c in &payload.changes {
        let Some(price) = c.price else {
            continue;
        };
        let side = c
            .side
            .as_deref()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        let size = c.size.unwrap_or(0.0);
        if side == "sell" || side == "ask" {
            if best_ask.map(|v| price < v).unwrap_or(true) {
                best_ask = Some(price);
                ask_size = Some(size);
            }
        } else if best_bid.map(|v| price > v).unwrap_or(true) {
            best_bid = Some(price);
            bid_size = Some(size);
        }
    }

    if best_bid.is_none() && best_ask.is_none() {
        return None;
    }

    Some(AssetUpdate {
        asset_id,
        best_bid,
        best_bid_size: bid_size,
        best_ask,
        best_ask_size: ask_size,
        ts_exchange_ms: payload.timestamp.unwrap_or_else(now_ms),
        recv_ts_local_ns: now_ns(),
    })
}

fn parse_trade_based_update(payload: &WsEvent) -> Option<AssetUpdate> {
    if payload.best_bid.is_some()
        || payload.best_ask.is_some()
        || !payload.changes.is_empty()
        || payload.bids.as_ref().is_some()
        || payload.asks.as_ref().is_some()
        || payload.buys.as_ref().is_some()
        || payload.sells.as_ref().is_some()
    {
        return None;
    }

    let asset_id = payload.asset_id.clone()?;
    let price = payload.price?;
    if !validate_price(price) {
        return None;
    }
    let size = payload.size;

    // last_trade_price events carry executed price rather than top-of-book.
    // Using bid=ask=trade keeps the displayed midpoint aligned with actual tape moves.
    Some(AssetUpdate {
        asset_id,
        best_bid: Some(price),
        best_bid_size: size,
        best_ask: Some(price),
        best_ask_size: size,
        ts_exchange_ms: payload.timestamp.unwrap_or_else(now_ms),
        recv_ts_local_ns: now_ns(),
    })
}

fn parse_snapshot(payload: &WsEvent) -> Option<BookSnapshot> {
    let event_type = payload
        .event_type
        .as_deref()
        .or(payload.kind.as_deref())
        .unwrap_or_default();
    if !event_type.contains("book") {
        return None;
    }

    let asset_id = payload.asset_id.clone()?;
    let bids = parse_levels(payload.bids.as_ref().or(payload.buys.as_ref()));
    let asks = parse_levels(payload.asks.as_ref().or(payload.sells.as_ref()));

    Some(BookSnapshot {
        market_id: "unknown".to_string(),
        asset_id,
        bids,
        asks,
        ts_exchange_ms: payload.timestamp.unwrap_or_else(now_ms),
        recv_ts_local_ns: now_ns(),
        hash: payload.hash.clone(),
    })
}

fn parse_deltas(payload: &WsEvent) -> Vec<BookDelta> {
    let mut out = Vec::new();

    if let Some(update) = parse_single_asset_update(payload) {
        if update.best_bid.is_some() || update.best_ask.is_some() {
            out.push(BookDelta {
                market_id: "unknown".to_string(),
                asset_id: update.asset_id,
                side: BookSide::Bid,
                price: update.best_bid.or(update.best_ask).unwrap_or_default(),
                size: 0.0,
                best_bid: update.best_bid,
                best_ask: update.best_ask,
                ts_exchange_ms: update.ts_exchange_ms,
                recv_ts_local_ns: update.recv_ts_local_ns,
                hash: None,
            });
        }
    }

    let asset_id = payload
        .asset_id
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    let ts_exchange_ms = payload.timestamp.unwrap_or_else(now_ms);
    for c in &payload.changes {
        let side = match c
            .side
            .as_deref()
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "sell" | "ask" => BookSide::Ask,
            _ => BookSide::Bid,
        };
        let Some(price) = c.price else {
            continue;
        };
        let size = c.size.unwrap_or_default();
        out.push(BookDelta {
            market_id: "unknown".to_string(),
            asset_id: asset_id.clone(),
            side,
            price,
            size,
            best_bid: payload.best_bid,
            best_ask: payload.best_ask,
            ts_exchange_ms,
            recv_ts_local_ns: now_ns(),
            hash: payload.hash.clone(),
        });
    }

    out
}

fn parse_levels(value: Option<&Vec<WsLevel>>) -> Vec<BookLevel> {
    let mut out = Vec::new();
    let Some(arr) = value else {
        return out;
    };
    for item in arr {
        if let (Some(price), Some(size)) = (item.price, item.size) {
            out.push(BookLevel { price, size });
        }
    }
    out
}

fn top_level_price_and_size(value: Option<&Vec<WsLevel>>) -> (Option<f64>, Option<f64>) {
    let first = value.and_then(|v| v.first());
    (first.and_then(|l| l.price), first.and_then(|l| l.size))
}

/// Fast timestamp using SystemTime (more efficient than chrono::Utc::now())
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

async fn sleep_with_jitter(base: Duration) {
    let base_ms = base.as_millis() as u64;
    let jitter_ms = rand::rng().random_range(0..=300);
    tokio::time::sleep(Duration::from_millis(base_ms.saturating_add(jitter_ms))).await;
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or_else(|_| now_ms() * 1_000_000)
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum WsEnvelope {
    One(Box<WsEvent>),
    Many(Vec<WsEvent>),
}

impl WsEnvelope {
    fn into_events(self) -> Vec<WsEvent> {
        match self {
            Self::One(v) => vec![*v],
            Self::Many(v) => v,
        }
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
struct WsEvent {
    #[serde(default, alias = "type")]
    kind: Option<String>,
    #[serde(default)]
    event_type: Option<String>,
    #[serde(default, alias = "assetId")]
    asset_id: Option<String>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    best_bid: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    best_ask: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    timestamp: Option<i64>,
    #[serde(default)]
    hash: Option<String>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    price: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    size: Option<f64>,
    #[serde(default)]
    bids: Option<Vec<WsLevel>>,
    #[serde(default)]
    asks: Option<Vec<WsLevel>>,
    #[serde(default)]
    buys: Option<Vec<WsLevel>>,
    #[serde(default)]
    sells: Option<Vec<WsLevel>>,
    #[serde(default)]
    changes: Vec<WsChange>,
    #[serde(default)]
    #[serde(alias = "priceChanges")]
    price_changes: Vec<WsEvent>,
}

#[derive(Debug, Deserialize, Clone, Default)]
struct WsLevel {
    #[serde(default, deserialize_with = "de_opt_f64")]
    price: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    size: Option<f64>,
}

#[derive(Debug, Deserialize, Clone, Default)]
struct WsChange {
    #[serde(default)]
    side: Option<String>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    price: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    size: Option<f64>,
}

fn de_opt_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(f64),
        Str(String),
        Null,
    }
    let parsed = Option::<NumOrStr>::deserialize(deserializer)?;
    Ok(match parsed {
        Some(NumOrStr::Num(v)) => Some(v),
        Some(NumOrStr::Str(s)) => s.parse::<f64>().ok(),
        Some(NumOrStr::Null) | None => None,
    })
}

fn de_opt_i64<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(i64),
        Str(String),
        Null,
    }
    let parsed = Option::<NumOrStr>::deserialize(deserializer)?;
    Ok(match parsed {
        Some(NumOrStr::Num(v)) => Some(v),
        Some(NumOrStr::Str(s)) => s.parse::<i64>().ok(),
        Some(NumOrStr::Null) | None => None,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AssetTop {
    bid: f64,
    ask: f64,
    bid_size: f64,
    ask_size: f64,
    ts_exchange_ms: i64,
    recv_ts_local_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MarketState {
    market_id: String,
    #[serde(default)]
    timeframe: Option<String>,
    yes_token: String,
    no_token: String,
    yes: AssetTop,
    no: AssetTop,
}

impl MarketState {
    fn to_book_top(&self) -> BookTop {
        BookTop {
            market_id: self.market_id.clone(),
            token_id_yes: self.yes_token.clone(),
            token_id_no: self.no_token.clone(),
            bid_yes: self.yes.bid,
            ask_yes: self.yes.ask,
            bid_no: self.no.bid,
            ask_no: self.no.ask,
            bid_size_yes: self.yes.bid_size,
            ask_size_yes: self.yes.ask_size,
            bid_size_no: self.no.bid_size,
            ask_size_no: self.no.ask_size,
            ts_ms: self.yes.ts_exchange_ms.max(self.no.ts_exchange_ms),
            recv_ts_local_ns: self.yes.recv_ts_local_ns.max(self.no.recv_ts_local_ns),
        }
    }
}

#[derive(Debug, Clone)]
struct AssetUpdate {
    asset_id: String,
    best_bid: Option<f64>,
    best_bid_size: Option<f64>,
    best_ask: Option<f64>,
    best_ask_size: Option<f64>,
    ts_exchange_ms: i64,
    recv_ts_local_ns: i64,
}

pub fn verify_probability(price: f64) -> Result<f64> {
    if (0.0..=1.0).contains(&price) {
        Ok(price)
    } else {
        Err(anyhow!("price out of [0,1]: {price}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_pair_parser() {
        let t = parse_token_pair(Some("[\"a\",\"b\"]"));
        assert_eq!(t, Some(("a".to_string(), "b".to_string())));
    }

    #[test]
    fn verify_prob_bounds() {
        assert!(verify_probability(0.42).is_ok());
        assert!(verify_probability(1.1).is_err());
    }

    #[test]
    fn parse_asset_updates_supports_changes_and_nested_price_changes() {
        let payload: WsEvent = serde_json::from_str(
            r#"{
                "asset_id":"yes_token",
                "timestamp":"1700000000000",
                "changes":[{"side":"BUY","price":"0.63","size":"10"},{"side":"SELL","price":"0.64","size":"9"}],
                "price_changes":[
                    {
                        "asset_id":"no_token",
                        "changes":[{"side":"BUY","price":"0.36","size":"7"},{"side":"SELL","price":"0.37","size":"8"}]
                    },
                    {
                        "asset_id":"tick_token",
                        "price":"0.41",
                        "size":"12",
                        "side":"BUY"
                    }
                ]
            }"#,
        )
        .expect("parse event");

        let mut updates = parse_asset_updates(&payload);
        updates.sort_by(|a, b| a.asset_id.cmp(&b.asset_id));
        assert_eq!(updates.len(), 3);

        let no = &updates[0];
        assert_eq!(no.asset_id, "no_token");
        assert_eq!(no.best_bid, Some(0.36));
        assert_eq!(no.best_ask, Some(0.37));

        let tick = &updates[1];
        assert_eq!(tick.asset_id, "tick_token");
        assert_eq!(tick.best_bid, Some(0.41));
        assert_eq!(tick.best_ask, Some(0.41));

        let yes = &updates[2];
        assert_eq!(yes.asset_id, "yes_token");
        assert_eq!(yes.best_bid, Some(0.63));
        assert_eq!(yes.best_ask, Some(0.64));
    }

    fn mk_state(market_id: &str, timeframe: &str, ts_ms: i64) -> MarketState {
        MarketState {
            market_id: market_id.to_string(),
            timeframe: Some(timeframe.to_string()),
            yes_token: format!("{market_id}-yes"),
            no_token: format!("{market_id}-no"),
            yes: AssetTop {
                bid: 0.5,
                ask: 0.5,
                bid_size: 1.0,
                ask_size: 1.0,
                ts_exchange_ms: ts_ms,
                recv_ts_local_ns: ts_ms.saturating_mul(1_000_000),
            },
            no: AssetTop {
                bid: 0.5,
                ask: 0.5,
                bid_size: 1.0,
                ask_size: 1.0,
                ts_exchange_ms: ts_ms,
                recv_ts_local_ns: ts_ms.saturating_mul(1_000_000),
            },
        }
    }

    #[test]
    fn merge_market_state_maps_keeps_newer_entry() {
        let mut old = HashMap::<String, MarketState>::new();
        old.insert("m1".to_string(), mk_state("m1", "5m", 100));
        let mut latest = HashMap::<String, MarketState>::new();
        latest.insert("m1".to_string(), mk_state("m1", "5m", 200));

        let merged = merge_market_state_maps(&old, &latest);
        let state = merged.get("m1").expect("merged state exists");
        assert_eq!(market_state_recency_ms(state), 200);
    }

    #[test]
    fn merge_market_state_maps_preserves_timeframe_coverage() {
        // Use a wide limit to verify merge keeps markets from both timeframes.
        std::env::set_var("POLYEDGE_TARGET_MARKET_CACHE_MAX_PER_TF", "16");
        let mut old = HashMap::<String, MarketState>::new();
        old.insert("old-5m".to_string(), mk_state("old-5m", "5m", 100));
        old.insert("old-15m".to_string(), mk_state("old-15m", "15m", 100));

        let mut latest = HashMap::<String, MarketState>::new();
        latest.insert("new-5m".to_string(), mk_state("new-5m", "5m", 200));

        let merged = merge_market_state_maps(&old, &latest);
        let has_5m = merged
            .values()
            .any(|s| s.timeframe.as_deref() == Some("5m"));
        let has_15m = merged
            .values()
            .any(|s| s.timeframe.as_deref() == Some("15m"));
        assert!(has_5m);
        assert!(has_15m);
    }
}
