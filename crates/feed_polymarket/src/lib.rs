use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use core_types::{
    BookDelta, BookLevel, BookSide, BookSnapshot, BookTop, BookUpdate, DynStream, MarketFeed,
    OrderbookStateDigest, PolymarketBookWsFeed,
};
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

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
    http: Client,
    pub endpoints: PolymarketEndpoints,
    reconnect_backoff: Duration,
}

impl PolymarketFeed {
    pub fn new(_poll_interval: Duration) -> Self {
        Self {
            http: Client::new(),
            endpoints: PolymarketEndpoints::default(),
            reconnect_backoff: Duration::from_secs(1),
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
        let mut out = HashMap::<String, MarketState>::new();
        let aliases: [(&str, [&str; 2]); 4] = [
            ("BTCUSDT", ["bitcoin", "btc"]),
            ("ETHUSDT", ["ethereum", "eth"]),
            ("SOLUSDT", ["solana", "sol"]),
            ("XRPUSDT", ["xrp", "xrp"]),
        ];

        let markets: Vec<GammaMarket> = self
            .http
            .get(&self.endpoints.gamma_markets)
            .query(&[
                ("closed", "false"),
                ("archived", "false"),
                ("active", "true"),
                ("limit", "1000"),
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

        for market in markets {
            if !market.active || market.closed || !market.accepting_orders {
                continue;
            }
            let Some((yes, no)) = parse_token_pair(market.clob_token_ids.as_deref()) else {
                continue;
            };
            let text = format!(
                "{} {}",
                market.question.to_ascii_lowercase(),
                market.slug.clone().unwrap_or_default().to_ascii_lowercase()
            );

            let matched_symbol = aliases
                .iter()
                .find(|(_, keys)| text.contains(keys[0]) || text.contains(keys[1]))
                .map(|(s, _)| (*s).to_string());
            let Some(_symbol) = matched_symbol else {
                continue;
            };

            if out.contains_key(&market.id) {
                continue;
            }

            let bid_yes = market.best_bid.unwrap_or(0.0);
            let ask_yes = market.best_ask.unwrap_or(1.0);
            let bid_no = (1.0 - ask_yes).max(0.0);
            let ask_no = (1.0 - bid_yes).min(1.0);

            let ts_ms = chrono::Utc::now().timestamp_millis();
            out.insert(
                market.id.clone(),
                MarketState {
                    market_id: market.id,
                    yes_token: yes,
                    no_token: no,
                    yes: AssetTop {
                        bid: bid_yes,
                        ask: ask_yes,
                        ts_exchange_ms: ts_ms,
                        recv_ts_local_ns: now_ns(),
                    },
                    no: AssetTop {
                        bid: bid_no,
                        ask: ask_no,
                        ts_exchange_ms: ts_ms,
                        recv_ts_local_ns: now_ns(),
                    },
                },
            );
        }

        if out.is_empty() {
            return Err(anyhow!("no active target markets discovered"));
        }

        Ok(out)
    }

    async fn stream_books_ws(&self) -> Result<DynStream<BookTop>> {
        let (tx, rx) = mpsc::channel::<BookTop>(16_384);
        let this = self.clone();

        tokio::spawn(async move {
            loop {
                if let Err(err) = this.run_market_loop(&tx).await {
                    tracing::warn!(?err, "polymarket market ws loop failed; reconnecting");
                }
                tokio::time::sleep(this.reconnect_backoff).await;
            }
        });

        let stream = ReceiverStream::new(rx).map(Ok);
        Ok(Box::pin(stream))
    }

    async fn run_market_loop(&self, tx: &mpsc::Sender<BookTop>) -> Result<()> {
        let mut markets = self.discover_target_markets().await?;

        for state in markets.values() {
            if tx.send(state.to_book_top()).await.is_err() {
                return Ok(());
            }
        }

        let asset_map = build_asset_to_market_map(&markets);
        let assets = asset_map.keys().cloned().collect::<Vec<_>>();

        let (mut ws, _) = connect_async(&self.endpoints.clob_ws_market)
            .await
            .context("connect polymarket market ws")?;

        let sub = serde_json::json!({
            "type": "market",
            "assets_ids": assets,
        });
        ws.send(Message::Text(sub.to_string().into()))
            .await
            .context("send polymarket subscribe")?;

        while let Some(msg) = ws.next().await {
            let msg = msg.context("polymarket ws read")?;
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

            let Ok(payload) = serde_json::from_str::<WsEnvelope>(&text) else {
                continue;
            };

            for event in payload.into_events() {
                for update in parse_asset_updates(&event) {
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
                    if let Some(v) = update.best_bid {
                        target.bid = v;
                    }
                    if let Some(v) = update.best_ask {
                        target.ask = v;
                    }
                    target.ts_exchange_ms = update.ts_exchange_ms;
                    target.recv_ts_local_ns = update.recv_ts_local_ns;

                    if tx.send(state.to_book_top()).await.is_err() {
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }
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
        let backoff = self.reconnect_backoff;

        let (tx, rx) = mpsc::channel::<BookUpdate>(16_384);
        tokio::spawn(async move {
            loop {
                if let Err(err) = run_book_update_loop(&endpoint, &token_ids, &tx).await {
                    tracing::warn!(?err, "book update ws loop failed; reconnecting");
                }
                tokio::time::sleep(backoff).await;
            }
        });

        let stream = ReceiverStream::new(rx).map(Ok);
        Ok(Box::pin(stream))
    }
}

async fn run_book_update_loop(
    endpoint: &str,
    token_ids: &[String],
    tx: &mpsc::Sender<BookUpdate>,
) -> Result<()> {
    let (mut ws, _) = connect_async(endpoint)
        .await
        .with_context(|| format!("connect polymarket ws: {endpoint}"))?;

    let sub = serde_json::json!({
        "type": "market",
        "assets_ids": token_ids,
    });
    ws.send(Message::Text(sub.to_string().into()))
        .await
        .context("send polymarket subscribe")?;

    while let Some(msg) = ws.next().await {
        let msg = msg.context("polymarket book ws read")?;
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

        let Ok(payload) = serde_json::from_str::<WsEnvelope>(&text) else {
            continue;
        };

        for event in payload.into_events() {
            if let Some(snapshot) = parse_snapshot(&event) {
                if tx.send(BookUpdate::Snapshot(snapshot)).await.is_err() {
                    return Ok(());
                }
            }

            for delta in parse_deltas(&event) {
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

    if let Some(update) = parse_single_asset_update(payload) {
        out.push(update);
    }

    for item in &payload.price_changes {
        if let Some(update) = parse_single_asset_update(item) {
            out.push(update);
        }
    }

    out
}

fn parse_single_asset_update(payload: &WsEvent) -> Option<AssetUpdate> {
    let asset_id = payload.asset_id.clone()?;

    let best_bid = payload
        .best_bid
        .or_else(|| top_level_price(payload.bids.as_ref()))
        .or_else(|| top_level_price(payload.buys.as_ref()));

    let best_ask = payload
        .best_ask
        .or_else(|| top_level_price(payload.asks.as_ref()))
        .or_else(|| top_level_price(payload.sells.as_ref()));

    Some(AssetUpdate {
        asset_id,
        best_bid,
        best_ask,
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

fn top_level_price(value: Option<&Vec<WsLevel>>) -> Option<f64> {
    value?.first()?.price
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn now_ns() -> i64 {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| now_ms() * 1_000_000)
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

#[derive(Debug, Clone)]
struct AssetTop {
    bid: f64,
    ask: f64,
    ts_exchange_ms: i64,
    recv_ts_local_ns: i64,
}

#[derive(Debug, Clone)]
struct MarketState {
    market_id: String,
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
            ts_ms: self.yes.ts_exchange_ms.max(self.no.ts_exchange_ms),
        }
    }
}

#[derive(Debug, Clone)]
struct AssetUpdate {
    asset_id: String,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    ts_exchange_ms: i64,
    recv_ts_local_ns: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    id: String,
    question: String,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    closed: bool,
    #[serde(default)]
    accepting_orders: bool,
    #[serde(default)]
    best_bid: Option<f64>,
    #[serde(default)]
    best_ask: Option<f64>,
    #[serde(default)]
    clob_token_ids: Option<String>,
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
}
