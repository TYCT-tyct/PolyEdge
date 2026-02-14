use std::time::Duration;

use anyhow::{Context, Result};
use core_types::{DynStream, RefPriceFeed, RefPriceWsFeed, RefTick};
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

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
        let (tx, rx) = mpsc::channel::<RefTick>(16_384);

        let binance_symbols = symbols.clone();
        let tx_binance = tx.clone();
        let backoff = self.reconnect_backoff;
        tokio::spawn(async move {
            loop {
                if let Err(err) = run_binance_stream(&binance_symbols, &tx_binance).await {
                    tracing::warn!(?err, "binance ws stream failed; reconnecting");
                }
                tokio::time::sleep(backoff).await;
            }
        });

        let bybit_symbols = symbols.clone();
        let tx_bybit = tx.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = run_bybit_stream(&bybit_symbols, &tx_bybit).await {
                    tracing::warn!(?err, "bybit ws stream failed; reconnecting");
                }
                tokio::time::sleep(backoff).await;
            }
        });

        let coinbase_symbols = symbols.clone();
        let tx_coinbase = tx.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = run_coinbase_stream(&coinbase_symbols, &tx_coinbase).await {
                    tracing::warn!(?err, "coinbase ws stream failed; reconnecting");
                }
                tokio::time::sleep(backoff).await;
            }
        });

        drop(tx);

        let stream = ReceiverStream::new(rx).map(Ok);
        Ok(Box::pin(stream))
    }
}

async fn run_binance_stream(symbols: &[String], tx: &mpsc::Sender<RefTick>) -> Result<()> {
    if symbols.is_empty() {
        anyhow::bail!("binance symbols list is empty");
    }

    let streams = symbols
        .iter()
        .map(|s| format!("{}@trade", s.to_lowercase()))
        .collect::<Vec<_>>()
        .join("/");
    let endpoint = format!("wss://stream.binance.com:9443/stream?streams={streams}");

    let (mut ws, _) = connect_async(&endpoint)
        .await
        .with_context(|| format!("connect binance ws: {endpoint}"))?;

    while let Some(msg) = ws.next().await {
        let msg = msg.context("binance ws read")?;
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

        let Ok(payload) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        let data = payload.get("data").unwrap_or(&payload);

        let symbol = data.get("s").and_then(Value::as_str).map(ToOwned::to_owned);
        let price = data
            .get("p")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<f64>().ok());
        let event_ts = data.get("E").and_then(Value::as_i64).unwrap_or_else(now_ms);

        let (Some(symbol), Some(price)) = (symbol, price) else {
            continue;
        };

        let tick = RefTick {
            source: "binance_ws".to_string(),
            symbol,
            event_ts_ms: event_ts,
            recv_ts_ms: now_ms(),
            event_ts_exchange_ms: event_ts,
            recv_ts_local_ns: now_ns(),
            price,
        };

        if tx.send(tick).await.is_err() {
            break;
        }
    }

    Ok(())
}

async fn run_bybit_stream(symbols: &[String], tx: &mpsc::Sender<RefTick>) -> Result<()> {
    if symbols.is_empty() {
        anyhow::bail!("bybit symbols list is empty");
    }

    let endpoint = "wss://stream.bybit.com/v5/public/spot";
    let (mut ws, _) = connect_async(endpoint).await.context("connect bybit ws")?;

    let args = symbols
        .iter()
        .map(|s| format!("tickers.{s}"))
        .collect::<Vec<_>>();
    let sub = serde_json::json!({
        "op": "subscribe",
        "args": args,
    });
    ws.send(Message::Text(sub.to_string().into()))
        .await
        .context("send bybit subscribe")?;

    while let Some(msg) = ws.next().await {
        let msg = msg.context("bybit ws read")?;
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

        let Ok(payload) = serde_json::from_str::<Value>(&text) else {
            continue;
        };

        if payload.get("success").is_some() {
            continue;
        }

        let topic = payload
            .get("topic")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !topic.starts_with("tickers.") {
            continue;
        }

        let data = payload.get("data").unwrap_or(&Value::Null);
        let symbol = data
            .get("symbol")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| topic.split('.').nth(1).map(ToOwned::to_owned));

        let price = data
            .get("lastPrice")
            .or_else(|| data.get("markPrice"))
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<f64>().ok());

        let event_ts = payload
            .get("ts")
            .and_then(Value::as_i64)
            .or_else(|| data.get("ts").and_then(Value::as_i64))
            .unwrap_or_else(now_ms);

        let (Some(symbol), Some(price)) = (symbol, price) else {
            continue;
        };

        let tick = RefTick {
            source: "bybit_ws".to_string(),
            symbol,
            event_ts_ms: event_ts,
            recv_ts_ms: now_ms(),
            event_ts_exchange_ms: event_ts,
            recv_ts_local_ns: now_ns(),
            price,
        };

        if tx.send(tick).await.is_err() {
            break;
        }
    }

    Ok(())
}

async fn run_coinbase_stream(symbols: &[String], tx: &mpsc::Sender<RefTick>) -> Result<()> {
    if symbols.is_empty() {
        anyhow::bail!("coinbase symbols list is empty");
    }

    let products = symbols
        .iter()
        .filter_map(|s| to_coinbase_pair(s))
        .collect::<Vec<_>>();
    if products.is_empty() {
        anyhow::bail!("coinbase has no supported symbols");
    }

    let endpoint = "wss://ws-feed.exchange.coinbase.com";
    let (mut ws, _) = connect_async(endpoint)
        .await
        .context("connect coinbase ws")?;

    let sub = serde_json::json!({
        "type": "subscribe",
        "product_ids": products,
        "channels": ["ticker"],
    });
    ws.send(Message::Text(sub.to_string().into()))
        .await
        .context("send coinbase subscribe")?;

    while let Some(msg) = ws.next().await {
        let msg = msg.context("coinbase ws read")?;
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

        let Ok(payload) = serde_json::from_str::<Value>(&text) else {
            continue;
        };

        let msg_type = payload
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if msg_type != "ticker" {
            continue;
        }

        let product_id = payload
            .get("product_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(symbol) = from_coinbase_pair(product_id) else {
            continue;
        };

        let price = payload
            .get("price")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<f64>().ok());
        let event_ts = payload
            .get("time")
            .and_then(Value::as_str)
            .and_then(parse_rfc3339_ms)
            .unwrap_or_else(now_ms);

        let Some(price) = price else {
            continue;
        };

        let tick = RefTick {
            source: "coinbase_ws".to_string(),
            symbol,
            event_ts_ms: event_ts,
            recv_ts_ms: now_ms(),
            event_ts_exchange_ms: event_ts,
            recv_ts_local_ns: now_ns(),
            price,
        };

        if tx.send(tick).await.is_err() {
            break;
        }
    }

    Ok(())
}

fn parse_rfc3339_ms(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

fn to_coinbase_pair(symbol: &str) -> Option<String> {
    let base = symbol.strip_suffix("USDT")?;
    Some(format!("{base}-USD"))
}

fn from_coinbase_pair(product_id: &str) -> Option<String> {
    let base = product_id.strip_suffix("-USD")?;
    Some(format!("{base}USDT"))
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn now_ns() -> i64 {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| now_ms() * 1_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coinbase_pair_conversion() {
        assert_eq!(to_coinbase_pair("BTCUSDT").as_deref(), Some("BTC-USD"));
        assert_eq!(to_coinbase_pair("FOO"), None);
        assert_eq!(from_coinbase_pair("ETH-USD").as_deref(), Some("ETHUSDT"));
    }

    #[test]
    fn parse_time_works() {
        let ts = parse_rfc3339_ms("2026-02-13T12:34:56.789Z").expect("parse");
        assert!(ts > 0);
    }
}
