use std::collections::VecDeque;
use std::time::Duration;

use anyhow::{Context, Result};
use core_types::{DynStream, RefPriceFeed, RefTick};
use futures::stream;
use reqwest::Client;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct MultiSourceRefFeed {
    http: Client,
    poll_interval: Duration,
}

impl MultiSourceRefFeed {
    pub fn new(poll_interval: Duration) -> Self {
        Self {
            http: Client::new(),
            poll_interval,
        }
    }

    async fn fetch_batch(&self, symbols: &[String]) -> Result<Vec<RefTick>> {
        let mut out = Vec::new();
        for symbol in symbols {
            if let Some(t) = self.fetch_binance(symbol).await? {
                out.push(t);
            }
            if let Some(t) = self.fetch_coinbase(symbol).await? {
                out.push(t);
            }
            if let Some(t) = self.fetch_bybit(symbol).await? {
                out.push(t);
            }
        }
        Ok(out)
    }

    async fn fetch_binance(&self, symbol: &str) -> Result<Option<RefTick>> {
        let body: BinanceTicker = self
            .http
            .get("https://api.binance.com/api/v3/ticker/price")
            .query(&[("symbol", symbol)])
            .send()
            .await
            .context("binance request")?
            .error_for_status()
            .context("binance status")?
            .json()
            .await
            .context("binance json")?;

        let recv = chrono::Utc::now().timestamp_millis();
        Ok(Some(RefTick {
            source: "binance".to_string(),
            symbol: body.symbol,
            event_ts_ms: recv,
            recv_ts_ms: recv,
            price: body.price.parse().unwrap_or_default(),
        }))
    }

    async fn fetch_coinbase(&self, symbol: &str) -> Result<Option<RefTick>> {
        let Some(pair) = to_coinbase_pair(symbol) else {
            return Ok(None);
        };

        let url = format!("https://api.coinbase.com/v2/prices/{pair}/spot");
        let body: CoinbaseSpotResp = self
            .http
            .get(url)
            .send()
            .await
            .context("coinbase request")?
            .error_for_status()
            .context("coinbase status")?
            .json()
            .await
            .context("coinbase json")?;

        let recv = chrono::Utc::now().timestamp_millis();
        Ok(Some(RefTick {
            source: "coinbase".to_string(),
            symbol: symbol.to_string(),
            event_ts_ms: recv,
            recv_ts_ms: recv,
            price: body.data.amount.parse().unwrap_or_default(),
        }))
    }

    async fn fetch_bybit(&self, symbol: &str) -> Result<Option<RefTick>> {
        let body: BybitResp = self
            .http
            .get("https://api.bybit.com/v5/market/tickers")
            .query(&[("category", "spot"), ("symbol", symbol)])
            .send()
            .await
            .context("bybit request")?
            .error_for_status()
            .context("bybit status")?
            .json()
            .await
            .context("bybit json")?;

        let Some(first) = body.result.list.first() else {
            return Ok(None);
        };

        let recv = chrono::Utc::now().timestamp_millis();
        Ok(Some(RefTick {
            source: "bybit".to_string(),
            symbol: symbol.to_string(),
            event_ts_ms: recv,
            recv_ts_ms: recv,
            price: first.last_price.parse().unwrap_or_default(),
        }))
    }
}

#[async_trait::async_trait]
impl RefPriceFeed for MultiSourceRefFeed {
    async fn stream_ticks(&self, symbols: Vec<String>) -> Result<DynStream<RefTick>> {
        let state = (VecDeque::<RefTick>::new(), self.clone(), symbols);
        let interval = self.poll_interval;

        let s = stream::try_unfold(state, move |(mut queue, feed, symbols)| async move {
            loop {
                if let Some(item) = queue.pop_front() {
                    return Ok(Some((item, (queue, feed, symbols))));
                }

                tokio::time::sleep(interval).await;
                let ticks = feed.fetch_batch(&symbols).await?;
                for t in ticks {
                    queue.push_back(t);
                }
            }
        });

        Ok(Box::pin(s))
    }
}

fn to_coinbase_pair(symbol: &str) -> Option<String> {
    let base = symbol.strip_suffix("USDT")?;
    Some(format!("{base}-USD"))
}

#[derive(Debug, Deserialize)]
struct BinanceTicker {
    symbol: String,
    price: String,
}

#[derive(Debug, Deserialize)]
struct CoinbaseSpotResp {
    data: CoinbaseSpotData,
}

#[derive(Debug, Deserialize)]
struct CoinbaseSpotData {
    amount: String,
}

#[derive(Debug, Deserialize)]
struct BybitResp {
    result: BybitResult,
}

#[derive(Debug, Deserialize)]
struct BybitResult {
    list: Vec<BybitTicker>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitTicker {
    last_price: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coinbase_pair_conversion() {
        assert_eq!(to_coinbase_pair("BTCUSDT").as_deref(), Some("BTC-USD"));
        assert_eq!(to_coinbase_pair("FOO"), None);
    }
}
