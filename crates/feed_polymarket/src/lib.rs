use std::collections::VecDeque;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use core_types::{BookTop, DynStream, MarketFeed};
use futures::stream;
use reqwest::Client;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct PolymarketEndpoints {
    pub gamma_markets: String,
}

impl Default for PolymarketEndpoints {
    fn default() -> Self {
        Self {
            gamma_markets: "https://gamma-api.polymarket.com/markets".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PolymarketFeed {
    http: Client,
    pub endpoints: PolymarketEndpoints,
    poll_interval: Duration,
}

impl PolymarketFeed {
    pub fn new(poll_interval: Duration) -> Self {
        Self {
            http: Client::new(),
            endpoints: PolymarketEndpoints::default(),
            poll_interval,
        }
    }

    pub async fn fetch_active_books(&self) -> Result<Vec<BookTop>> {
        let markets: Vec<GammaMarket> = self
            .http
            .get(&self.endpoints.gamma_markets)
            .query(&[
                ("closed", "false"),
                ("archived", "false"),
                ("active", "true"),
                ("limit", "500"),
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

        let mut out = Vec::new();
        for m in markets {
            let Some((yes, no)) = parse_token_pair(m.clob_token_ids.as_deref()) else {
                continue;
            };
            let bid_yes = m.best_bid.unwrap_or(0.0);
            let ask_yes = m.best_ask.unwrap_or(1.0);
            if !(0.0..=1.0).contains(&bid_yes) || !(0.0..=1.0).contains(&ask_yes) {
                continue;
            }
            let ts_ms = chrono::Utc::now().timestamp_millis();
            out.push(BookTop {
                market_id: m.id,
                token_id_yes: yes,
                token_id_no: no,
                bid_yes,
                ask_yes,
                bid_no: (1.0 - ask_yes).max(0.0),
                ask_no: (1.0 - bid_yes).min(1.0),
                ts_ms,
            });
        }

        Ok(out)
    }
}

#[async_trait::async_trait]
impl MarketFeed for PolymarketFeed {
    async fn stream_books(&self) -> Result<DynStream<BookTop>> {
        let client = self.clone();
        let interval = self.poll_interval;

        let state = (VecDeque::<BookTop>::new(), client);
        let s = stream::try_unfold(state, move |(mut queue, client)| async move {
            loop {
                if let Some(item) = queue.pop_front() {
                    return Ok(Some((item, (queue, client))));
                }

                tokio::time::sleep(interval).await;
                let books = client.fetch_active_books().await?;
                for b in books {
                    queue.push_back(b);
                }
            }
        });

        Ok(Box::pin(s))
    }
}

fn parse_token_pair(input: Option<&str>) -> Option<(String, String)> {
    let raw = input?;
    let parsed: Vec<String> = serde_json::from_str(raw).ok()?;
    if parsed.len() < 2 {
        return None;
    }
    Some((parsed[0].clone(), parsed[1].clone()))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    id: String,
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
