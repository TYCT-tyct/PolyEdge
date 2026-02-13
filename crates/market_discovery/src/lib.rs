use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketDescriptor {
    pub market_id: String,
    pub question: String,
    pub symbol: String,
    pub event_slug: Option<String>,
    pub end_date: Option<String>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    pub symbols: Vec<String>,
    pub endpoint: String,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            symbols: vec![
                "BTCUSDT".to_string(),
                "ETHUSDT".to_string(),
                "SOLUSDT".to_string(),
                "XRPUSDT".to_string(),
            ],
            endpoint: "https://gamma-api.polymarket.com/markets".to_string(),
        }
    }
}

pub struct MarketDiscovery {
    http: Client,
    cfg: DiscoveryConfig,
}

impl MarketDiscovery {
    pub fn new(cfg: DiscoveryConfig) -> Self {
        Self {
            http: Client::new(),
            cfg,
        }
    }

    pub async fn discover(&self) -> Result<Vec<MarketDescriptor>> {
        let markets: Vec<GammaMarket> = self
            .http
            .get(&self.cfg.endpoint)
            .query(&[("closed", "false"), ("archived", "false"), ("limit", "500")])
            .send()
            .await
            .context("discovery request")?
            .error_for_status()
            .context("discovery status")?
            .json()
            .await
            .context("discovery json")?;

        let mut out = Vec::new();
        for market in markets {
            let text = format!(
                "{} {}",
                market.question.to_ascii_uppercase(),
                market.slug.clone().unwrap_or_default().to_ascii_uppercase()
            );
            let Some(symbol) = self
                .cfg
                .symbols
                .iter()
                .find(|s| text.contains(s.trim_end_matches("USDT")))
                .cloned()
            else {
                continue;
            };

            out.push(MarketDescriptor {
                market_id: market.id,
                question: market.question,
                symbol,
                event_slug: market.event_slug,
                end_date: market.end_date,
                best_bid: market.best_bid,
                best_ask: market.best_ask,
            });
        }

        Ok(out)
    }
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_symbols_include_all() {
        let cfg = DiscoveryConfig::default();
        assert!(cfg.symbols.contains(&"BTCUSDT".to_string()));
        assert!(cfg.symbols.contains(&"ETHUSDT".to_string()));
        assert!(cfg.symbols.contains(&"SOLUSDT".to_string()));
        assert!(cfg.symbols.contains(&"XRPUSDT".to_string()));
    }
}
