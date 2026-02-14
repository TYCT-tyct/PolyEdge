use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketDescriptor {
    pub market_id: String,
    pub question: String,
    pub symbol: String,
    pub token_id_yes: Option<String>,
    pub token_id_no: Option<String>,
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
            .context("discovery request")?
            .error_for_status()
            .context("discovery status")?
            .json()
            .await
            .context("discovery json")?;

        let mut out = Vec::new();
        for market in markets {
            if !market.active || market.closed || !market.accepting_orders {
                continue;
            }
            let text = format!(
                "{} {}",
                market.question.to_ascii_uppercase(),
                market.slug.clone().unwrap_or_default().to_ascii_uppercase()
            );
            let Some(symbol) = detect_symbol(&text, &self.cfg.symbols) else {
                continue;
            };

            out.push(MarketDescriptor {
                market_id: market.id,
                question: market.question,
                symbol,
                token_id_yes: parse_token_pair(market.clob_token_ids.as_deref()).map(|x| x.0),
                token_id_no: parse_token_pair(market.clob_token_ids.as_deref()).map(|x| x.1),
                event_slug: market.event_slug,
                end_date: market.end_date,
                best_bid: market.best_bid,
                best_ask: market.best_ask,
            });
        }

        Ok(out)
    }
}

fn detect_symbol(text: &str, allowed_symbols: &[String]) -> Option<String> {
    let aliases: [(&str, [&str; 3]); 4] = [
        ("BTCUSDT", ["BITCOIN", "BTC", "XBT"]),
        ("ETHUSDT", ["ETHEREUM", "ETH", "ETHER"]),
        ("SOLUSDT", ["SOLANA", "SOL", "SOLAN"]),
        ("XRPUSDT", ["RIPPLE", "XRP", "XRP"]),
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
    #[serde(default)]
    clob_token_ids: Option<String>,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    closed: bool,
    #[serde(default)]
    accepting_orders: bool,
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
}
