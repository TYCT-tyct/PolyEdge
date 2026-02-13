use core_types::{InventoryState, OrderSide, QuoteIntent, QuotePolicy, Signal};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MakerConfig {
    pub base_quote_size: f64,
    pub min_edge_bps: f64,
    pub inventory_skew: f64,
    pub max_spread: f64,
    pub ttl_ms: u64,
}

impl Default for MakerConfig {
    fn default() -> Self {
        Self {
            base_quote_size: 2.0,
            min_edge_bps: 3.0,
            inventory_skew: 0.15,
            max_spread: 0.03,
            ttl_ms: 1500,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MakerQuotePolicy {
    cfg: MakerConfig,
}

impl MakerQuotePolicy {
    pub fn new(cfg: MakerConfig) -> Self {
        Self { cfg }
    }

    fn inventory_shift(&self, inventory: &InventoryState) -> f64 {
        let net = inventory.net_yes - inventory.net_no;
        (net * self.cfg.inventory_skew).clamp(-0.05, 0.05)
    }
}

impl QuotePolicy for MakerQuotePolicy {
    fn build_quotes(&self, signal: &Signal, inventory: &InventoryState) -> Vec<QuoteIntent> {
        let mut out = Vec::new();

        if signal.confidence <= 0.05 {
            return out;
        }

        let shift = self.inventory_shift(inventory);
        let bid_price = (signal.fair_yes - 0.01 - shift).clamp(0.001, 0.999);
        let ask_price = (signal.fair_yes + 0.01 - shift).clamp(0.001, 0.999);

        if signal.edge_bps_bid >= self.cfg.min_edge_bps {
            out.push(QuoteIntent {
                market_id: signal.market_id.clone(),
                side: OrderSide::BuyYes,
                price: bid_price,
                size: self.cfg.base_quote_size,
                ttl_ms: self.cfg.ttl_ms,
            });
        }

        if signal.edge_bps_ask >= self.cfg.min_edge_bps {
            out.push(QuoteIntent {
                market_id: signal.market_id.clone(),
                side: OrderSide::SellYes,
                price: ask_price,
                size: self.cfg.base_quote_size,
                ttl_ms: self.cfg.ttl_ms,
            });
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_quotes_when_edge_positive() {
        let policy = MakerQuotePolicy::new(MakerConfig::default());
        let signal = Signal {
            market_id: "m".to_string(),
            fair_yes: 0.5,
            edge_bps_bid: 5.0,
            edge_bps_ask: 6.0,
            confidence: 0.9,
        };
        let inventory = InventoryState {
            market_id: "m".to_string(),
            net_yes: 0.0,
            net_no: 0.0,
            exposure_notional: 0.0,
        };
        let intents = policy.build_quotes(&signal, &inventory);
        assert_eq!(intents.len(), 2);
    }
}
