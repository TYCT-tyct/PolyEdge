use core_types::{
    InventoryState, OrderSide, QuoteIntent, QuotePolicy, Signal, ToxicDecision, ToxicRegime,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MakerConfig {
    pub base_quote_size: f64,
    pub min_edge_bps: f64,
    pub inventory_skew: f64,
    pub max_spread: f64,
    pub ttl_ms: u64,
    pub taker_trigger_bps: f64,
    pub taker_max_slippage_bps: f64,
    pub stale_tick_filter_ms: f64,
    pub market_tier_profile: String,
    pub capital_fraction_kelly: f64,
    pub variance_penalty_lambda: f64,
}

impl Default for MakerConfig {
    fn default() -> Self {
        Self {
            base_quote_size: 2.0,
            min_edge_bps: 5.0,
            inventory_skew: 0.15,
            max_spread: 0.03,
            ttl_ms: 400,
            taker_trigger_bps: 8.0,
            taker_max_slippage_bps: 25.0,
            stale_tick_filter_ms: 400.0,
            market_tier_profile: "balanced".to_string(),
            capital_fraction_kelly: 0.35,
            variance_penalty_lambda: 0.25,
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

        if signal.confidence <= 0.0 {
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

    fn build_quotes_with_toxicity(
        &self,
        signal: &Signal,
        inventory: &InventoryState,
        toxicity: &ToxicDecision,
    ) -> Vec<QuoteIntent> {
        let mut quotes = self.build_quotes(signal, inventory);
        if quotes.is_empty() {
            return quotes;
        }

        match toxicity.regime {
            ToxicRegime::Safe => quotes,
            ToxicRegime::Danger => Vec::new(),
            ToxicRegime::Caution => {
                let tox = toxicity.tox_score.clamp(0.0, 1.0);
                let spread_mult = 1.0 + tox * 1.5;
                let size_mult = (1.0 - tox).max(0.2) * 0.5;
                let ttl_ms = lerp_u64(600, 120, tox);
                for q in &mut quotes {
                    let pad = 0.005 * spread_mult;
                    q.price = match q.side {
                        OrderSide::BuyYes | OrderSide::BuyNo => (q.price - pad).clamp(0.001, 0.999),
                        OrderSide::SellYes | OrderSide::SellNo => {
                            (q.price + pad).clamp(0.001, 0.999)
                        }
                    };
                    q.size = (q.size * size_mult).max(self.cfg.base_quote_size * 0.2);
                    q.ttl_ms = ttl_ms.max(50);
                }
                quotes
            }
        }
    }
}

fn lerp_u64(from: u64, to: u64, t: f64) -> u64 {
    let t = t.clamp(0.0, 1.0);
    ((from as f64) + (to as f64 - from as f64) * t).round() as u64
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

    #[test]
    fn danger_regime_blocks_quotes() {
        let policy = MakerQuotePolicy::new(MakerConfig::default());
        let signal = Signal {
            market_id: "m".to_string(),
            fair_yes: 0.5,
            edge_bps_bid: 10.0,
            edge_bps_ask: 10.0,
            confidence: 0.9,
        };
        let inventory = InventoryState {
            market_id: "m".to_string(),
            net_yes: 0.0,
            net_no: 0.0,
            exposure_notional: 0.0,
        };
        let tox = ToxicDecision {
            market_id: "m".to_string(),
            symbol: "BTCUSDT".to_string(),
            tox_score: 0.9,
            regime: ToxicRegime::Danger,
            reason_codes: vec!["markout_10s_negative".to_string()],
            ts_ns: 1,
        };
        let intents = policy.build_quotes_with_toxicity(&signal, &inventory, &tox);
        assert!(intents.is_empty());
    }
}
