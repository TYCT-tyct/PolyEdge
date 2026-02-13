use core_types::{BookTop, FairValueModel, RefTick, Signal};

#[derive(Debug, Clone)]
pub struct SimpleFairValue {
    pub max_jump_bps: f64,
}

impl Default for SimpleFairValue {
    fn default() -> Self {
        Self { max_jump_bps: 35.0 }
    }
}

impl FairValueModel for SimpleFairValue {
    fn evaluate(&self, tick: &RefTick, book: &BookTop) -> Signal {
        let mid_yes = (book.bid_yes + book.ask_yes) * 0.5;
        let spread = (book.ask_yes - book.bid_yes).max(0.0001);

        let normalized = ((tick.price / 100_000.0) - 0.5).clamp(-0.5, 0.5);
        let jump = (normalized * self.max_jump_bps / 10_000.0).clamp(-0.02, 0.02);

        let fair_yes = (mid_yes + jump).clamp(0.001, 0.999);
        let edge_bps_bid = ((fair_yes - book.ask_yes) / spread) * 10_000.0;
        let edge_bps_ask = ((book.bid_yes - fair_yes) / spread) * 10_000.0;

        let confidence = (1.0 - spread * 10.0).clamp(0.0, 1.0);

        Signal {
            market_id: book.market_id.clone(),
            fair_yes,
            edge_bps_bid,
            edge_bps_ask,
            confidence,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outputs_valid_probability() {
        let model = SimpleFairValue::default();
        let tick = RefTick {
            source: "binance".to_string(),
            symbol: "BTCUSDT".to_string(),
            event_ts_ms: 1,
            recv_ts_ms: 1,
            price: 70_000.0,
        };
        let book = BookTop {
            market_id: "m".to_string(),
            token_id_yes: "y".to_string(),
            token_id_no: "n".to_string(),
            bid_yes: 0.49,
            ask_yes: 0.51,
            bid_no: 0.49,
            ask_no: 0.51,
            ts_ms: 1,
        };
        let s = model.evaluate(&tick, &book);
        assert!((0.0..=1.0).contains(&s.fair_yes));
    }
}
