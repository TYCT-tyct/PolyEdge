use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use core_types::{BookTop, FairValueModel, RefTick, Signal};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BasisMrConfig {
    pub enabled: bool,
    pub alpha_mean: f64,
    pub alpha_var: f64,
    pub alpha_ret: f64,
    pub alpha_vol: f64,
    pub k_revert: f64,
    pub z_cap: f64,
    pub min_confidence: f64,
    pub warmup_ticks: usize,
}

impl Default for BasisMrConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            alpha_mean: 0.08,
            alpha_var: 0.10,
            alpha_ret: 0.15,
            alpha_vol: 0.12,
            k_revert: 0.85,
            z_cap: 3.0,
            min_confidence: 0.05,
            warmup_ticks: 24,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct BasisMrState {
    ewma_mean: f64,
    ewma_var: f64,
    ref_return_ewma: f64,
    ref_vol_ewma: f64,
    last_ref_px: f64,
    last_ts_ms: i64,
    warmup: usize,
}

#[derive(Clone)]
pub struct BasisMrFairValue {
    cfg: Arc<RwLock<BasisMrConfig>>,
    state: Arc<Mutex<HashMap<String, BasisMrState>>>,
}

impl BasisMrFairValue {
    pub fn new(cfg: Arc<RwLock<BasisMrConfig>>) -> Self {
        Self {
            cfg,
            state: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for BasisMrFairValue {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(BasisMrConfig::default())))
    }
}

impl FairValueModel for BasisMrFairValue {
    fn evaluate(&self, tick: &RefTick, book: &BookTop) -> Signal {
        let cfg = self
            .cfg
            .read()
            .map(|g| g.clone())
            .unwrap_or_else(|_| BasisMrConfig::default());
        let mid_yes = ((book.bid_yes + book.ask_yes) * 0.5).clamp(0.001, 0.999);
        let spread = (book.ask_yes - book.bid_yes).max(0.0001);
        let key = format!("{}:{}", book.market_id, tick.symbol);

        let mut map = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let st = map.entry(key).or_default();

        let ret = if st.last_ref_px > 0.0 {
            (tick.price / st.last_ref_px).ln()
        } else {
            0.0
        };
        st.ref_return_ewma += cfg.alpha_ret * (ret - st.ref_return_ewma);
        let ret_sq = ret * ret;
        st.ref_vol_ewma += cfg.alpha_vol * (ret_sq - st.ref_vol_ewma);
        let ref_vol = st.ref_vol_ewma.abs().sqrt().max(1e-9);
        let anchor_prob = (mid_yes + (st.ref_return_ewma / ref_vol) * 0.01).clamp(0.001, 0.999);

        let basis = mid_yes - anchor_prob;
        st.ewma_mean += cfg.alpha_mean * (basis - st.ewma_mean);
        let dev = basis - st.ewma_mean;
        st.ewma_var += cfg.alpha_var * (dev * dev - st.ewma_var);
        let ewma_std = st.ewma_var.max(1e-10).sqrt();
        let z = (dev / ewma_std).clamp(-cfg.z_cap, cfg.z_cap);

        st.last_ref_px = tick.price.max(1.0);
        st.last_ts_ms = tick.event_ts_ms.max(tick.recv_ts_ms);
        st.warmup = st.warmup.saturating_add(1);

        let fair_yes = if cfg.enabled && st.warmup >= cfg.warmup_ticks {
            (mid_yes - cfg.k_revert * z * ewma_std).clamp(0.001, 0.999)
        } else {
            mid_yes
        };
        let edge_bps_bid = ((fair_yes - book.ask_yes) / book.ask_yes.max(0.0001)) * 10_000.0;
        let edge_bps_ask = ((book.bid_yes - fair_yes) / book.bid_yes.max(0.0001)) * 10_000.0;

        let mut confidence = (1.0 - spread * 10.0).clamp(0.0, 1.0);
        if z.abs() > cfg.z_cap * 0.8 && spread > 0.02 {
            confidence *= 0.5;
        }
        confidence = confidence.max(cfg.min_confidence.min(1.0));

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

    fn tick(px: f64, ts: i64) -> RefTick {
        RefTick {
            source: "binance".to_string(),
            symbol: "BTCUSDT".to_string(),
            event_ts_ms: ts,
            recv_ts_ms: ts,
            event_ts_exchange_ms: ts,
            recv_ts_local_ns: ts * 1_000_000,
            price: px,
        }
    }

    fn book(bid: f64, ask: f64) -> BookTop {
        BookTop {
            market_id: "m".to_string(),
            token_id_yes: "y".to_string(),
            token_id_no: "n".to_string(),
            bid_yes: bid,
            ask_yes: ask,
            bid_no: 1.0 - ask,
            ask_no: 1.0 - bid,
            ts_ms: 1,
            recv_ts_local_ns: 1_000_000,
        }
    }

    #[test]
    fn outputs_valid_probability() {
        let model = BasisMrFairValue::default();
        let s = model.evaluate(&tick(70_000.0, 1), &book(0.49, 0.51));
        assert!((0.0..=1.0).contains(&s.fair_yes));
    }

    #[test]
    fn edge_is_scaled_by_entry_price_not_spread() {
        let model = BasisMrFairValue::default();
        for i in 0..30 {
            let _ = model.evaluate(&tick(70_000.0 + i as f64, i), &book(0.499, 0.501));
        }
        let s = model.evaluate(&tick(70_050.0, 40), &book(0.499, 0.501));
        assert!(s.edge_bps_bid.abs() < 1_000.0);
        assert!(s.edge_bps_ask.abs() < 1_000.0);
    }

    #[test]
    fn basis_reverts_when_z_expands() {
        let cfg = BasisMrConfig {
            warmup_ticks: 1,
            ..BasisMrConfig::default()
        };
        let model = BasisMrFairValue::new(Arc::new(RwLock::new(cfg)));
        let b = book(0.49, 0.51);
        let mid = (b.bid_yes + b.ask_yes) * 0.5;
        let _ = model.evaluate(&tick(70_000.0, 1), &b);
        let _ = model.evaluate(&tick(68_000.0, 2), &b);
        let s2 = model.evaluate(&tick(62_000.0, 3), &b);
        assert!(s2.fair_yes <= mid + 1e-9);
    }
}
