use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use core_types::ToxicRegime;
use execution_clob::ClobExecution;
use tokio::sync::RwLock;

use crate::state::{
    MarketToxicState, ShadowStats, ToxicityConfig, ToxicityLiveReport, ToxicityMarketRow,
};
use crate::stats_utils::percentile_deque_capped;
use crate::strategy_policy::compute_market_score;

pub(super) async fn build_toxicity_live_report(
    tox_state: Arc<RwLock<HashMap<String, MarketToxicState>>>,
    shadow_stats: Arc<ShadowStats>,
    execution: Arc<ClobExecution>,
    toxicity_cfg: Arc<RwLock<Arc<ToxicityConfig>>>,
) -> ToxicityLiveReport {
    let cfg = toxicity_cfg.read().await.clone();
    let states = tox_state.read().await.clone();
    let shots = shadow_stats.shots.read().await.clone();
    let outcomes = shadow_stats.outcomes.read().await.clone();

    let mut rows = Vec::<ToxicityMarketRow>::new();
    let mut safe = 0usize;
    let mut caution = 0usize;
    let mut danger = 0usize;
    let mut tox_sum = 0.0;

    for (market_id, st) in states {
        let symbol = if !st.symbol.is_empty() {
            st.symbol.clone()
        } else {
            shots
                .iter()
                .find(|s| s.market_id == market_id)
                .map(|s| s.symbol.clone())
                .or_else(|| {
                    outcomes
                        .iter()
                        .find(|o| o.market_id == market_id)
                        .map(|o| o.symbol.clone())
                })
                .unwrap_or_else(|| "UNKNOWN".to_string())
        };
        let attempted = st.attempted.max(1);
        let no_quote_rate = st.no_quote as f64 / attempted as f64;
        let symbol_missing_rate = st.symbol_missing as f64 / attempted as f64;
        let markout_10s_bps = percentile_deque_capped(&st.markout_10s, 0.50, 2048).unwrap_or(0.0);
        let markout_samples = st
            .markout_1s
            .len()
            .max(st.markout_5s.len())
            .max(st.markout_10s.len());
        let market_score = if st.market_score > 0.0 {
            st.market_score
        } else {
            compute_market_score(&st, st.last_tox_score, markout_samples)
        };
        let pending_exposure = execution.open_order_notional_for_market(&market_id);

        tox_sum += st.last_tox_score;
        match st.last_regime {
            ToxicRegime::Safe => safe += 1,
            ToxicRegime::Caution => caution += 1,
            ToxicRegime::Danger => danger += 1,
        }

        rows.push(ToxicityMarketRow {
            market_rank: 0,
            market_id,
            symbol,
            tox_score: st.last_tox_score,
            regime: st.last_regime,
            market_score,
            markout_10s_bps,
            no_quote_rate,
            symbol_missing_rate,
            pending_exposure,
            active_for_quoting: false,
        });
    }

    rows.sort_by(|a, b| b.market_score.total_cmp(&a.market_score));
    let top_n = cfg.active_top_n_markets;
    for (idx, row) in rows.iter_mut().enumerate() {
        row.market_rank = idx + 1;
        row.active_for_quoting = top_n == 0 || (idx < top_n);
    }
    let avg = if rows.is_empty() {
        0.0
    } else {
        tox_sum / (rows.len() as f64)
    };

    ToxicityLiveReport {
        ts_ms: Utc::now().timestamp_millis(),
        average_tox_score: avg,
        safe_count: safe,
        caution_count: caution,
        danger_count: danger,
        rows,
    }
}
