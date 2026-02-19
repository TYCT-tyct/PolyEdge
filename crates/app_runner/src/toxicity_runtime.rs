use std::collections::VecDeque;

use core_types::ShadowOutcome;

use crate::state::EngineShared;
use crate::strategy_policy::compute_market_score;

pub(super) async fn update_toxic_state_from_outcome(
    shared: &EngineShared,
    outcome: &ShadowOutcome,
) {
    if !outcome.fillable || outcome.delay_ms != 10 {
        return;
    }
    let mut states = shared.tox_state.write().await;
    let st = states.entry(outcome.market_id.clone()).or_default();
    if let Some(v) = outcome.net_markout_1s_bps.or(outcome.pnl_1s_bps) {
        push_rolling(&mut st.markout_1s, v, 2048);
    }
    if let Some(v) = outcome.net_markout_5s_bps.or(outcome.pnl_5s_bps) {
        push_rolling(&mut st.markout_5s, v, 2048);
    }
    if let Some(v) = outcome.net_markout_10s_bps.or(outcome.pnl_10s_bps) {
        push_rolling(&mut st.markout_10s, v, 2048);
    }
    let samples = st
        .markout_1s
        .len()
        .max(st.markout_5s.len())
        .max(st.markout_10s.len());
    st.market_score = compute_market_score(st, st.last_tox_score, samples);
}

pub(super) fn push_rolling(dst: &mut VecDeque<f64>, value: f64, cap: usize) {
    dst.push_back(value);
    while dst.len() > cap {
        dst.pop_front();
    }
}
