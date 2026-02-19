use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

use core_types::{
    BookTop, InventoryState, OrderSide, ShadowShot, TimeframeClass, ToxicDecision, ToxicFeatures,
    ToxicRegime,
};
use portfolio::PortfolioBook;
use risk_engine::RiskLimits;
use strategy_maker::MakerConfig;

use crate::state::{EdgeModelConfig, EngineShared, MarketToxicState, ToxicityConfig};
use crate::stats_utils::{now_ns, percentile, percentile_deque_capped};

pub(super) fn inventory_for_market(portfolio: &PortfolioBook, market_id: &str) -> InventoryState {
    let positions = portfolio.positions();
    if let Some(pos) = positions.get(market_id) {
        InventoryState {
            market_id: market_id.to_string(),
            net_yes: pos.yes,
            net_no: pos.no,
            exposure_notional: pos.yes.abs() + pos.no.abs(),
        }
    } else {
        InventoryState {
            market_id: market_id.to_string(),
            net_yes: 0.0,
            net_no: 0.0,
            exposure_notional: 0.0,
        }
    }
}
pub(super) fn build_toxic_features(
    book: &BookTop,
    symbol: &str,
    stale_ms: f64,
    fair_yes: f64,
    attempted: u64,
    no_quote: u64,
    markout_1s_p50: f64,
    markout_5s_p50: f64,
    markout_10s_p50: f64,
) -> ToxicFeatures {
    let mid_yes = ((book.bid_yes + book.ask_yes) * 0.5).max(0.0001);
    let spread_bps = ((book.ask_yes - book.bid_yes).max(0.0) / mid_yes) * 10_000.0;
    let microprice_drift = fair_yes - mid_yes;
    let imbalance_den = (book.bid_yes + book.ask_yes + book.bid_no + book.ask_no).abs();
    let imbalance = if imbalance_den <= 1e-12 {
        0.0
    } else {
        ((book.bid_yes + book.ask_no) - (book.ask_yes + book.bid_no)) / imbalance_den
    };
    let attempted = attempted.max(1);
    let cancel_burst = (no_quote as f64 / attempted as f64).clamp(0.0, 1.0);

    ToxicFeatures {
        market_id: book.market_id.clone(),
        symbol: symbol.to_string(),
        markout_1s: markout_1s_p50,
        markout_5s: markout_5s_p50,
        markout_10s: markout_10s_p50,
        spread_bps,
        microprice_drift,
        stale_ms,
        imbalance,
        cancel_burst,
        ts_ns: now_ns(),
    }
}

pub(super) fn evaluate_toxicity(features: &ToxicFeatures, cfg: &ToxicityConfig) -> ToxicDecision {
    let neg_markout_1s = (-features.markout_1s).max(0.0) / 20.0;
    let neg_markout_5s = (-features.markout_5s).max(0.0) / 20.0;
    let neg_markout_10s = (-features.markout_10s).max(0.0) / 20.0;
    let spread_z = features.spread_bps / 50.0;
    let microprice_drift_z = (features.microprice_drift.abs() * 10_000.0) / 20.0;
    let stale_z = features.stale_ms / 1_500.0;
    let raw = cfg.w1 * neg_markout_1s
        + cfg.w2 * neg_markout_5s
        + cfg.w3 * neg_markout_10s
        + cfg.w4 * spread_z
        + cfg.w5 * microprice_drift_z
        + cfg.w6 * stale_z;
    let markout_1s_danger = features.markout_1s <= cfg.markout_1s_danger_bps;
    let markout_5s_danger = features.markout_5s <= cfg.markout_5s_danger_bps;
    let markout_10s_danger = features.markout_10s <= cfg.markout_10s_danger_bps;
    let markout_1s_caution = features.markout_1s <= cfg.markout_1s_caution_bps;
    let markout_5s_caution = features.markout_5s <= cfg.markout_5s_caution_bps;
    let markout_10s_caution = features.markout_10s <= cfg.markout_10s_caution_bps;

    let mut horizon_boost = 0.0;
    if markout_1s_danger {
        horizon_boost += 0.30;
    } else if markout_1s_caution {
        horizon_boost += 0.15;
    }
    if markout_5s_danger {
        horizon_boost += 0.25;
    } else if markout_5s_caution {
        horizon_boost += 0.12;
    }
    if markout_10s_danger {
        horizon_boost += 0.20;
    } else if markout_10s_caution {
        horizon_boost += 0.10;
    }
    let tox_score = (sigmoid(raw) + horizon_boost).clamp(0.0, 1.0);

    let mut reasons = Vec::new();
    if markout_1s_danger {
        reasons.push("markout_1s_danger".to_string());
    } else if markout_1s_caution {
        reasons.push("markout_1s_caution".to_string());
    } else if features.markout_1s < 0.0 {
        reasons.push("markout_1s_negative".to_string());
    }
    if markout_5s_danger {
        reasons.push("markout_5s_danger".to_string());
    } else if markout_5s_caution {
        reasons.push("markout_5s_caution".to_string());
    } else if features.markout_5s < 0.0 {
        reasons.push("markout_5s_negative".to_string());
    }
    if markout_10s_danger {
        reasons.push("markout_10s_danger".to_string());
    } else if markout_10s_caution {
        reasons.push("markout_10s_caution".to_string());
    } else if features.markout_10s < 0.0 {
        reasons.push("markout_10s_negative".to_string());
    }
    if features.spread_bps > 60.0 {
        reasons.push("spread_wide".to_string());
    }
    if features.stale_ms > 1_500.0 {
        reasons.push("stale_feed".to_string());
    }
    if reasons.is_empty() {
        reasons.push("normal".to_string());
    }

    let regime = if markout_1s_danger || markout_5s_danger || markout_10s_danger {
        ToxicRegime::Danger
    } else if markout_1s_caution || markout_5s_caution || markout_10s_caution {
        ToxicRegime::Caution
    } else if tox_score >= cfg.caution_threshold {
        ToxicRegime::Danger
    } else if tox_score >= cfg.safe_threshold {
        ToxicRegime::Caution
    } else {
        ToxicRegime::Safe
    };

    ToxicDecision {
        market_id: features.market_id.clone(),
        symbol: features.symbol.clone(),
        tox_score,
        regime,
        reason_codes: reasons,
        ts_ns: now_ns(),
    }
}

pub(super) fn compute_market_score(
    state: &MarketToxicState,
    tox_score: f64,
    markout_samples: usize,
) -> f64 {
    let attempted = state.attempted.max(1);
    let no_quote_rate = state.no_quote as f64 / attempted as f64;
    let symbol_missing_rate = state.symbol_missing as f64 / attempted as f64;
    let markout_10s = percentile_deque_capped(&state.markout_10s, 0.50, 2048).unwrap_or(0.0);
    if markout_samples < 20 {
        let warmup_score = 80.0 - no_quote_rate * 6.0 - symbol_missing_rate * 6.0;
        return warmup_score.clamp(45.0, 100.0);
    }
    let score = 70.0 + (markout_10s * 1.5).clamp(-30.0, 30.0)
        - no_quote_rate * 25.0
        - symbol_missing_rate * 30.0
        - tox_score * 20.0;
    score.clamp(0.0, 100.0)
}

pub(super) fn compute_market_score_from_snapshot(
    attempted: u64,
    no_quote: u64,
    symbol_missing: u64,
    tox_score: f64,
    markout_samples: usize,
    markout_10s_p50: f64,
) -> f64 {
    let attempted = attempted.max(1);
    let no_quote_rate = no_quote as f64 / attempted as f64;
    let symbol_missing_rate = symbol_missing as f64 / attempted as f64;
    if markout_samples < 20 {
        let warmup_score = 80.0 - no_quote_rate * 6.0 - symbol_missing_rate * 6.0;
        return warmup_score.clamp(45.0, 100.0);
    }
    let score = 70.0 + (markout_10s_p50 * 1.5).clamp(-30.0, 30.0)
        - no_quote_rate * 25.0
        - symbol_missing_rate * 30.0
        - tox_score * 20.0;
    score.clamp(0.0, 100.0)
}

pub(super) fn non_risk_gate_relax_ratio() -> f64 {
    static RELAX_RATIO: OnceLock<f64> = OnceLock::new();
    *RELAX_RATIO.get_or_init(|| {
        std::env::var("POLYEDGE_NON_RISK_GATE_RELAX_RATIO")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.85)
            .clamp(0.50, 1.0)
    })
}

pub(super) fn adaptive_min_edge_bps(
    base_min_edge_bps: f64,
    tox_score: f64,
    markout_samples: usize,
    no_quote_rate: f64,
    markout_10s_p50: f64,
    markout_10s_p25: f64,
    window_outcomes: usize,
    gate_min_outcomes: usize,
) -> f64 {
    let relax_ratio = non_risk_gate_relax_ratio();
    if window_outcomes < gate_min_outcomes {
        let progress = (window_outcomes as f64 / gate_min_outcomes.max(1) as f64).clamp(0.0, 1.0);
        let warmup_floor = (base_min_edge_bps * 0.15).max(0.5);
        let warmup_target = (base_min_edge_bps * 0.50).max(warmup_floor);
        let mut warmup_edge = warmup_floor + (warmup_target - warmup_floor) * progress;
        if no_quote_rate > 0.85 {
            warmup_edge *= 0.80;
        }
        return (warmup_edge * relax_ratio).clamp(0.25, base_min_edge_bps.max(0.25));
    }

    if markout_samples < 20 {
        return ((base_min_edge_bps * 0.5) * relax_ratio).max(1.0);
    }
    let mut out = base_min_edge_bps * (1.0 + tox_score * 0.6);
    if markout_10s_p50 < 0.0 {
        out += (-markout_10s_p50) * 0.50;
    }
    if markout_10s_p25 < 0.0 {
        out += (-markout_10s_p25) * 0.35;
    }
    if no_quote_rate > 0.95 {
        out *= 0.9;
    }
    (out * relax_ratio).clamp(1.0, base_min_edge_bps * 2.5)
}

pub(super) fn edge_gate_bps(
    cfg: &EdgeModelConfig,
    tox_score: f64,
    local_backlog_ms: f64,
    source_latency_ms: f64,
    no_quote_rate: f64,
) -> f64 {
    if !cfg.gate_mode.eq_ignore_ascii_case("dynamic") {
        return cfg.base_gate_bps.max(0.0);
    }
    let congestion = cfg.congestion_penalty_bps * no_quote_rate.clamp(0.0, 1.0);
    let latency = cfg.latency_penalty_bps
        * ((local_backlog_ms / 100.0).clamp(0.0, 1.0)
            + (source_latency_ms / 800.0).clamp(0.0, 1.0));
    let tox = cfg.fail_cost_bps * tox_score.clamp(0.0, 1.0);
    (cfg.base_gate_bps + congestion + latency + tox).max(0.0)
}

pub(super) fn adaptive_max_spread(
    base_max_spread: f64,
    tox_score: f64,
    markout_samples: usize,
) -> f64 {
    let relax_ratio = non_risk_gate_relax_ratio();
    if markout_samples < 20 {
        return (base_max_spread * (1.2 + (1.0 - relax_ratio) * 0.5)).clamp(0.003, 0.08);
    }
    (base_max_spread * (1.0 - tox_score * 0.35 + (1.0 - relax_ratio) * 0.2))
        .clamp(0.002, base_max_spread * 1.15)
}

pub(super) async fn timeframe_weight(
    shared: &Arc<EngineShared>,
    timeframe: &TimeframeClass,
) -> f64 {
    let baseline = match timeframe {
        TimeframeClass::Tf5m | TimeframeClass::Tf15m => 1.0,
        TimeframeClass::Tf1h => 0.35,
        TimeframeClass::Tf1d => 0.20,
    };
    if matches!(timeframe, TimeframeClass::Tf5m | TimeframeClass::Tf15m) {
        return baseline;
    }

    let outcomes = shared.shadow_stats.outcomes.read().await;
    if outcomes.is_empty() {
        return baseline;
    }
    let market_tf = shared.market_to_timeframe.read().await;
    let mut markouts = Vec::new();
    for o in outcomes.iter().rev() {
        if o.delay_ms != 10 || !o.fillable {
            continue;
        }
        let Some(tf) = market_tf.get(&o.market_id) else {
            continue;
        };
        if tf != timeframe {
            continue;
        }
        if let Some(v) = o.net_markout_10s_bps {
            markouts.push(v);
        }
        if markouts.len() >= 200 {
            break;
        }
    }

    if markouts.len() < 30 {
        return baseline;
    }
    let p50 = percentile(&markouts, 0.50).unwrap_or(0.0);
    if p50 <= -8.0 {
        0.05
    } else if p50 <= -2.0 {
        0.12
    } else if p50 <= 0.0 {
        0.20
    } else if p50 <= 3.0 {
        baseline
    } else {
        (baseline * 1.4).clamp(0.0, 1.0)
    }
}

pub(super) fn should_force_taker(
    cfg: &MakerConfig,
    tox: &ToxicDecision,
    edge_net_bps: f64,
    confidence: f64,
    markout_samples: usize,
    no_quote_rate: f64,
    window_outcomes: usize,
    gate_min_outcomes: usize,
    symbol: &str,
) -> bool {
    let profile = cfg.market_tier_profile.to_ascii_lowercase();
    let aggressive_profile =
        profile.contains("taker") || profile.contains("aggressive") || profile.contains("latency");

    let warmup_factor = if window_outcomes < gate_min_outcomes {
        let progress = (window_outcomes as f64 / gate_min_outcomes.max(1) as f64).clamp(0.0, 1.0);
        // Lower trigger during warmup so the funnel can collect enough comparable samples.
        (0.70 + 0.30 * progress).clamp(0.60, 1.0)
    } else {
        1.0
    };
    let no_quote_factor = if no_quote_rate > 0.90 { 0.85 } else { 1.0 };
    let mut trigger_bps = cfg.taker_trigger_bps.max(0.0) * warmup_factor * no_quote_factor;
    if symbol.eq_ignore_ascii_case("SOLUSDT")
        && (profile.contains("sol_guard") || !aggressive_profile)
    {
        trigger_bps *= 1.25;
    }

    if edge_net_bps < trigger_bps {
        return false;
    }
    if matches!(tox.regime, ToxicRegime::Danger) {
        return false;
    }

    if matches!(tox.regime, ToxicRegime::Caution) && !aggressive_profile {
        return false;
    }

    let min_conf = if markout_samples < 20 || window_outcomes < gate_min_outcomes {
        0.45
    } else {
        0.55
    };
    if confidence < min_conf && !aggressive_profile {
        return false;
    }

    true
}

pub(super) fn adaptive_taker_slippage_bps(
    base_slippage_bps: f64,
    market_tier_profile: &str,
    symbol: &str,
    markout_samples: usize,
    no_quote_rate: f64,
    window_outcomes: usize,
    gate_min_outcomes: usize,
) -> f64 {
    let mut out = base_slippage_bps.max(1.0);
    let profile = market_tier_profile.to_ascii_lowercase();
    let aggressive = profile.contains("aggressive") || profile.contains("latency");

    if window_outcomes < gate_min_outcomes || markout_samples < 20 {
        out *= if no_quote_rate > 0.85 { 1.40 } else { 1.20 };
    }
    if aggressive {
        out *= 1.10;
    }
    if symbol.eq_ignore_ascii_case("SOLUSDT") && (profile.contains("sol_guard") || !aggressive) {
        out *= 0.80;
    }

    out.clamp(5.0, 60.0)
}

pub(super) fn should_observe_only_symbol(
    symbol: &str,
    cfg: &MakerConfig,
    tox: &ToxicDecision,
    stale_ms: f64,
    spread_yes: f64,
    book_top_lag_ms: f64,
) -> bool {
    if !symbol.eq_ignore_ascii_case("SOLUSDT") {
        return false;
    }
    let profile = cfg.market_tier_profile.to_ascii_lowercase();
    if !(profile.contains("sol_guard") || profile.contains("balanced")) {
        return false;
    }

    // Keep SOL observe-only unless the local "ref lead vs book" lag is within a tight bound.
    // This avoids letting one slow/volatile venue degrade the overall engine quality.
    let relax = (1.0 - non_risk_gate_relax_ratio()).clamp(0.0, 0.5);
    let lag_guard_ms = 130.0 + 80.0 * relax;
    let stale_guard_ms = 250.0 + 120.0 * relax;
    let spread_guard = 0.020 + 0.006 * relax;
    book_top_lag_ms > lag_guard_ms
        || matches!(tox.regime, ToxicRegime::Danger)
        || stale_ms > stale_guard_ms
        || spread_yes > spread_guard
}

pub(super) fn estimate_queue_fill_proxy(tox_score: f64, spread_yes: f64, stale_ms: f64) -> f64 {
    let spread_pen = (spread_yes / 0.03).clamp(0.0, 1.0);
    let latency_pen = (stale_ms / 800.0).clamp(0.0, 1.0);
    (1.0 - (tox_score * 0.45 + spread_pen * 0.35 + latency_pen * 0.20)).clamp(0.0, 1.0)
}

pub(super) fn estimate_queue_fill_prob(shot: &ShadowShot, book: &BookTop, latency_ms: f64) -> f64 {
    let spread = match shot.side {
        OrderSide::BuyYes | OrderSide::SellYes => (book.ask_yes - book.bid_yes).max(0.0),
        OrderSide::BuyNo | OrderSide::SellNo => (book.ask_no - book.bid_no).max(0.0),
    };
    let spread_pen = (spread / 0.03).clamp(0.0, 1.0);
    // P3: 调整 delay_pen 分母从 25 到 50，减少延迟惩罚权重
    let delay_pen = (shot.delay_ms as f64 / 50.0).clamp(0.0, 1.0);
    let latency_pen = (latency_ms / 100.0).clamp(0.0, 1.0);
    (1.0 - (shot.tox_score * 0.35 + spread_pen * 0.30 + delay_pen * 0.20 + latency_pen * 0.15))
        .clamp(0.0, 1.0)
}

pub(super) fn is_market_in_top_n(
    states: &HashMap<String, MarketToxicState>,
    market_id: &str,
    top_n: usize,
) -> bool {
    if top_n == 0 {
        return true;
    }
    if states.is_empty() {
        return true;
    }
    let Some(target) = states.get(market_id) else {
        return false;
    };
    let target_score = target.market_score;

    // O(n) rank check without heap allocations/sorting.
    let better = states
        .iter()
        .filter(|(id, st)| {
            st.market_score > target_score
                || (st.market_score == target_score && id.as_str() < market_id)
        })
        .count();
    better < top_n.max(1)
}

pub(super) fn cooldown_secs_for_score(tox_score: f64, cfg: &ToxicityConfig) -> u64 {
    let t = tox_score.clamp(0.0, 1.0);
    ((cfg.cooldown_min_sec as f64)
        + ((cfg.cooldown_max_sec as f64) - (cfg.cooldown_min_sec as f64)) * t)
        .round() as u64
}

pub(super) fn adaptive_size_scale(
    drawdown_pct: f64,
    market_notional: f64,
    asset_notional: f64,
    limits: &RiskLimits,
    tox_score: f64,
    tox_regime: &ToxicRegime,
) -> f64 {
    let dd_cap = limits.max_drawdown_pct.abs().max(0.01);
    let dd_ratio = (drawdown_pct.abs() / dd_cap).clamp(0.0, 1.0);
    let drawdown_scale = (1.0 - 0.75 * dd_ratio).clamp(0.25, 1.0);

    let market_util = if limits.max_market_notional > 0.0 {
        (market_notional.max(0.0) / limits.max_market_notional).clamp(0.0, 2.0)
    } else {
        1.0
    };
    let asset_util = if limits.max_asset_notional > 0.0 {
        (asset_notional.max(0.0) / limits.max_asset_notional).clamp(0.0, 2.0)
    } else {
        1.0
    };
    let util = market_util.max(asset_util);
    let exposure_scale = (1.0 - 0.80 * util).clamp(0.20, 1.0);

    let regime_scale = match tox_regime {
        ToxicRegime::Safe => 1.0,
        ToxicRegime::Caution => 0.70,
        ToxicRegime::Danger => 0.40,
    };
    let tox_scale = (regime_scale * (1.0 - 0.35 * tox_score.clamp(0.0, 1.0))).clamp(0.20, 1.0);

    (drawdown_scale * exposure_scale * tox_scale).clamp(0.05, 1.0)
}

pub(super) fn net_markout(markout_bps: Option<f64>, shot: &ShadowShot) -> Option<f64> {
    markout_bps.map(|v| v - shot.fee_paid_bps + shot.rebate_est_bps)
}

pub(super) fn estimate_entry_notional_usdc(shot: &ShadowShot) -> f64 {
    (shot.intended_price.max(0.0) * shot.size.max(0.0)).max(0.0)
}

pub(super) fn bps_to_usdc(bps: Option<f64>, notional_usdc: f64) -> Option<f64> {
    if notional_usdc <= 0.0 {
        return None;
    }
    bps.map(|v| (v / 10_000.0) * notional_usdc)
}

pub(super) fn roi_bps_from_usdc(markout_usdc: Option<f64>, notional_usdc: f64) -> Option<f64> {
    if notional_usdc <= 0.0 {
        return None;
    }
    markout_usdc.map(|v| (v / notional_usdc) * 10_000.0)
}

pub(super) fn sigmoid(x: f64) -> f64 {
    if x >= 0.0 {
        1.0 / (1.0 + (-x).exp())
    } else {
        let ex = x.exp();
        ex / (1.0 + ex)
    }
}
