use crate::common::timeframe_to_ms;
use crate::market_data_exchange::market_in_sampling_window;
use crate::models::MarketMeta;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwitchPhase {
    Active,
    Prewarm,
    FreezeEnter,
    CutoverReady,
    Degraded,
}

#[derive(Debug, Clone)]
pub struct SwitchSnapshot {
    pub pair_key: String,
    pub expected_start_ms: i64,
    pub active_market: MarketMeta,
    pub next_market: Option<MarketMeta>,
    pub next2_market: Option<MarketMeta>,
    pub phase: SwitchPhase,
    pub degraded: bool,
    pub reason: &'static str,
}

fn expected_round_start_ms(now_ms: i64, timeframe: &str) -> Option<i64> {
    let tf_ms = timeframe_to_ms(timeframe)?;
    if tf_ms <= 0 {
        return None;
    }
    Some(now_ms.div_euclid(tf_ms) * tf_ms)
}

fn phase_for_switch(
    now_ms: i64,
    active: &MarketMeta,
    next: Option<&MarketMeta>,
    degraded: bool,
) -> SwitchPhase {
    if degraded {
        return SwitchPhase::Degraded;
    }
    let remaining_ms = active.end_ts_ms.saturating_sub(now_ms);
    if remaining_ms <= 2_000 && next.is_some() {
        return SwitchPhase::CutoverReady;
    }
    if remaining_ms <= 10_000 {
        return SwitchPhase::FreezeEnter;
    }
    if let Some(next_market) = next {
        if next_market.start_ts_ms.saturating_sub(now_ms) <= 45_000 {
            return SwitchPhase::Prewarm;
        }
    }
    SwitchPhase::Active
}

pub fn resolve_switch_snapshot(
    pair_key: &str,
    candidates: &[MarketMeta],
    now_ms: i64,
    prestart_allow_ms: i64,
    near_future_allow_ms: i64,
    sample_end_grace_ms: i64,
    stale_guard_ms: i64,
) -> Option<SwitchSnapshot> {
    if candidates.is_empty() {
        return None;
    }
    let timeframe = candidates.first()?.timeframe.as_str();
    let expected_start_ms = expected_round_start_ms(now_ms, timeframe)?;

    let mut ordered = candidates.to_vec();
    ordered.sort_by_key(|m| m.start_ts_ms);
    ordered.dedup_by(|a, b| a.market_id == b.market_id);

    let exact_expected = ordered
        .iter()
        .find(|m| m.start_ts_ms == expected_start_ms)
        .cloned();

    let in_window = ordered
        .iter()
        .filter(|m| market_in_sampling_window(m, now_ms, prestart_allow_ms, sample_end_grace_ms))
        .min_by_key(|m| m.start_ts_ms.saturating_sub(expected_start_ms).abs())
        .cloned();

    let recent_past = ordered
        .iter()
        .filter(|m| {
            m.start_ts_ms <= expected_start_ms
                && now_ms <= m.end_ts_ms.saturating_add(stale_guard_ms)
        })
        .max_by_key(|m| m.start_ts_ms)
        .cloned();

    let near_future = ordered
        .iter()
        .filter(|m| {
            m.start_ts_ms > now_ms && m.start_ts_ms.saturating_sub(now_ms) <= near_future_allow_ms
        })
        .min_by_key(|m| m.start_ts_ms)
        .cloned();

    let (active_market, degraded, reason) = if let Some(m) = exact_expected {
        (m, false, "exact_expected_round")
    } else if let Some(m) = in_window {
        (m, false, "in_window_fallback")
    } else if let Some(m) = recent_past {
        (m, true, "recent_past_fallback")
    } else if let Some(m) = near_future {
        (m, true, "near_future_fallback")
    } else {
        return None;
    };

    let mut upcoming = ordered
        .iter()
        .filter(|m| m.start_ts_ms > active_market.start_ts_ms)
        .cloned()
        .collect::<Vec<_>>();
    upcoming.sort_by_key(|m| m.start_ts_ms);

    let next_market = upcoming.first().cloned();
    let next2_market = upcoming.get(1).cloned();
    let phase = phase_for_switch(now_ms, &active_market, next_market.as_ref(), degraded);

    Some(SwitchSnapshot {
        pair_key: pair_key.to_string(),
        expected_start_ms,
        active_market,
        next_market,
        next2_market,
        phase,
        degraded,
        reason,
    })
}

#[cfg(test)]
mod tests {
    use super::{resolve_switch_snapshot, SwitchPhase};
    use crate::models::MarketMeta;

    fn mk(id: &str, start: i64, end: i64) -> MarketMeta {
        MarketMeta {
            market_id: id.to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "5m".to_string(),
            title: "BTC".to_string(),
            target_price: None,
            start_ts_ms: start,
            end_ts_ms: end,
        }
    }

    #[test]
    fn switch_prefers_expected_round_when_available() {
        let now_ms = 1_705_000;
        let snapshot = resolve_switch_snapshot(
            "BTCUSDT:5m",
            &[
                mk("current", 1_500_000, 1_800_000),
                mk("next", 1_800_000, 2_100_000),
                mk("next2", 2_100_000, 2_400_000),
            ],
            now_ms,
            30_000,
            300_000,
            300,
            5_000,
        )
        .expect("snapshot");
        assert_eq!(snapshot.active_market.market_id, "current");
        assert!(!snapshot.degraded);
        assert_eq!(snapshot.reason, "exact_expected_round");
    }

    #[test]
    fn switch_does_not_jump_to_far_future_round() {
        let now_ms = 2_000_000;
        let snapshot = resolve_switch_snapshot(
            "BTCUSDT:5m",
            &[mk("future", 2_240_000, 2_540_000)],
            now_ms,
            30_000,
            30_000,
            300,
            5_000,
        );
        assert!(snapshot.is_none());
    }

    #[test]
    fn switch_marks_degraded_when_only_recent_past_exists() {
        let now_ms = 2_001_000;
        let snapshot = resolve_switch_snapshot(
            "BTCUSDT:5m",
            &[mk("prev", 1_700_000, 2_000_000)],
            now_ms,
            30_000,
            30_000,
            300,
            5_000,
        )
        .expect("snapshot");
        assert!(snapshot.degraded);
        assert_eq!(snapshot.phase, SwitchPhase::Degraded);
    }
}
