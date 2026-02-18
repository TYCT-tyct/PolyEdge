use chrono::Utc;
use std::cmp::Ordering;
use std::collections::VecDeque;

pub fn push_capped<T>(dst: &mut Vec<T>, value: T, cap: usize) {
    dst.push(value);
    if dst.len() > cap {
        let drop_n = dst.len() - cap;
        dst.drain(0..drop_n);
    }
}

pub fn percentile(values: &[f64], p: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut v = values.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((v.len() as f64 - 1.0) * p.clamp(0.0, 1.0)).round() as usize;
    v.get(idx).copied()
}

pub fn percentile_deque_capped(values: &VecDeque<f64>, p: f64, cap: usize) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let n = values.len().min(cap.max(1));
    if n == 0 {
        return None;
    }
    // Most recent samples are more relevant; take from the back.
    let mut v: Vec<f64> = values.iter().rev().take(n).copied().collect();
    let idx = ((v.len() as f64 - 1.0) * p.clamp(0.0, 1.0)).round() as usize;
    let (_, nth, _) =
        v.select_nth_unstable_by(idx, |a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    Some(*nth)
}

pub fn robust_filter_iqr(values: &[f64]) -> (Vec<f64>, f64) {
    if values.len() < 5 {
        return (values.to_vec(), 0.0);
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let q1 = percentile(&sorted, 0.25).unwrap_or(0.0);
    let q3 = percentile(&sorted, 0.75).unwrap_or(0.0);
    let iqr = q3 - q1;
    if iqr <= f64::EPSILON {
        return (values.to_vec(), 0.0);
    }
    let low = q1 - 1.5 * iqr;
    let high = q3 + 1.5 * iqr;
    let filtered = values
        .iter()
        .copied()
        .filter(|v| *v >= low && *v <= high)
        .collect::<Vec<_>>();
    let outlier_ratio = 1.0 - (filtered.len() as f64 / values.len() as f64);
    (filtered, outlier_ratio.clamp(0.0, 1.0))
}

pub fn freshness_ms(now_ms: i64, last_ms: i64) -> i64 {
    if last_ms <= 0 {
        return i64::MAX / 4;
    }
    (now_ms - last_ms).max(0)
}

pub fn ratio_u64(num: u64, denom: u64) -> f64 {
    if denom == 0 {
        0.0
    } else {
        (num as f64 / denom as f64).clamp(0.0, 1.0)
    }
}

pub fn quote_block_ratio(quote_attempted: u64, quote_blocked: u64) -> f64 {
    ratio_u64(quote_blocked, quote_attempted.saturating_add(quote_blocked))
}

pub fn policy_block_ratio(quote_attempted: u64, policy_blocked: u64) -> f64 {
    ratio_u64(
        policy_blocked,
        quote_attempted.saturating_add(policy_blocked),
    )
}

pub fn value_to_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

pub fn now_ns() -> i64 {
    Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_millis() * 1_000_000)
}
