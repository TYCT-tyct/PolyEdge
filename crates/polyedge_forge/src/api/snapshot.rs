use super::*;

pub(super) fn is_live_snapshot_fresh(snapshot: &Value, market_type: &str, now_ms: i64) -> bool {
    let ts_ms = snapshot
        .get("ts_ireland_sample_ms")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    if ts_ms <= 0 {
        return false;
    }
    if now_ms.saturating_sub(ts_ms) > LIVE_SNAPSHOT_MAX_AGE_MS {
        return false;
    }

    let remaining_ms = snapshot
        .get("remaining_ms")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    if remaining_ms <= 0 && now_ms.saturating_sub(ts_ms) > LIVE_ROUND_END_GRACE_MS {
        return false;
    }

    let round_id = snapshot
        .get("round_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let start_ms = parse_round_start_ms(round_id).unwrap_or(0);
    if start_ms > 0 {
        let end_ms = start_ms.saturating_add(market_type_to_ms(market_type));
        if now_ms > end_ms.saturating_add(LIVE_ROUND_END_GRACE_MS) {
            return false;
        }
    }

    true
}

pub(super) fn snapshot_remaining_ms(snapshot: &Value) -> i64 {
    snapshot
        .get("remaining_ms")
        .and_then(Value::as_i64)
        .or_else(|| {
            snapshot
                .get("time_remaining_s")
                .and_then(Value::as_f64)
                .map(|v| (v * 1000.0).round() as i64)
        })
        .unwrap_or(0)
}

pub(super) fn snapshot_priority(
    snapshot: &Value,
    market_type: &str,
    now_ms: i64,
) -> (i64, i64, i64) {
    let rem_ms = snapshot_remaining_ms(snapshot);
    let active = if rem_ms > 0 {
        1
    } else {
        let rid = snapshot
            .get("round_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let start_ms = parse_round_start_ms(rid).unwrap_or(0);
        if start_ms > 0 {
            let end_ms = start_ms.saturating_add(market_type_to_ms(market_type));
            if now_ms <= end_ms.saturating_add(LIVE_ROUND_END_GRACE_MS) {
                1
            } else {
                0
            }
        } else {
            0
        }
    };
    let ts = snapshot
        .get("ts_ireland_sample_ms")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let start_ms = snapshot
        .get("round_id")
        .and_then(Value::as_str)
        .and_then(parse_round_start_ms)
        .unwrap_or(0);
    (active, ts, start_ms)
}
