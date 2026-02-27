use chrono::DateTime;

pub(super) fn normalize_market_type(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "5m" | "5min" | "5" => Some("5m"),
        "15m" | "15min" | "15" => Some("15m"),
        "30m" | "30min" | "30" => Some("30m"),
        "1h" | "60m" | "60min" => Some("1h"),
        "2h" | "120m" | "120min" => Some("2h"),
        "4h" | "240m" | "240min" => Some("4h"),
        _ => None,
    }
}

pub(super) fn market_type_to_ms(market_type: &str) -> i64 {
    match market_type {
        "5m" => 5 * 60 * 1000,
        "15m" => 15 * 60 * 1000,
        "30m" => 30 * 60 * 1000,
        "1h" => 60 * 60 * 1000,
        "2h" => 2 * 60 * 60 * 1000,
        "4h" => 4 * 60 * 60 * 1000,
        _ => 0,
    }
}

pub(super) fn infer_market_type_from_round_id(round_id: &str) -> Option<&'static str> {
    let mut parts = round_id.split('_');
    let _symbol = parts.next()?;
    let market_type = parts.next()?;
    normalize_market_type(market_type)
}

pub(super) fn parse_round_start_ms(round_id: &str) -> Option<i64> {
    let raw = round_id.rsplit('_').next()?;
    raw.parse::<i64>().ok().filter(|v| *v > 0)
}

pub(super) fn parse_end_date_ms(raw: Option<&str>) -> Option<i64> {
    let value = raw?.trim();
    if value.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

pub(super) fn is_safe_round_id(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 128
        && v.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}
