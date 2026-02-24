use anyhow::{Context, Result};
use chrono::{Datelike, TimeZone, Timelike, Utc};

pub fn install_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info,polyedge_forge=debug".to_string()),
        )
        .try_init();
}

pub fn parse_upper_csv(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|v| v.trim().to_ascii_uppercase())
        .filter(|v| !v.is_empty())
        .collect()
}

pub fn parse_lower_csv(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty())
        .collect()
}

pub fn timeframe_to_ms(tf: &str) -> Option<i64> {
    match tf {
        "5m" => Some(5 * 60 * 1000),
        "15m" => Some(15 * 60 * 1000),
        "30m" => Some(30 * 60 * 1000),
        "1h" => Some(60 * 60 * 1000),
        "2h" => Some(2 * 60 * 60 * 1000),
        "4h" => Some(4 * 60 * 60 * 1000),
        _ => None,
    }
}

pub fn parse_timestamp_ms(input: Option<&str>) -> Option<i64> {
    let raw = input?.trim();
    if raw.is_empty() {
        return None;
    }

    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(raw) {
        return Some(dt.timestamp_millis());
    }
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(raw, "%m/%d/%Y %H:%M:%S") {
        return Some(Utc.from_utc_datetime(&naive).timestamp_millis());
    }
    None
}

pub fn ts_to_date_hour(ts_ms: i64) -> Result<(String, u32)> {
    let dt = Utc
        .timestamp_millis_opt(ts_ms)
        .single()
        .with_context(|| format!("invalid timestamp {ts_ms}"))?;
    Ok((
        format!("{:04}-{:02}-{:02}", dt.year(), dt.month(), dt.day()),
        dt.hour(),
    ))
}
