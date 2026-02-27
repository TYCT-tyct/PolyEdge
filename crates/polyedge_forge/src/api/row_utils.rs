use serde_json::Value;

pub(super) fn rows_from_json(v: Value) -> Vec<Value> {
    v.get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

pub(super) fn row_i64(v: &Value, key: &str) -> Option<i64> {
    let val = v.get(key)?;
    if let Some(n) = val.as_i64() {
        return Some(n);
    }
    if let Some(u) = val.as_u64() {
        return i64::try_from(u).ok();
    }
    if let Some(f) = val.as_f64() {
        if f.is_finite() {
            return Some(f.round() as i64);
        }
    }
    val.as_str()?.trim().parse::<i64>().ok()
}

pub(super) fn row_f64(v: &Value, key: &str) -> Option<f64> {
    let val = v.get(key)?;
    if let Some(f) = val.as_f64() {
        return Some(f);
    }
    if let Some(i) = val.as_i64() {
        return Some(i as f64);
    }
    if let Some(u) = val.as_u64() {
        return Some(u as f64);
    }
    val.as_str()?.trim().parse::<f64>().ok()
}

pub(super) fn stride_downsample(rows: Vec<Value>, step: usize) -> Vec<Value> {
    if step <= 1 || rows.len() <= 2 {
        return rows;
    }
    let mut out = Vec::with_capacity(rows.len() / step + 2);
    for (idx, row) in rows.iter().enumerate() {
        if idx == 0 || idx + 1 == rows.len() || idx % step == 0 {
            out.push(row.clone());
        }
    }
    out
}
