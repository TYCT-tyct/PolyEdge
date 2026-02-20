use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{Datelike, TimeZone, Utc};
use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::json;

use crate::seat_types::{SeatDecisionRecord, SeatRuntimeState};

pub(crate) fn seat_dir() -> PathBuf {
    let root = std::env::var("POLYEDGE_DATASET_ROOT")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("datasets"));
    root.join("reports").join("seat")
}

pub(crate) fn seat_state_path() -> PathBuf {
    seat_dir().join("seat_state.json")
}

pub(crate) fn seat_decisions_path() -> PathBuf {
    seat_dir().join("seat_decisions.jsonl")
}

pub(crate) fn seat_reports_dir() -> PathBuf {
    seat_dir().join("reports")
}

pub(crate) fn ensure_seat_dir() -> Result<()> {
    fs::create_dir_all(seat_dir()).context("create seat report dir")
}

pub(crate) fn write_state_atomic(state: &SeatRuntimeState) -> Result<()> {
    ensure_seat_dir()?;
    let target = seat_state_path();
    let tmp = target.with_extension("json.tmp");
    let payload = serde_json::to_vec_pretty(state).context("serialize seat state")?;

    {
        let mut file = File::create(&tmp).context("create seat state tmp")?;
        file.write_all(&payload).context("write seat state tmp")?;
        file.sync_all().context("sync seat state tmp")?;
    }

    if let Err(err) = fs::rename(&tmp, &target) {
        if target.exists() {
            let _ = fs::remove_file(&target);
            fs::rename(&tmp, &target).context("replace seat state file")?;
        } else {
            return Err(err).context("rename seat state tmp");
        }
    }
    Ok(())
}

pub(crate) fn append_decision(record: &SeatDecisionRecord) -> Result<()> {
    ensure_seat_dir()?;
    let path = seat_decisions_path();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .context("open seat decisions jsonl")?;
    let line = serde_json::to_string(record).context("serialize seat decision")?;
    writeln!(file, "{line}").context("append seat decision line")?;
    Ok(())
}

pub(crate) fn load_state() -> Option<SeatRuntimeState> {
    let path = seat_state_path();
    let raw = fs::read_to_string(path).ok()?;
    serde_json::from_str::<SeatRuntimeState>(&raw).ok()
}

fn last_decision_record(path: &Path) -> Option<SeatDecisionRecord> {
    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);
    let lines = reader.lines().map_while(Result::ok).collect::<Vec<_>>();
    lines
        .into_iter()
        .rev()
        .find_map(|line| serde_json::from_str::<SeatDecisionRecord>(&line).ok())
}

pub(crate) fn recover_state() -> SeatRuntimeState {
    if let Some(state) = load_state() {
        return state;
    }
    let path = seat_decisions_path();
    let Some(last) = last_decision_record(&path) else {
        return SeatRuntimeState::default();
    };
    let mut state = SeatRuntimeState::default();
    state.current_layer = last.layer;
    state.last_params = last.candidate;
    state.last_objective = Some(last.baseline);
    state.last_decision_ts_ms = last.ts_ms;
    state.trade_count_source = last.trade_count_source;
    state
}

pub(crate) fn load_history(limit: usize) -> Vec<SeatDecisionRecord> {
    let path = seat_decisions_path();
    let Ok(file) = File::open(path) else {
        return Vec::new();
    };
    let reader = BufReader::new(file);
    let mut out = Vec::new();
    for line in reader.lines().map_while(Result::ok) {
        if let Ok(record) = serde_json::from_str::<SeatDecisionRecord>(&line) {
            out.push(record);
        }
    }
    if out.len() > limit {
        out[out.len() - limit..].to_vec()
    } else {
        out
    }
}

fn month_bucket(ts_ms: i64) -> String {
    let dt = Utc
        .timestamp_millis_opt(ts_ms)
        .single()
        .unwrap_or_else(Utc::now);
    format!("{:04}-{:02}", dt.year(), dt.month())
}

pub(crate) fn archive_old_decisions(retention_days: u32) -> Result<()> {
    ensure_seat_dir()?;
    let path = seat_decisions_path();
    if !path.exists() {
        return Ok(());
    }
    let cutoff_ms = Utc::now()
        .timestamp_millis()
        .saturating_sub((retention_days as i64) * 24 * 3_600 * 1_000);

    let file = File::open(&path).context("open seat decision file for archive")?;
    let reader = BufReader::new(file);
    let mut keep_lines = Vec::<String>::new();
    let mut archive_buckets = std::collections::BTreeMap::<String, Vec<String>>::new();

    for line in reader.lines().map_while(Result::ok) {
        let Ok(record) = serde_json::from_str::<SeatDecisionRecord>(&line) else {
            continue;
        };
        if record.ts_ms < cutoff_ms {
            archive_buckets
                .entry(month_bucket(record.ts_ms))
                .or_default()
                .push(line);
        } else {
            keep_lines.push(line);
        }
    }

    for (month, lines) in archive_buckets {
        let gz_path = seat_dir().join(format!("seat_decisions_{month}.jsonl.gz"));
        let gz_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(gz_path)
            .context("open monthly archive")?;
        let mut encoder = GzEncoder::new(gz_file, Compression::default());
        for line in lines {
            writeln!(encoder, "{line}").context("append line to monthly archive")?;
        }
        encoder.finish().context("finalize monthly archive")?;
    }

    let tmp = path.with_extension("jsonl.tmp");
    {
        let mut out = File::create(&tmp).context("create trimmed decision temp file")?;
        for line in keep_lines {
            writeln!(out, "{line}").context("write trimmed decision line")?;
        }
        out.sync_all().context("sync trimmed decision file")?;
    }
    if let Err(err) = fs::rename(&tmp, &path) {
        if path.exists() {
            let _ = fs::remove_file(&path);
            fs::rename(&tmp, &path).context("replace trimmed decision file")?;
        } else {
            return Err(err).context("rename trimmed decision file");
        }
    }
    Ok(())
}

fn decision_file_slug(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    while out.contains("__") {
        out = out.replace("__", "_");
    }
    out.trim_matches('_').to_string()
}

fn numeric_delta(prev: Option<f64>, next: Option<f64>) -> Option<f64> {
    match (prev, next) {
        (Some(a), Some(b)) if a.is_finite() && b.is_finite() => Some(b - a),
        _ => None,
    }
}

pub(crate) fn write_tune_report(
    record: &SeatDecisionRecord,
    state: &SeatRuntimeState,
) -> Result<()> {
    ensure_seat_dir()?;
    fs::create_dir_all(seat_reports_dir()).context("create seat reports dir")?;

    let curve_start = record.ts_ms.saturating_sub(24 * 3_600 * 1_000);
    let objective_curve = state
        .objective_history
        .iter()
        .filter(|point| point.ts_ms >= curve_start)
        .map(|point| json!({"ts_ms": point.ts_ms, "objective": point.objective}))
        .collect::<Vec<_>>();
    let volatility_curve = state
        .volatility_history
        .iter()
        .filter(|point| point.ts_ms >= curve_start)
        .map(|point| json!({"ts_ms": point.ts_ms, "volatility_proxy": point.objective}))
        .collect::<Vec<_>>();

    let param_delta = json!({
        "position_fraction": numeric_delta(record.previous.position_fraction, record.candidate.position_fraction),
        "early_size_scale": numeric_delta(record.previous.early_size_scale, record.candidate.early_size_scale),
        "maturity_size_scale": numeric_delta(record.previous.maturity_size_scale, record.candidate.maturity_size_scale),
        "late_size_scale": numeric_delta(record.previous.late_size_scale, record.candidate.late_size_scale),
        "min_edge_net_bps": numeric_delta(record.previous.min_edge_net_bps, record.candidate.min_edge_net_bps),
        "convergence_exit_ratio": numeric_delta(record.previous.convergence_exit_ratio, record.candidate.convergence_exit_ratio),
        "min_velocity_bps_per_sec": numeric_delta(record.previous.min_velocity_bps_per_sec, record.candidate.min_velocity_bps_per_sec),
        "capital_fraction_kelly": numeric_delta(record.previous.capital_fraction_kelly, record.candidate.capital_fraction_kelly),
        "t100ms_reversal_bps": numeric_delta(record.previous.t100ms_reversal_bps, record.candidate.t100ms_reversal_bps),
        "t300ms_reversal_bps": numeric_delta(record.previous.t300ms_reversal_bps, record.candidate.t300ms_reversal_bps),
        "max_single_trade_loss_usdc": numeric_delta(record.previous.max_single_trade_loss_usdc, record.candidate.max_single_trade_loss_usdc),
        "risk_max_drawdown_pct": numeric_delta(record.previous.risk_max_drawdown_pct, record.candidate.risk_max_drawdown_pct),
        "risk_max_market_notional": numeric_delta(record.previous.risk_max_market_notional, record.candidate.risk_max_market_notional),
        "maker_min_edge_bps": numeric_delta(record.previous.maker_min_edge_bps, record.candidate.maker_min_edge_bps),
        "basis_k_revert": numeric_delta(record.previous.basis_k_revert, record.candidate.basis_k_revert),
        "basis_z_cap": numeric_delta(record.previous.basis_z_cap, record.candidate.basis_z_cap),
    });

    let style_match_score = record
        .notes
        .iter()
        .find_map(|note| note.strip_prefix("style_match_score="))
        .and_then(|v| v.parse::<f64>().ok())
        .or_else(|| {
            record
                .notes
                .iter()
                .find_map(|note| note.strip_prefix("style_objective="))
                .and_then(|v| v.parse::<f64>().ok())
        })
        .unwrap_or(0.0);
    let shadow_pnl_proxy = record
        .notes
        .iter()
        .find_map(|note| note.strip_prefix("shadow_ev_usdc_p50="))
        .and_then(|v| v.parse::<f64>().ok());

    let payload = json!({
        "ts_ms": record.ts_ms,
        "layer": record.layer.as_str(),
        "decision": record.decision,
        "rollback": record.rollback,
        "trade_count_source": record.trade_count_source,
        "params_previous": record.previous,
        "params_candidate": record.candidate,
        "params_delta": param_delta,
        "baseline": record.baseline,
        "risk_metrics": {
            "max_drawdown_pct_baseline": record.baseline.max_drawdown_pct,
            "max_drawdown_pct_latest": state.last_objective.as_ref().map(|v| v.max_drawdown_pct),
            "source_health_min_baseline": record.baseline.source_health_min,
            "source_health_min_latest": state.last_objective.as_ref().map(|v| v.source_health_min),
        },
        "shadow_metrics": {
            "shadow_ev_usdc_p50": shadow_pnl_proxy,
            "active_shadow_until_ms": record.lock_state.active_shadow_until_ms,
        },
        "style_metrics": {
            "style_match_score": style_match_score,
            "style_memory_size": state.style_memory.len(),
        },
        "curves": {
            "objective_24h": objective_curve,
            "volatility_24h": volatility_curve,
        },
        "notes": record.notes,
    });

    let slug = decision_file_slug(&record.decision);
    let json_path = seat_reports_dir().join(format!("seat_tune_{}_{}.json", record.ts_ms, slug));
    let md_path = seat_reports_dir().join(format!("seat_tune_{}_{}.md", record.ts_ms, slug));
    fs::write(
        &json_path,
        serde_json::to_vec_pretty(&payload).context("serialize seat tune report")?,
    )
    .context("write seat tune json report")?;

    let mut md = String::new();
    md.push_str("# SEAT Tune Report\n\n");
    md.push_str(&format!("- ts_ms: {}\n", record.ts_ms));
    md.push_str(&format!("- layer: {}\n", record.layer.as_str()));
    md.push_str(&format!("- decision: {}\n", record.decision));
    md.push_str(&format!("- rollback: {}\n", record.rollback));
    md.push_str(&format!(
        "- trade_count_source: {}\n",
        record.trade_count_source
    ));
    md.push_str(&format!(
        "- baseline_ev_usdc_p50: {:.6}\n",
        record.baseline.ev_usdc_p50
    ));
    md.push_str(&format!(
        "- baseline_max_drawdown_pct: {:.6}\n",
        record.baseline.max_drawdown_pct
    ));
    md.push_str(&format!("- style_match_score: {:.6}\n", style_match_score));
    if let Some(shadow_ev) = shadow_pnl_proxy {
        md.push_str(&format!("- shadow_ev_usdc_p50: {:.6}\n", shadow_ev));
    }
    fs::write(md_path, md).context("write seat tune markdown report")?;
    Ok(())
}
