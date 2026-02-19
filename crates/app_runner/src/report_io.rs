use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use chrono::Utc;
use core_types::{ShadowOutcome, ShadowShot};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, RwLock};

use crate::state::{
    EnginePnlReport, MarketScoreRow, PerfProfile, ShadowFinalReport, ShadowLiveReport,
    ToxicityLiveReport,
};
use crate::stats_utils::percentile;
use crate::spawn_detached;

pub(super) fn ensure_dataset_dirs() {
    for bucket in ["raw", "normalized", "reports"] {
        let path = dataset_dir(bucket);
        let _ = fs::create_dir_all(path);
    }
}

pub(super) fn dataset_date() -> String {
    Utc::now().format("%Y-%m-%d").to_string()
}

pub(super) fn dataset_dir(kind: &str) -> PathBuf {
    let root = std::env::var("POLYEDGE_DATASET_ROOT")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("datasets"));
    root.join(kind).join(dataset_date())
}

pub(super) fn dataset_path(kind: &str, filename: &str) -> PathBuf {
    dataset_dir(kind).join(filename)
}

pub(super) fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[derive(Debug)]
pub(super) struct JsonlWriteReq {
    path: PathBuf,
    line: String,
}

pub(super) static JSONL_WRITER: OnceLock<mpsc::Sender<JsonlWriteReq>> = OnceLock::new();
pub(super) static JSONL_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
pub(super) static JSONL_QUEUE_CAP: AtomicU64 = AtomicU64::new(0);
pub(super) static JSONL_DROP_ON_FULL: AtomicBool = AtomicBool::new(true);
pub(super) static NORMALIZED_INGEST_SEQ: AtomicU64 = AtomicU64::new(0);

pub(super) fn next_normalized_ingest_seq() -> u64 {
    NORMALIZED_INGEST_SEQ.fetch_add(1, Ordering::Relaxed) + 1
}

pub(super) async fn init_jsonl_writer(perf_profile: Arc<RwLock<PerfProfile>>) {
    if JSONL_WRITER.get().is_some() {
        return;
    }
    let cfg = perf_profile.read().await.clone();
    let (tx, mut rx) = mpsc::channel::<JsonlWriteReq>(cfg.io_queue_capacity.max(256));
    JSONL_QUEUE_CAP.store(cfg.io_queue_capacity.max(256) as u64, Ordering::Relaxed);
    JSONL_DROP_ON_FULL.store(cfg.io_drop_on_full, Ordering::Relaxed);
    if JSONL_WRITER.set(tx.clone()).is_err() {
        return;
    }
    spawn_detached("jsonl_writer", true, async move {
        let mut batch = Vec::<JsonlWriteReq>::new();
        let mut ticker = tokio::time::interval(Duration::from_millis(200));
        loop {
            tokio::select! {
                maybe_req = rx.recv() => {
                    match maybe_req {
                        Some(req) => {
                            batch.push(req);
                            let flush_batch = perf_profile.read().await.io_flush_batch.max(1);
                            if batch.len() >= flush_batch {
                                let to_flush = std::mem::take(&mut batch);
                                let _ = tokio::task::spawn_blocking(move || flush_jsonl_batch_sync(to_flush)).await;
                            }
                        }
                        None => {
                            if !batch.is_empty() {
                                let to_flush = std::mem::take(&mut batch);
                                let _ = tokio::task::spawn_blocking(move || flush_jsonl_batch_sync(to_flush)).await;
                            }
                            break;
                        }
                    }
                }
                _ = ticker.tick() => {
                    if !batch.is_empty() {
                        let to_flush = std::mem::take(&mut batch);
                        let _ = tokio::task::spawn_blocking(move || flush_jsonl_batch_sync(to_flush)).await;
                    }
                }
            }
            let cap = JSONL_QUEUE_CAP.load(Ordering::Relaxed) as usize;
            JSONL_QUEUE_DEPTH.store(cap.saturating_sub(tx.capacity()) as u64, Ordering::Relaxed);
        }
    });
}

pub(super) fn flush_jsonl_batch_sync(batch: Vec<JsonlWriteReq>) {
    let mut grouped = HashMap::<PathBuf, Vec<String>>::new();
    for req in batch {
        grouped.entry(req.path).or_default().push(req.line);
    }
    for (path, lines) in grouped {
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&path) {
            for line in lines {
                let _ = writeln!(file, "{line}");
            }
        }
    }
}

pub(super) fn current_jsonl_queue_depth() -> u64 {
    JSONL_QUEUE_DEPTH.load(Ordering::Relaxed)
}

pub(super) fn append_jsonl_sync(path: &Path, line: &str) {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
    }
}

pub(super) fn append_jsonl_line(path: &Path, line: String) {
    if let Some(tx) = JSONL_WRITER.get() {
        let req = JsonlWriteReq {
            path: path.to_path_buf(),
            line,
        };
        match tx.try_send(req) {
            Ok(_) => {
                let cap = JSONL_QUEUE_CAP.load(Ordering::Relaxed) as usize;
                JSONL_QUEUE_DEPTH
                    .store(cap.saturating_sub(tx.capacity()) as u64, Ordering::Relaxed);
                return;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(req)) => {
                metrics::counter!("io.jsonl.queue_full").increment(1);
                if JSONL_DROP_ON_FULL.load(Ordering::Relaxed) {
                    metrics::counter!("io.jsonl.dropped").increment(1);
                    return;
                }
                append_jsonl_sync(&req.path, &req.line);
                return;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(req)) => {
                metrics::counter!("io.jsonl.queue_closed").increment(1);
                if JSONL_DROP_ON_FULL.load(Ordering::Relaxed) {
                    metrics::counter!("io.jsonl.dropped").increment(1);
                    return;
                }
                append_jsonl_sync(&req.path, &req.line);
                return;
            }
        }
    }
    append_jsonl_sync(path, &line);
}

pub(super) fn append_jsonl(path: &Path, value: &serde_json::Value) {
    append_jsonl_line(path, value.to_string());
}

pub(super) static LAST_LIVE_REPORT_PERSIST_MS: AtomicI64 = AtomicI64::new(0);

pub(super) fn persist_live_report_files(live: &ShadowLiveReport) {
    // Throttle file persistence. /report/shadow/live can be polled at high frequency (storm tests)
    // and pretty-json serialization + fs::write per request is unnecessary and can destabilize the
    // process under load.
    let now_ms = Utc::now().timestamp_millis();
    let last_ms = LAST_LIVE_REPORT_PERSIST_MS.load(Ordering::Relaxed);
    if now_ms.saturating_sub(last_ms) < 1_000 {
        return;
    }
    if LAST_LIVE_REPORT_PERSIST_MS
        .compare_exchange(last_ms, now_ms, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }

    let reports_dir = dataset_dir("reports");
    let _ = fs::create_dir_all(&reports_dir);

    let live_json_path = reports_dir.join("shadow_live_latest.json");
    if let Ok(raw) = serde_json::to_string_pretty(live) {
        let _ = fs::write(live_json_path, raw);
    }
}

pub(super) fn persist_engine_pnl_report(report: &EnginePnlReport) {
    let reports_dir = dataset_dir("reports");
    let _ = fs::create_dir_all(&reports_dir);

    let json_path = reports_dir.join("engine_pnl_breakdown_latest.json");
    if let Ok(raw) = serde_json::to_string_pretty(report) {
        let _ = fs::write(json_path, raw);
    }

    let csv_path = reports_dir.join("engine_pnl_breakdown.csv");
    let mut rows = String::new();
    rows.push_str("window_id,engine,samples,total_usdc,p50_usdc,p10_usdc,positive_ratio\n");
    for row in &report.rows {
        rows.push_str(&format!(
            "{},{},{},{:.6},{:.6},{:.6},{:.6}\n",
            report.window_id,
            row.engine,
            row.samples,
            row.total_usdc,
            row.p50_usdc,
            row.p10_usdc,
            row.positive_ratio
        ));
    }
    let _ = fs::write(csv_path, rows);
}

pub(super) fn persist_final_report_files(report: &ShadowFinalReport) {
    let reports_dir = dataset_dir("reports");
    let _ = fs::create_dir_all(&reports_dir);

    let md_path = reports_dir.join("report_shadow_12h.md");
    let gate_label = if report.gate.pass { "PASS" } else { "FAIL" };
    let mut md = String::new();
    md.push_str("# Shadow 12h Report\n\n");
    md.push_str(&format!("- gate: {gate_label}\n"));
    md.push_str(&format!(
        "- fillability@10ms: {:.4}\n",
        report.gate.fillability_10ms
    ));
    md.push_str(&format!(
        "- net_edge_p50_bps: {:.4}\n",
        report.gate.net_edge_p50_bps
    ));
    md.push_str(&format!(
        "- net_edge_p10_bps: {:.4}\n",
        report.gate.net_edge_p10_bps
    ));
    md.push_str(&format!(
        "- net_markout_10s_usdc_p50: {:.6}\n",
        report.gate.net_markout_10s_usdc_p50
    ));
    md.push_str(&format!(
        "- roi_notional_10s_bps_p50: {:.6}\n",
        report.gate.roi_notional_10s_bps_p50
    ));
    md.push_str(&format!(
        "- ev_net_usdc_p50: {:.6}\n",
        report.gate.ev_net_usdc_p50
    ));
    md.push_str(&format!(
        "- ev_net_usdc_p10: {:.6}\n",
        report.gate.ev_net_usdc_p10
    ));
    md.push_str(&format!(
        "- ev_positive_ratio: {:.4}\n",
        report.gate.ev_positive_ratio
    ));
    md.push_str(&format!(
        "- executed_over_eligible: {:.4}\n",
        report.gate.executed_over_eligible
    ));
    md.push_str(&format!(
        "- eligible_count: {}\n",
        report.gate.eligible_count
    ));
    md.push_str(&format!(
        "- executed_count: {}\n",
        report.gate.executed_count
    ));
    md.push_str(&format!(
        "- pnl_10s_p50_bps_raw: {:.4}\n",
        report.gate.pnl_10s_p50_bps_raw
    ));
    md.push_str(&format!(
        "- pnl_10s_p50_bps_robust: {:.4}\n",
        report.gate.pnl_10s_p50_bps_robust
    ));
    md.push_str(&format!(
        "- pnl_10s_sample_count: {}\n",
        report.gate.pnl_10s_sample_count
    ));
    md.push_str(&format!(
        "- pnl_10s_outlier_ratio: {:.4}\n",
        report.gate.pnl_10s_outlier_ratio
    ));
    md.push_str(&format!(
        "- quote_block_ratio: {:.4}\n",
        report.gate.quote_block_ratio
    ));
    md.push_str(&format!(
        "- policy_block_ratio: {:.4}\n",
        report.gate.policy_block_ratio
    ));
    md.push_str(&format!(
        "- gate_block_ratio: {:.4}\n",
        report.gate.gate_block_ratio
    ));
    md.push_str(&format!(
        "- strategy_uptime_pct: {:.2}\n",
        report.gate.strategy_uptime_pct
    ));
    md.push_str(&format!(
        "- data_valid_ratio: {:.5}\n",
        report.gate.data_valid_ratio
    ));
    md.push_str(&format!(
        "- seq_gap_rate: {:.5}\n",
        report.gate.seq_gap_rate
    ));
    md.push_str(&format!(
        "- ts_inversion_rate: {:.5}\n",
        report.gate.ts_inversion_rate
    ));
    md.push_str(&format!(
        "- stale_tick_drop_ratio: {:.5}\n",
        report.gate.stale_tick_drop_ratio
    ));
    md.push_str(&format!(
        "- tick_to_ack_p99_ms: {:.4}\n\n",
        report.gate.tick_to_ack_p99_ms
    ));
    md.push_str(&format!(
        "- tick_to_decision_p99_ms: {:.4}\n",
        report.live.tick_to_decision_p99_ms
    ));
    md.push_str(&format!(
        "- ack_only_p99_ms: {:.4}\n",
        report.live.ack_only_p99_ms
    ));
    md.push_str(&format!(
        "- decision_queue_wait_p99_ms: {:.4}\n",
        report.gate.decision_queue_wait_p99_ms
    ));
    md.push_str(&format!(
        "- decision_compute_p99_ms: {:.4}\n",
        report.gate.decision_compute_p99_ms
    ));
    md.push_str(&format!(
        "- source_latency_p99_ms: {:.4}\n",
        report.gate.source_latency_p99_ms
    ));
    md.push_str(&format!(
        "- local_backlog_p99_ms: {:.4}\n",
        report.gate.local_backlog_p99_ms
    ));
    md.push_str(&format!(
        "- queue_depth_p99: {:.4}\n",
        report.live.queue_depth_p99
    ));
    md.push_str(&format!(
        "- event_backlog_p99: {:.4}\n\n",
        report.live.event_backlog_p99
    ));
    md.push_str(&format!(
        "- quote_attempted: {}\n- quote_blocked: {}\n- policy_blocked: {}\n- ref_ticks_total: {}\n- book_ticks_total: {}\n- ref_freshness_ms: {}\n- book_freshness_ms: {}\n\n",
        report.live.quote_attempted,
        report.live.quote_blocked,
        report.live.policy_blocked,
        report.live.ref_ticks_total,
        report.live.book_ticks_total,
        report.live.ref_freshness_ms,
        report.live.book_freshness_ms
    ));
    if report.gate.failed_reasons.is_empty() {
        md.push_str("## Failed Reasons\n- none\n");
    } else {
        md.push_str("## Failed Reasons\n");
        for reason in &report.gate.failed_reasons {
            md.push_str(&format!("- {reason}\n"));
        }
    }
    if report.live.blocked_reason_counts.is_empty() {
        md.push_str("\n## Blocked Reasons\n- none\n");
    } else {
        md.push_str("\n## Blocked Reasons\n");
        let mut rows = report.live.blocked_reason_counts.iter().collect::<Vec<_>>();
        rows.sort_by(|a, b| b.1.cmp(a.1));
        for (reason, count) in rows {
            md.push_str(&format!("- {}: {}\n", reason, count));
        }
    }
    if report.live.policy_block_reason_distribution.is_empty() {
        md.push_str("\n## Policy Block Reason Distribution\n- none\n");
    } else {
        md.push_str("\n## Policy Block Reason Distribution\n");
        let mut rows = report
            .live
            .policy_block_reason_distribution
            .iter()
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| b.1.cmp(a.1));
        for (reason, count) in rows {
            md.push_str(&format!("- {}: {}\n", reason, count));
        }
    }
    if report.live.gate_block_reason_distribution.is_empty() {
        md.push_str("\n## Gate Block Reason Distribution\n- none\n");
    } else {
        md.push_str("\n## Gate Block Reason Distribution\n");
        let mut rows = report
            .live
            .gate_block_reason_distribution
            .iter()
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| b.1.cmp(a.1));
        for (reason, count) in rows {
            md.push_str(&format!("- {}: {}\n", reason, count));
        }
    }
    let _ = fs::write(md_path, md);

    let latency_csv = reports_dir.join("latency_breakdown_12h.csv");
    let mut latency_rows = String::new();
    latency_rows.push_str("stage,p50,p90,p99,unit\n");
    latency_rows.push_str(&format!(
        "feed_in,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.feed_in_p50_ms,
        report.live.latency.feed_in_p90_ms,
        report.live.latency.feed_in_p99_ms
    ));
    latency_rows.push_str(&format!(
        "signal,{:.6},{:.6},{:.6},us\n",
        report.live.latency.signal_p50_us,
        report.live.latency.signal_p90_us,
        report.live.latency.signal_p99_us
    ));
    latency_rows.push_str(&format!(
        "quote,{:.6},{:.6},{:.6},us\n",
        report.live.latency.quote_p50_us,
        report.live.latency.quote_p90_us,
        report.live.latency.quote_p99_us
    ));
    latency_rows.push_str(&format!(
        "risk,{:.6},{:.6},{:.6},us\n",
        report.live.latency.risk_p50_us,
        report.live.latency.risk_p90_us,
        report.live.latency.risk_p99_us
    ));
    latency_rows.push_str(&format!(
        "decision_queue_wait,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.decision_queue_wait_p50_ms,
        report.live.latency.decision_queue_wait_p90_ms,
        report.live.latency.decision_queue_wait_p99_ms
    ));
    latency_rows.push_str(&format!(
        "decision_compute,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.decision_compute_p50_ms,
        report.live.latency.decision_compute_p90_ms,
        report.live.latency.decision_compute_p99_ms
    ));
    latency_rows.push_str(&format!(
        "tick_to_decision,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.tick_to_decision_p50_ms,
        report.live.latency.tick_to_decision_p90_ms,
        report.live.latency.tick_to_decision_p99_ms
    ));
    latency_rows.push_str(&format!(
        "ack_only,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.ack_only_p50_ms,
        report.live.latency.ack_only_p90_ms,
        report.live.latency.ack_only_p99_ms
    ));
    latency_rows.push_str(&format!(
        "tick_to_ack,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.tick_to_ack_p50_ms,
        report.live.latency.tick_to_ack_p90_ms,
        report.live.latency.tick_to_ack_p99_ms
    ));
    latency_rows.push_str(&format!(
        "parse,{:.6},{:.6},{:.6},us\n",
        0.0, 0.0, report.live.latency.parse_p99_us
    ));
    latency_rows.push_str(&format!(
        "io_queue,{:.6},{:.6},{:.6},count\n",
        0.0, 0.0, report.live.latency.io_queue_p99_ms
    ));
    latency_rows.push_str(&format!(
        "bus_lag,{:.6},{:.6},{:.6},count\n",
        0.0, 0.0, report.live.latency.bus_lag_p99_ms
    ));
    latency_rows.push_str(&format!(
        "shadow_fill,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.shadow_fill_p50_ms,
        report.live.latency.shadow_fill_p90_ms,
        report.live.latency.shadow_fill_p99_ms
    ));
    latency_rows.push_str(&format!(
        "source_latency,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.source_latency_p50_ms,
        report.live.latency.source_latency_p90_ms,
        report.live.latency.source_latency_p99_ms
    ));
    latency_rows.push_str(&format!(
        "local_backlog,{:.6},{:.6},{:.6},ms\n",
        report.live.latency.local_backlog_p50_ms,
        report.live.latency.local_backlog_p90_ms,
        report.live.latency.local_backlog_p99_ms
    ));
    let _ = fs::write(latency_csv, latency_rows);

    let score_path = reports_dir.join("market_scorecard.csv");
    let mut score_rows = String::new();
    score_rows.push_str("market_id,symbol,shots,outcomes,fillability_10ms,net_edge_p50_bps,net_edge_p10_bps,pnl_10s_p50_bps,net_markout_10s_usdc_p50,roi_notional_10s_bps_p50\n");
    for row in &report.live.market_scorecard {
        score_rows.push_str(&format!(
            "{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6}\n",
            row.market_id,
            row.symbol,
            row.shots,
            row.outcomes,
            row.fillability_10ms,
            row.net_edge_p50_bps,
            row.net_edge_p10_bps,
            row.pnl_10s_p50_bps,
            row.net_markout_10s_usdc_p50,
            row.roi_notional_10s_bps_p50
        ));
    }
    let _ = fs::write(score_path, score_rows);

    let fixlist_path = reports_dir.join("next_fixlist.md");
    let mut fixlist = String::new();
    fixlist.push_str("# Next Fixlist\n\n");
    if report.gate.failed_reasons.is_empty() {
        fixlist.push_str("- Gate passed. Keep conservative limits and continue monitoring.\n");
    } else {
        for reason in &report.gate.failed_reasons {
            fixlist.push_str(&format!("- {reason}\n"));
        }
    }
    let _ = fs::write(fixlist_path, fixlist);

    let truth_manifest_path = reports_dir.join("truth_manifest.json");
    let truth_manifest = serde_json::json!({
        "generated_at_utc": Utc::now().to_rfc3339(),
        "metrics_contract_version": "2026-02-14.v1",
        "window": {
            "window_id": report.live.window_id,
            "window_shots": report.live.window_shots,
            "window_outcomes": report.live.window_outcomes,
            "gate_ready": report.live.gate_ready,
            "gate_ready_strict": report.live.gate_ready_strict,
            "gate_ready_effective": report.live.gate_ready_effective,
            "last_30s_taker_fallback_count": report.live.last_30s_taker_fallback_count,
        },
        "data_chain": {
            "raw_fields": ["sha256", "source_seq", "ingest_seq", "event_ts_exchange_ms", "recv_ts_local_ns"],
            "normalized_fields": ["source_seq", "ingest_seq", "market_id", "symbol", "delay_ms", "fillable", "net_markout_10s_usdc"],
            "invalid_excluded_from_gate": true
        },
        "formulas": {
            "quote_block_ratio": "quote_blocked / (quote_attempted + quote_blocked)",
            "policy_block_ratio": "policy_blocked / (quote_attempted + policy_blocked)",
            "policy_blocked_scope": "risk:* and risk_capped_zero only",
            "executed_over_eligible": "executed_count / eligible_count",
            "ev_net_usdc_p50": "p50(net_markout_10s_usdc)",
            "ev_positive_ratio": "count(net_markout_10s_usdc > 0) / count(valid outcomes)"
        },
        "hard_gates": {
            "data_valid_ratio_min": 0.999,
            "seq_gap_rate_max": 0.001,
            "ts_inversion_rate_max": 0.0005,
            "tick_to_ack_p99_ms_max": 450.0,
            "decision_compute_p99_ms_max": 2.0,
            "feed_in_p99_ms_max": 800.0,
            "executed_over_eligible_min": 0.60,
            "quote_block_ratio_max": 0.10,
            "policy_block_ratio_max": 0.10,
            "ev_net_usdc_p50_min": 0.0,
            "roi_notional_10s_bps_p50_min": 0.0
        }
    });
    if let Ok(raw) = serde_json::to_string_pretty(&truth_manifest) {
        let _ = fs::write(truth_manifest_path, raw);
    }
}

pub(super) fn persist_toxicity_report_files(report: &ToxicityLiveReport) {
    let reports_dir = dataset_dir("reports");
    let _ = fs::create_dir_all(&reports_dir);

    let live_json_path = reports_dir.join("toxicity_live_latest.json");
    if let Ok(raw) = serde_json::to_string_pretty(report) {
        let _ = fs::write(live_json_path, raw);
    }

    let csv_path = reports_dir.join("toxicity_scorecard.csv");
    let mut rows = String::new();
    rows.push_str("market_rank,active_for_quoting,market_id,symbol,tox_score,regime,market_score,markout_10s_bps,no_quote_rate,symbol_missing_rate,pending_exposure\n");
    for row in &report.rows {
        rows.push_str(&format!(
            "{},{},{},{},{:.6},{:?},{:.6},{:.6},{:.6},{:.6},{:.6}\n",
            row.market_rank,
            row.active_for_quoting,
            row.market_id,
            row.symbol,
            row.tox_score,
            row.regime,
            row.market_score,
            row.markout_10s_bps,
            row.no_quote_rate,
            row.symbol_missing_rate,
            row.pending_exposure
        ));
    }
    let _ = fs::write(csv_path, rows);
}

pub(super) fn fillability_ratio(outcomes: &[ShadowOutcome], delay_ms: u64) -> f64 {
    let mut total = 0_u64;
    let mut filled = 0_u64;
    for o in outcomes {
        if o.delay_ms != delay_ms {
            continue;
        }
        total = total.saturating_add(1);
        if o.fillable {
            filled = filled.saturating_add(1);
        }
    }
    if total == 0 {
        0.0
    } else {
        filled as f64 / total as f64
    }
}

pub(super) fn survival_ratio(outcomes: &[ShadowOutcome], delay_ms: u64) -> f64 {
    let mut total = 0_u64;
    let mut survived = 0_u64;
    for o in outcomes {
        if o.delay_ms != delay_ms {
            continue;
        }
        total = total.saturating_add(1);
        if o.survived {
            survived = survived.saturating_add(1);
        }
    }
    if total == 0 {
        0.0
    } else {
        survived as f64 / total as f64
    }
}

pub(super) fn build_market_scorecard(
    shots: &[ShadowShot],
    outcomes: &[ShadowOutcome],
) -> Vec<MarketScoreRow> {
    const PRIMARY_DELAY_MS: u64 = 10;
    const MAX_ROWS: usize = 200;

    #[derive(Default)]
    struct Agg {
        market_id: String,
        symbol: String,
        shots_primary: usize,
        outcomes_primary: usize,
        filled_10ms: u64,
        total_10ms: u64,
        net_edges: Vec<f64>,
        pnl_10s: Vec<f64>,
        net_markout_10s_usdc: Vec<f64>,
        roi_notional_10s_bps: Vec<f64>,
    }

    // Single-pass aggregation to avoid O(N^2) cloning/filtering. This keeps /report/shadow/live
    // stable under stress polling.
    let mut by_key: HashMap<(String, String), Agg> = HashMap::new();

    for s in shots {
        if s.delay_ms != PRIMARY_DELAY_MS {
            continue;
        }
        let key = (s.market_id.clone(), s.symbol.clone());
        let entry = by_key.entry(key).or_insert_with(|| Agg {
            market_id: s.market_id.clone(),
            symbol: s.symbol.clone(),
            ..Agg::default()
        });
        entry.shots_primary = entry.shots_primary.saturating_add(1);
        entry.net_edges.push(s.edge_net_bps);
    }

    for o in outcomes {
        if o.delay_ms != PRIMARY_DELAY_MS {
            continue;
        }
        let key = (o.market_id.clone(), o.symbol.clone());
        let entry = by_key.entry(key).or_insert_with(|| Agg {
            market_id: o.market_id.clone(),
            symbol: o.symbol.clone(),
            ..Agg::default()
        });
        entry.outcomes_primary = entry.outcomes_primary.saturating_add(1);
        entry.total_10ms = entry.total_10ms.saturating_add(1);
        if o.fillable {
            entry.filled_10ms = entry.filled_10ms.saturating_add(1);
        }
        if let Some(v) = o.net_markout_10s_bps.or(o.pnl_10s_bps) {
            entry.pnl_10s.push(v);
        }
        if let Some(v) = o.net_markout_10s_usdc {
            entry.net_markout_10s_usdc.push(v);
        }
        if let Some(v) = o.roi_notional_10s_bps {
            entry.roi_notional_10s_bps.push(v);
        }
    }

    let mut rows = Vec::with_capacity(by_key.len());
    for (_, agg) in by_key {
        let fillability_10ms = if agg.total_10ms == 0 {
            0.0
        } else {
            agg.filled_10ms as f64 / agg.total_10ms as f64
        };
        rows.push(MarketScoreRow {
            market_id: agg.market_id,
            symbol: agg.symbol,
            shots: agg.shots_primary,
            outcomes: agg.outcomes_primary,
            fillability_10ms,
            net_edge_p50_bps: percentile(&agg.net_edges, 0.50).unwrap_or(0.0),
            net_edge_p10_bps: percentile(&agg.net_edges, 0.10).unwrap_or(0.0),
            pnl_10s_p50_bps: percentile(&agg.pnl_10s, 0.50).unwrap_or(0.0),
            net_markout_10s_usdc_p50: percentile(&agg.net_markout_10s_usdc, 0.50).unwrap_or(0.0),
            roi_notional_10s_bps_p50: percentile(&agg.roi_notional_10s_bps, 0.50).unwrap_or(0.0),
        });
    }

    rows.sort_by(|a, b| {
        b.net_markout_10s_usdc_p50
            .total_cmp(&a.net_markout_10s_usdc_p50)
    });
    rows.truncate(MAX_ROWS);
    rows
}
