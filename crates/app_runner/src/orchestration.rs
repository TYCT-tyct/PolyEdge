use super::*;

pub(super) fn spawn_periodic_report_persistor(
    stats: Arc<ShadowStats>,
    tox_state: Arc<RwLock<HashMap<String, MarketToxicState>>>,
    execution: Arc<ClobExecution>,
    toxicity_cfg: Arc<RwLock<ToxicityConfig>>,
) {
    tokio::spawn(async move {
        let mut last_final = Instant::now() - Duration::from_secs(600);
        loop {
            let live = stats.build_live_report().await;
            persist_live_report_files(&live);
            let engine_pnl = stats.build_engine_pnl_report().await;
            persist_engine_pnl_report(&engine_pnl);
            let tox_live = build_toxicity_live_report(
                tox_state.clone(),
                stats.clone(),
                execution.clone(),
                toxicity_cfg.clone(),
            )
            .await;
            persist_toxicity_report_files(&tox_live);

            if last_final.elapsed() >= Duration::from_secs(300) {
                let final_report = stats.build_final_report().await;
                persist_final_report_files(&final_report);
                last_final = Instant::now();
            }

            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });
}

pub(super) fn spawn_data_reconcile_task(
    bus: RingBus<EngineEvent>,
    paused: Arc<RwLock<bool>>,
    stats: Arc<ShadowStats>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(600));
        loop {
            interval.tick().await;
            let live = stats.build_live_report().await;
            let ref_lines = count_jsonl_lines(&dataset_path("raw", "ref_ticks.jsonl"));
            let book_lines = count_jsonl_lines(&dataset_path("raw", "book_tops.jsonl"));
            let ref_expected = live.ref_ticks_total as i64;
            let book_expected = live.book_ticks_total as i64;
            let ref_gap_ratio = if ref_expected <= 0 {
                0.0
            } else {
                ((ref_lines - ref_expected).abs() as f64) / (ref_expected as f64)
            };
            let book_gap_ratio = if book_expected <= 0 {
                0.0
            } else {
                ((book_lines - book_expected).abs() as f64) / (book_expected as f64)
            };
            let reconcile_fail = ref_gap_ratio > 0.05
                || book_gap_ratio > 0.05
                || live.data_valid_ratio < 0.999
                || live.seq_gap_rate > 0.001
                || live.ts_inversion_rate > 0.0005;

            if reconcile_fail {
                stats.set_observe_only(true);
                *paused.write().await = true;
                stats.set_paused(true);
                let _ = bus.publish(EngineEvent::Control(ControlCommand::Pause));
            }

            append_jsonl(
                &dataset_path("reports", "data_reconcile.jsonl"),
                &serde_json::json!({
                    "ts_ms": Utc::now().timestamp_millis(),
                    "window_id": live.window_id,
                    "data_valid_ratio": live.data_valid_ratio,
                    "seq_gap_rate": live.seq_gap_rate,
                    "ts_inversion_rate": live.ts_inversion_rate,
                    "stale_tick_drop_ratio": live.stale_tick_drop_ratio,
                    "ref_lines": ref_lines,
                    "book_lines": book_lines,
                    "ref_expected": ref_expected,
                    "book_expected": book_expected,
                    "ref_gap_ratio": ref_gap_ratio,
                    "book_gap_ratio": book_gap_ratio,
                    "reconcile_fail": reconcile_fail,
                    "observe_only": stats.observe_only()
                }),
            );
        }
    });
}
