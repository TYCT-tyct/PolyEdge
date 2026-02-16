use super::*;

pub(super) fn spawn_periodic_report_persistor(
    stats: Arc<ShadowStats>,
    tox_state: Arc<RwLock<HashMap<String, MarketToxicState>>>,
    execution: Arc<ClobExecution>,
    toxicity_cfg: Arc<RwLock<Arc<ToxicityConfig>>>,
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
        // The raw JSONL files are bucketed by date, while `ShadowStats` counters are reset on
        // `/control/reset_shadow` (window reset). Comparing absolute totals would therefore
        // generate false "gap" alarms after any reset. Track deltas between intervals and
        // automatically re-baseline on resets/rotations.
        let mut last_window_id: u64 = 0;
        let mut last_ref_lines: i64 = 0;
        let mut last_book_lines: i64 = 0;
        let mut last_ref_expected: i64 = 0;
        let mut last_book_expected: i64 = 0;
        loop {
            interval.tick().await;
            let live = stats.build_live_report().await;
            let ref_lines = count_jsonl_lines(&dataset_path("raw", "ref_ticks.jsonl"));
            let book_lines = count_jsonl_lines(&dataset_path("raw", "book_tops.jsonl"));
            let ref_expected = live.ref_ticks_total as i64;
            let book_expected = live.book_ticks_total as i64;
            let baseline_reset = last_window_id == 0
                || live.window_id != last_window_id
                || ref_lines < last_ref_lines
                || book_lines < last_book_lines
                || ref_expected < last_ref_expected
                || book_expected < last_book_expected;

            let (ref_gap_ratio, book_gap_ratio) = if baseline_reset {
                (0.0, 0.0)
            } else {
                let ref_lines_delta = ref_lines - last_ref_lines;
                let book_lines_delta = book_lines - last_book_lines;
                let ref_expected_delta = ref_expected - last_ref_expected;
                let book_expected_delta = book_expected - last_book_expected;
                let ref_gap = if ref_expected_delta <= 0 {
                    0.0
                } else {
                    ((ref_lines_delta - ref_expected_delta).abs() as f64)
                        / (ref_expected_delta as f64)
                };
                let book_gap = if book_expected_delta <= 0 {
                    0.0
                } else {
                    ((book_lines_delta - book_expected_delta).abs() as f64)
                        / (book_expected_delta as f64)
                };
                (ref_gap, book_gap)
            };

            let reconcile_fail = !baseline_reset
                && (ref_gap_ratio > 0.05
                    || book_gap_ratio > 0.05
                    || live.data_valid_ratio < 0.999
                    || live.seq_gap_rate > 0.001
                    || live.ts_inversion_rate > 0.0005);

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
                    "baseline_reset": baseline_reset,
                    "observe_only": stats.observe_only()
                }),
            );

            last_window_id = live.window_id;
            last_ref_lines = ref_lines;
            last_book_lines = book_lines;
            last_ref_expected = ref_expected;
            last_book_expected = book_expected;
        }
    });
}
