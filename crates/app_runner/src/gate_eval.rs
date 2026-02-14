use super::*;

pub(super) fn compute_gate_fail_reasons(
    live: &ShadowLiveReport,
    min_outcomes: usize,
) -> Vec<String> {
    let mut failed = Vec::new();
    if live.window_outcomes < min_outcomes {
        failed.push(format!(
            "insufficient_window_outcomes {} < {}",
            live.window_outcomes, min_outcomes
        ));
    }
    if live.fillability_10ms < 0.60 {
        failed.push(format!(
            "fillability_10ms {:.4} < 0.6000",
            live.fillability_10ms
        ));
    }
    if live.net_edge_p50_bps <= 0.0 {
        failed.push(format!("net_edge_p50_bps {:.4} <= 0", live.net_edge_p50_bps));
    }
    if live.net_edge_p10_bps < -1.0 {
        failed.push(format!("net_edge_p10_bps {:.4} < -1", live.net_edge_p10_bps));
    }
    if live.quote_block_ratio >= 0.10 {
        failed.push(format!("quote_block_ratio {:.4} >= 0.10", live.quote_block_ratio));
    }
    if live.policy_block_ratio >= 0.10 {
        failed.push(format!(
            "policy_block_ratio {:.4} >= 0.10",
            live.policy_block_ratio
        ));
    }
    if live.executed_over_eligible < 0.60 {
        failed.push(format!(
            "executed_over_eligible {:.4} < 0.60",
            live.executed_over_eligible
        ));
    }
    if live.data_valid_ratio < 0.999 {
        failed.push(format!("data_valid_ratio {:.5} < 0.99900", live.data_valid_ratio));
    }
    if live.seq_gap_rate > 0.001 {
        failed.push(format!("seq_gap_rate {:.5} > 0.00100", live.seq_gap_rate));
    }
    if live.ts_inversion_rate > 0.0005 {
        failed.push(format!(
            "ts_inversion_rate {:.5} > 0.00050",
            live.ts_inversion_rate
        ));
    }
    if live.tick_to_ack_p99_ms >= 450.0 {
        failed.push(format!(
            "tick_to_ack_p99_ms {:.3} >= 450",
            live.tick_to_ack_p99_ms
        ));
    }
    if live.decision_compute_p99_ms >= 2.0 {
        failed.push(format!(
            "decision_compute_p99_ms {:.3} >= 2",
            live.decision_compute_p99_ms
        ));
    }
    if live.latency.feed_in_p99_ms >= 800.0 {
        failed.push(format!(
            "feed_in_p99_ms {:.3} >= 800",
            live.latency.feed_in_p99_ms
        ));
    }
    if live.book_freshness_ms >= 1_500 {
        failed.push(format!("book_freshness_ms {} >= 1500", live.book_freshness_ms));
    }
    if live.ref_freshness_ms >= 1_000 {
        failed.push(format!("ref_freshness_ms {} >= 1000", live.ref_freshness_ms));
    }
    if live.net_markout_10s_usdc_p50 <= 0.0 {
        failed.push(format!(
            "net_markout_10s_usdc_p50 {:.6} <= 0",
            live.net_markout_10s_usdc_p50
        ));
    }
    if live.roi_notional_10s_bps_p50 <= 0.0 {
        failed.push(format!(
            "roi_notional_10s_bps_p50 {:.6} <= 0",
            live.roi_notional_10s_bps_p50
        ));
    }
    failed
}
