#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import itertools
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

import requests


def utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def default_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"reg-{ts}-{os.getpid()}"


def parse_float_grid(raw: str) -> List[float]:
    out: List[float] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        out.append(float(token))
    return out


def parse_int_grid(raw: str) -> List[int]:
    out: List[int] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        out.append(int(token))
    return out

PROFILE_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "quick": {
        # 5-hour sprint default: fewer trials, longer per-trial window, bounded runtime.
        "window_sec": 300,
        "poll_interval_sec": 3.0,
        "eval_window_sec": 300,
        "max_trials": 6,
        "max_runtime_sec": 3600,
        "heartbeat_sec": 15.0,
        # Don't stop after the first failure; we want at least a few parameter probes in quick mode.
        "fail_fast_threshold": 3,
        "min_outcomes": 30,
        "min_edge_grid": "0.5,1.0,2.0",
        "ttl_grid": "250,400,700",
        "max_spread_grid": "0.03,0.05,0.08",
        "base_quote_size_grid": "2,5",
        "min_eval_notional_usdc_grid": "0.01,0.05",
        "min_expected_edge_usdc_grid": "0.0002",
        "taker_trigger_grid": "3,4,6",
        "taker_max_slippage_grid": "20,30",
        "stale_tick_filter_ms": 2000,
        "market_tier_profile": "balanced_sol_guard",
        "active_top_n_markets": 12,
        "basis_k_grid": "0.8",
        "basis_z_grid": "3.0",
        "safe_threshold_grid": "0.35",
        "caution_threshold_grid": "0.65",
        "warmup_sec": 5,
        "max_estimated_sec": 900,
        "walkforward_windows": 2,
        "walkforward_cooldown_sec": 2.0,
        "selection_mode": "robust",
        "top_k_consensus": 3,
    },
    "standard": {
        "window_sec": 300,
        "poll_interval_sec": 10.0,
        "eval_window_sec": 300,
        "max_trials": 12,
        "max_runtime_sec": 0,
        "heartbeat_sec": 30.0,
        "fail_fast_threshold": 0,
        "min_outcomes": 30,
        "min_edge_grid": "0.5,1.0,2.0,3.0",
        "ttl_grid": "250,400,700",
        "max_spread_grid": "0.03,0.05,0.08",
        "base_quote_size_grid": "2,5",
        "min_eval_notional_usdc_grid": "0.01,0.05",
        "min_expected_edge_usdc_grid": "0.0002",
        "taker_trigger_grid": "3,4,6",
        "taker_max_slippage_grid": "20,30",
        "stale_tick_filter_ms": 2000,
        "market_tier_profile": "balanced_sol_guard",
        "active_top_n_markets": 12,
        "basis_k_grid": "0.70,0.85,1.00",
        "basis_z_grid": "2.0,3.0",
        "safe_threshold_grid": "0.35",
        "caution_threshold_grid": "0.65",
        "warmup_sec": 15,
        "max_estimated_sec": 0,
        "walkforward_windows": 2,
        "walkforward_cooldown_sec": 3.0,
        "selection_mode": "robust",
        "top_k_consensus": 3,
    },
    "deep": {
        "window_sec": 600,
        "poll_interval_sec": 10.0,
        "eval_window_sec": 600,
        "max_trials": 24,
        "max_runtime_sec": 0,
        "heartbeat_sec": 30.0,
        "fail_fast_threshold": 0,
        "min_outcomes": 50,
        "min_edge_grid": "0.5,1.0,2.0,3.0,4.0",
        "ttl_grid": "250,350,500,700",
        "max_spread_grid": "0.03,0.05,0.08",
        "base_quote_size_grid": "2,5,8",
        "min_eval_notional_usdc_grid": "0.01,0.05,0.10",
        "min_expected_edge_usdc_grid": "0.0002,0.001",
        "taker_trigger_grid": "3,4,6,8",
        "taker_max_slippage_grid": "15,20,30",
        "stale_tick_filter_ms": 2000,
        "market_tier_profile": "balanced_sol_guard",
        "active_top_n_markets": 12,
        "basis_k_grid": "0.70,0.80,0.90,1.00",
        "basis_z_grid": "2.0,2.5,3.0",
        "safe_threshold_grid": "0.30,0.35",
        "caution_threshold_grid": "0.60,0.65",
        "warmup_sec": 20,
        "max_estimated_sec": 0,
        "walkforward_windows": 3,
        "walkforward_cooldown_sec": 5.0,
        "selection_mode": "robust",
        "top_k_consensus": 5,
    },
}


def apply_profile_defaults(args: argparse.Namespace) -> argparse.Namespace:
    profile = PROFILE_DEFAULTS[args.profile]
    for key, default_value in profile.items():
        if getattr(args, key) is None:
            setattr(args, key, default_value)
    return args


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    idx = int(round((len(sorted_values) - 1) * p))
    idx = max(0, min(len(sorted_values) - 1, idx))
    return sorted_values[idx]


@dataclass
class TrialResult:
    run_id: str
    trial_index: int
    min_edge_bps: float
    ttl_ms: int
    max_spread: float
    base_quote_size: float
    min_eval_notional_usdc: float
    min_expected_edge_usdc: float
    taker_trigger_bps: float
    taker_max_slippage_bps: float
    stale_tick_filter_ms: float
    market_tier_profile: str
    active_top_n_markets: int
    basis_k_revert: float
    basis_z_cap: float
    safe_threshold: float
    caution_threshold: float
    window_id: int
    gate_ready: bool
    samples: int
    fillability_10ms: float
    pnl_10s_p50_bps_raw: float
    pnl_10s_p50_bps_robust: float
    pnl_10s_p25_bps_robust: float
    ev_net_usdc_p50: float
    ev_net_usdc_p10: float
    ev_positive_ratio: float
    net_markout_10s_usdc_p50: float
    roi_notional_10s_bps_p50: float
    eligible_count: int
    executed_count: int
    executed_over_eligible: float
    quote_block_ratio: float
    policy_block_ratio: float
    tick_to_ack_p99_ms: float
    decision_compute_p99_ms: float
    feed_in_p99_ms: float
    data_valid_ratio: float
    net_edge_p50_bps: float
    gate_fail_reasons: List[str]
    blocked_reason_top: str
    walkforward_windows: int = 1
    stability_penalty: float = 0.0
    consistency_score: float = 1.0

    def gate_pass(self) -> bool:
        return (
            self.gate_ready
            and self.ev_net_usdc_p50 > 0.0
            and self.ev_positive_ratio >= 0.55
            and self.net_markout_10s_usdc_p50 > 0.0
            and self.roi_notional_10s_bps_p50 > 0.0
            and self.pnl_10s_p25_bps_robust > -20.0
            and self.fillability_10ms >= 0.60
            and self.executed_over_eligible >= 0.60
            and self.quote_block_ratio < 0.10
            and self.policy_block_ratio < 0.10
            and self.tick_to_ack_p99_ms < 450.0
            and self.decision_compute_p99_ms < 2.0
            and self.feed_in_p99_ms < 800.0
            and self.data_valid_ratio >= 0.999
        )


def score_trial(row: TrialResult) -> float:
    """Single-score ranking used for best trial selection in reports."""
    objective = objective_ev_net_penalty(row)
    score = 0.0
    if row.gate_ready:
        score += 200.0
    if row.gate_pass():
        score += 1000.0
    score += row.ev_net_usdc_p50 * 40.0
    score += row.ev_positive_ratio * 120.0
    score += row.net_markout_10s_usdc_p50 * 50.0
    score += row.roi_notional_10s_bps_p50 * 0.3
    score += row.pnl_10s_p50_bps_robust * 0.2
    score += row.fillability_10ms * 100.0
    score += row.net_edge_p50_bps * 0.1
    score += row.consistency_score * 80.0
    score -= row.quote_block_ratio * 300.0
    score -= row.policy_block_ratio * 120.0
    score -= row.tick_to_ack_p99_ms * 0.2
    score -= row.feed_in_p99_ms * 0.05
    score -= row.decision_compute_p99_ms * 20.0
    score -= row.stability_penalty
    score -= max(0.0, 0.999 - row.data_valid_ratio) * 10_000.0
    score -= max(0.0, 0.60 - row.executed_over_eligible) * 500.0
    score += objective * 120.0
    return score


def objective_penalty(row: TrialResult) -> float:
    penalty = 0.0
    penalty += max(0.0, row.tick_to_ack_p99_ms - 450.0) * 0.00002
    penalty += max(0.0, row.decision_compute_p99_ms - 2.0) * 0.05
    penalty += max(0.0, row.feed_in_p99_ms - 800.0) * 0.000005
    penalty += max(0.0, row.quote_block_ratio - 0.10) * 0.20
    penalty += max(0.0, row.policy_block_ratio - 0.10) * 0.20
    penalty += max(0.0, 0.60 - row.executed_over_eligible) * 0.50
    penalty += max(0.0, 0.999 - row.data_valid_ratio) * 80.0
    penalty += row.stability_penalty * 0.01
    if not row.gate_pass():
        penalty += 0.20
    return penalty


def objective_ev_net_penalty(row: TrialResult) -> float:
    return row.ev_net_usdc_p50 - objective_penalty(row)


def aggregate_walkforward_results(
    run_id: str,
    trial_index: int,
    windows: List[TrialResult],
    selection_mode: str,
) -> TrialResult:
    if not windows:
        raise ValueError("walkforward windows must not be empty")

    mode = selection_mode.strip().lower()
    if mode not in {"score", "robust"}:
        mode = "robust"

    def as_list(field: str) -> List[float]:
        return [float(getattr(w, field)) for w in windows]

    def aggregate_good(field: str) -> float:
        values = as_list(field)
        q = 0.50 if mode == "score" else 0.25
        return percentile(values, q)

    def aggregate_bad(field: str) -> float:
        values = as_list(field)
        q = 0.50 if mode == "score" else 0.75
        return percentile(values, q)

    def span(values: List[float]) -> float:
        return max(values) - min(values) if values else 0.0

    basis = windows[0]
    gate_ready_vals = [1.0 if w.gate_ready else 0.0 for w in windows]
    ev_vals = as_list("ev_net_usdc_p50")
    markout_vals = as_list("net_markout_10s_usdc_p50")
    ack_vals = as_list("tick_to_ack_p99_ms")
    block_vals = as_list("quote_block_ratio")
    exec_vals = as_list("executed_over_eligible")
    data_valid_vals = as_list("data_valid_ratio")
    unique_reasons = sorted({r for w in windows for r in w.gate_fail_reasons})
    blocked_by_row = sorted(
        windows,
        key=lambda w: (
            w.quote_block_ratio + w.policy_block_ratio,
            w.tick_to_ack_p99_ms,
            -w.ev_net_usdc_p50,
        ),
        reverse=True,
    )

    stability_penalty = (
        span(ev_vals) * 60.0
        + span(markout_vals) * 80.0
        + span(ack_vals) * 0.15
        + span(block_vals) * 320.0
        + max(0.0, 0.999 - min(data_valid_vals)) * 30_000.0
        + max(0.0, 0.60 - min(exec_vals)) * 800.0
    )
    consistency_score = 1.0 / (1.0 + (stability_penalty / 200.0))

    if mode == "robust":
        gate_ready = all(w.gate_ready for w in windows)
    else:
        gate_ready = bool(percentile(gate_ready_vals, 0.50) >= 1.0)

    return TrialResult(
        run_id=run_id,
        trial_index=trial_index,
        min_edge_bps=basis.min_edge_bps,
        ttl_ms=basis.ttl_ms,
        max_spread=basis.max_spread,
        base_quote_size=basis.base_quote_size,
        min_eval_notional_usdc=basis.min_eval_notional_usdc,
        min_expected_edge_usdc=basis.min_expected_edge_usdc,
        taker_trigger_bps=basis.taker_trigger_bps,
        taker_max_slippage_bps=basis.taker_max_slippage_bps,
        stale_tick_filter_ms=basis.stale_tick_filter_ms,
        market_tier_profile=basis.market_tier_profile,
        active_top_n_markets=basis.active_top_n_markets,
        basis_k_revert=basis.basis_k_revert,
        basis_z_cap=basis.basis_z_cap,
        safe_threshold=basis.safe_threshold,
        caution_threshold=basis.caution_threshold,
        window_id=windows[-1].window_id,
        gate_ready=gate_ready,
        samples=sum(w.samples for w in windows),
        fillability_10ms=aggregate_good("fillability_10ms"),
        pnl_10s_p50_bps_raw=aggregate_good("pnl_10s_p50_bps_raw"),
        pnl_10s_p50_bps_robust=aggregate_good("pnl_10s_p50_bps_robust"),
        pnl_10s_p25_bps_robust=min(as_list("pnl_10s_p25_bps_robust")),
        ev_net_usdc_p50=aggregate_good("ev_net_usdc_p50"),
        ev_net_usdc_p10=min(as_list("ev_net_usdc_p10")),
        ev_positive_ratio=aggregate_good("ev_positive_ratio"),
        net_markout_10s_usdc_p50=aggregate_good("net_markout_10s_usdc_p50"),
        roi_notional_10s_bps_p50=aggregate_good("roi_notional_10s_bps_p50"),
        eligible_count=int(aggregate_good("eligible_count")),
        executed_count=int(aggregate_good("executed_count")),
        executed_over_eligible=aggregate_good("executed_over_eligible"),
        quote_block_ratio=aggregate_bad("quote_block_ratio"),
        policy_block_ratio=aggregate_bad("policy_block_ratio"),
        tick_to_ack_p99_ms=aggregate_bad("tick_to_ack_p99_ms"),
        decision_compute_p99_ms=aggregate_bad("decision_compute_p99_ms"),
        feed_in_p99_ms=aggregate_bad("feed_in_p99_ms"),
        data_valid_ratio=aggregate_good("data_valid_ratio"),
        net_edge_p50_bps=aggregate_good("net_edge_p50_bps"),
        gate_fail_reasons=unique_reasons,
        blocked_reason_top=blocked_by_row[0].blocked_reason_top if blocked_by_row else "",
        walkforward_windows=len(windows),
        stability_penalty=stability_penalty,
        consistency_score=consistency_score,
    )


def build_consensus_params(rows: List[TrialResult], top_k: int) -> Dict[str, Any]:
    if not rows:
        return {}

    preferred = [row for row in rows if row.gate_pass()]
    if not preferred:
        preferred = [row for row in rows if row.gate_ready]
    if not preferred:
        preferred = rows
    chosen = preferred[: max(1, top_k)]

    def median(values: List[float]) -> float:
        return percentile(values, 0.50)

    def medf(field: str) -> float:
        return median([float(getattr(row, field)) for row in chosen])

    def medi(field: str) -> int:
        return int(round(medf(field)))

    first = chosen[0]
    return {
        "source_trial_indices": [row.trial_index for row in chosen],
        "selection_count": len(chosen),
        "reload_strategy": {
            "min_edge_bps": medf("min_edge_bps"),
            "ttl_ms": medi("ttl_ms"),
            "max_spread": medf("max_spread"),
            "base_quote_size": medf("base_quote_size"),
            "min_eval_notional_usdc": medf("min_eval_notional_usdc"),
            "min_expected_edge_usdc": medf("min_expected_edge_usdc"),
            "basis_k_revert": medf("basis_k_revert"),
            "basis_z_cap": medf("basis_z_cap"),
        },
        "reload_taker": {
            "trigger_bps": medf("taker_trigger_bps"),
            "max_slippage_bps": medf("taker_max_slippage_bps"),
            "stale_tick_filter_ms": medf("stale_tick_filter_ms"),
            "market_tier_profile": first.market_tier_profile,
        },
        "reload_allocator": {
            "capital_fraction_kelly": 0.35,
            "variance_penalty_lambda": 0.25,
            "active_top_n_markets": medi("active_top_n_markets"),
            "taker_weight": 0.7,
            "maker_weight": 0.2,
            "arb_weight": 0.1,
        },
        "reload_toxicity": {
            "safe_threshold": medf("safe_threshold"),
            "caution_threshold": medf("caution_threshold"),
        },
    }


def write_auto_tuned_thresholds(
    path: Path,
    rows: List[TrialResult],
    top_k: int,
) -> Dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "optimizer": "top-k-consensus",
        "top_k": max(1, top_k),
        "consensus": build_consensus_params(rows, top_k),
    }
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return payload


def fetch_json(
    session: requests.Session,
    url: str,
    timeout: float = 15.0,
    retries: int = 3,
    backoff_sec: float = 0.5,
) -> Dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(max(1, retries)):
        try:
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_exc = exc
            time.sleep(backoff_sec * float(attempt + 1))
    assert last_exc is not None
    raise last_exc


def post_json(
    session: requests.Session,
    url: str,
    payload: Dict[str, Any],
    timeout: float = 10.0,
    retries: int = 3,
    backoff_sec: float = 0.5,
) -> Dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(max(1, retries)):
        try:
            resp = session.post(url, json=payload, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_exc = exc
            time.sleep(backoff_sec * float(attempt + 1))
    assert last_exc is not None
    raise last_exc


def post_json_optional(
    session: requests.Session, url: str, payload: Dict[str, Any], timeout: float = 5.0
) -> Dict[str, Any]:
    try:
        return post_json(session, url, payload, timeout=timeout)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status in (404, 405):
            return {}
        raise


def run_trial(
    session: requests.Session,
    base_url: str,
    run_id: str,
    trial_index: int,
    min_outcomes: int,
    eval_window_sec: int,
    heartbeat_sec: float,
    window_sec: int,
    poll_interval_sec: float,
    min_edge_bps: float,
    ttl_ms: int,
    max_spread: float,
    base_quote_size: float,
    min_eval_notional_usdc: float,
    min_expected_edge_usdc: float,
    taker_trigger_bps: float,
    taker_max_slippage_bps: float,
    stale_tick_filter_ms: float,
    market_tier_profile: str,
    active_top_n_markets: int,
    basis_k_revert: float,
    basis_z_cap: float,
    safe_threshold: float,
    caution_threshold: float,
    warmup_sec: int,
) -> TrialResult:
    base = base_url.rstrip("/")
    # Ensure the runtime isn't stuck paused from a previous test cycle.
    post_json_optional(session, f"{base}/control/resume", {})
    reset_resp = post_json(session, f"{base}/control/reset_shadow", {})
    reset_window_id = int(reset_resp.get("window_id", 0) or 0)
    post_json(
        session,
        f"{base}/control/reload_strategy",
        {
            "min_edge_bps": min_edge_bps,
            "ttl_ms": ttl_ms,
            "max_spread": max_spread,
            "base_quote_size": base_quote_size,
            "min_eval_notional_usdc": min_eval_notional_usdc,
            "min_expected_edge_usdc": min_expected_edge_usdc,
            "basis_k_revert": basis_k_revert,
            "basis_z_cap": basis_z_cap,
        },
    )
    post_json_optional(
        session,
        f"{base}/control/reload_taker",
        {
            "trigger_bps": taker_trigger_bps,
            "max_slippage_bps": taker_max_slippage_bps,
            "stale_tick_filter_ms": stale_tick_filter_ms,
            "market_tier_profile": market_tier_profile,
        },
    )
    post_json_optional(
        session,
        f"{base}/control/reload_allocator",
        {
            "capital_fraction_kelly": 0.35,
            "variance_penalty_lambda": 0.25,
            "active_top_n_markets": active_top_n_markets,
            "taker_weight": 0.7,
            "maker_weight": 0.2,
            "arb_weight": 0.1,
        },
    )
    post_json(
        session,
        f"{base}/control/reload_toxicity",
        {
            "safe_threshold": safe_threshold,
            "caution_threshold": caution_threshold,
        },
    )
    if warmup_sec > 0:
        time.sleep(warmup_sec)

    fillability: List[float] = []
    pnl10: List[float] = []
    pnl10_raw: List[float] = []
    ev_net_usdc: List[float] = []
    ev_positive_ratio: List[float] = []
    net_markout_usdc: List[float] = []
    roi_notional_bps: List[float] = []
    eligible_count_vals: List[float] = []
    executed_count_vals: List[float] = []
    executed_over_eligible_vals: List[float] = []
    block_ratio: List[float] = []
    policy_block_ratio: List[float] = []
    tick_ack: List[float] = []
    decision_compute_p99: List[float] = []
    feed_in_p99: List[float] = []
    data_valid_ratio: List[float] = []
    net_edge: List[float] = []
    gate_ready_vals: List[float] = []
    window_outcomes_vals: List[float] = []
    gate_fail_reasons: List[str] = []
    observed_window_id = reset_window_id
    next_heartbeat = time.time() + max(1.0, heartbeat_sec)
    last_live: Dict[str, Any] | None = None

    deadline = time.time() + min(window_sec, eval_window_sec)
    while time.time() < deadline:
        live = fetch_json(session, f"{base}/report/shadow/live")
        last_live = live
        observed_window_id = int(live.get("window_id", observed_window_id) or observed_window_id)
        fillability.append(float(live.get("fillability_10ms", 0.0)))
        pnl10_raw.append(float(live.get("pnl_10s_p50_bps_raw", live.get("pnl_10s_p50_bps", 0.0))))
        pnl10.append(
            float(live.get("pnl_10s_p50_bps_robust", live.get("pnl_10s_p50_bps", 0.0)))
        )
        ev_net_usdc.append(float(live.get("ev_net_usdc_p50", live.get("net_markout_10s_usdc_p50", 0.0))))
        ev_positive_ratio.append(float(live.get("ev_positive_ratio", 0.0)))
        net_markout_usdc.append(float(live.get("net_markout_10s_usdc_p50", 0.0)))
        roi_notional_bps.append(float(live.get("roi_notional_10s_bps_p50", 0.0)))
        eligible_count_vals.append(float(live.get("eligible_count", 0.0)))
        executed_count_vals.append(float(live.get("executed_count", 0.0)))
        executed_over_eligible_vals.append(float(live.get("executed_over_eligible", 0.0)))
        block_ratio.append(float(live.get("quote_block_ratio", 0.0)))
        policy_block_ratio.append(float(live.get("policy_block_ratio", 0.0)))
        tick_ack.append(float(live.get("tick_to_ack_p99_ms", 0.0)))
        decision_compute_p99.append(float(live.get("decision_compute_p99_ms", 0.0)))
        feed_in_p99.append(float((live.get("latency") or {}).get("feed_in_p99_ms", 0.0)))
        data_valid_ratio.append(float(live.get("data_valid_ratio", 1.0)))
        net_edge.append(float(live.get("net_edge_p50_bps", 0.0)))
        gate_ready_vals.append(1.0 if bool(live.get("gate_ready", False)) else 0.0)
        window_outcomes_vals.append(float(live.get("window_outcomes", live.get("total_outcomes", 0.0))))
        reasons = live.get("gate_fail_reasons") or []
        if isinstance(reasons, list):
            gate_fail_reasons = [str(v) for v in reasons]
        if time.time() >= next_heartbeat:
            print(
                f"[heartbeat] trial={trial_index} window_id={observed_window_id} "
                f"outcomes={int(window_outcomes_vals[-1])} gate_ready={bool(gate_ready_vals[-1])}"
            )
            next_heartbeat = time.time() + max(1.0, heartbeat_sec)
        time.sleep(max(1.0, poll_interval_sec))

    blocked_top = ""
    if isinstance(last_live, dict):
        br = last_live.get("blocked_reason_counts")
        if isinstance(br, dict) and br:
            top_items = sorted(br.items(), key=lambda kv: float(kv[1]), reverse=True)[:8]
            blocked_top = ";".join(f"{k}:{int(v)}" for k, v in top_items)

    return TrialResult(
        run_id=run_id,
        trial_index=trial_index,
        min_edge_bps=min_edge_bps,
        ttl_ms=ttl_ms,
        max_spread=max_spread,
        base_quote_size=base_quote_size,
        min_eval_notional_usdc=min_eval_notional_usdc,
        min_expected_edge_usdc=min_expected_edge_usdc,
        taker_trigger_bps=taker_trigger_bps,
        taker_max_slippage_bps=taker_max_slippage_bps,
        stale_tick_filter_ms=stale_tick_filter_ms,
        market_tier_profile=market_tier_profile,
        active_top_n_markets=active_top_n_markets,
        basis_k_revert=basis_k_revert,
        basis_z_cap=basis_z_cap,
        safe_threshold=safe_threshold,
        caution_threshold=caution_threshold,
        window_id=observed_window_id,
        gate_ready=(
            bool(percentile(gate_ready_vals, 0.50) >= 1.0)
            and int(percentile(window_outcomes_vals, 0.90)) >= min_outcomes
        ),
        samples=len(fillability),
        fillability_10ms=percentile(fillability, 0.50),
        pnl_10s_p50_bps_raw=percentile(pnl10_raw, 0.50),
        pnl_10s_p50_bps_robust=percentile(pnl10, 0.50),
        pnl_10s_p25_bps_robust=percentile(pnl10, 0.25),
        ev_net_usdc_p50=percentile(ev_net_usdc, 0.50),
        ev_net_usdc_p10=percentile(ev_net_usdc, 0.10),
        ev_positive_ratio=percentile(ev_positive_ratio, 0.50),
        net_markout_10s_usdc_p50=percentile(net_markout_usdc, 0.50),
        roi_notional_10s_bps_p50=percentile(roi_notional_bps, 0.50),
        eligible_count=int(percentile(eligible_count_vals, 0.50)),
        executed_count=int(percentile(executed_count_vals, 0.50)),
        executed_over_eligible=percentile(executed_over_eligible_vals, 0.50),
        quote_block_ratio=percentile(block_ratio, 0.50),
        policy_block_ratio=percentile(policy_block_ratio, 0.50),
        tick_to_ack_p99_ms=percentile(tick_ack, 0.50),
        decision_compute_p99_ms=percentile(decision_compute_p99, 0.50),
        feed_in_p99_ms=percentile(feed_in_p99, 0.50),
        data_valid_ratio=percentile(data_valid_ratio, 0.50),
        net_edge_p50_bps=percentile(net_edge, 0.50),
        gate_fail_reasons=gate_fail_reasons,
        blocked_reason_top=blocked_top,
    )


def write_ablation_csv(path: Path, rows: Iterable[TrialResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "min_edge_bps",
                "ttl_ms",
                "max_spread",
                "base_quote_size",
                "min_eval_notional_usdc",
                "min_expected_edge_usdc",
                "taker_trigger_bps",
                "taker_max_slippage_bps",
                "stale_tick_filter_ms",
                "market_tier_profile",
                "active_top_n_markets",
                "basis_k_revert",
                "basis_z_cap",
                "safe_threshold",
                "caution_threshold",
                "run_id",
                "trial_index",
                "window_id",
                "gate_ready",
                "samples",
                "fillability_10ms",
                "pnl_10s_p50_bps_raw",
                "pnl_10s_p50_bps_robust",
                "pnl_10s_p25_bps_robust",
                "ev_net_usdc_p50",
                "ev_net_usdc_p10",
                "ev_positive_ratio",
                "net_markout_10s_usdc_p50",
                "roi_notional_10s_bps_p50",
                "eligible_count",
                "executed_count",
                "executed_over_eligible",
                "quote_block_ratio",
                "policy_block_ratio",
                "tick_to_ack_p99_ms",
                "decision_compute_p99_ms",
                "feed_in_p99_ms",
                "data_valid_ratio",
                "net_edge_p50_bps",
                "walkforward_windows",
                "consistency_score",
                "stability_penalty",
                "objective_penalty",
                "objective_ev_net_penalty",
                "score",
                "gate_pass",
                "blocked_reason_top",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.min_edge_bps,
                    row.ttl_ms,
                    row.max_spread,
                    row.base_quote_size,
                    row.min_eval_notional_usdc,
                    row.min_expected_edge_usdc,
                    row.taker_trigger_bps,
                    row.taker_max_slippage_bps,
                    row.stale_tick_filter_ms,
                    row.market_tier_profile,
                    row.active_top_n_markets,
                    row.basis_k_revert,
                    row.basis_z_cap,
                    row.safe_threshold,
                    row.caution_threshold,
                    row.run_id,
                    row.trial_index,
                    row.window_id,
                    row.gate_ready,
                    row.samples,
                    f"{row.fillability_10ms:.6f}",
                    f"{row.pnl_10s_p50_bps_raw:.6f}",
                    f"{row.pnl_10s_p50_bps_robust:.6f}",
                    f"{row.pnl_10s_p25_bps_robust:.6f}",
                    f"{row.ev_net_usdc_p50:.6f}",
                    f"{row.ev_net_usdc_p10:.6f}",
                    f"{row.ev_positive_ratio:.6f}",
                    f"{row.net_markout_10s_usdc_p50:.6f}",
                    f"{row.roi_notional_10s_bps_p50:.6f}",
                    row.eligible_count,
                    row.executed_count,
                    f"{row.executed_over_eligible:.6f}",
                    f"{row.quote_block_ratio:.6f}",
                    f"{row.policy_block_ratio:.6f}",
                    f"{row.tick_to_ack_p99_ms:.6f}",
                    f"{row.decision_compute_p99_ms:.6f}",
                    f"{row.feed_in_p99_ms:.6f}",
                    f"{row.data_valid_ratio:.6f}",
                    f"{row.net_edge_p50_bps:.6f}",
                    row.walkforward_windows,
                    f"{row.consistency_score:.6f}",
                    f"{row.stability_penalty:.6f}",
                    f"{objective_penalty(row):.6f}",
                    f"{objective_ev_net_penalty(row):.6f}",
                    f"{score_trial(row):.6f}",
                    row.gate_pass(),
                    row.blocked_reason_top,
                ]
            )


def write_summary_md(path: Path, rows: List[TrialResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: List[str] = []
    lines.append("# Regression Summary")
    lines.append("")
    lines.append(f"- generated_at_utc: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"- trials: {len(rows)}")
    if rows:
        best = rows[0]
        lines.append(f"- best_score: {score_trial(best):.3f}")
        lines.append(f"- best_objective_ev_net_penalty: {objective_ev_net_penalty(best):.6f}")
        lines.append(f"- best_gate_pass: {best.gate_pass()}")
        lines.append(f"- best_gate_ready: {best.gate_ready}")
        lines.append(f"- best_walkforward_windows: {best.walkforward_windows}")
        lines.append(f"- best_consistency_score: {best.consistency_score:.3f}")
        lines.append(f"- best_stability_penalty: {best.stability_penalty:.3f}")
        lines.append(
            f"- best_params: edge={best.min_edge_bps}, ttl={best.ttl_ms}, base_quote={best.base_quote_size}, "
            f"min_eval_notional={best.min_eval_notional_usdc}, "
            f"taker_trigger={best.taker_trigger_bps}, taker_slip={best.taker_max_slippage_bps}, "
            f"stale_ms={best.stale_tick_filter_ms}, tier={best.market_tier_profile}, topn={best.active_top_n_markets}, "
            f"k={best.basis_k_revert}, z={best.basis_z_cap}, "
            f"safe={best.safe_threshold}, caution={best.caution_threshold}"
        )
    lines.append("")
    top = rows[:5]
    lines.append("## Top Trials")
    for i, row in enumerate(top, start=1):
        lines.append(
                f"- #{i} edge={row.min_edge_bps}, ttl={row.ttl_ms}, base_quote={row.base_quote_size}, "
                f"min_eval_notional={row.min_eval_notional_usdc}, "
                f"taker_trigger={row.taker_trigger_bps}, taker_slip={row.taker_max_slippage_bps}, "
                f"stale_ms={row.stale_tick_filter_ms}, tier={row.market_tier_profile}, topn={row.active_top_n_markets}, "
                f"k={row.basis_k_revert}, z={row.basis_z_cap}, "
                f"pnl10_raw_p50={row.pnl_10s_p50_bps_raw:.3f}, "
                f"pnl10_robust_p50={row.pnl_10s_p50_bps_robust:.3f}, "
                f"ev_usdc_p50={row.ev_net_usdc_p50:.4f}, "
                f"ev_pos_ratio={row.ev_positive_ratio:.3f}, "
                f"net_usdc_p50={row.net_markout_10s_usdc_p50:.4f}, "
                f"fill10={row.fillability_10ms:.3f}, block={row.quote_block_ratio:.3f}, "
                f"exec_over_eligible={row.executed_over_eligible:.3f}, "
                f"ready={row.gate_ready}, "
                f"tick_to_ack_p99={row.tick_to_ack_p99_ms:.3f}, "
                f"wf={row.walkforward_windows}, "
                f"consistency={row.consistency_score:.3f}, "
                f"objective={objective_ev_net_penalty(row):.6f}, "
                f"score={score_trial(row):.2f}, gate={row.gate_pass()}"
            )
        if row.blocked_reason_top:
            lines.append(f"  blocked_top: {row.blocked_reason_top}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_fixlist(path: Path, best: TrialResult | None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: List[str] = ["# Next Fixlist", ""]
    if best is None:
        lines.append("- No trial results. Check app health and API connectivity.")
    elif best.gate_pass():
        lines.append("- Conservative gate passed on best trial. Keep Shadow mode and extend soak run.")
        if best.policy_block_ratio >= 0.10:
            lines.append(
                "- policy_block_ratio is still high. Strategy is profitable but opportunity suppression is excessive."
            )
    else:
        if best.ev_net_usdc_p50 <= 0.0:
            lines.append("- ev_net_usdc_p50 <= 0. Current opportunity quality is not truly positive after friction.")
        if best.ev_positive_ratio < 0.55:
            lines.append("- ev_positive_ratio < 0.55. Too many candidate trades are negative EV.")
        if best.executed_over_eligible < 0.60:
            lines.append("- executed_over_eligible < 0.60. Execution funnel is leaking too much under pressure.")
        if best.net_markout_10s_usdc_p50 <= 0.0:
            lines.append("- net_markout_10s_usdc_p50 still <= 0. Keep Shadow mode and reduce toxic market exposure.")
        if best.roi_notional_10s_bps_p50 <= 0.0:
            lines.append("- roi_notional_10s_bps_p50 <= 0. Raise min_edge_bps and tighten stale filters.")
        if best.pnl_10s_p25_bps_robust <= -20.0:
            lines.append("- pnl_10s_p25_bps_robust <= -20. Tighten toxicity danger threshold and shorter TTL.")
        if best.fillability_10ms < 0.60:
            lines.append("- fillability_10ms below 0.60. Revisit spread/size and queue proxy assumptions.")
        if best.quote_block_ratio >= 0.10:
            lines.append("- quote_block_ratio too high. Relax min_edge or widen active market top-N.")
        if best.policy_block_ratio >= 0.10:
            lines.append("- policy_block_ratio too high. Re-tune no_quote_policy and market rank gating.")
        if best.data_valid_ratio < 0.999:
            lines.append("- data_valid_ratio below 99.9%. Fix feed validity/reconcile pipeline before parameter tuning.")
        if best.feed_in_p99_ms >= 800.0:
            lines.append("- feed_in_p99_ms too high. Apply stale tick filter and investigate source/local backlog split.")
        if best.decision_compute_p99_ms >= 2.0:
            lines.append("- decision_compute_p99_ms too high. Reduce lock contention/log overhead on hot path.")
        if not best.gate_ready:
            lines.append("- gate_ready is false. Increase eval window or reduce min_outcomes.")
        if best.tick_to_ack_p99_ms >= 450.0:
            lines.append("- tick_to_ack_p99_ms too high. Investigate WS lag spikes and execution path.")
        if best.gate_fail_reasons:
            lines.append("- latest gate_fail_reasons:")
            for reason in best.gate_fail_reasons[:8]:
                lines.append(f"  - {reason}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PolyEdge parameter regression")
    p.add_argument("--profile", choices=["quick", "standard", "deep"], default="quick")
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--out-root", default="datasets/reports")
    p.add_argument("--run-id", default=default_run_id())
    p.add_argument("--window-sec", type=int, default=None)
    p.add_argument("--poll-interval-sec", type=float, default=None)
    p.add_argument("--eval-window-sec", type=int, default=None)
    p.add_argument("--max-trials", type=int, default=None)
    p.add_argument("--max-runtime-sec", type=int, default=None)
    p.add_argument("--heartbeat-sec", type=float, default=None)
    p.add_argument("--fail-fast-threshold", type=int, default=None)
    p.add_argument("--min-outcomes", type=int, default=None)
    p.add_argument("--min-edge-grid", default=None)
    p.add_argument("--ttl-grid", default=None)
    p.add_argument("--max-spread-grid", default=None)
    p.add_argument("--base-quote-size-grid", default=None)
    p.add_argument("--min-eval-notional-usdc-grid", default=None)
    p.add_argument("--min-expected-edge-usdc-grid", default=None)
    p.add_argument("--taker-trigger-grid", default=None)
    p.add_argument("--taker-max-slippage-grid", default=None)
    p.add_argument("--stale-tick-filter-ms", type=float, default=None)
    p.add_argument("--market-tier-profile", default=None)
    p.add_argument("--active-top-n-markets", type=int, default=None)
    p.add_argument("--basis-k-grid", default=None)
    p.add_argument("--basis-z-grid", default=None)
    p.add_argument("--safe-threshold-grid", default=None)
    p.add_argument("--caution-threshold-grid", default=None)
    p.add_argument("--warmup-sec", type=int, default=None)
    p.add_argument(
        "--selection-mode",
        choices=["score", "robust"],
        default=None,
        help="score=median ranking, robust=conservative walk-forward aggregation",
    )
    p.add_argument("--walkforward-windows", type=int, default=None)
    p.add_argument("--walkforward-cooldown-sec", type=float, default=None)
    p.add_argument(
        "--top-k-consensus",
        type=int,
        default=None,
        help="Use top-K ranked trials to build consensus auto-tuned thresholds",
    )
    p.add_argument(
        "--max-estimated-sec",
        type=int,
        default=None,
        help="Hard cap for estimated total runtime; trims trial count automatically when exceeded",
    )
    return p.parse_args()


def main() -> int:
    args = apply_profile_defaults(parse_args())
    day_dir = Path(args.out_root) / utc_day()
    run_dir = day_dir / "runs" / str(args.run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    started = time.monotonic()

    edge_grid = parse_float_grid(args.min_edge_grid)
    ttl_grid = parse_int_grid(args.ttl_grid)
    max_spread_grid = parse_float_grid(args.max_spread_grid)
    base_quote_grid = parse_float_grid(args.base_quote_size_grid)
    min_eval_grid = parse_float_grid(args.min_eval_notional_usdc_grid)
    min_edge_usdc_grid = parse_float_grid(args.min_expected_edge_usdc_grid)
    taker_trigger_grid = parse_float_grid(args.taker_trigger_grid)
    taker_slip_grid = parse_float_grid(args.taker_max_slippage_grid)
    k_grid = parse_float_grid(args.basis_k_grid)
    z_grid = parse_float_grid(args.basis_z_grid)
    safe_grid = parse_float_grid(args.safe_threshold_grid)
    caution_grid = parse_float_grid(args.caution_threshold_grid)

    # For small max_trials, prefer a deterministic OFAT-style set instead of a huge cartesian product.
    def pick_default(seq: List[Any], prefer_last: bool = False) -> Any:
        if not seq:
            raise ValueError("empty grid")
        return seq[-1] if prefer_last else seq[0]

    base = {
        "edge": edge_grid[min(1, len(edge_grid) - 1)] if edge_grid else 1.0,
        "ttl": ttl_grid[min(1, len(ttl_grid) - 1)] if ttl_grid else 400,
        "max_spread": pick_default(max_spread_grid, prefer_last=True) if max_spread_grid else 0.03,
        "base_quote": pick_default(base_quote_grid, prefer_last=True),
        "min_eval": pick_default(min_eval_grid, prefer_last=False),
        "min_edge_usdc": pick_default(min_edge_usdc_grid, prefer_last=False),
        "taker_trigger": taker_trigger_grid[min(1, len(taker_trigger_grid) - 1)]
        if taker_trigger_grid
        else 4.0,
        "taker_slip": pick_default(taker_slip_grid, prefer_last=True),
        "k": pick_default(k_grid, prefer_last=False),
        "z": pick_default(z_grid, prefer_last=False),
        "safe": pick_default(safe_grid, prefer_last=False),
        "caution": pick_default(caution_grid, prefer_last=False),
    }

    combos: List[tuple[Any, ...]] = []
    combos.append(
        (
            base["edge"],
            base["ttl"],
            base["max_spread"],
            base["base_quote"],
            base["min_eval"],
            base["min_edge_usdc"],
            base["taker_trigger"],
            base["taker_slip"],
            base["k"],
            base["z"],
            base["safe"],
            base["caution"],
        )
    )

    def add_variants(key: str, grid: List[Any]) -> None:
        for v in grid:
            if len(combos) >= max(1, args.max_trials):
                return
            if v == base[key]:
                continue
            spec = dict(base)
            spec[key] = v
            combos.append(
                (
                    spec["edge"],
                    spec["ttl"],
                    spec["max_spread"],
                    spec["base_quote"],
                    spec["min_eval"],
                    spec["min_edge_usdc"],
                    spec["taker_trigger"],
                    spec["taker_slip"],
                    spec["k"],
                    spec["z"],
                    spec["safe"],
                    spec["caution"],
                )
            )

    add_variants("edge", edge_grid)
    add_variants("ttl", ttl_grid)
    add_variants("max_spread", max_spread_grid)
    add_variants("base_quote", base_quote_grid)
    add_variants("min_eval", min_eval_grid)
    add_variants("taker_trigger", taker_trigger_grid)
    add_variants("taker_slip", taker_slip_grid)

    # If still short, fall back to cartesian product, deterministic order.
    if len(combos) < max(1, args.max_trials):
        prod = itertools.product(
            edge_grid,
            ttl_grid,
            max_spread_grid,
            base_quote_grid,
            min_eval_grid,
            min_edge_usdc_grid,
            taker_trigger_grid,
            taker_slip_grid,
            k_grid,
            z_grid,
            safe_grid,
            caution_grid,
        )
        for row in prod:
            if len(combos) >= max(1, args.max_trials):
                break
            if row in combos:
                continue
            combos.append(row)

    single_window_sec = max(1, int(args.warmup_sec + min(args.window_sec, args.eval_window_sec)))
    per_trial_sec = max(
        1,
        int(
            single_window_sec * max(1, int(args.walkforward_windows))
            + max(0.0, float(args.walkforward_cooldown_sec))
            * max(0, int(args.walkforward_windows) - 1)
        ),
    )
    estimated_full_sec = per_trial_sec * len(combos)
    if args.max_estimated_sec and args.max_estimated_sec > 0 and estimated_full_sec > args.max_estimated_sec:
        allowed_trials = max(1, int(args.max_estimated_sec // per_trial_sec))
        if allowed_trials < len(combos):
            print(
                f"[budget] estimated={estimated_full_sec}s exceeds cap={args.max_estimated_sec}s; "
                f"trimming trials {len(combos)} -> {allowed_trials}"
            )
            combos = combos[:allowed_trials]
            estimated_full_sec = per_trial_sec * len(combos)
    print(
        f"[profile] {args.profile} trials={len(combos)} per_trial~{per_trial_sec}s "
        f"estimated~{estimated_full_sec}s max_runtime={args.max_runtime_sec}s "
        f"mode={args.selection_mode} wf_windows={args.walkforward_windows}"
    )

    rows: List[TrialResult] = []
    consecutive_failures = 0
    for idx, combo in enumerate(combos, start=1):
        if args.max_runtime_sec > 0 and (time.monotonic() - started) >= args.max_runtime_sec:
            print("[stop] max-runtime-sec reached; ending regression loop")
            break
        (
            edge,
            ttl,
            max_spread,
            base_quote_size,
            min_eval_notional_usdc,
            min_expected_edge_usdc,
            taker_trigger_bps,
            taker_max_slippage_bps,
            k,
            z,
            safe,
            caution,
        ) = combo  # type: ignore[misc]
        print(
            f"[trial {idx}/{len(combos)}] edge={edge} ttl={ttl} "
            f"max_spread={max_spread} "
            f"base_quote={base_quote_size} min_eval={min_eval_notional_usdc} min_edge_usdc={min_expected_edge_usdc} "
            f"taker_trigger={taker_trigger_bps} taker_slip={taker_max_slippage_bps} "
            f"k={k} z={z} safe={safe} caution={caution}"
        )
        window_rows: List[TrialResult] = []
        for wf_idx in range(max(1, int(args.walkforward_windows))):
            if args.max_runtime_sec > 0 and (time.monotonic() - started) >= args.max_runtime_sec:
                print("[stop] max-runtime-sec reached while running walk-forward windows")
                break
            print(
                f"[trial {idx}/{len(combos)} window {wf_idx + 1}/{max(1, int(args.walkforward_windows))}] "
                f"running..."
            )
            row_window = run_trial(
                session=session,
                base_url=args.base_url,
                run_id=args.run_id,
                trial_index=idx,
                min_outcomes=args.min_outcomes,
                eval_window_sec=args.eval_window_sec,
                heartbeat_sec=args.heartbeat_sec,
                window_sec=args.window_sec,
                poll_interval_sec=args.poll_interval_sec,
                min_edge_bps=edge,
                ttl_ms=ttl,
                max_spread=max_spread,
                base_quote_size=base_quote_size,
                min_eval_notional_usdc=min_eval_notional_usdc,
                min_expected_edge_usdc=min_expected_edge_usdc,
                taker_trigger_bps=taker_trigger_bps,
                taker_max_slippage_bps=taker_max_slippage_bps,
                stale_tick_filter_ms=float(args.stale_tick_filter_ms),
                market_tier_profile=str(args.market_tier_profile),
                active_top_n_markets=int(args.active_top_n_markets),
                basis_k_revert=k,
                basis_z_cap=z,
                safe_threshold=safe,
                caution_threshold=caution,
                warmup_sec=args.warmup_sec,
            )
            window_rows.append(row_window)
            if wf_idx + 1 < max(1, int(args.walkforward_windows)):
                time.sleep(max(0.0, float(args.walkforward_cooldown_sec)))

        if not window_rows:
            break
        row = aggregate_walkforward_results(
            run_id=args.run_id,
            trial_index=idx,
            windows=window_rows,
            selection_mode=str(args.selection_mode),
        )
        rows.append(row)
        consecutive_failures = 0 if row.gate_pass() else consecutive_failures + 1
        if args.fail_fast_threshold > 0 and consecutive_failures >= args.fail_fast_threshold:
            print(
                f"[stop] fail-fast-threshold reached with {consecutive_failures} consecutive gate failures"
            )
            break

    rows.sort(key=score_trial, reverse=True)

    ablation_path = run_dir / "ablation_toxicity.csv"
    summary_path = run_dir / "regression_summary.md"
    fixlist_path = run_dir / "next_fixlist.md"
    write_ablation_csv(ablation_path, rows)
    write_summary_md(summary_path, rows)
    write_fixlist(fixlist_path, rows[0] if rows else None)
    auto_tuned_path = run_dir / "auto_tuned_thresholds.json"
    auto_tuned = write_auto_tuned_thresholds(auto_tuned_path, rows, int(args.top_k_consensus))

    out_json = run_dir / "regression_summary.json"
    best = rows[0] if rows else None
    out_json.write_text(
        json.dumps(
            {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "run_id": args.run_id,
                "min_outcomes": args.min_outcomes,
                "eval_window_sec": args.eval_window_sec,
                "selection_mode": args.selection_mode,
                "walkforward_windows": args.walkforward_windows,
                "best_score": score_trial(best) if best else None,
                "best_objective_ev_net_penalty": objective_ev_net_penalty(best) if best else None,
                "best_pass": best.gate_pass() if best else None,
                "best_gate_ready": best.gate_ready if best else None,
                "best_params": (
                    {
                        "min_edge_bps": best.min_edge_bps,
                        "ttl_ms": best.ttl_ms,
                        "base_quote_size": best.base_quote_size,
                        "min_eval_notional_usdc": best.min_eval_notional_usdc,
                        "min_expected_edge_usdc": best.min_expected_edge_usdc,
                        "taker_trigger_bps": best.taker_trigger_bps,
                        "taker_max_slippage_bps": best.taker_max_slippage_bps,
                        "stale_tick_filter_ms": best.stale_tick_filter_ms,
                        "market_tier_profile": best.market_tier_profile,
                        "active_top_n_markets": best.active_top_n_markets,
                        "basis_k_revert": best.basis_k_revert,
                        "basis_z_cap": best.basis_z_cap,
                        "safe_threshold": best.safe_threshold,
                        "caution_threshold": best.caution_threshold,
                        "policy_block_ratio": best.policy_block_ratio,
                        "walkforward_windows": best.walkforward_windows,
                        "consistency_score": best.consistency_score,
                        "stability_penalty": best.stability_penalty,
                        "objective_penalty": objective_penalty(best),
                        "objective_ev_net_penalty": objective_ev_net_penalty(best),
                    }
                    if best
                    else None
                ),
                "auto_tuned_thresholds": auto_tuned,
                "trials": [
                    row.__dict__
                    | {
                        "gate_pass": row.gate_pass(),
                        "objective_penalty": objective_penalty(row),
                        "objective_ev_net_penalty": objective_ev_net_penalty(row),
                    }
                    for row in rows
                ],
            },
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"wrote={ablation_path}")
    print(f"wrote={summary_path}")
    print(f"wrote={fixlist_path}")
    print(f"wrote={auto_tuned_path}")
    print(f"wrote={out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
