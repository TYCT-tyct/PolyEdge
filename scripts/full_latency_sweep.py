#!/usr/bin/env python3
"""Bounded full-latency sweep (engine metrics + WS lag) across all symbols.

This is meant to be fast and repeatable: one command produces a single JSON artifact
under datasets/reports/<day>/, suitable for comparing runs over time.
"""

from __future__ import annotations

import atexit
import argparse
import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import requests

from latency_probe.models import SYMBOL_TO_NAME
from latency_probe.stats import summarize
from latency_probe.ws_probe import run_ws_latency

PROFILE_DEFAULTS: Dict[str, Dict[str, float]] = {
    "quick_60s": {"seconds": 60, "poll_interval": 2.0},
    "quick": {"seconds": 60, "poll_interval": 2.0},
    "standard": {"seconds": 120, "poll_interval": 2.0},
    "deep": {"seconds": 300, "poll_interval": 2.0},
}


def post_json(base_url: str, path: str, payload: dict) -> dict:
    resp = requests.post(f"{base_url.rstrip('/')}{path}", json=payload, timeout=8)
    resp.raise_for_status()
    return resp.json()


def get_json(base_url: str, path: str) -> dict:
    resp = requests.get(f"{base_url.rstrip('/')}{path}", timeout=8)
    resp.raise_for_status()
    return resp.json()


def fusion_payload(
    mode: str,
    dedupe_window_ms: int,
    udp_share_cap: float | None = None,
    jitter_threshold_ms: float | None = None,
    fallback_arm_duration_ms: int | None = None,
    fallback_cooldown_sec: int | None = None,
    udp_local_only: bool | None = None,
) -> dict:
    norm = mode.strip().lower()
    if norm in {"udp_only", "websocket_primary"}:
        norm = "hyper_mesh"
    if norm not in {"direct_only", "active_active", "hyper_mesh"}:
        raise ValueError(f"unsupported fusion mode: {mode}")
    payload = {
        "enable_udp": norm != "direct_only",
        "mode": norm,
        "dedupe_window_ms": int(dedupe_window_ms),
    }

    if udp_share_cap is not None:
        payload["udp_share_cap"] = float(udp_share_cap)
    if jitter_threshold_ms is not None:
        payload["jitter_threshold_ms"] = float(jitter_threshold_ms)
    if fallback_arm_duration_ms is not None:
        payload["fallback_arm_duration_ms"] = int(fallback_arm_duration_ms)
    if fallback_cooldown_sec is not None:
        payload["fallback_cooldown_sec"] = int(fallback_cooldown_sec)
    if udp_local_only is not None:
        payload["udp_local_only"] = bool(udp_local_only)
    return payload


def utc_day(ts: float | None = None) -> str:
    dt = datetime.fromtimestamp(ts or time.time(), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def collect_engine_series(
    base_url: str,
    seconds: int,
    poll_interval: float,
    progress_sec: int,
) -> dict:
    session = requests.Session()
    atexit.register(session.close)
    started = time.time()
    deadline = time.time() + max(1, seconds)
    next_progress = started + max(1, progress_sec)
    samples = 0
    failures = 0
    last_live: dict | None = None

    series: Dict[str, List[float]] = {
        "tick_to_ack_p99_ms": [],
        "tick_to_decision_p99_ms": [],
        "decision_queue_wait_p99_ms": [],
        "decision_compute_p99_ms": [],
        "feed_in_p99_ms": [],
        "source_latency_p99_ms": [],
        "exchange_lag_p99_ms": [],
        "path_lag_p99_ms": [],
        "local_backlog_p99_ms": [],
        "book_top_lag_p50_ms": [],
        "book_top_lag_p90_ms": [],
        "book_top_lag_p99_ms": [],
        "quote_block_ratio": [],
        "policy_block_ratio": [],
        "executed_over_eligible": [],
        "ev_net_usdc_p50": [],
        "ev_positive_ratio": [],
        "survival_5ms": [],
        "survival_10ms": [],
        "survival_25ms": [],
        "survival_probe_5ms": [],
        "survival_probe_10ms": [],
        "survival_probe_25ms": [],
        "survival_probe_5ms_n": [],
        "survival_probe_10ms_n": [],
        "survival_probe_25ms_n": [],
        "fillability_10ms": [],
        "window_outcomes": [],
        "udp_share_effective": [],
        "udp_local_drop_count": [],
        "share_cap_drop_count": [],
    }

    while time.time() < deadline:
        try:
            resp = session.get(f"{base_url.rstrip('/')}/report/shadow/live", timeout=5)
            resp.raise_for_status()
            live = resp.json()
            last_live = live
            latency = live.get("latency") or {}

            def push(key: str, value: object) -> None:
                if isinstance(value, (int, float)):
                    series[key].append(float(value))

            push("tick_to_ack_p99_ms", live.get("tick_to_ack_p99_ms"))
            push("tick_to_decision_p99_ms", live.get("tick_to_decision_p99_ms"))
            push("decision_queue_wait_p99_ms", live.get("decision_queue_wait_p99_ms"))
            push("decision_compute_p99_ms", live.get("decision_compute_p99_ms"))
            push("quote_block_ratio", live.get("quote_block_ratio"))
            push("policy_block_ratio", live.get("policy_block_ratio"))
            push("executed_over_eligible", live.get("executed_over_eligible"))
            push("ev_net_usdc_p50", live.get("ev_net_usdc_p50"))
            push("ev_positive_ratio", live.get("ev_positive_ratio"))
            push("window_outcomes", live.get("window_outcomes"))
            push("udp_share_effective", live.get("udp_share_effective"))
            push("udp_local_drop_count", live.get("udp_local_drop_count"))
            push("share_cap_drop_count", live.get("share_cap_drop_count"))
            push("survival_5ms", live.get("survival_5ms"))
            push("survival_10ms", live.get("survival_10ms"))
            push("survival_25ms", live.get("survival_25ms"))
            push("survival_probe_5ms", live.get("survival_probe_5ms"))
            push("survival_probe_10ms", live.get("survival_probe_10ms"))
            push("survival_probe_25ms", live.get("survival_probe_25ms"))
            push("survival_probe_5ms_n", live.get("survival_probe_5ms_n"))
            push("survival_probe_10ms_n", live.get("survival_probe_10ms_n"))
            push("survival_probe_25ms_n", live.get("survival_probe_25ms_n"))
            push("fillability_10ms", live.get("fillability_10ms"))
            push("book_top_lag_p50_ms", live.get("book_top_lag_p50_ms"))
            push("book_top_lag_p90_ms", live.get("book_top_lag_p90_ms"))
            push("book_top_lag_p99_ms", live.get("book_top_lag_p99_ms"))

            push("feed_in_p99_ms", latency.get("feed_in_p99_ms"))
            push("source_latency_p99_ms", live.get("source_latency_p99_ms"))
            push("exchange_lag_p99_ms", live.get("exchange_lag_p99_ms"))
            push("path_lag_p99_ms", live.get("path_lag_p99_ms"))
            push("local_backlog_p99_ms", live.get("local_backlog_p99_ms"))

            samples += 1
        except Exception:
            raise  # Linus: Fail loudly and explicitly
        now = time.time()
        if progress_sec > 0 and now >= next_progress:
            elapsed = int(now - started)
            print(
                f"[sweep] elapsed={elapsed}s/{seconds}s samples={samples} failures={failures}",
                flush=True,
            )
            next_progress = now + max(1, progress_sec)
        time.sleep(max(0.2, poll_interval))

    return {
        "samples": samples,
        "failures": failures,
        "stats": {k: summarize(v) for k, v in series.items()},
        "last": {
            "window_id": (last_live or {}).get("window_id"),
            "gate_ready": (last_live or {}).get("gate_ready"),
            "window_outcomes": (last_live or {}).get("window_outcomes"),
            "book_top_lag_by_symbol_p50_ms": (last_live or {}).get("book_top_lag_by_symbol_p50_ms"),
            "survival_10ms_by_symbol": (last_live or {}).get("survival_10ms_by_symbol"),
            "survival_probe_10ms_by_symbol": (last_live or {}).get("survival_probe_10ms_by_symbol"),
            "blocked_reason_counts": (last_live or {}).get("blocked_reason_counts"),
            "policy_block_reason_distribution": (last_live or {}).get("policy_block_reason_distribution"),
            "gate_block_reason_distribution": (last_live or {}).get("gate_block_reason_distribution"),
            "source_mix_ratio": (last_live or {}).get("source_mix_ratio"),
            "source_health": (last_live or {}).get("source_health"),
            "last_30s_taker_fallback_count": (last_live or {}).get("last_30s_taker_fallback_count"),
            "udp_share_effective": (last_live or {}).get("udp_share_effective"),
            "udp_local_drop_count": (last_live or {}).get("udp_local_drop_count"),
            "share_cap_drop_count": (last_live or {}).get("share_cap_drop_count"),
            "fallback_state": (last_live or {}).get("fallback_state"),
            "fallback_trigger_reason_distribution": (last_live or {}).get(
                "fallback_trigger_reason_distribution"
            ),
        },
    }


async def collect_ws_all(seconds: int, symbols: List[str]) -> dict:
    results = await asyncio.gather(*(run_ws_latency(seconds, s) for s in symbols))
    return {sym: res for sym, res in zip(symbols, results)}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PolyEdge full latency sweep (bounded)")
    p.add_argument(
        "--profile",
        default="quick_60s",
        choices=["quick_60s", "quick", "standard", "deep"],
    )
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--seconds", type=int, default=None)
    p.add_argument("--poll-interval", type=float, default=None)
    p.add_argument("--symbols", default=",".join(SYMBOL_TO_NAME.keys()))
    p.add_argument("--out-root", default="datasets/reports")
    # Optional: write artifacts under datasets/reports/<day>/runs/<run_id>/ for easier A/B comparison.
    # When omitted, preserves the legacy location under datasets/reports/<day>/.
    p.add_argument("--run-id", default=None)
    p.add_argument("--skip-ws", action="store_true", help="skip CEX websocket latency probe (engine metrics only)")
    p.add_argument(
        "--fusion-mode",
        choices=["direct_only", "active_active", "hyper_mesh"],
        default=None,
        help="optionally force fusion mode before sampling",
    )
    p.add_argument(
        "--dedupe-window-ms",
        type=int,
        default=120,
        help="dedupe window when applying fusion mode",
    )
    p.add_argument(
        "--reset-shadow",
        action="store_true",
        help="reset shadow metrics before sampling",
    )
    p.add_argument(
        "--warmup-sec",
        type=int,
        default=0,
        help="sleep after control actions before sampling",
    )
    p.add_argument(
        "--udp-share-cap",
        type=float,
        default=None,
        help="optional udp share cap when applying fusion mode",
    )
    p.add_argument(
        "--jitter-threshold-ms",
        type=float,
        default=None,
        help="optional jitter threshold when applying fusion mode",
    )
    p.add_argument(
        "--fallback-cooldown-sec",
        type=int,
        default=None,
        help="optional websocket-primary fallback cooldown",
    )
    p.add_argument(
        "--fallback-arm-duration-ms",
        type=int,
        default=None,
        help="optional websocket-primary fallback arm duration",
    )
    p.add_argument(
        "--udp-local-only",
        choices=["true", "false"],
        default=None,
        help="override udp_local_only during fusion reload",
    )
    p.add_argument(
        "--progress-sec",
        type=int,
        default=5,
        help="print progress heartbeat every N seconds (0 disables)",
    )
    p.add_argument(
        "--allow-observe-only",
        action="store_true",
        help="allow sampling even when runtime reports observe_only=true/paused=true",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    profile = PROFILE_DEFAULTS[args.profile]
    seconds = int(args.seconds if args.seconds is not None else profile["seconds"])
    poll_interval = float(args.poll_interval if args.poll_interval is not None else profile["poll_interval"])
    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]

    started_ms = int(time.time() * 1000)
    control: dict = {}
    if args.fusion_mode:
        control["fusion_applied"] = post_json(
            args.base_url,
            "/control/reload_fusion",
            fusion_payload(
                args.fusion_mode,
                args.dedupe_window_ms,
                args.udp_share_cap,
                args.jitter_threshold_ms,
                args.fallback_arm_duration_ms,
                args.fallback_cooldown_sec,
                (
                    None
                    if args.udp_local_only is None
                    else args.udp_local_only.lower() == "true"
                ),
            ),
        )
    if args.reset_shadow:
        control["shadow_reset"] = post_json(args.base_url, "/control/reset_shadow", {})
    control["resume"] = post_json(args.base_url, "/control/resume", {})
    if args.warmup_sec > 0:
        time.sleep(max(0, int(args.warmup_sec)))
    try:
        control["pre_live"] = get_json(args.base_url, "/report/shadow/live")
    except Exception:
        raise  # Linus: Fail loudly and explicitly
    pre_live = control.get("pre_live") or {}
    pre_live_paused = bool(pre_live.get("paused", False))
    pre_live_observe_only = bool(pre_live.get("observe_only", False))
    if (pre_live_paused or pre_live_observe_only) and not args.allow_observe_only:
        state_flags = []
        if pre_live_paused:
            state_flags.append("paused=true")
        if pre_live_observe_only:
            state_flags.append("observe_only=true")
        flags = ",".join(state_flags) if state_flags else "invalid_state"
        raise RuntimeError(
            f"runtime is not in active trading state ({flags}); "
            "fix control mode first or pass --allow-observe-only"
        )
    print(
        f"[sweep] start profile={args.profile} mode={args.fusion_mode or 'unchanged'} "
        f"seconds={seconds} poll={poll_interval}s",
        flush=True,
    )

    async def run_all() -> tuple[dict, dict]:
        engine_task = asyncio.to_thread(
            collect_engine_series,
            args.base_url,
            seconds,
            poll_interval,
            int(args.progress_sec),
        )
        if args.skip_ws:
            engine_res = await engine_task
            return engine_res, {}
        ws_task = collect_ws_all(seconds, symbols)
        engine_res, ws_res = await asyncio.gather(engine_task, ws_task)
        return engine_res, ws_res

    engine, ws = asyncio.run(run_all())

    payload: dict = {
        "meta": {
            "profile": args.profile,
            "base_url": args.base_url,
            "seconds": seconds,
            "poll_interval": poll_interval,
            "symbols": symbols,
            "ts_ms": started_ms,
            "run_id": args.run_id,
            "skip_ws": bool(args.skip_ws),
            "fusion_mode": args.fusion_mode,
            "reset_shadow": bool(args.reset_shadow),
            "warmup_sec": int(args.warmup_sec),
            "dedupe_window_ms": int(args.dedupe_window_ms),
            "udp_share_cap": args.udp_share_cap,
            "jitter_threshold_ms": args.jitter_threshold_ms,
            "fallback_cooldown_sec": args.fallback_cooldown_sec,
            "fallback_arm_duration_ms": args.fallback_arm_duration_ms,
            "udp_local_only": args.udp_local_only,
            "control": control,
        },
        "engine": engine,
        "ws": ws,
    }

    if args.run_id:
        out_dir = Path(args.out_root) / utc_day() / "runs" / str(args.run_id)
    else:
        out_dir = Path(args.out_root) / utc_day()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"full_latency_sweep_{started_ms}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    print(f"wrote_json={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
