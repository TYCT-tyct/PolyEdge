#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import os
import random
import statistics
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


L1_FIELDS: Tuple[str, ...] = (
    "position_fraction",
    "min_edge_net_bps",
    "convergence_exit_ratio",
    "min_velocity_bps_per_sec",
)

L2_EXTRA_FIELDS: Tuple[str, ...] = (
    "capital_fraction_kelly",
    "t100ms_reversal_bps",
    "t300ms_reversal_bps",
    "max_single_trade_loss_usdc",
    "risk_max_drawdown_pct",
    "risk_max_market_notional",
)

L3_EXTRA_FIELDS: Tuple[str, ...] = (
    "maker_min_edge_bps",
    "basis_k_revert",
    "basis_z_cap",
)

# Coarse absolute bounds to avoid optimizer drift.
FIELD_BOUNDS: Dict[str, Tuple[float, float]] = {
    "position_fraction": (0.01, 1.0),
    "min_edge_net_bps": (1.0, 5_000.0),
    "convergence_exit_ratio": (0.05, 0.99),
    "min_velocity_bps_per_sec": (0.1, 1_000.0),
    "capital_fraction_kelly": (0.01, 1.0),
    "t100ms_reversal_bps": (0.1, 2_000.0),
    "t300ms_reversal_bps": (0.1, 2_000.0),
    "max_single_trade_loss_usdc": (0.0, 5_000.0),
    "risk_max_drawdown_pct": (0.001, 1.0),
    "risk_max_market_notional": (0.0, 5_000_000.0),
    "maker_min_edge_bps": (0.1, 1_000.0),
    "basis_k_revert": (0.0, 5.0),
    "basis_z_cap": (0.5, 8.0),
}

STYLE_STORE: Dict[str, dict] = {}
RL_TABLE: Dict[str, Dict[str, float]] = {}


@dataclass
class CandidateEval:
    params: Dict[str, float]
    objective: float
    ev_pred: float
    dd_pred: float


def now_ms() -> int:
    return int(time.time() * 1000)


def clamp(v: float, lo: float, hi: float) -> float:
    return min(max(v, lo), hi)


def percentile(values: Sequence[float], p: float) -> float:
    if not values:
        return 0.0
    data = sorted(values)
    idx = int(round((len(data) - 1) * p))
    idx = max(0, min(idx, len(data) - 1))
    return float(data[idx])


def get_float(data: dict, key: str, default: float) -> float:
    value = data.get(key, default)
    try:
        out = float(value)
        if math.isfinite(out):
            return out
    except Exception:
        raise  # Linus: Fail loudly and explicitly
    return float(default)


def parse_param_set(raw: dict) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for k, v in raw.items():
        if v is None:
            continue
        try:
            fv = float(v)
            if math.isfinite(fv):
                out[k] = fv
        except Exception:
            raise  # Linus: Fail loudly and explicitly
    return out


def get_layer_fields(layer: str) -> Tuple[str, ...]:
    if layer == "layer1":
        return L1_FIELDS
    if layer == "layer2":
        return L1_FIELDS + L2_EXTRA_FIELDS
    if layer == "layer3":
        return L1_FIELDS + L2_EXTRA_FIELDS + L3_EXTRA_FIELDS
    return L1_FIELDS


def relative_clip(current: float, candidate: float, pct: float) -> float:
    if not math.isfinite(current) or not math.isfinite(candidate):
        return current
    step = abs(current) * max(0.0, pct)
    if step <= 0.0:
        return candidate
    return clamp(candidate, current - step, current + step)


def apply_bounds(field: str, value: float) -> float:
    lo, hi = FIELD_BOUNDS.get(field, (-1.0e18, 1.0e18))
    return clamp(value, lo, hi)


def style_distance(a: Dict[str, float], b: Dict[str, float]) -> float:
    # Weighted normalized distance for 4-d style vector.
    weights = {
        "volatility_proxy": 0.35,
        "source_health_min": 0.30,
        "roi_notional_10s_bps_p50": 0.20,
        "win_rate": 0.15,
    }
    total = 0.0
    for key, w in weights.items():
        av = float(a.get(key, 0.0))
        bv = float(b.get(key, 0.0))
        scale = max(1.0, abs(av), abs(bv))
        z = (av - bv) / scale
        total += w * (z * z)
    return math.sqrt(max(0.0, total))


def weighted_median(values: List[Tuple[float, float]]) -> float:
    if not values:
        return 0.0
    data = sorted(values, key=lambda item: item[0])
    total = sum(max(0.0, w) for _, w in data)
    if total <= 0.0:
        return data[len(data) // 2][0]
    acc = 0.0
    for v, w in data:
        acc += max(0.0, w)
        if acc >= total * 0.5:
            return v
    return data[-1][0]


def knn_warm_start(
    current: Dict[str, float],
    current_style: Dict[str, float],
    style_memory: List[dict],
    fields: Tuple[str, ...],
) -> Tuple[Dict[str, float], float, int, str]:
    if len(style_memory) < 8:
        return dict(current), 0.0, 0, "fallback_layer2_bo"

    scored: List[Tuple[float, dict]] = []
    for entry in style_memory:
        vector = entry.get("vector") or {}
        params = entry.get("params") or {}
        if not isinstance(vector, dict) or not isinstance(params, dict):
            continue
        d = style_distance(current_style, parse_param_set(vector))
        scored.append((d, entry))
    if not scored:
        return dict(current), 0.0, 0, "fallback_layer2_bo"

    scored.sort(key=lambda item: item[0])
    neighbors = scored[: min(5, len(scored))]
    warm = dict(current)
    for field in fields:
        weighted_values: List[Tuple[float, float]] = []
        for dist, entry in neighbors:
            params = parse_param_set(entry.get("params") or {})
            if field not in params:
                continue
            weight = 1.0 / (1.0e-6 + dist)
            weighted_values.append((params[field], weight))
        if weighted_values:
            warm[field] = weighted_median(weighted_values)

    mean_dist = statistics.mean(d for d, _ in neighbors)
    match_score = 1.0 / (1.0 + mean_dist)
    return warm, match_score, len(neighbors), "knn_memory"


def infer_style_id(style: Dict[str, float]) -> str:
    vol = "hv" if float(style.get("volatility_proxy", 0.0)) > 8.0 else "lv"
    health = "hh" if float(style.get("source_health_min", 0.0)) > 0.6 else "lh"
    drift = "pr" if float(style.get("roi_notional_10s_bps_p50", 0.0)) > 0.0 else "ng"
    return f"{vol}_{health}_{drift}"


def choose_action(layer: str, baseline: Dict[str, float]) -> str:
    ev = float(baseline.get("ev_usdc_p50", 0.0))
    dd = float(baseline.get("max_drawdown_pct", 0.0))
    win = float(baseline.get("win_rate", 0.5))
    if ev < 0.0 or dd > 0.06 or win < 0.5:
        return "tighten"
    if layer == "layer3" and ev > 0.0 and dd < 0.03 and win > 0.55:
        return "loosen"
    return "neutral"


def rl_signal(style_id: str, action: str) -> float:
    table = RL_TABLE.get(style_id, {})
    return float(table.get(action, 0.0))


def surrogate_eval(
    current: Dict[str, float],
    candidate: Dict[str, float],
    baseline: Dict[str, float],
    layer: str,
    action: str,
    rl: float,
) -> CandidateEval:
    base_obj = float(baseline.get("objective", 0.0))
    base_ev = float(baseline.get("ev_usdc_p50", 0.0))
    base_dd = max(0.0, float(baseline.get("max_drawdown_pct", 0.0)))
    base_win = float(baseline.get("win_rate", 0.5))

    def rel_delta(field: str) -> float:
        cur = float(current.get(field, candidate.get(field, 0.0)))
        nxt = float(candidate.get(field, cur))
        scale = max(1.0e-6, abs(cur))
        return (nxt - cur) / scale

    d_pos = rel_delta("position_fraction")
    d_edge = rel_delta("min_edge_net_bps")
    d_exit = rel_delta("convergence_exit_ratio")
    d_vel = rel_delta("min_velocity_bps_per_sec")
    d_kelly = rel_delta("capital_fraction_kelly")
    d_risk_dd = rel_delta("risk_max_drawdown_pct")
    d_risk_notional = rel_delta("risk_max_market_notional")
    d_maker = rel_delta("maker_min_edge_bps")
    d_basis_k = rel_delta("basis_k_revert")

    ev_gain = (
        0.22 * d_pos
        - 0.14 * d_edge
        + 0.07 * d_exit
        - 0.08 * d_vel
        + 0.19 * d_kelly
        - 0.10 * d_maker
        + 0.05 * d_basis_k
    )
    dd_risk = (
        0.20 * max(0.0, d_pos)
        + 0.20 * max(0.0, d_kelly)
        + 0.15 * max(0.0, d_risk_notional)
        + 0.10 * max(0.0, d_risk_dd)
        + 0.08 * max(0.0, -d_edge)
    )

    if action == "tighten":
        ev_gain -= 0.03
        dd_risk *= 0.75
    elif action == "loosen":
        ev_gain += 0.03
        dd_risk *= 1.15

    uplift = ev_gain * max(0.5, abs(base_ev) * 0.15 + 1.0)
    ev_pred = base_ev + uplift + rl * 0.02
    dd_pred = clamp(base_dd + dd_risk * 0.04 - max(0.0, base_win - 0.5) * 0.01, 0.0, 1.0)

    obj = base_obj + uplift - dd_risk * 0.35 + rl * 0.04
    return CandidateEval(params=candidate, objective=obj, ev_pred=ev_pred, dd_pred=dd_pred)


def sample_candidate(
    rng: random.Random,
    current: Dict[str, float],
    warm: Dict[str, float],
    fields: Tuple[str, ...],
    step_limit: float,
    layer: str,
    baseline: Dict[str, float],
    action: str,
    rl: float,
) -> CandidateEval:
    out = dict(current)
    for field in fields:
        if field not in current and field not in warm:
            continue
        cur = float(current.get(field, warm.get(field, 0.0)))
        prior = float(warm.get(field, cur))
        scale = max(1.0e-6, abs(cur), abs(prior))
        sigma = scale * step_limit * (0.22 if layer == "layer1" else 0.45)
        proposal = rng.gauss(prior, sigma)
        proposal = relative_clip(cur, proposal, step_limit)
        out[field] = apply_bounds(field, proposal)

    return surrogate_eval(current, out, baseline, layer, action, rl)


def bayesian_consensus(
    current: Dict[str, float],
    warm: Dict[str, float],
    fields: Tuple[str, ...],
    ranked: List[CandidateEval],
) -> Dict[str, float]:
    if not ranked:
        return dict(current)
    top_k = ranked
    out = dict(current)
    for field in fields:
        prior = float(warm.get(field, current.get(field, 0.0)))
        values = [row.params[field] for row in top_k if field in row.params]
        if not values:
            continue
        consensus = percentile(values, 0.50)
        spread = max(1.0e-6, percentile(values, 0.75) - percentile(values, 0.25))
        prior_var = max(1.0e-6, abs(prior) * 0.12)
        obs_var = spread if spread > 0.0 else prior_var
        posterior = (prior / prior_var + consensus / obs_var) / (1.0 / prior_var + 1.0 / obs_var)
        out[field] = apply_bounds(field, posterior)
    return out


def apply_rl_post_adjust(
    candidate: Dict[str, float],
    current: Dict[str, float],
    action: str,
    rl: float,
    step_limit: float,
) -> Dict[str, float]:
    out = dict(candidate)
    mag = clamp(abs(rl), 0.0, 0.35) * 0.02
    if action == "tighten":
        if "position_fraction" in out:
            out["position_fraction"] = relative_clip(
                current.get("position_fraction", out["position_fraction"]),
                out["position_fraction"] * (1.0 - mag),
                step_limit,
            )
        if "capital_fraction_kelly" in out:
            out["capital_fraction_kelly"] = relative_clip(
                current.get("capital_fraction_kelly", out["capital_fraction_kelly"]),
                out["capital_fraction_kelly"] * (1.0 - mag),
                step_limit,
            )
    elif action == "loosen":
        if "position_fraction" in out:
            out["position_fraction"] = relative_clip(
                current.get("position_fraction", out["position_fraction"]),
                out["position_fraction"] * (1.0 + mag),
                step_limit,
            )
    for k, v in list(out.items()):
        out[k] = apply_bounds(k, v)
    return out


def compute_walk_forward(
    layer: str,
    baseline: Dict[str, float],
    candidate_eval: CandidateEval,
    objective_history: List[dict],
) -> Tuple[bool, int, float]:
    required = 6 if layer == "layer1" else 12 if layer == "layer2" else 20
    values = [
        get_float(point, "objective", float(baseline.get("objective", 0.0)))
        for point in objective_history[-required * 12 :]
    ]
    if not values:
        values = [float(baseline.get("objective", 0.0))] * required

    windows: List[float] = []
    window_size = max(1, len(values) // required)
    for i in range(0, len(values), window_size):
        seg = values[i : i + window_size]
        if not seg:
            continue
        windows.append(sum(seg) / len(seg))
    if len(windows) < required:
        windows.extend([windows[-1] if windows else float(baseline.get("objective", 0.0))] * (required - len(windows)))

    baseline_obj = float(baseline.get("objective", 0.0))
    # Candidate should show robust uplift under a conservative discount.
    uplift = candidate_eval.objective - baseline_obj
    adjusted = [w + uplift * 0.65 for w in windows[:required]]
    wins = sum(1 for v in adjusted if v >= baseline_obj)
    score = wins / max(1, required)

    threshold = 0.52 if layer == "layer1" else 0.58 if layer == "layer2" else 0.60
    return score >= threshold, required, score


def simulate_max_drawdown(path: Iterable[float]) -> float:
    peak = 0.0
    pnl = 0.0
    max_dd = 0.0
    for x in path:
        pnl += x
        peak = max(peak, pnl)
        dd = peak - pnl
        max_dd = max(max_dd, dd)
    return max_dd


def run_monte_carlo(
    layer: str,
    baseline: Dict[str, float],
    candidate_eval: CandidateEval,
    runs: int,
    seed: int,
) -> Tuple[bool, float, float]:
    rng = random.Random(seed)
    base_ev = float(baseline.get("ev_usdc_p50", 0.0))
    base_dd = max(0.0, float(baseline.get("max_drawdown_pct", 0.0)))
    uplift = candidate_eval.ev_pred - base_ev

    horizon = 40 if layer == "layer1" else 70 if layer == "layer2" else 100
    sigma = max(0.02, abs(float(baseline.get("volatility_proxy", 0.0))) * 0.002)

    ev_delta_series: List[float] = []
    dd_series: List[float] = []
    for _ in range(max(1, runs)):
        path: List[float] = []
        mean_step = (base_ev + uplift) / max(1, horizon)
        for _ in range(horizon):
            # Mixture noise approximates heavy tails without extra deps.
            noise = rng.gauss(0.0, sigma)
            if rng.random() < 0.06:
                noise += rng.gauss(0.0, sigma * 2.6)
            path.append(mean_step + noise)
        ev_new = sum(path)
        ev_delta_series.append(ev_new - base_ev)

        dd_path = [x - statistics.mean(path) for x in path]
        dd_series.append(simulate_max_drawdown(dd_path) * 0.001)

    ev_delta_p50 = percentile(ev_delta_series, 0.50)
    ev_delta_p25 = percentile(ev_delta_series, 0.25)
    ev_delta_p10 = percentile(ev_delta_series, 0.10)
    dd_p95 = percentile(dd_series, 0.95)

    if layer == "layer1":
        passed = ev_delta_p25 >= -abs(base_ev) * 0.08 and dd_p95 <= max(base_dd * 1.35, 0.06)
    elif layer == "layer2":
        passed = ev_delta_p50 >= -abs(base_ev) * 0.03 and dd_p95 <= max(base_dd * 1.25, 0.06)
    else:
        passed = ev_delta_p50 >= -abs(base_ev) * 0.02 and dd_p95 <= max(base_dd * 1.20, 0.06)

    return passed, ev_delta_p50, dd_p95


def optimize_payload(layer: str, data: dict) -> dict:
    baseline = data.get("baseline") or {}
    current = parse_param_set(data.get("current_params") or {})
    if not current:
        return {
            "layer": layer,
            "candidate": {},
            "validation": {
                "mc_runs": 0,
                "mc_pass": False,
                "walk_forward_pass": False,
                "shadow_pass": False,
                "walk_forward_windows": 0,
                "walk_forward_score": 0.0,
                "mc_ev_delta_p50": 0.0,
                "mc_drawdown_p95": 0.0,
            },
            "meta": {
                "style_match_score": 0.0,
                "style_match_count": 0,
                "top_k_size": 0,
                "objective_uplift_estimate": 0.0,
                "rl_signal": 0.0,
            },
            "notes": ["current_params_empty"],
        }

    fields = get_layer_fields(layer)
    step_limit = 0.05 if layer == "layer1" else 0.12
    mc_runs = 100 if layer == "layer1" else 500 if layer == "layer2" else 1000
    sample_count = 64 if layer == "layer1" else 120 if layer == "layer2" else 160
    top_k_size = 5 if layer == "layer1" else 7 if layer == "layer2" else 9

    style_memory = data.get("style_memory") or []
    current_style = parse_param_set(data.get("current_style") or {})
    warm, match_score, match_count, warm_source = knn_warm_start(
        current=current,
        current_style=current_style,
        style_memory=style_memory if isinstance(style_memory, list) else [],
        fields=fields,
    )

    style_id = infer_style_id(current_style)
    action = choose_action(layer, baseline)
    rl = rl_signal(style_id, action)

    seed = (
        now_ms()
        ^ int(abs(get_float(baseline, "objective", 0.0)) * 10_000)
        ^ (hash(style_id) & 0xFFFF)
        ^ hash(layer)
    )
    rng = random.Random(seed)

    ranked: List[CandidateEval] = []
    for _ in range(sample_count):
        eval_row = sample_candidate(
            rng=rng,
            current=current,
            warm=warm,
            fields=fields,
            step_limit=step_limit,
            layer=layer,
            baseline=baseline,
            action=action,
            rl=rl,
        )
        ranked.append(eval_row)
    ranked.sort(key=lambda row: row.objective, reverse=True)

    top = ranked[: max(1, top_k_size)]
    posterior = bayesian_consensus(current, warm, fields, top)
    candidate = apply_rl_post_adjust(posterior, current, action, rl, step_limit)

    final_eval = surrogate_eval(current, candidate, baseline, layer, action, rl)
    objective_uplift = final_eval.objective - get_float(baseline, "objective", 0.0)

    objective_history = data.get("objective_history") if isinstance(data.get("objective_history"), list) else []
    wf_pass, wf_windows, wf_score = compute_walk_forward(layer, baseline, final_eval, objective_history)

    mc_pass, mc_ev_delta_p50, mc_dd_p95 = run_monte_carlo(
        layer=layer,
        baseline=baseline,
        candidate_eval=final_eval,
        runs=mc_runs,
        seed=seed ^ 0x5A5A,
    )

    # Shadow gate: same hard rule as runtime. Keep here so runtime can audit service opinion.
    ev_old = get_float(baseline, "ev_usdc_p50", 0.0)
    dd_old = max(0.0, get_float(baseline, "max_drawdown_pct", 0.0))
    shadow_pass = final_eval.ev_pred >= ev_old * 1.05 and final_eval.dd_pred <= dd_old * 1.12

    notes = [
        f"warm_source={warm_source}",
        f"action={action}",
        f"sample_count={sample_count}",
        f"style_id={style_id}",
        f"ev_pred={final_eval.ev_pred:.6f}",
        f"dd_pred={final_eval.dd_pred:.6f}",
    ]

    return {
        "layer": layer,
        "candidate": candidate,
        "validation": {
            "mc_runs": mc_runs,
            "mc_pass": bool(mc_pass),
            "walk_forward_pass": bool(wf_pass),
            "shadow_pass": bool(shadow_pass),
            "walk_forward_windows": int(wf_windows),
            "walk_forward_score": float(wf_score),
            "mc_ev_delta_p50": float(mc_ev_delta_p50),
            "mc_drawdown_p95": float(mc_dd_p95),
        },
        "meta": {
            "style_match_score": float(match_score),
            "style_match_count": int(match_count),
            "top_k_size": int(len(top)),
            "objective_uplift_estimate": float(objective_uplift),
            "rl_signal": float(rl),
        },
        "notes": notes,
    }


def run_shadow_compare(data: dict) -> dict:
    ev_new = get_float(data, "ev_new", 0.0)
    ev_old = get_float(data, "ev_old", 0.0)
    dd_new = max(0.0, get_float(data, "dd_new", 0.0))
    dd_old = max(0.0, get_float(data, "dd_old", 0.0))
    cycles_observed = int(get_float(data, "cycles_observed", 0.0))
    cycles_required = int(get_float(data, "cycles_required", 0.0))

    pass_ev = ev_new >= ev_old * 1.05
    pass_dd = dd_new <= dd_old * 1.12
    pass_cycles = cycles_observed >= cycles_required
    passed = pass_ev and pass_dd and pass_cycles
    return {
        "ok": True,
        "pass": passed,
        "checks": {
            "pass_ev": pass_ev,
            "pass_dd": pass_dd,
            "pass_cycles": pass_cycles,
        },
    }


def run_monitor_60m(data: dict) -> dict:
    ev_new = get_float(data, "ev_new", 0.0)
    ev_old = get_float(data, "ev_old", 0.0)
    dd_new = max(0.0, get_float(data, "dd_new", 0.0))
    dd_old = max(0.0, get_float(data, "dd_old", 0.0))

    rollback = ev_new < ev_old or dd_new > dd_old
    return {"ok": True, "rollback": rollback}


def update_style_memory(data: dict) -> dict:
    entry = data.get("entry") or {}
    style_id = str(entry.get("style_id") or infer_style_id(parse_param_set(entry.get("vector") or {})))
    reward = get_float(data, "reward", get_float(data, "objective", 0.0))
    action = str(data.get("action") or "neutral")

    STYLE_STORE[style_id] = {
        "entry": entry,
        "reward": reward,
        "ts_ms": now_ms(),
    }
    table = RL_TABLE.setdefault(style_id, {"tighten": 0.0, "neutral": 0.0, "loosen": 0.0})
    old = float(table.get(action, 0.0))
    table[action] = old * 0.8 + reward * 0.2

    return {
        "ok": True,
        "accepted": True,
        "style_id": style_id,
        "rl_value": table[action],
        "style_memory_size": len(STYLE_STORE),
        "ts_ms": now_ms(),
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "seat-opt/2.3"

    def _send(self, code: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        try:
            size = int(self.headers.get("content-length", "0"))
        except Exception:
            raise  # Linus: Fail loudly and explicitly
        if size <= 0:
            return {}
        raw = self.rfile.read(size)
        try:
            data = json.loads(raw.decode("utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            raise  # Linus: Fail loudly and explicitly
    def do_GET(self) -> None:
        if self.path == "/health":
            self._send(200, {"ok": True, "ts_ms": now_ms(), "style_memory_size": len(STYLE_STORE)})
            return
        self._send(404, {"ok": False, "error": "not_found"})

    def do_POST(self) -> None:
        data = self._read_json()
        path = self.path

        if path == "/v1/seat/l1/optimize":
            self._send(200, optimize_payload("layer1", data))
            return
        if path == "/v1/seat/l2/optimize":
            self._send(200, optimize_payload("layer2", data))
            return
        if path == "/v1/seat/l3/optimize":
            self._send(200, optimize_payload("layer3", data))
            return
        if path == "/v1/seat/shadow_compare":
            self._send(200, run_shadow_compare(data))
            return
        if path == "/v1/seat/monitor_60m":
            self._send(200, run_monitor_60m(data))
            return
        if path == "/v1/seat/style_memory/update":
            self._send(200, update_style_memory(data))
            return

        self._send(404, {"ok": False, "error": "not_found"})

    def log_message(self, fmt: str, *args: object) -> None:
        return


def main() -> None:
    host = os.environ.get("POLYEDGE_SEAT_OPTIMIZER_HOST", "127.0.0.1")
    port = int(os.environ.get("POLYEDGE_SEAT_OPTIMIZER_PORT", "8091"))
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"[seat-optimizer] listening on {host}:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
