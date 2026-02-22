#!/usr/bin/env python3
"""
Build a standalone interactive HTML dashboard for roll_v1 analysis.

Input:
  --run-dir datasets/paper_runs/.../inst1  (or run root containing inst1)
Output:
  datasets/reports/web/<run_name>_roll_v1_dashboard.html
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--run-dir", required=True, help="run root or inst1 directory")
    p.add_argument("--out", default="", help="optional output html path")
    p.add_argument("--suggest-slope-up-bps", type=float, default=35.0)
    p.add_argument("--suggest-slope-down-bps", type=float, default=-35.0)
    p.add_argument("--suggest-lookback-sec", type=int, default=5)
    p.add_argument(
        "--min-span-sec",
        type=float,
        default=30.0,
        help="drop markets whose tape span is shorter than this window",
    )
    return p.parse_args()


def resolve_inst_dir(run_dir: Path) -> Path:
    if (run_dir / "normalized").exists() and (run_dir / "reports").exists():
        return run_dir
    inst1 = run_dir / "inst1"
    if (inst1 / "normalized").exists() and (inst1 / "reports").exists():
        return inst1
    raise FileNotFoundError(f"cannot resolve inst1 from: {run_dir}")


def latest_date_dir(parent: Path) -> Path:
    items = sorted([p for p in parent.iterdir() if p.is_dir()])
    if not items:
        raise FileNotFoundError(f"no partition dir under {parent}")
    return items[-1]


def iter_jsonl(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def side_to_yes_prob(side: str, px: float) -> Optional[float]:
    side = side.strip()
    if not (0.0 < px < 1.0):
        return None
    if side in ("BuyYes", "SellYes"):
        return px
    if side in ("BuyNo", "SellNo"):
        return 1.0 - px
    return None


@dataclass
class ShotMarker:
    t_ms: int
    yes_prob: float
    label: str
    side: str
    style: str
    market_id: str


@dataclass
class FillMarker:
    t_ms: int
    yes_prob: float
    label: str
    side: str
    style: str
    market_id: str


def load_price_tape(path: Path) -> Dict[str, List[dict]]:
    by_market: Dict[str, List[dict]] = defaultdict(list)
    for row in iter_jsonl(path):
        market_id = str(row.get("market_id", "")).strip()
        if not market_id:
            continue
        ts_ms = int(row.get("ts_ms", 0))
        mid_yes = float(row.get("mid_yes", 0.0))
        if ts_ms <= 0 or not (0.0 <= mid_yes <= 1.0):
            continue
        by_market[market_id].append(
            {
                "ts_ms": ts_ms,
                "mid_yes": mid_yes,
                "symbol": str(row.get("symbol", "")),
                "timeframe": str(row.get("timeframe", "")),
                "title": str(row.get("title", "")),
                "bid_yes": float(row.get("bid_yes", 0.0)),
                "ask_yes": float(row.get("ask_yes", 0.0)),
                "bid_no": float(row.get("bid_no", 0.0)),
                "ask_no": float(row.get("ask_no", 0.0)),
            }
        )
    for rows in by_market.values():
        rows.sort(key=lambda r: r["ts_ms"])
    return by_market


def load_shots(path: Path) -> Dict[str, List[ShotMarker]]:
    out: Dict[str, List[ShotMarker]] = defaultdict(list)
    if not path.exists():
        return out
    for row in iter_jsonl(path):
        shot = row.get("shot") or {}
        market_id = str(shot.get("market_id", "")).strip()
        ts_ms = int(row.get("ts_ms", 0))
        side = str(shot.get("side", ""))
        style = str(shot.get("execution_style", ""))
        px = float(shot.get("intended_price", 0.0))
        yp = side_to_yes_prob(side, px)
        if not market_id or ts_ms <= 0 or yp is None:
            continue
        out[market_id].append(
            ShotMarker(
                t_ms=ts_ms,
                yes_prob=yp,
                label=f"{side} {style} @ {px:.3f}",
                side=side,
                style=style,
                market_id=market_id,
            )
        )
    for arr in out.values():
        arr.sort(key=lambda x: x.t_ms)
    return out


def load_fills(path: Path) -> Dict[str, List[FillMarker]]:
    out: Dict[str, List[FillMarker]] = defaultdict(list)
    if not path.exists():
        return out
    for row in iter_jsonl(path):
        fill = row.get("fill") or {}
        market_id = str(fill.get("market_id", "")).strip()
        ts_ms = int(row.get("ts_ms", 0))
        side = str(fill.get("side", ""))
        style = str(fill.get("style", ""))
        px = float(fill.get("price", 0.0))
        yp = side_to_yes_prob(side, px)
        if not market_id or ts_ms <= 0 or yp is None:
            continue
        out[market_id].append(
            FillMarker(
                t_ms=ts_ms,
                yes_prob=yp,
                label=f"{side} {style} @ {px:.3f}",
                side=side,
                style=style,
                market_id=market_id,
            )
        )
    for arr in out.values():
        arr.sort(key=lambda x: x.t_ms)
    return out


def suggest_points(
    rows: List[dict],
    lookback_sec: int,
    slope_up_bps: float,
    slope_down_bps: float,
) -> List[Tuple[int, float, str]]:
    """
    Simple deterministic suggestion layer for quick strategy debug:
    - slope over lookback > up threshold: suggest BUY YES
    - slope over lookback < down threshold: suggest BUY NO
    """
    if len(rows) < 3:
        return []
    out: List[Tuple[int, float, str]] = []
    lookback_ms = max(1, lookback_sec) * 1000
    i0 = 0
    last_label = ""
    for i, cur in enumerate(rows):
        while i0 < i and rows[i0]["ts_ms"] < cur["ts_ms"] - lookback_ms:
            i0 += 1
        base = rows[i0]
        dt_s = max(1e-3, (cur["ts_ms"] - base["ts_ms"]) / 1000.0)
        dp = cur["mid_yes"] - base["mid_yes"]
        slope_bps_per_sec = (dp * 10_000.0) / dt_s
        label = ""
        if slope_bps_per_sec >= slope_up_bps:
            label = "SUGGEST_BUY_YES"
        elif slope_bps_per_sec <= slope_down_bps:
            label = "SUGGEST_BUY_NO"
        if label and label != last_label:
            out.append((cur["ts_ms"], cur["mid_yes"], label))
            last_label = label
        elif not label:
            last_label = ""
    return out


def load_summary(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def build_dataset(
    price_by_market: Dict[str, List[dict]],
    shots_by_market: Dict[str, List[ShotMarker]],
    fills_by_market: Dict[str, List[FillMarker]],
    summary: dict,
    lookback_sec: int,
    slope_up_bps: float,
    slope_down_bps: float,
    min_span_sec: float,
) -> dict:
    markets = []
    for market_id, rows in price_by_market.items():
        if not rows:
            continue
        span_sec = (rows[-1]["ts_ms"] - rows[0]["ts_ms"]) / 1000.0
        if span_sec < min_span_sec:
            continue
        symbol = rows[0]["symbol"]
        timeframe = rows[0]["timeframe"]
        title = rows[0]["title"]
        t0 = rows[0]["ts_ms"]
        prices = [
            {"t_ms": r["ts_ms"], "t_sec": (r["ts_ms"] - t0) / 1000.0, "yes_pct": r["mid_yes"] * 100.0}
            for r in rows
        ]
        shots = [
            {
                "t_ms": s.t_ms,
                "t_sec": (s.t_ms - t0) / 1000.0,
                "yes_pct": s.yes_prob * 100.0,
                "label": s.label,
                "side": s.side,
                "style": s.style,
            }
            for s in shots_by_market.get(market_id, [])
        ]
        fills = [
            {
                "t_ms": f.t_ms,
                "t_sec": (f.t_ms - t0) / 1000.0,
                "yes_pct": f.yes_prob * 100.0,
                "label": f.label,
                "side": f.side,
                "style": f.style,
            }
            for f in fills_by_market.get(market_id, [])
        ]
        suggested = [
            {
                "t_ms": ts,
                "t_sec": (ts - t0) / 1000.0,
                "yes_pct": yp * 100.0,
                "label": lab,
            }
            for ts, yp, lab in suggest_points(rows, lookback_sec, slope_up_bps, slope_down_bps)
        ]
        markets.append(
            {
                "market_id": market_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "title": title,
                "span_sec": span_sec,
                "prices": prices,
                "shots": shots,
                "fills": fills,
                "suggested": suggested,
            }
        )
    markets.sort(key=lambda x: (x["symbol"], x["timeframe"], x["market_id"]))
    return {"summary": summary, "markets": markets}


def html_template(payload: dict, run_name: str) -> str:
    payload_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Roll v1 Dashboard - {run_name}</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body {{ font-family: ui-sans-serif, system-ui, Segoe UI, Arial; margin: 16px; background: #0b1220; color: #dbe4ff; }}
    .row {{ display: flex; gap: 12px; align-items: center; flex-wrap: wrap; margin-bottom: 10px; }}
    .card {{ background: #111b2f; border: 1px solid #263759; padding: 10px 12px; border-radius: 10px; }}
    .k {{ color: #8ea2cc; font-size: 12px; }}
    .v {{ font-size: 18px; font-weight: 700; }}
    select {{ background:#111b2f; color:#dbe4ff; border:1px solid #31476f; border-radius:8px; padding:8px; }}
    #chart {{ height: 72vh; }}
  </style>
</head>
<body>
  <h2>PolyEdge Roll v1 Strategy Dashboard</h2>
  <div class="row">
    <div class="card"><div class="k">Run</div><div class="v">{run_name}</div></div>
    <div class="card"><div class="k">Markets</div><div class="v" id="mcount">-</div></div>
    <div class="card"><div class="k">Eligible</div><div class="v" id="eligible">-</div></div>
    <div class="card"><div class="k">Executed</div><div class="v" id="executed">-</div></div>
    <div class="card"><div class="k">Outcomes</div><div class="v" id="outcomes">-</div></div>
  </div>
  <div class="row">
    <label>Market</label>
    <select id="marketSel"></select>
  </div>
  <div id="chart"></div>
  <script>
    const payload = {payload_json};
    const summary = payload.summary || {{}};
    const markets = payload.markets || [];
    document.getElementById('mcount').textContent = markets.length;
    document.getElementById('eligible').textContent = summary.eligible_count ?? '-';
    document.getElementById('executed').textContent = summary.executed_count ?? '-';
    document.getElementById('outcomes').textContent = summary.total_outcomes ?? '-';

    const sel = document.getElementById('marketSel');
    markets.forEach((m, i) => {{
      const o = document.createElement('option');
      o.value = String(i);
      o.textContent = `${{m.symbol}} ${{m.timeframe}} | ${{m.market_id}} | ${{m.title}}`;
      sel.appendChild(o);
    }});

    function mkTrace(x, y, name, mode, color, symbol='circle', size=7) {{
      return {{ x, y, name, mode, type:'scatter', marker: {{ color, symbol, size }}, line: {{ color, width: 2 }} }};
    }}

    function render(idx) {{
      const m = markets[idx];
      if (!m) return;
      const priceX = m.prices.map(p => p.t_sec);
      const priceY = m.prices.map(p => p.yes_pct);
      const shotsX = m.shots.map(p => p.t_sec);
      const shotsY = m.shots.map(p => p.yes_pct);
      const fillsX = m.fills.map(p => p.t_sec);
      const fillsY = m.fills.map(p => p.yes_pct);
      const sugX = m.suggested.map(p => p.t_sec);
      const sugY = m.suggested.map(p => p.yes_pct);

      const traces = [
        mkTrace(priceX, priceY, 'YES probability', 'lines', '#22d3ee'),
        mkTrace(shotsX, shotsY, 'Actual shots', 'markers', '#f59e0b', 'triangle-up', 10),
        mkTrace(fillsX, fillsY, 'Actual fills', 'markers', '#34d399', 'x', 10),
        mkTrace(sugX, sugY, 'Suggested points', 'markers', '#a78bfa', 'diamond', 9),
      ];
      traces[1].text = m.shots.map(s => s.label);
      traces[2].text = m.fills.map(s => s.label);
      traces[3].text = m.suggested.map(s => s.label);
      traces[1].hovertemplate = 't=%{{x:.2f}}s yes=%{{y:.2f}}%<br>%{{text}}<extra></extra>';
      traces[2].hovertemplate = 't=%{{x:.2f}}s yes=%{{y:.2f}}%<br>%{{text}}<extra></extra>';
      traces[3].hovertemplate = 't=%{{x:.2f}}s yes=%{{y:.2f}}%<br>%{{text}}<extra></extra>';

      const layout = {{
        title: `${{m.symbol}} ${{m.timeframe}} | ${{m.market_id}}`,
        paper_bgcolor: '#0b1220',
        plot_bgcolor: '#0b1220',
        font: {{ color: '#dbe4ff' }},
        xaxis: {{ title: 'Elapsed Time (s)', gridcolor: '#21304d' }},
        yaxis: {{ title: 'YES Probability (%)', gridcolor: '#21304d', range: [0, 100] }},
        legend: {{ orientation: 'h', x:0, y:1.15 }},
        margin: {{ t: 60, l: 60, r: 20, b: 55 }}
      }};
      Plotly.newPlot('chart', traces, layout, {{responsive:true}});
    }}

    sel.addEventListener('change', () => render(Number(sel.value)));
    if (markets.length > 0) {{
      sel.value = '0';
      render(0);
    }}
  </script>
</body>
</html>"""


def main() -> None:
    args = parse_args()
    inst_dir = resolve_inst_dir(Path(args.run_dir).resolve())
    run_name = inst_dir.parent.name if inst_dir.name == "inst1" else inst_dir.name
    normalized_date = latest_date_dir(inst_dir / "normalized")
    report_date = latest_date_dir(inst_dir / "reports")

    price_path = normalized_date / "market_price_tape.jsonl"
    shots_path = normalized_date / "shadow_shots.jsonl"
    fills_path = normalized_date / "fills.jsonl"
    summary_path = report_date / "shadow_live_latest.json"
    if not price_path.exists():
        raise FileNotFoundError(f"missing {price_path}")

    price_by_market = load_price_tape(price_path)
    shots_by_market = load_shots(shots_path)
    fills_by_market = load_fills(fills_path)
    summary = load_summary(summary_path)
    payload = build_dataset(
        price_by_market,
        shots_by_market,
        fills_by_market,
        summary,
        args.suggest_lookback_sec,
        args.suggest_slope_up_bps,
        args.suggest_slope_down_bps,
        args.min_span_sec,
    )

    if args.out:
        out = Path(args.out)
    else:
        out = Path("datasets/reports/web") / f"{run_name}_roll_v1_dashboard.html"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html_template(payload, run_name), encoding="utf-8")
    print(out)


if __name__ == "__main__":
    main()
