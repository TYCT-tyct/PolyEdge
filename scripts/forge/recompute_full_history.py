#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Dict, List

import requests


class CH:
    def __init__(self, url: str, timeout_sec: float = 30.0) -> None:
        self.url = url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.http = requests.Session()

    def query_json(self, sql: str) -> Dict:
        r = self.http.post(
            self.url,
            data=sql.encode("utf-8"),
            headers={"Content-Type": "text/plain; charset=utf-8"},
            timeout=self.timeout_sec,
        )
        r.raise_for_status()
        return r.json()

    def exec(self, sql: str) -> None:
        r = self.http.post(
            self.url,
            data=sql.encode("utf-8"),
            headers={"Content-Type": "text/plain; charset=utf-8"},
            timeout=self.timeout_sec,
        )
        r.raise_for_status()


def tf_sql_list(tfs: List[str]) -> str:
    return ",".join(f"'{x}'" for x in tfs)


def min_snapshot_ts(ch: CH, db: str, snapshot_table: str, symbol: str, tfs: List[str]) -> int:
    q = f"""
    SELECT min(ts_ireland_sample_ms) AS ts
    FROM {db}.{snapshot_table}
    WHERE symbol='{symbol}'
      AND timeframe IN ({tf_sql_list(tfs)})
    FORMAT JSON
    """
    rows = ch.query_json(q).get("data", [])
    if not rows:
        return 0
    ts = rows[0].get("ts")
    return int(ts) if isinstance(ts, int) else 0


def run_py(script: Path, args: List[str]) -> Dict:
    proc = subprocess.run(
        ["python3", str(script), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=True,
    )
    parsed: Dict | None = None
    for line in proc.stdout.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                parsed = obj
        except json.JSONDecodeError:
            continue
    if parsed is None:
        # Fallback: parse first JSON object block from mixed stdout.
        text = proc.stdout
        start = text.find("{")
        if start >= 0:
            depth = 0
            end = -1
            for i, ch in enumerate(text[start:], start=start):
                if ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        end = i
                        break
            if end > start:
                try:
                    obj = json.loads(text[start : end + 1])
                    if isinstance(obj, dict):
                        parsed = obj
                except json.JSONDecodeError:
                    pass
    if parsed is None:
        raise RuntimeError(
            f"cannot parse JSON from script output: {script}\nstdout={proc.stdout}\nstderr={proc.stderr}"
        )
    return parsed


def create_dataset_view(ch: CH, db: str, snapshot_table: str, round_table: str, symbol: str, tfs: List[str], view_name: str) -> None:
    tf_pred = tf_sql_list(tfs)
    sql = f"""
    CREATE OR REPLACE VIEW {db}.{view_name} AS
    SELECT
      s.ts_ireland_sample_ms,
      s.symbol,
      s.timeframe,
      s.market_id,
      s.round_id,
      s.target_price,
      s.binance_price,
      s.pm_live_btc_price,
      s.chainlink_price,
      s.bid_yes,
      s.ask_yes,
      s.bid_no,
      s.ask_no,
      s.mid_yes,
      s.mid_no,
      s.mid_yes_smooth,
      s.mid_no_smooth,
      s.delta_price,
      s.delta_pct,
      s.delta_pct_smooth,
      s.remaining_ms,
      s.velocity_bps_per_sec,
      s.acceleration,
      toUInt8(r.settle_price > r.target_price) AS round_outcome_up,
      if(toUInt8(r.settle_price > r.target_price)=1, 99.0, 0.0) AS resolved_up_cents,
      if(toUInt8(r.settle_price > r.target_price)=1, 0.0, 99.0) AS resolved_down_cents,
      coalesce(s.mid_yes_smooth, s.mid_yes) * 100.0 AS display_up_cents,
      (1.0 - coalesce(s.mid_yes_smooth, s.mid_yes)) * 100.0 AS display_down_cents
    FROM {db}.{snapshot_table} s
    LEFT JOIN {db}.{round_table} r ON s.round_id = r.round_id
    WHERE s.symbol='{symbol}'
      AND s.timeframe IN ({tf_pred})
    """
    ch.exec(sql)


def count_quality(ch: CH, db: str, snapshot_table: str, round_table: str, symbol: str, tfs: List[str]) -> Dict:
    tf_pred = tf_sql_list(tfs)
    q_rounds = f"""
    SELECT
      timeframe,
      count() AS rounds,
      sum(target_price<=0 OR NOT isFinite(target_price)) AS bad_target,
      sum(label_up != toUInt8(settle_price > target_price)) AS bad_label
    FROM {db}.{round_table}
    WHERE symbol='{symbol}'
      AND timeframe IN ({tf_pred})
    GROUP BY timeframe
    ORDER BY timeframe
    FORMAT JSON
    """
    q_snap = f"""
    SELECT
      timeframe,
      count() AS rows,
      sum(target_price IS NULL OR target_price<=0 OR NOT isFinite(target_price)) AS bad_target,
      sum(mid_yes<0 OR mid_yes>1 OR mid_no<0 OR mid_no>1 OR NOT isFinite(mid_yes) OR NOT isFinite(mid_no)) AS bad_mid,
      sum(mid_yes_smooth<0 OR mid_yes_smooth>1 OR mid_no_smooth<0 OR mid_no_smooth>1 OR NOT isFinite(mid_yes_smooth) OR NOT isFinite(mid_no_smooth)) AS bad_smooth
    FROM {db}.{snapshot_table}
    WHERE symbol='{symbol}'
      AND timeframe IN ({tf_pred})
    GROUP BY timeframe
    ORDER BY timeframe
    FORMAT JSON
    """
    return {
        "rounds": ch.query_json(q_rounds).get("data", []),
        "snapshot": ch.query_json(q_snap).get("data", []),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Recompute historical BTCUSDT dataset using latest PolyEdge logic.")
    ap.add_argument("--clickhouse-url", default="http://127.0.0.1:8123")
    ap.add_argument("--db", default="polyedge_forge")
    ap.add_argument("--snapshot-table", default="snapshot_100ms")
    ap.add_argument("--round-table", default="rounds")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--timeframes", default="5m,15m")
    ap.add_argument("--start-ms", type=int, default=0)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dataset-view", default="snapshot_100ms_dataset_v1")
    ap.add_argument("--report-json", default="")
    args = ap.parse_args()

    tfs = [x.strip() for x in args.timeframes.split(",") if x.strip()]
    if not tfs:
        raise SystemExit("timeframes must not be empty")

    ch = CH(args.clickhouse_url)
    start_ms = args.start_ms if args.start_ms > 0 else min_snapshot_ts(
        ch, args.db, args.snapshot_table, args.symbol, tfs
    )
    if start_ms <= 0:
        raise SystemExit("cannot determine start_ms")

    base = Path(__file__).resolve().parent
    repair_script = base / "repair_targets_backfill.py"
    rebuild_script = base / "rebuild_probability_columns.py"

    before = count_quality(ch, args.db, args.snapshot_table, args.round_table, args.symbol, tfs)
    out: Dict = {
        "start_ms": start_ms,
        "timeframes": tfs,
        "apply": bool(args.apply),
        "before": before,
    }

    if args.apply:
        out["repair_targets"] = run_py(
            repair_script,
            [
                "--clickhouse-url",
                args.clickhouse_url,
                "--db",
                args.db,
                "--snapshot-table",
                args.snapshot_table,
                "--round-table",
                args.round_table,
                "--symbol",
                args.symbol,
                "--timeframes",
                ",".join(tfs),
                "--start-ms",
                str(start_ms),
                "--apply",
                "--rewrite-smooth",
            ],
        )
        out["rebuild_prob"] = run_py(
            rebuild_script,
            [
                "--clickhouse-url",
                args.clickhouse_url,
                "--db",
                args.db,
                "--table",
                args.snapshot_table,
                "--symbol",
                args.symbol,
                "--timeframes",
                ",".join(tfs),
                "--start-ms",
                str(start_ms),
                "--apply",
            ],
        )

        # Force rounds label to match latest outcome rule.
        ch.exec(
            f"""
            ALTER TABLE {args.db}.{args.round_table}
            UPDATE label_up = settle_price > target_price
            WHERE symbol='{args.symbol}'
              AND timeframe IN ({tf_sql_list(tfs)})
              AND end_ts_ms >= {start_ms}
            """
        )
        create_dataset_view(
            ch,
            args.db,
            args.snapshot_table,
            args.round_table,
            args.symbol,
            tfs,
            args.dataset_view,
        )

    out["after"] = count_quality(ch, args.db, args.snapshot_table, args.round_table, args.symbol, tfs)
    out["dataset_view"] = f"{args.db}.{args.dataset_view}"

    if args.report_json:
        with open(args.report_json, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)

    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
