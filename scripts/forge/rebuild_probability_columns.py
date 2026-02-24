#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from typing import Dict, List

import requests


class CH:
    def __init__(self, url: str, timeout_sec: float = 30.0) -> None:
        self.url = url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.http = requests.Session()

    def query_json(self, sql: str) -> Dict:
        resp = self.http.post(
            self.url,
            data=sql.encode("utf-8"),
            headers={"Content-Type": "text/plain; charset=utf-8"},
            timeout=self.timeout_sec,
        )
        resp.raise_for_status()
        return resp.json()

    def exec(self, sql: str) -> None:
        resp = self.http.post(
            self.url,
            data=sql.encode("utf-8"),
            headers={"Content-Type": "text/plain; charset=utf-8"},
            timeout=self.timeout_sec,
        )
        resp.raise_for_status()


def tf_sql_list(tfs: List[str]) -> str:
    return ",".join(f"'{x}'" for x in tfs)


def min_start_ms(ch: CH, db: str, table: str, symbol: str, tfs: List[str]) -> int:
    q = f"""
    SELECT min(ts_ireland_sample_ms) AS ts
    FROM {db}.{table}
    WHERE symbol='{symbol}'
      AND timeframe IN ({tf_sql_list(tfs)})
    FORMAT JSON
    """
    rows = ch.query_json(q).get("data", [])
    if not rows:
        return 0
    ts = rows[0].get("ts")
    return int(ts) if isinstance(ts, int) else 0


def row_count(ch: CH, db: str, table: str, symbol: str, tfs: List[str], start_ms: int) -> int:
    q = f"""
    SELECT count() AS n
    FROM {db}.{table}
    WHERE symbol='{symbol}'
      AND timeframe IN ({tf_sql_list(tfs)})
      AND ts_ireland_sample_ms >= {start_ms}
    FORMAT JSON
    """
    rows = ch.query_json(q).get("data", [])
    if not rows:
        return 0
    n = rows[0].get("n")
    return int(n) if isinstance(n, int) else 0


def build_update_sql(db: str, table: str, symbol: str, tfs: List[str], start_ms: int) -> str:
    yes_bid = "least(greatest(coalesce(bid_yes, 0.5), 0.0), 1.0)"
    yes_ask = "least(greatest(coalesce(ask_yes, 0.5), 0.0), 1.0)"
    no_bid = "least(greatest(coalesce(bid_no, 0.5), 0.0), 1.0)"
    no_ask = "least(greatest(coalesce(ask_no, 0.5), 0.0), 1.0)"
    up_yes = f"(({yes_bid} + {yes_ask}) * 0.5)"
    up_no = f"(1.0 - (({no_bid} + {no_ask}) * 0.5))"
    spread_yes = f"abs({yes_ask} - {yes_bid})"
    spread_no = f"abs({no_ask} - {no_bid})"
    diff = f"abs({up_yes} - {up_no})"
    up = (
        "greatest(0.0, least(1.0, "
        f"if({diff} <= 0.10, ({up_yes} + {up_no}) * 0.5, "
        f"if({spread_yes} + 0.005 < {spread_no}, {up_yes}, "
        f"if({spread_no} + 0.005 < {spread_yes}, {up_no}, ({up_yes} + {up_no}) * 0.5)))"
        "))"
    )
    down = f"(1.0 - ({up}))"
    return f"""
    ALTER TABLE {db}.{table}
    UPDATE
      mid_yes = {up},
      mid_no = {down},
      mid_yes_smooth = {up},
      mid_no_smooth = {down},
      delta_pct_smooth = delta_pct
    WHERE symbol='{symbol}'
      AND timeframe IN ({tf_sql_list(tfs)})
      AND ts_ireland_sample_ms >= {start_ms}
    """


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Rebuild probability columns from bid/ask and reset old smoothing."
    )
    ap.add_argument("--clickhouse-url", default="http://127.0.0.1:8123")
    ap.add_argument("--db", default="polyedge_forge")
    ap.add_argument("--table", default="snapshot_100ms")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--timeframes", default="5m,15m")
    ap.add_argument("--start-ms", type=int, default=0)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--report-json", default="")
    args = ap.parse_args()

    tfs = [x.strip() for x in args.timeframes.split(",") if x.strip()]
    if not tfs:
        raise SystemExit("timeframes must not be empty")

    ch = CH(args.clickhouse_url)
    start_ms = args.start_ms if args.start_ms > 0 else min_start_ms(
        ch, args.db, args.table, args.symbol, tfs
    )
    if start_ms <= 0:
        raise SystemExit("cannot determine start_ms")

    affected_rows = row_count(ch, args.db, args.table, args.symbol, tfs, start_ms)
    sql = build_update_sql(args.db, args.table, args.symbol, tfs, start_ms).strip()

    report = {
        "symbol": args.symbol,
        "timeframes": tfs,
        "start_ms": start_ms,
        "affected_rows": affected_rows,
        "apply": bool(args.apply),
        "status": "dry_run",
    }

    if args.apply and affected_rows > 0:
        ch.exec(sql)
        report["status"] = "mutation_submitted"

    if args.report_json:
        with open(args.report_json, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
