#!/usr/bin/env python3
from __future__ import annotations

import argparse
import dataclasses
import json
import math
import time
from typing import Dict, List, Optional, Tuple

import requests


CHAINLINK_BACKED = {"5m", "15m", "4h"}


@dataclasses.dataclass
class RoundRow:
    round_id: str
    market_id: str
    symbol: str
    timeframe: str
    start_ts_ms: int
    end_ts_ms: int
    settle_price: float
    target_price: float


@dataclasses.dataclass
class RepairDecision:
    round_id: str
    timeframe: str
    start_ts_ms: int
    end_ts_ms: int
    old_target: float
    new_target: float
    source: str
    settle_price: float
    changed: bool


def tf_to_vatic(tf: str) -> Optional[str]:
    return {
        "5m": "5min",
        "15m": "15min",
        "30m": "30min",
        "1h": "1hour",
        "2h": "2hour",
        "4h": "4hour",
    }.get(tf)


def aligned_start_sec(start_ms: int, tf: str) -> int:
    sec = start_ms // 1000
    bucket = {
        "5m": 5 * 60,
        "15m": 15 * 60,
        "30m": 30 * 60,
        "1h": 60 * 60,
        "2h": 2 * 60 * 60,
        "4h": 4 * 60 * 60,
    }.get(tf, 5 * 60)
    return (sec // bucket) * bucket


class CH:
    def __init__(self, url: str, timeout_sec: float = 20.0) -> None:
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


def parse_target(v: Dict) -> Optional[float]:
    candidates = []
    dp = v.get("datapoint")
    if isinstance(dp, dict):
        candidates.append(dp.get("price"))
    candidates.extend([v.get("target_price"), v.get("target"), v.get("price")])
    for c in candidates:
        if isinstance(c, (int, float)) and math.isfinite(c) and c > 0:
            return float(c)
    return None


def vatic_get(url: str, timeout: float) -> Optional[Dict]:
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code != 200:
            return None
        return r.json()
    except requests.RequestException:
        return None


def official_from_snapshot(ch: CH, db: str, snap_tbl: str, rr: RoundRow) -> Optional[float]:
    start = rr.start_ts_ms
    end = min(rr.end_ts_ms, start + 120_000)
    if rr.timeframe in CHAINLINK_BACKED:
        q = f"""
        SELECT argMin(chainlink_price, abs(ts_ireland_sample_ms - {start})) AS v
        FROM {db}.{snap_tbl}
        WHERE round_id = '{rr.round_id}'
          AND chainlink_price IS NOT NULL
          AND ts_ireland_sample_ms BETWEEN {start - 120_000} AND {end}
        FORMAT JSON
        """
    else:
        q = f"""
        SELECT argMin(binance_price, abs(ts_ireland_sample_ms - {start})) AS v
        FROM {db}.{snap_tbl}
        WHERE round_id = '{rr.round_id}'
          AND binance_price IS NOT NULL
          AND ts_ireland_sample_ms BETWEEN {start - 120_000} AND {end}
        FORMAT JSON
        """
    rows = ch.query_json(q).get("data", [])
    if not rows:
        return None
    v = rows[0].get("v")
    if isinstance(v, (int, float)) and math.isfinite(v) and v > 0:
        return float(v)
    return None


def first_missing_ts(ch: CH, db: str, snap_tbl: str, symbol: str, tfs: List[str]) -> Optional[int]:
    tf_list = ",".join(f"'{x}'" for x in tfs)
    q = f"""
    SELECT min(ts_ireland_sample_ms) AS ts
    FROM {db}.{snap_tbl}
    WHERE symbol = '{symbol}'
      AND timeframe IN ({tf_list})
      AND (target_price IS NULL OR target_price <= 0 OR NOT isFinite(target_price))
    FORMAT JSON
    """
    rows = ch.query_json(q).get("data", [])
    if not rows:
        return None
    ts = rows[0].get("ts")
    if isinstance(ts, int) and ts > 0:
        return ts
    return None


def load_rounds(ch: CH, db: str, round_tbl: str, symbol: str, tfs: List[str], start_ms: int) -> List[RoundRow]:
    tf_list = ",".join(f"'{x}'" for x in tfs)
    q = f"""
    SELECT round_id, market_id, symbol, timeframe, start_ts_ms, end_ts_ms, settle_price, target_price
    FROM {db}.{round_tbl}
    WHERE symbol = '{symbol}'
      AND timeframe IN ({tf_list})
      AND end_ts_ms >= {start_ms}
    ORDER BY start_ts_ms ASC
    FORMAT JSON
    """
    rows = ch.query_json(q).get("data", [])
    out: List[RoundRow] = []
    for r in rows:
        out.append(
            RoundRow(
                round_id=str(r["round_id"]),
                market_id=str(r["market_id"]),
                symbol=str(r["symbol"]),
                timeframe=str(r["timeframe"]),
                start_ts_ms=int(r["start_ts_ms"]),
                end_ts_ms=int(r["end_ts_ms"]),
                settle_price=float(r["settle_price"]),
                target_price=float(r["target_price"]),
            )
        )
    return out


def decide_target(
    ch: CH,
    db: str,
    snap_tbl: str,
    rr: RoundRow,
    vatic_timeout: float,
    cache: Dict[Tuple[str, int], Tuple[float, str]],
) -> Optional[Tuple[float, str]]:
    sec = aligned_start_sec(rr.start_ts_ms, rr.timeframe)
    cache_key = (rr.timeframe, sec)
    if cache_key in cache:
        return cache[cache_key]

    asset = rr.symbol.removesuffix("USDT").lower()
    vt = tf_to_vatic(rr.timeframe)
    if vt:
        j = vatic_get(
            f"https://api.vatic.trading/api/v1/history/market?asset={asset}&type={vt}&marketStart={sec}",
            timeout=vatic_timeout,
        )
        if j:
            v = parse_target(j)
            if v:
                cache[cache_key] = (v, "vatic_market")
                return cache[cache_key]

        j = vatic_get(
            f"https://api.vatic.trading/api/v1/targets/timestamp?asset={asset}&type={vt}&timestamp={sec}",
            timeout=vatic_timeout,
        )
        if j:
            v = parse_target(j)
            if v:
                cache[cache_key] = (v, "vatic_timestamp")
                return cache[cache_key]

        j = vatic_get(
            f"https://api.vatic.trading/api/v1/targets/active?asset={asset}&types={vt}",
            timeout=vatic_timeout,
        )
        if j:
            # active may return top-level price or result array.
            v = parse_target(j)
            if not v and isinstance(j.get("results"), list):
                for item in j["results"]:
                    if str(item.get("marketType", "")).lower() == vt.lower():
                        v = parse_target(item)
                        if v:
                            break
            if v:
                cache[cache_key] = (v, "vatic_active")
                return cache[cache_key]

    off = official_from_snapshot(ch, db, snap_tbl, rr)
    if off:
        src = "official_chainlink_snapshot" if rr.timeframe in CHAINLINK_BACKED else "official_binance_snapshot"
        cache[cache_key] = (off, src)
        return cache[cache_key]

    return None


def apply_updates(
    ch: CH,
    db: str,
    snap_tbl: str,
    round_tbl: str,
    decisions: List[RepairDecision],
    rewrite_smooth: bool,
) -> None:
    for d in decisions:
        if not d.changed:
            continue
        t = d.new_target
        smooth_expr = (
            f"delta_pct_smooth = if(isNull(binance_price), NULL, ((binance_price - {t}) / {t}) * 100.0),"
            if rewrite_smooth
            else ""
        )
        q_snap = f"""
        ALTER TABLE {db}.{snap_tbl}
        UPDATE
            target_price = {t},
            delta_price = if(isNull(binance_price), NULL, binance_price - {t}),
            delta_pct = if(isNull(binance_price), NULL, ((binance_price - {t}) / {t}) * 100.0),
            {smooth_expr}
            schema_version = schema_version
        WHERE round_id = '{d.round_id}'
        """
        q_round = f"""
        ALTER TABLE {db}.{round_tbl}
        UPDATE
            target_price = {t},
            label_up = settle_price > {t}
        WHERE round_id = '{d.round_id}'
        """
        ch.exec(q_snap)
        ch.exec(q_round)


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill and repair target_price/delta for polyedge_forge history.")
    ap.add_argument("--clickhouse-url", default="http://127.0.0.1:8123")
    ap.add_argument("--db", default="polyedge_forge")
    ap.add_argument("--snapshot-table", default="snapshot_100ms")
    ap.add_argument("--round-table", default="rounds")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--timeframes", default="5m,15m")
    ap.add_argument("--start-ms", type=int, default=0, help="If 0, auto-detect earliest missing target row.")
    ap.add_argument("--limit-rounds", type=int, default=0)
    ap.add_argument("--vatic-timeout-sec", type=float, default=2.0)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--rewrite-smooth", action="store_true")
    ap.add_argument("--report-json", default="")
    args = ap.parse_args()

    tfs = [x.strip() for x in args.timeframes.split(",") if x.strip()]
    ch = CH(args.clickhouse_url)

    start_ms = args.start_ms
    if start_ms <= 0:
        auto = first_missing_ts(ch, args.db, args.snapshot_table, args.symbol, tfs)
        if not auto:
            print("no missing target_price rows found for requested symbol/timeframes")
            return
        start_ms = auto

    rounds = load_rounds(ch, args.db, args.round_table, args.symbol, tfs, start_ms)
    if args.limit_rounds > 0:
        rounds = rounds[: args.limit_rounds]
    if not rounds:
        print("no rounds found to repair")
        return

    cache: Dict[Tuple[str, int], Tuple[float, str]] = {}
    decisions: List[RepairDecision] = []
    for rr in rounds:
        picked = decide_target(
            ch,
            args.db,
            args.snapshot_table,
            rr,
            vatic_timeout=args.vatic_timeout_sec,
            cache=cache,
        )
        if not picked:
            continue
        new_target, source = picked
        old_target = rr.target_price
        changed = not (math.isfinite(old_target) and abs(old_target - new_target) < 1e-9)
        decisions.append(
            RepairDecision(
                round_id=rr.round_id,
                timeframe=rr.timeframe,
                start_ts_ms=rr.start_ts_ms,
                end_ts_ms=rr.end_ts_ms,
                old_target=old_target,
                new_target=new_target,
                source=source,
                settle_price=rr.settle_price,
                changed=changed,
            )
        )

    changed = [d for d in decisions if d.changed]
    by_source: Dict[str, int] = {}
    for d in changed:
        by_source[d.source] = by_source.get(d.source, 0) + 1

    summary = {
        "start_ms": start_ms,
        "rounds_total": len(rounds),
        "rounds_decided": len(decisions),
        "rounds_changed": len(changed),
        "by_source": by_source,
        "apply": args.apply,
        "rewrite_smooth": args.rewrite_smooth,
        "sample": [dataclasses.asdict(d) for d in changed[:20]],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if args.report_json:
        with open(args.report_json, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "summary": summary,
                    "all_decisions": [dataclasses.asdict(d) for d in decisions],
                },
                f,
                ensure_ascii=False,
                indent=2,
            )

    if not args.apply:
        return

    if not changed:
        print("no rows need update")
        return

    apply_updates(
        ch,
        args.db,
        args.snapshot_table,
        args.round_table,
        changed,
        rewrite_smooth=args.rewrite_smooth,
    )
    print("backfill mutations submitted")

    # Give immediate feedback that mutations are queued.
    time.sleep(0.2)
    m = ch.query_json(
        f"SELECT table, mutation_id, is_done FROM system.mutations "
        f"WHERE database = '{args.db}' AND table IN ('{args.snapshot_table}','{args.round_table}') "
        f"ORDER BY create_time DESC LIMIT 10 FORMAT JSON"
    )
    print(json.dumps({"latest_mutations": m.get("data", [])}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

