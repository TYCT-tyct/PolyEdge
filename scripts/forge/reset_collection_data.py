#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any, Dict, List

import requests


class CH:
    def __init__(self, url: str, timeout_sec: float = 30.0) -> None:
        self.url = url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.http = requests.Session()

    def query_json(self, sql: str) -> Dict[str, Any]:
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


def count_rows(ch: CH, db: str, table: str) -> int:
    q = f"SELECT count() AS n FROM {db}.{table} FORMAT JSON"
    rows = ch.query_json(q).get("data", [])
    if not rows:
        return 0
    return int(rows[0].get("n", 0))


def table_exists(ch: CH, db: str, table: str) -> bool:
    q = (
        "SELECT count() AS n "
        "FROM system.tables "
        f"WHERE database='{db}' AND name='{table}' "
        "FORMAT JSON"
    )
    rows = ch.query_json(q).get("data", [])
    if not rows:
        return False
    return int(rows[0].get("n", 0)) > 0


def redis_delete_prefix(redis_url: str, prefix: str, apply: bool) -> Dict[str, Any]:
    try:
        import redis
    except ImportError:
        return {
            "skipped": True,
            "reason": "python redis package not installed",
        }

    cli = redis.Redis.from_url(redis_url, decode_responses=False)
    cursor = 0
    keys: List[bytes] = []
    pattern = f"{prefix}:*".encode("utf-8")
    while True:
        cursor, batch = cli.scan(cursor=cursor, match=pattern, count=2000)
        if batch:
            keys.extend(batch)
        if cursor == 0:
            break

    deleted = 0
    if apply and keys:
        pipe = cli.pipeline(transaction=False)
        for k in keys:
            pipe.unlink(k)
        res = pipe.execute()
        deleted = sum(int(v or 0) for v in res)

    return {
        "matched": len(keys),
        "deleted": deleted,
    }


def reset_local_dirs(data_root: Path, apply: bool) -> Dict[str, Any]:
    targets = [
        data_root / "snapshot_100ms",
        data_root / "rounds",
        data_root / "ingest_log",
    ]
    existing = [str(p) for p in targets if p.exists()]
    if apply:
        for p in targets:
            if p.exists():
                shutil.rmtree(p, ignore_errors=True)
            p.mkdir(parents=True, exist_ok=True)
    return {"existing": existing, "reset": bool(apply)}


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Reset PolyEdge data collection state (ClickHouse + Redis + local persisted files)."
    )
    ap.add_argument("--clickhouse-url", default="http://127.0.0.1:8123")
    ap.add_argument("--db", default="polyedge_forge")
    ap.add_argument("--snapshot-table", default="snapshot_100ms")
    ap.add_argument("--round-table", default="rounds")
    ap.add_argument("--processed-table", default="snapshot_100ms_processed")
    ap.add_argument("--redis-url", default="")
    ap.add_argument("--redis-prefix", default="forge")
    ap.add_argument("--data-root", default="/data/polyedge-forge")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--report-json", default="")
    args = ap.parse_args()

    ch = CH(args.clickhouse_url)
    before: Dict[str, Any] = {}
    targets = [args.snapshot_table, args.round_table, args.processed_table]
    for t in targets:
        if table_exists(ch, args.db, t):
            before[t] = count_rows(ch, args.db, t)
        else:
            before[t] = None

    out: Dict[str, Any] = {
        "apply": bool(args.apply),
        "clickhouse_url": args.clickhouse_url,
        "db": args.db,
        "before_counts": before,
    }

    if args.apply:
        for t in targets:
            if before[t] is not None:
                ch.exec(f"TRUNCATE TABLE {args.db}.{t}")

    after: Dict[str, Any] = {}
    for t in targets:
        if table_exists(ch, args.db, t):
            after[t] = count_rows(ch, args.db, t)
        else:
            after[t] = None
    out["after_counts"] = after

    if args.redis_url.strip():
        out["redis"] = redis_delete_prefix(args.redis_url, args.redis_prefix, args.apply)
    else:
        out["redis"] = {"skipped": True}

    out["local_files"] = reset_local_dirs(Path(args.data_root), args.apply)

    if args.report_json:
        with open(args.report_json, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)

    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
