#!/usr/bin/env python3
import argparse
import csv
import json
import os
import socket
import subprocess
import sys
import time
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, TextIO

import requests


def now_run_id() -> str:
    return time.strftime("paper-%Y%m%d-%H%M%S", time.gmtime())


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_binary() -> Path:
    name = "app_runner.exe" if os.name == "nt" else "app_runner"
    return repo_root() / "target" / "release" / name


def fallback_debug_binary() -> Path:
    name = "app_runner.exe" if os.name == "nt" else "app_runner"
    return repo_root() / "target" / "debug" / name


def bool_env(v: bool) -> str:
    return "true" if v else "false"


def get_json(base_url: str, path: str, timeout: float = 5.0) -> Optional[dict]:
    try:
        resp = requests.get(f"{base_url.rstrip('/')}{path}", timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None
@dataclass
class InstanceRun:
    idx: int
    run_id: str
    base_url: str
    dataset_root: Path
    proc: subprocess.Popen
    strategy_config: Optional[str]
    log_path: Path
    log_file: TextIO
    initial_capital: float


def spawn_instance(
    idx: int,
    run_id: str,
    binary_path: Path,
    control_port: int,
    initial_capital: float,
    dataset_root: Path,
    strategy_config: Optional[str],
) -> InstanceRun:
    udp_port = 20_000 + idx * 1_000
    inst_run_id = f"{run_id}-i{idx + 1}"
    inst_dataset = dataset_root / run_id / f"inst{idx + 1}"
    inst_dataset.mkdir(parents=True, exist_ok=True)
    log_path = inst_dataset / "app_runner.log"
    log_file = log_path.open("w", encoding="utf-8")
    env = os.environ.copy()
    env["POLYEDGE_FORCE_PAPER"] = bool_env(True)
    env["POLYEDGE_PAPER_ENABLED"] = bool_env(True)
    env["POLYEDGE_PAPER_SQLITE_ENABLED"] = bool_env(True)
    env["POLYEDGE_PAPER_RUN_ID"] = inst_run_id
    env["POLYEDGE_PAPER_INITIAL_CAPITAL"] = str(initial_capital)
    env["POLYEDGE_SEAT_ENABLED"] = bool_env(False)
    env["POLYEDGE_CONTROL_PORT"] = str(control_port)
    env["POLYEDGE_UDP_PORT"] = str(udp_port)
    env["POLYEDGE_DATASET_ROOT"] = str(inst_dataset)
    # Force per-instance UDP listener isolation even if parent shell exported
    # symbol->port maps for production wire setups.
    env.pop("POLYEDGE_UDP_SYMBOL_PORTS", None)
    env.pop("POLYEDGE_UDP_PIN_CORES", None)
    if strategy_config:
        env["POLYEDGE_STRATEGY_CONFIG_PATH"] = strategy_config

    proc = subprocess.Popen(
        [str(binary_path)],
        cwd=str(repo_root()),
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return InstanceRun(
        idx=idx,
        run_id=inst_run_id,
        base_url=f"http://127.0.0.1:{control_port}",
        dataset_root=inst_dataset,
        proc=proc,
        strategy_config=strategy_config,
        log_path=log_path,
        log_file=log_file,
        initial_capital=initial_capital,
    )


def pick_available_port(start: int, used: set[int]) -> int:
    for port in range(max(1025, start), 65535):
        if port in used:
            continue
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("127.0.0.1", port))
            except OSError:
                continue
        used.add(port)
        return port
    raise RuntimeError("unable to find available control port")


def wait_ready(inst: InstanceRun, timeout_sec: int = 30) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if inst.proc.poll() is not None:
            return False
        payload = get_json(inst.base_url, "/health", timeout=1.5)
        if payload and payload.get("status") == "ok":
            return True
        time.sleep(0.5)
    return False


def stop_instance(inst: InstanceRun) -> None:
    try:
        if inst.proc.poll() is None:
            try:
                inst.proc.terminate()
                inst.proc.wait(timeout=8)
            except Exception:
                raise  # Linus: Fail loudly and explicitly
    finally:
        inst.log_file.close()


def read_summary_from_sqlite(inst: InstanceRun) -> Optional[dict]:
    db_path = inst.dataset_root / "reports" / "paper_summary.sqlite"
    if not db_path.exists():
        return None
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            try:
                cursor.execute("SELECT * FROM paper_run_summary ORDER BY ts_ms DESC LIMIT 1")
                row = cursor.fetchone()
                if not row:
                    return None

                return {
                    "run_id": row["run_id"],
                    "roi_pct": row["roi_pct"],
                    "win_rate": row["win_rate"],
                    "fee_ratio": row["fee_ratio"],
                    "trades": row["trades"],
                    "pnl_total_usdc": row["pnl_total_usdc"],
                    "avg_trade_duration_ms": row["avg_trade_duration_ms"],
                    "median_trade_duration_ms": row["median_trade_duration_ms"],
                    "bankroll": row["bankroll"],
                }
            except sqlite3.OperationalError:
                # Table might not exist yet if Rust hasn't flushed
                return None

    except Exception as e:
        print(f"[warning] Failed to read sqlite reports for {inst.run_id}: {e}")
        return None

def collect_final_summary(inst: InstanceRun) -> dict:
    api = get_json(inst.base_url, "/report/paper/summary", timeout=3.0)
    if api:
        return api
    file_payload = read_summary_from_sqlite(inst)
    if file_payload:
        return file_payload
    return {
        "run_id": inst.run_id,
        "roi_pct": 0.0,
        "win_rate": 0.0,
        "fee_ratio": 0.0,
        "trades": 0,
        "pnl_total_usdc": 0.0,
        "avg_trade_duration_ms": 0.0,
        "median_trade_duration_ms": 0.0,
        "bankroll": inst.initial_capital,
    }


def write_parallel_compare(run_dir: Path, rows: List[dict]) -> Path:
    out = run_dir / "paper_parallel_compare.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "instance",
                "run_id",
                "strategy_config",
                "roi_pct",
                "win_rate",
                "fee_ratio",
                "trades",
                "pnl_total_usdc",
                "bankroll",
                "avg_trade_duration_ms",
                "median_trade_duration_ms",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return out


def run(args: argparse.Namespace) -> int:
    root = repo_root()
    if args.binary:
        binary = Path(args.binary)
        if not binary.exists():
            print(f"[error] app_runner binary not found: {binary}", file=sys.stderr)
            return 2
    else:
        release_bin = default_binary()
        debug_bin = fallback_debug_binary()
        if release_bin.exists():
            binary = release_bin
        elif debug_bin.exists():
            binary = debug_bin
        else:
            print(
                f"[error] app_runner binary not found: {release_bin} or {debug_bin}",
                file=sys.stderr,
            )
            print(
                "[hint] build first: cargo build -p app_runner --release (or --debug)",
                file=sys.stderr,
            )
            return 2

    instances = max(1, int(args.instances))
    run_id = args.run_id or now_run_id()
    dataset_root = Path(args.dataset_root)
    run_dir = dataset_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    strategy_configs: List[Optional[str]] = []
    for i in range(instances):
        key = f"config{i + 1}"
        strategy_configs.append(getattr(args, key))

    runners: List[InstanceRun] = []
    used_ports: set[int] = set()
    try:
        for i in range(instances):
            preferred_port = int(args.base_port) + i * max(2, int(args.port_stride))
            control_port = pick_available_port(preferred_port, used_ports)
            inst = spawn_instance(
                idx=i,
                run_id=run_id,
                binary_path=binary,
                control_port=control_port,
                initial_capital=float(args.initial_capital),
                dataset_root=dataset_root,
                strategy_config=strategy_configs[i],
            )
            runners.append(inst)
            ok = wait_ready(inst, timeout_sec=45)
            if not ok:
                print(f"[error] instance {i+1} failed health check: {inst.base_url}")
                print(f"[hint] inspect log: {inst.log_path}")
                return 3
            print(
                f"[ready] instance={i+1} run_id={inst.run_id} url={inst.base_url} log={inst.log_path}"
            )

        t_end = time.time() + int(args.duration)
        while time.time() < t_end:
            for inst in runners:
                live = get_json(inst.base_url, "/report/paper/live", timeout=2.5) or {}
                roi = float(live.get("roi_pct", 0.0))
                trades = int(live.get("trades", 0))
                win_rate = float(live.get("win_rate", 0.0))
                print(
                    f"[live] inst={inst.idx+1} roi={roi:.4f}% trades={trades} win_rate={win_rate:.4f}"
                )
            time.sleep(float(args.poll_sec))

        rows: List[dict] = []
        for inst in runners:
            summary = collect_final_summary(inst)
            row = {
                "instance": inst.idx + 1,
                "run_id": summary.get("run_id", inst.run_id),
                "strategy_config": inst.strategy_config or "",
                "roi_pct": float(summary.get("roi_pct", 0.0)),
                "win_rate": float(summary.get("win_rate", 0.0)),
                "fee_ratio": float(summary.get("fee_ratio", 0.0)),
                "trades": int(summary.get("trades", 0)),
                "pnl_total_usdc": float(summary.get("pnl_total_usdc", 0.0)),
                "bankroll": float(summary.get("bankroll", 0.0)),
                "avg_trade_duration_ms": float(summary.get("avg_trade_duration_ms", 0.0)),
                "median_trade_duration_ms": float(summary.get("median_trade_duration_ms", 0.0)),
            }
            rows.append(row)
            (run_dir / f"summary_inst{inst.idx+1}.json").write_text(
                json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
            )

        compare_path = write_parallel_compare(run_dir, rows)
        print(f"[done] compare_csv={compare_path}")
        return 0
    finally:
        for inst in runners:
            stop_instance(inst)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="PolyEdge Paper runner with optional parallel mode")
    p.add_argument("--run-id", default=None)
    p.add_argument("--duration", type=int, default=3600)
    p.add_argument("--initial-capital", type=float, default=100.0)
    p.add_argument("--instances", type=int, default=1)
    p.add_argument("--base-port", type=int, default=38080)
    p.add_argument("--port-stride", type=int, default=10)
    p.add_argument("--binary", default=None)
    p.add_argument("--poll-sec", type=float, default=5.0)
    p.add_argument("--dataset-root", default="datasets/paper_runs")
    for idx in range(1, 9):
        p.add_argument(f"--config{idx}", default=None)
    return p


if __name__ == "__main__":
    parser = build_parser()
    raise SystemExit(run(parser.parse_args()))
