#!/usr/bin/env python3
from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "metrics_contract.md"
SOURCES = [
    ROOT / "crates" / "app_runner" / "src" / "main.rs",
    ROOT / "crates" / "app_runner" / "src" / "control_api.rs",
    ROOT / "scripts" / "runtime_watchdog.py",
    ROOT / "scripts" / "generate_morning_report.py",
    ROOT / "scripts" / "param_regression.py",
    ROOT / "scripts" / "long_regression_orchestrator.py",
]

REQUIRED_FIELDS = [
    "data_valid_ratio",
    "seq_gap_rate",
    "ts_inversion_rate",
    "stale_tick_drop_ratio",
    "tick_to_ack_p99_ms",
    "decision_compute_p99_ms",
    "feed_in_p99_ms",
    "executed_over_eligible",
    "quote_block_ratio",
    "policy_block_ratio",
    "ev_net_usdc_p50",
    "roi_notional_10s_bps_p50",
    "gate_ready",
    "gate_fail_reasons",
    "window_id",
]


def die(msg: str) -> int:
    print(f"[metrics-contract] FAIL: {msg}")
    return 1


def read(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(path)
    return path.read_text(encoding="utf-8")


def main() -> int:
    try:
        doc_text = read(DOC)
    except FileNotFoundError:
        return die(f"missing {DOC}")

    source_text = ""
    for path in SOURCES:
        try:
            source_text += "\n" + read(path)
        except FileNotFoundError:
            return die(f"missing source file: {path}")

    m_doc = re.search(r"Version:\s*`([^`]+)`", doc_text)
    if not m_doc:
        return die("unable to read version from docs/metrics_contract.md")
    contract_version = m_doc.group(1)

    m_code = re.search(r'"metrics_contract_version":\s*"([^"]+)"', source_text)
    if not m_code:
        return die("metrics_contract_version not found in code paths")
    code_version = m_code.group(1)
    if contract_version != code_version:
        return die(
            f"version mismatch docs={contract_version} code={code_version}. "
            "update both in one change."
        )

    missing = [k for k in REQUIRED_FIELDS if k not in source_text]
    if missing:
        return die(f"required fields missing from code/scripts: {', '.join(missing)}")

    print(
        "[metrics-contract] PASS: "
        f"version={contract_version}, fields={len(REQUIRED_FIELDS)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
