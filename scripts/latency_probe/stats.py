from __future__ import annotations

import math
import statistics
from typing import Dict, List


def percentile(values: List[float], p: float) -> float:
    if not values:
        return float("nan")
    if len(values) == 1:
        return values[0]
    sorted_values = sorted(values)
    idx = int(math.ceil(p * len(sorted_values))) - 1
    idx = max(0, min(idx, len(sorted_values) - 1))
    return sorted_values[idx]


def summarize(values: List[float]) -> Dict[str, float]:
    if not values:
        return {"n": 0}
    return {
        "n": float(len(values)),
        "avg": statistics.mean(values),
        "p50": percentile(values, 0.50),
        "p90": percentile(values, 0.90),
        "p99": percentile(values, 0.99),
        "min": min(values),
        "max": max(values),
    }


def print_stat_block(name: str, stat: Dict[str, float], unit: str) -> None:
    if stat.get("n", 0) == 0:
        print(f"{name}: no data")
        return
    print(
        f"{name:<18} n={int(stat['n']):4d} "
        f"avg={stat['avg']:.3f}{unit} p50={stat['p50']:.3f}{unit} "
        f"p90={stat['p90']:.3f}{unit} p99={stat['p99']:.3f}{unit} "
        f"min={stat['min']:.3f}{unit} max={stat['max']:.3f}{unit}"
    )

