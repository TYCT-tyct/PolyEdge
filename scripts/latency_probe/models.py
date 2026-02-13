from __future__ import annotations

from dataclasses import dataclass

SYMBOL_TO_NAME = {
    "BTCUSDT": "Bitcoin",
    "ETHUSDT": "Ethereum",
    "SOLUSDT": "Solana",
    "XRPUSDT": "XRP",
}


@dataclass
class StageSample:
    ref_fetch_ms: float
    book_fetch_ms: float
    signal_us: float
    quote_us: float
    risk_us: float
    exec_us: float
    total_ms: float
    intents: int

