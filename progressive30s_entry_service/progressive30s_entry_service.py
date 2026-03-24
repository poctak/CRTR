#!/usr/bin/env python3
# accumulation_breakout_replay_single_run.py
# ------------------------------------------------------------
# Single-run historical replay (NO grid)
# Uses tuned "best" configuration
# ------------------------------------------------------------

import os
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import asyncpg


# ==========================================================
# Silent logging
# ==========================================================
logging.basicConfig(level=logging.CRITICAL)


# ==========================================================
# ENV helpers
# ==========================================================
def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v and v.strip() else default


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v and v.strip() else default
    except Exception:
        return default


def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v.strip() if v and v.strip() else default


def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")


def env_list(name: str, default: str = "") -> List[str]:
    v = os.getenv(name, default)
    return [x.strip().upper() for x in (v or "").split(",") if x.strip()]


# ==========================================================
# Config (FIXED BEST CONFIG)
# ==========================================================
@dataclass
class Config:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    candles_table: str
    symbols: List[str]
    btc_symbol: str

    start_ts: Optional[str]
    end_ts: Optional[str]
    max_bars_per_symbol: int

    # setup
    lookback_bars: int = 18
    setup_bars: int = 6
    compression_bars: int = 4

    # compression
    compression_range_pct_max: float = 0.0045
    compression_avg_range_pct_max: float = 0.0035

    # absorption
    absorption_min_count: int = 2
    absorption_delta_ratio_max: float = -0.25
    absorption_max_down_move_pct: float = 0.0035

    # accumulation
    accumulation_min_count: int = 2
    accumulation_buy_ratio_min: float = 0.65
    accumulation_delta_ratio_min: float = 0.18
    accumulation_max_move_pct: float = 0.0035

    # trigger
    trigger_change_pct_min: float = 0.0035
    trigger_range_pct_min: float = 0.0045
    trigger_close_pos_min: float = 0.80
    trigger_volume_vs_setup_avg_min: float = 2.5
    trigger_buy_ratio_min: float = 0.60
    trigger_delta_ratio_min: float = 0.15

    # liquidity
    min_setup_quote_volume_sum: float = 12000.0
    min_trigger_quote_volume: float = 2000.0
    min_avg_trade_quote: float = 50.0

    # resistance
    resistance_lookback_bars: int = 8
    breakout_above_recent_close_pct: float = 0.0010

    # filters
    min_score: int = 7
    max_distance_from_support_pct: float = 0.012

    # replay
    cooldown_bars_after_signal: int = 12
    allow_multiple_signals_per_symbol: bool = True

    # forward
    forward_bars: int = 72


def load_config() -> Config:
    return Config(
        db_host=env_str("DB_HOST", "db"),
        db_port=env_int("DB_PORT", 5432),
        db_name=env_str("DB_NAME", "pumpdb"),
        db_user=env_str("DB_USER", "pumpuser"),
        db_password=env_str("DB_PASSWORD", ""),

        candles_table=env_str("CANDLES_TABLE", "public.candles_5m"),
        symbols=env_list("SYMBOLS", ""),
        btc_symbol=env_str("BTC_SYMBOL", "BTCUSDC"),

        start_ts=env_str("START_TS", "") or None,
        end_ts=env_str("END_TS", "") or None,
        max_bars_per_symbol=env_int("MAX_BARS_PER_SYMBOL", 50000),
    )


# ==========================================================
# Models
# ==========================================================
@dataclass
class Candle:
    ts: datetime
    o: float
    h: float
    l: float
    c: float
    v_quote: float
    trades_count: int
    buy_ratio_quote: float
    taker_delta_ratio_quote: float
    change_pct: float
    range_pct: float
    body_pct: float
    close_pos_in_range: float
    avg_trade_quote: float
    is_green: bool
    is_red: bool


# ==========================================================
# Helpers
# ==========================================================
def avg(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def pct_change(a: float, b: float) -> float:
    return (b / a) - 1.0 if a > 0 else 0.0


# ==========================================================
# Core logic (zkráceno – stejné jako v grid verzi)
# ==========================================================
def analyze_symbol(history: List[Candle], cfg: Config) -> bool:
    setup = history[-1 - cfg.setup_bars:-1]
    trigger = history[-1]

    if sum(c.v_quote for c in setup) < cfg.min_setup_quote_volume_sum:
        return False

    # compression
    last = setup[-cfg.compression_bars:]
    if max(c.range_pct for c in last) > cfg.compression_range_pct_max:
        return False

    # trigger
    if trigger.change_pct < cfg.trigger_change_pct_min:
        return False
    if trigger.range_pct < cfg.trigger_range_pct_min:
        return False
    if trigger.close_pos_in_range < cfg.trigger_close_pos_min:
        return False

    return True


def compute_forward(hist: List[Candle], idx: int, forward_bars: int):
    entry = hist[idx]
    future = hist[idx + 1: idx + 1 + forward_bars]
    if not future:
        return None

    max_price = max(c.h for c in future)
    min_price = min(c.l for c in future)

    return (
        pct_change(entry.c, max_price),
        pct_change(entry.c, min_price),
    )


def evaluate(cfg: Config, histories: Dict[str, List[Candle]]):
    profits = []
    drawdowns = []

    for sym in cfg.symbols:
        hist = histories[sym]
        for i in range(cfg.lookback_bars, len(hist)):
            if not analyze_symbol(hist[:i+1], cfg):
                continue

            fwd = compute_forward(hist, i, cfg.forward_bars)
            if fwd:
                profits.append(fwd[0])
                drawdowns.append(fwd[1])

    return len(profits), avg(profits), avg(drawdowns)


# ==========================================================
# Runner
# ==========================================================
async def run():
    cfg = load_config()

    pool = await asyncpg.create_pool(
        host=cfg.db_host,
        port=cfg.db_port,
        database=cfg.db_name,
        user=cfg.db_user,
        password=cfg.db_password,
    )

    histories = {}
    for sym in cfg.symbols:
        rows = await pool.fetch(
            f"SELECT * FROM {cfg.candles_table} WHERE symbol=$1 ORDER BY ts ASC",
            sym,
        )
        histories[sym] = [Candle(**r) for r in rows]

    count, profit, dd = evaluate(cfg, histories)

    print(f"valid_forward_samples={count}")
    print(f"avg_profit_max={profit*100:.3f}%")
    print(f"avg_drawdown_min={dd*100:.3f}%")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(run())
