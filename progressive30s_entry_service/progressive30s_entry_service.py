#!/usr/bin/env python3
# accumulation_breakout_replay_final.py
# ------------------------------------------------------------
# FINAL single-run replay (NO GRID)
# ------------------------------------------------------------

import os
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

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


def env_list(name: str, default: str = "") -> List[str]:
    v = os.getenv(name, default)
    return [x.strip().upper() for x in v.split(",") if x.strip()]


# ==========================================================
# Config (FINAL BEST)
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

    lookback_bars: int
    setup_bars: int
    compression_bars: int

    compression_range_pct_max: float
    compression_avg_range_pct_max: float

    absorption_delta_ratio_max: float
    absorption_max_down_move_pct: float
    absorption_min_count: int

    accumulation_buy_ratio_min: float
    accumulation_delta_ratio_min: float
    accumulation_max_move_pct: float
    accumulation_min_count: int

    trigger_change_pct_min: float
    trigger_range_pct_min: float
    trigger_close_pos_min: float
    trigger_volume_vs_setup_avg_min: float
    trigger_buy_ratio_min: float
    trigger_delta_ratio_min: float

    min_setup_quote_volume_sum: float
    min_trigger_quote_volume: float
    min_avg_trade_quote: float

    resistance_lookback_bars: int
    breakout_above_recent_close_pct: float

    min_score: int
    max_distance_from_support_pct: float

    forward_bars: int


def load_config() -> Config:
    return Config(
        db_host=env_str("DB_HOST", "db"),
        db_port=env_int("DB_PORT", 5432),
        db_name=env_str("DB_NAME", "pumpdb"),
        db_user=env_str("DB_USER", "pumpuser"),
        db_password=env_str("DB_PASSWORD", ""),

        candles_table="public.candles_5m",
        symbols=env_list("SYMBOLS"),
        btc_symbol="BTCUSDC",

        lookback_bars=18,
        setup_bars=6,
        compression_bars=5,

        compression_range_pct_max=0.0045,
        compression_avg_range_pct_max=0.0035,

        absorption_delta_ratio_max=-0.25,
        absorption_max_down_move_pct=0.0035,
        absorption_min_count=2,

        accumulation_buy_ratio_min=0.63,
        accumulation_delta_ratio_min=0.18,
        accumulation_max_move_pct=0.0035,
        accumulation_min_count=2,

        trigger_change_pct_min=0.0035,
        trigger_range_pct_min=0.0045,
        trigger_close_pos_min=0.80,
        trigger_volume_vs_setup_avg_min=2.5,
        trigger_buy_ratio_min=0.60,
        trigger_delta_ratio_min=0.15,

        min_setup_quote_volume_sum=12000,
        min_trigger_quote_volume=2000,
        min_avg_trade_quote=50,

        resistance_lookback_bars=8,
        breakout_above_recent_close_pct=0.001,

        min_score=7,
        max_distance_from_support_pct=0.012,

        forward_bars=72,
    )


# ==========================================================
# Candle model
# ==========================================================
@dataclass
class Candle:
    ts: datetime
    c: float
    h: float
    l: float
    v_quote: float
    buy_ratio_quote: float
    taker_delta_ratio_quote: float
    change_pct: float
    range_pct: float
    close_pos_in_range: float
    avg_trade_quote: float


# ==========================================================
# Helpers
# ==========================================================
def avg(xs):
    return sum(xs) / len(xs) if xs else 0.0


def pct_change(a, b):
    return (b / a) - 1.0 if a > 0 else 0.0


# ==========================================================
# DB load
# ==========================================================
async def fetch(pool, cfg, symbol):
    rows = await pool.fetch(f"""
        SELECT ts, c, h, l, v_quote,
               buy_ratio_quote,
               taker_delta_ratio_quote,
               change_pct,
               range_pct,
               close_pos_in_range,
               avg_trade_quote
        FROM {cfg.candles_table}
        WHERE symbol = $1
        ORDER BY ts
    """, symbol)

    return [Candle(**dict(r)) for r in rows]


# ==========================================================
# Detection
# ==========================================================
def detect(history, cfg):
    setup = history[-1 - cfg.setup_bars:-1]
    trigger = history[-1]

    if sum(c.v_quote for c in setup) < cfg.min_setup_quote_volume_sum:
        return None

    if max(c.range_pct for c in setup[-cfg.compression_bars:]) > cfg.compression_range_pct_max:
        return None

    if avg([c.range_pct for c in setup[-cfg.compression_bars:]]) > cfg.compression_avg_range_pct_max:
        return None

    if trigger.change_pct < cfg.trigger_change_pct_min:
        return None

    if trigger.range_pct < cfg.trigger_range_pct_min:
        return None

    if trigger.close_pos_in_range < cfg.trigger_close_pos_min:
        return None

    if trigger.v_quote / avg([c.v_quote for c in setup]) < cfg.trigger_volume_vs_setup_avg_min:
        return None

    if trigger.buy_ratio_quote < cfg.trigger_buy_ratio_min:
        return None

    if trigger.taker_delta_ratio_quote < cfg.trigger_delta_ratio_min:
        return None

    support = min(c.l for c in setup)

    if pct_change(support, trigger.c) > cfg.max_distance_from_support_pct:
        return None

    return True


# ==========================================================
# Replay
# ==========================================================
async def run(cfg):
    pool = await asyncpg.create_pool(
        host=cfg.db_host,
        port=cfg.db_port,
        database=cfg.db_name,
        user=cfg.db_user,
        password=cfg.db_password,
    )

    histories = {}
    for s in cfg.symbols:
        histories[s] = await fetch(pool, cfg, s)

    profits = []
    drawdowns = []

    for s, hist in histories.items():
        for i in range(cfg.lookback_bars, len(hist) - cfg.forward_bars):
            h = hist[:i+1]
            if not detect(h, cfg):
                continue

            entry = hist[i].c
            future = hist[i+1:i+1+cfg.forward_bars]

            max_h = max(c.h for c in future)
            min_l = min(c.l for c in future)

            profits.append(pct_change(entry, max_h))
            drawdowns.append(pct_change(entry, min_l))

    print(
        f"signals={len(profits)} | "
        f"avg_profit={avg(profits)*100:.3f}% | "
        f"avg_dd={avg(drawdowns)*100:.3f}%"
    )


def main():
    cfg = load_config()
    asyncio.run(run(cfg))


if __name__ == "__main__":
    main()
