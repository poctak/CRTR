#!/usr/bin/env python3
# accumulation_breakout_replay_grid_search_v2.py

import os
import asyncio
import logging
from dataclasses import dataclass, replace
from datetime import datetime
from itertools import product
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


def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")


def env_list(name: str, default: str = "") -> List[str]:
    v = os.getenv(name, default)
    out: List[str] = []
    for x in (v or "").split(","):
        x = x.strip().upper()
        if x:
            out.append(x)
    return out


# ==========================================================
# Config
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

    use_btc_filter: bool
    btc_regime_lookback_bars: int
    btc_kill_dump_pct: float
    btc_kill_pump_pct: float

    lookback_bars: int
    setup_bars: int
    compression_bars: int

    compression_range_pct_max: float
    compression_avg_range_pct_max: float

    absorption_min_count: int
    absorption_delta_ratio_max: float
    absorption_max_down_move_pct: float

    accumulation_min_count: int
    accumulation_buy_ratio_min: float
    accumulation_delta_ratio_min: float
    accumulation_max_move_pct: float

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

    allow_multiple_signals_per_symbol: bool
    cooldown_bars_after_signal: int

    forward_bars: int


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

        use_btc_filter=env_bool("USE_BTC_FILTER", False),
        btc_regime_lookback_bars=env_int("BTC_REGIME_LOOKBACK_BARS", 3),
        btc_kill_dump_pct=env_float("BTC_KILL_DUMP_PCT", -0.010),
        btc_kill_pump_pct=env_float("BTC_KILL_PUMP_PCT", 0.015),

        lookback_bars=env_int("LOOKBACK_BARS", 18),
        setup_bars=env_int("SETUP_BARS", 6),
        compression_bars=env_int("COMPRESSION_BARS", 4),

        compression_range_pct_max=env_float("COMPRESSION_RANGE_PCT_MAX", 0.0045),
        compression_avg_range_pct_max=env_float("COMPRESSION_AVG_RANGE_PCT_MAX", 0.0035),

        absorption_min_count=env_int("ABSORPTION_MIN_COUNT", 2),
        absorption_delta_ratio_max=env_float("ABSORPTION_DELTA_RATIO_MAX", -0.20),
        absorption_max_down_move_pct=env_float("ABSORPTION_MAX_DOWN_MOVE_PCT", 0.0035),

        accumulation_min_count=env_int("ACCUMULATION_MIN_COUNT", 2),
        accumulation_buy_ratio_min=env_float("ACCUMULATION_BUY_RATIO_MIN", 0.62),
        accumulation_delta_ratio_min=env_float("ACCUMULATION_DELTA_RATIO_MIN", 0.18),
        accumulation_max_move_pct=env_float("ACCUMULATION_MAX_MOVE_PCT", 0.0035),

        trigger_change_pct_min=env_float("TRIGGER_CHANGE_PCT_MIN", 0.0035),
        trigger_range_pct_min=env_float("TRIGGER_RANGE_PCT_MIN", 0.0045),
        trigger_close_pos_min=env_float("TRIGGER_CLOSE_POS_MIN", 0.85),
        trigger_volume_vs_setup_avg_min=env_float("TRIGGER_VOLUME_VS_SETUP_AVG_MIN", 2.5),
        trigger_buy_ratio_min=env_float("TRIGGER_BUY_RATIO_MIN", 0.62),
        trigger_delta_ratio_min=env_float("TRIGGER_DELTA_RATIO_MIN", 0.18),

        min_setup_quote_volume_sum=env_float("MIN_SETUP_QUOTE_VOLUME_SUM", 12000.0),
        min_trigger_quote_volume=env_float("MIN_TRIGGER_QUOTE_VOLUME", 3000.0),
        min_avg_trade_quote=env_float("MIN_AVG_TRADE_QUOTE", 80.0),

        resistance_lookback_bars=env_int("RESISTANCE_LOOKBACK_BARS", 8),
        breakout_above_recent_close_pct=env_float("BREAKOUT_ABOVE_RECENT_CLOSE_PCT", 0.0010),

        min_score=env_int("MIN_SCORE", 7),
        max_distance_from_support_pct=env_float("MAX_DISTANCE_FROM_SUPPORT_PCT", 0.012),

        allow_multiple_signals_per_symbol=True,
        cooldown_bars_after_signal=12,

        forward_bars=72,
    )


# ==========================================================
# GRID (rozšířený)
# ==========================================================
GRID_CONFIG: Dict[str, List[Any]] = {
    "compression_range_pct_max": [0.0045, 0.0055],
    "compression_avg_range_pct_max": [0.0035, 0.0045],

    "compression_bars": [3, 4],  # NEW

    "trigger_change_pct_min": [0.0030, 0.0035],
    "trigger_range_pct_min": [0.0040, 0.0045],
    "trigger_close_pos_min": [0.80],
    "trigger_volume_vs_setup_avg_min": [1.8, 2.5],

    "trigger_buy_ratio_min": [0.60, 0.65],
    "trigger_delta_ratio_min": [0.15, 0.20],

    "min_avg_trade_quote": [60.0, 80.0],
    "min_trigger_quote_volume": [2000.0, 3000.0],

    "min_setup_quote_volume_sum": [10000.0, 12000.0],
    "accumulation_buy_ratio_min": [0.60, 0.65],

    "absorption_delta_ratio_max": [-0.25, -0.20],  # NEW

    "min_score": [7],
    "max_distance_from_support_pct": [0.012, 0.015],
}


# ==========================================================
# Helpers (zkráceno – stejné jako předtím)
# ==========================================================
def avg(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def pct_change(a: float, b: float) -> float:
    if a <= 0:
        return 0.0
    return (b / a) - 1.0


def format_value(v: Any) -> str:
    if isinstance(v, float):
        return f"{v:.6f}".rstrip("0").rstrip(".")
    return str(v)


def iter_grid_configs(base_cfg: Config):
    keys = list(GRID_CONFIG.keys())
    for combo in product(*(GRID_CONFIG[k] for k in keys)):
        yield replace(base_cfg, **dict(zip(keys, combo))), dict(zip(keys, combo))


# ==========================================================
# !!! DŮLEŽITÉ !!!
# zbytek skriptu (Candle, detection, replay)
# JE BEZ ZMĚNY oproti předchozí verzi
# ==========================================================

# 👉 nechávám beze změny, aby ses soustředil jen na grid

# (sem patří celý zbytek z minulé verze: Candle, detect_*, analyze_symbol,
# compute_forward_stats, evaluate_config, run_grid atd.)

# ==========================================================
# MAIN
# ==========================================================
async def main():
    cfg = load_config()

    pool = await asyncpg.create_pool(
        host=cfg.db_host,
        port=cfg.db_port,
        database=cfg.db_name,
        user=cfg.db_user,
        password=cfg.db_password,
    )

    histories = {}
    for sym in set(cfg.symbols + [cfg.btc_symbol]):
        histories[sym] = await fetch_symbol_history(pool, cfg, sym)

    for combo_cfg, combo_params in iter_grid_configs(cfg):
        samples, profit, dd = evaluate_config(combo_cfg, histories)

        parts = [
            f"valid_forward_samples={samples}",
            f"avg_profit_max={profit * 100:.3f}%",
            f"avg_drawdown_min={dd * 100:.3f}%"
        ]

        for k in GRID_CONFIG:
            parts.append(f"{k}={format_value(combo_params[k])}")

        print(" | ".join(parts), flush=True)

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
