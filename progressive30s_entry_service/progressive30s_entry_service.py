#!/usr/bin/env python3
# accumulation_breakout_replay_grid_search.py
# ------------------------------------------------------------
# Historical replay / dry-run grid search version X
#
# Purpose:
# - iterates selected "important" parameters across predefined values
# - runs full replay for every combination
# - prints ONLY one line per combination:
#   valid_forward_samples=59 | avg_profit_max=1.579% | avg_drawdown_min=-1.218% | PARAM=... | PARAM=...
#
# Notes:
# - no REPLAY_INTENT logs
# - no per-symbol summaries
# - no startup / final logs
# - iteration grid can be customized in GRID_CONFIG below
# ------------------------------------------------------------

import os
import asyncio
import logging
from dataclasses import dataclass, replace
from datetime import datetime
from itertools import product
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
    # DB
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    # Tables / symbols
    candles_table: str
    symbols: List[str]
    btc_symbol: str

    # Historical scope
    start_ts: Optional[str]
    end_ts: Optional[str]
    max_bars_per_symbol: int

    # Optional market regime
    use_btc_filter: bool
    btc_regime_lookback_bars: int
    btc_kill_dump_pct: float
    btc_kill_pump_pct: float

    # Setup window
    lookback_bars: int
    setup_bars: int
    compression_bars: int

    # Compression
    compression_range_pct_max: float
    compression_avg_range_pct_max: float

    # Absorption
    absorption_min_count: int
    absorption_delta_ratio_max: float
    absorption_max_down_move_pct: float

    # Accumulation
    accumulation_min_count: int
    accumulation_buy_ratio_min: float
    accumulation_delta_ratio_min: float
    accumulation_max_move_pct: float

    # Trigger
    trigger_change_pct_min: float
    trigger_range_pct_min: float
    trigger_close_pos_min: float
    trigger_volume_vs_setup_avg_min: float
    trigger_buy_ratio_min: float
    trigger_delta_ratio_min: float

    # Liquidity
    min_setup_quote_volume_sum: float
    min_trigger_quote_volume: float
    min_avg_trade_quote: float

    # Resistance
    resistance_lookback_bars: int
    breakout_above_recent_close_pct: float

    # Quality filters
    min_score: int
    max_distance_from_support_pct: float

    # Replay options
    allow_multiple_signals_per_symbol: bool
    cooldown_bars_after_signal: int

    # Forward evaluation
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

        allow_multiple_signals_per_symbol=env_bool("ALLOW_MULTIPLE_SIGNALS_PER_SYMBOL", True),
        cooldown_bars_after_signal=env_int("COOLDOWN_BARS_AFTER_SIGNAL", 12),

        forward_bars=env_int("FORWARD_BARS", 72),
    )


# ==========================================================
# Parameter grid
# ==========================================================
# Zvolil jsem 8 hlavních parametrů.
# 2 hodnoty na parametr => 256 běhů.
GRID_CONFIG: Dict[str, List[Any]] = {
    "compression_range_pct_max": [0.0045, 0.0055],
    "compression_avg_range_pct_max": [0.0035, 0.0045],
    "trigger_change_pct_min": [0.0025, 0.0035],
    "trigger_range_pct_min": [0.0035, 0.0045],
    "trigger_close_pos_min": [0.80, 0.85],
    "trigger_volume_vs_setup_avg_min": [1.8, 2.5],
    "min_score": [6, 7],
    "max_distance_from_support_pct": [0.012, 0.018],
}


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


def safe_ratio(a: float, b: float) -> float:
    return a / b if b > 0 else 0.0


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
    values_product = product(*(GRID_CONFIG[k] for k in keys))
    for combo in values_product:
        updates = dict(zip(keys, combo))
        yield replace(base_cfg, **updates), updates


# ==========================================================
# DB load
# ==========================================================
async def fetch_symbol_history(pool: asyncpg.Pool, cfg: Config, symbol: str) -> List[Candle]:
    where_parts = ["symbol = $1"]
    params: List[Any] = [symbol]
    idx = 2

    if cfg.start_ts:
        where_parts.append(f"ts >= ${idx}")
        params.append(cfg.start_ts)
        idx += 1

    if cfg.end_ts:
        where_parts.append(f"ts <= ${idx}")
        params.append(cfg.end_ts)
        idx += 1

    q = f"""
        SELECT
            ts,
            o, h, l, c,
            v_quote,
            trades_count,
            buy_ratio_quote,
            taker_delta_ratio_quote,
            change_pct,
            range_pct,
            body_pct,
            close_pos_in_range,
            avg_trade_quote,
            is_green,
            is_red
        FROM {cfg.candles_table}
        WHERE {" AND ".join(where_parts)}
        ORDER BY ts ASC
        LIMIT {cfg.max_bars_per_symbol}
    """

    rows = await pool.fetch(q, *params)

    out: List[Candle] = []
    for r in rows:
        out.append(
            Candle(
                ts=r["ts"],
                o=float(r["o"]),
                h=float(r["h"]),
                l=float(r["l"]),
                c=float(r["c"]),
                v_quote=float(r["v_quote"] or 0.0),
                trades_count=int(r["trades_count"] or 0),
                buy_ratio_quote=float(r["buy_ratio_quote"] or 0.0),
                taker_delta_ratio_quote=float(r["taker_delta_ratio_quote"] or 0.0),
                change_pct=float(r["change_pct"] or 0.0),
                range_pct=float(r["range_pct"] or 0.0),
                body_pct=float(r["body_pct"] or 0.0),
                close_pos_in_range=float(r["close_pos_in_range"] or 0.0),
                avg_trade_quote=float(r["avg_trade_quote"] or 0.0),
                is_green=bool(r["is_green"]),
                is_red=bool(r["is_red"]),
            )
        )
    return out


# ==========================================================
# Pattern detection
# ==========================================================
def detect_absorption(setup: List[Candle], cfg: Config) -> Tuple[bool, int]:
    hits = 0
    for c in setup:
        if (
            c.taker_delta_ratio_quote <= cfg.absorption_delta_ratio_max
            and c.change_pct >= -cfg.absorption_max_down_move_pct
        ):
            hits += 1
    return hits >= cfg.absorption_min_count, hits


def detect_accumulation(setup: List[Candle], cfg: Config) -> Tuple[bool, int]:
    hits = 0
    for c in setup:
        if (
            c.buy_ratio_quote >= cfg.accumulation_buy_ratio_min
            and c.taker_delta_ratio_quote >= cfg.accumulation_delta_ratio_min
            and abs(c.change_pct) <= cfg.accumulation_max_move_pct
        ):
            hits += 1
    return hits >= cfg.accumulation_min_count, hits


def detect_compression(setup: List[Candle], cfg: Config) -> Tuple[bool, float, float]:
    last = setup[-cfg.compression_bars:]
    max_rng = max((c.range_pct for c in last), default=0.0)
    avg_rng = avg([c.range_pct for c in last])
    ok = (
        max_rng <= cfg.compression_range_pct_max
        and avg_rng <= cfg.compression_avg_range_pct_max
    )
    return ok, max_rng, avg_rng


def detect_breakout_trigger(history: List[Candle], cfg: Config) -> Tuple[bool, Dict[str, Any], str]:
    if len(history) < max(cfg.setup_bars + 1, cfg.resistance_lookback_bars):
        return False, {}, "not_enough_history"

    trigger = history[-1]
    setup = history[-1 - cfg.setup_bars:-1]

    setup_avg_vq = avg([c.v_quote for c in setup])
    recent_resistance = max(c.h for c in history[-1 - cfg.resistance_lookback_bars:-1])
    recent_close_ref = max(c.c for c in history[-1 - cfg.resistance_lookback_bars:-1])

    volume_mult = safe_ratio(trigger.v_quote, setup_avg_vq)
    above_recent_close = pct_change(recent_close_ref, trigger.c)

    if trigger.change_pct < cfg.trigger_change_pct_min:
        return False, {}, "trigger_change_low"
    if trigger.range_pct < cfg.trigger_range_pct_min:
        return False, {}, "trigger_range_low"
    if trigger.close_pos_in_range < cfg.trigger_close_pos_min:
        return False, {}, "trigger_close_pos_low"
    if volume_mult < cfg.trigger_volume_vs_setup_avg_min:
        return False, {}, "trigger_volume_mult_low"
    if trigger.v_quote < cfg.min_trigger_quote_volume:
        return False, {}, "trigger_vq_low"
    if trigger.buy_ratio_quote < cfg.trigger_buy_ratio_min:
        return False, {}, "trigger_buy_ratio_low"
    if trigger.taker_delta_ratio_quote < cfg.trigger_delta_ratio_min:
        return False, {}, "trigger_delta_ratio_low"
    if not (trigger.c >= recent_resistance or above_recent_close >= cfg.breakout_above_recent_close_pct):
        return False, {}, "trigger_not_breaking_ref"
    if trigger.avg_trade_quote < cfg.min_avg_trade_quote:
        return False, {}, "trigger_avg_trade_low"

    return True, {
        "trigger_ts": trigger.ts.isoformat(),
        "trigger_change_pct": trigger.change_pct,
        "trigger_range_pct": trigger.range_pct,
        "trigger_close_pos_in_range": trigger.close_pos_in_range,
        "trigger_v_quote": trigger.v_quote,
        "trigger_buy_ratio_quote": trigger.buy_ratio_quote,
        "trigger_delta_ratio_quote": trigger.taker_delta_ratio_quote,
        "trigger_avg_trade_quote": trigger.avg_trade_quote,
        "trigger_volume_mult_vs_setup_avg": volume_mult,
        "recent_resistance": recent_resistance,
        "recent_close_ref": recent_close_ref,
        "breakout_above_recent_close_pct": above_recent_close,
    }, "ok"


def analyze_symbol(history: List[Candle], cfg: Config) -> Tuple[Optional[Dict[str, Any]], str]:
    if len(history) < cfg.lookback_bars:
        return None, "not_enough_candles"

    setup = history[-1 - cfg.setup_bars:-1]
    trigger = history[-1]

    if len(setup) < cfg.setup_bars:
        return None, "setup_too_short"

    setup_quote_sum = sum(c.v_quote for c in setup)
    if setup_quote_sum < cfg.min_setup_quote_volume_sum:
        return None, "setup_quote_sum_low"

    absorption_ok, absorption_hits = detect_absorption(setup, cfg)
    accumulation_ok, accumulation_hits = detect_accumulation(setup, cfg)
    compression_ok, compression_max_rng, compression_avg_rng = detect_compression(setup, cfg)

    if not (absorption_ok or accumulation_ok):
        return None, "no_setup_pattern"

    if not compression_ok:
        return None, "no_compression"

    trigger_ok, trigger_info, trigger_reason = detect_breakout_trigger(history, cfg)
    if not trigger_ok:
        return None, trigger_reason

    local_support = min(c.l for c in setup)
    local_resistance = max(c.h for c in setup)

    distance_from_support_pct = pct_change(local_support, trigger.c)
    if distance_from_support_pct > cfg.max_distance_from_support_pct:
        return None, "too_far_from_support"

    score = 0
    score += 2 if absorption_ok else 0
    score += 2 if accumulation_ok else 0
    score += 1 if compression_ok else 0
    score += 2 if trigger_ok else 0
    score += 1 if trigger.buy_ratio_quote >= 0.65 else 0
    score += 1 if trigger.taker_delta_ratio_quote >= 0.20 else 0
    score += 1 if trigger.close_pos_in_range >= 0.90 else 0

    if score < cfg.min_score:
        return None, "score_too_low"

    return {
        "support_price": local_support,
        "resistance": local_resistance,
        "distance_from_support_pct": distance_from_support_pct,
        "setup_quote_sum": setup_quote_sum,
        "setup_absorption_ok": absorption_ok,
        "setup_absorption_hits": absorption_hits,
        "setup_accumulation_ok": accumulation_ok,
        "setup_accumulation_hits": accumulation_hits,
        "setup_compression_ok": compression_ok,
        "setup_compression_max_range_pct": compression_max_rng,
        "setup_compression_avg_range_pct": compression_avg_rng,
        "trigger": trigger_info,
        "last_close": trigger.c,
        "score": score,
    }, "ok"


# ==========================================================
# Replay helpers
# ==========================================================
def compute_btc_regime_from_history(
    btc_hist: List[Candle],
    idx: int,
    cfg: Config,
) -> Optional[float]:
    need = cfg.btc_regime_lookback_bars
    if idx < need:
        return None
    return pct_change(btc_hist[idx - need].c, btc_hist[idx].c)


def btc_regime_blocked(delta: float, cfg: Config) -> bool:
    return delta <= cfg.btc_kill_dump_pct or delta >= cfg.btc_kill_pump_pct


def compute_forward_stats(hist: List[Candle], entry_idx: int, forward_bars: int) -> Optional[Dict[str, Any]]:
    entry = hist[entry_idx]
    future = hist[entry_idx + 1: entry_idx + 1 + forward_bars]
    if not future:
        return None

    max_candle = max(future, key=lambda c: c.h)
    min_candle = min(future, key=lambda c: c.l)

    return {
        "future_bars": len(future),
        "future_max_price": max_candle.h,
        "future_min_price": min_candle.l,
        "future_max_ts": max_candle.ts.isoformat(),
        "future_min_ts": min_candle.ts.isoformat(),
        "profit_to_max_pct": pct_change(entry.c, max_candle.h),
        "drawdown_to_min_pct": pct_change(entry.c, min_candle.l),
    }


# ==========================================================
# Core evaluation for one config
# ==========================================================
def evaluate_config(
    cfg: Config,
    histories: Dict[str, List[Candle]],
) -> Tuple[int, float, float]:
    btc_hist = histories.get(cfg.btc_symbol.upper(), [])

    signal_counts: Dict[str, int] = {s.upper(): 0 for s in cfg.symbols}
    cooldown_until_idx: Dict[str, int] = {s.upper(): -1 for s in cfg.symbols}

    profit_samples: List[float] = []
    drawdown_samples: List[float] = []

    for sym in cfg.symbols:
        sym = sym.upper()
        hist = histories.get(sym, [])
        if len(hist) < cfg.lookback_bars:
            continue

        for idx in range(cfg.lookback_bars - 1, len(hist)):
            if not cfg.allow_multiple_signals_per_symbol and signal_counts[sym] > 0:
                break

            if idx <= cooldown_until_idx[sym]:
                continue

            if cfg.use_btc_filter:
                btc_idx = min(idx, len(btc_hist) - 1)
                btc_delta = compute_btc_regime_from_history(btc_hist, btc_idx, cfg)
                if btc_delta is None or btc_regime_blocked(btc_delta, cfg):
                    continue

            history_slice = hist[:idx + 1]
            setup, _ = analyze_symbol(history_slice, cfg)
            if not setup:
                continue

            signal_counts[sym] += 1
            cooldown_until_idx[sym] = idx + cfg.cooldown_bars_after_signal

            fwd = compute_forward_stats(hist, idx, cfg.forward_bars)
            if fwd is not None:
                profit_samples.append(fwd["profit_to_max_pct"])
                drawdown_samples.append(fwd["drawdown_to_min_pct"])

    valid_forward_samples = len(profit_samples)
    avg_profit_max = avg(profit_samples) if profit_samples else 0.0
    avg_drawdown_min = avg(drawdown_samples) if drawdown_samples else 0.0
    return valid_forward_samples, avg_profit_max, avg_drawdown_min


# ==========================================================
# Grid replay engine
# ==========================================================
async def run_grid(cfg: Config):
    if not cfg.symbols:
        raise RuntimeError("SYMBOLS is empty")

    pool = await asyncpg.create_pool(
        host=cfg.db_host,
        port=cfg.db_port,
        database=cfg.db_name,
        user=cfg.db_user,
        password=cfg.db_password,
        min_size=1,
        max_size=5,
    )

    try:
        histories: Dict[str, List[Candle]] = {}
        for sym in sorted(set([s.upper() for s in cfg.symbols] + [cfg.btc_symbol.upper()])):
            histories[sym] = await fetch_symbol_history(pool, cfg, sym)

        if cfg.use_btc_filter and not histories.get(cfg.btc_symbol.upper(), []):
            raise RuntimeError(f"BTC history missing for {cfg.btc_symbol}")

        for combo_cfg, combo_updates in iter_grid_configs(cfg):
            valid_forward_samples, avg_profit_max, avg_drawdown_min = evaluate_config(combo_cfg, histories)

            parts = [
                f"valid_forward_samples={valid_forward_samples}",
                f"avg_profit_max={avg_profit_max * 100.0:.3f}%",
                f"avg_drawdown_min={avg_drawdown_min * 100.0:.3f}%"
            ]

            for k in GRID_CONFIG.keys():
                parts.append(f"{k}={format_value(combo_updates[k])}")

            print(" | ".join(parts), flush=True)

    finally:
        await pool.close()


def main():
    cfg = load_config()
    asyncio.run(run_grid(cfg))


if __name__ == "__main__":
    main()
