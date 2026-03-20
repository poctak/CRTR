#!/usr/bin/env python3
# progressive_entry_replay_grid_search_v2.py
# ------------------------------------------------------------
# Historical replay / dry-run grid search for progressive BTC-pump strategy
#
# Purpose:
# - replays historical candles from DB
# - does NOT create intents
# - does NOT place orders
# - iterates selected important parameters
# - prints ONLY one line per parameter combination
#
# Main idea:
# - BTC candle acts as regime / trigger
# - targets are evaluated on the SAME timestamp
# - use richer candle features:
#   buy_ratio_quote
#   taker_delta_ratio_quote
#   close_pos_in_range
#   avg_trade_quote
#   body_pct
#   upper_wick_pct
#   range_pct
#
# Output example:
# valid_forward_samples=182 | avg_profit_max=0.742% | avg_drawdown_min=-0.518% |
# btc_triggers=27 | entries=182 | btc_change_pct_min=0.005 | ...
# ------------------------------------------------------------

import os
import asyncio
import logging
from dataclasses import dataclass, replace
from datetime import datetime
from itertools import product
from statistics import median
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
    v = os.getenv(name, "").strip()
    return int(v) if v else default


def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    try:
        return float(v) if v else default
    except Exception:
        return default


def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")


def env_str(name: str, default: str) -> str:
    v = os.getenv(name, "").strip()
    return v if v else default


def env_list(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default).strip()
    out: List[str] = []
    for s in raw.split(","):
        s = s.strip().upper()
        if s:
            out.append(s)
    return out


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
    v_base: float
    v_quote: float
    trades_count: int

    taker_buy_quote: float
    taker_sell_quote: float
    buy_ratio_quote: float
    sell_ratio_quote: float
    taker_delta_quote: float
    taker_delta_ratio_quote: float

    change_pct: float
    range_pct: float
    body_pct: float
    upper_wick_pct: float
    lower_wick_pct: float
    close_pos_in_range: float
    avg_trade_quote: float

    is_green: bool
    is_red: bool


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

    # Selection / replay
    max_targets_per_trigger: int
    cooldown_candles: int
    forward_bars: int

    # BTC regime / trigger
    btc_change_pct_min: float
    btc_min_vq: float
    btc_min_buy_ratio: float
    btc_min_delta_ratio: float
    btc_min_close_pos: float
    btc_min_body_pct: float
    btc_max_upper_wick_pct: float
    btc_min_avg_trade_quote: float
    btc_require_green: bool

    # Target filters
    target_max_change_pct: float
    target_max_range_pct: float
    target_max_body_pct: float
    target_max_upper_wick_pct: float
    target_min_buy_ratio: float
    target_min_delta_ratio: float
    target_min_close_pos: float
    target_min_avg_trade_quote: float
    target_require_green: bool

    # Relative laggard filter
    min_btc_minus_target_change_pct: float

    # Optional ranking
    top_n_per_trigger: int


# ==========================================================
# Config loader
# ==========================================================
def load_config() -> Config:
    btc_symbol = env_str("BTC_SYMBOL", "BTCUSDC").upper()
    symbols = [s for s in env_list("SYMBOLS", "") if s != btc_symbol]

    return Config(
        db_host=env_str("DB_HOST", "db"),
        db_port=env_int("DB_PORT", 5432),
        db_name=env_str("DB_NAME", "pumpdb"),
        db_user=env_str("DB_USER", "pumpuser"),
        db_password=env_str("DB_PASSWORD", ""),

        candles_table=env_str("CANDLES_TABLE", "public.candles_5m"),
        symbols=symbols,
        btc_symbol=btc_symbol,

        start_ts=env_str("START_TS", "") or None,
        end_ts=env_str("END_TS", "") or None,
        max_bars_per_symbol=env_int("MAX_BARS_PER_SYMBOL", 50000),

        max_targets_per_trigger=env_int("MAX_TARGETS_PER_TRIGGER", 50),
        cooldown_candles=env_int("COOLDOWN_CANDLES", 1),
        forward_bars=env_int("FORWARD_BARS", 12),

        btc_change_pct_min=env_float("BTC_CHANGE_PCT_MIN", 0.0050),
        btc_min_vq=env_float("BTC_MIN_VQ", 1000000.0),
        btc_min_buy_ratio=env_float("BTC_MIN_BUY_RATIO", 0.56),
        btc_min_delta_ratio=env_float("BTC_MIN_DELTA_RATIO", 0.08),
        btc_min_close_pos=env_float("BTC_MIN_CLOSE_POS", 0.75),
        btc_min_body_pct=env_float("BTC_MIN_BODY_PCT", 0.0030),
        btc_max_upper_wick_pct=env_float("BTC_MAX_UPPER_WICK_PCT", 0.0025),
        btc_min_avg_trade_quote=env_float("BTC_MIN_AVG_TRADE_QUOTE", 120.0),
        btc_require_green=env_bool("BTC_REQUIRE_GREEN", True),

        target_max_change_pct=env_float("TARGET_MAX_CHANGE_PCT", 0.0045),
        target_max_range_pct=env_float("TARGET_MAX_RANGE_PCT", 0.0100),
        target_max_body_pct=env_float("TARGET_MAX_BODY_PCT", 0.0040),
        target_max_upper_wick_pct=env_float("TARGET_MAX_UPPER_WICK_PCT", 0.0030),
        target_min_buy_ratio=env_float("TARGET_MIN_BUY_RATIO", 0.52),
        target_min_delta_ratio=env_float("TARGET_MIN_DELTA_RATIO", 0.02),
        target_min_close_pos=env_float("TARGET_MIN_CLOSE_POS", 0.55),
        target_min_avg_trade_quote=env_float("TARGET_MIN_AVG_TRADE_QUOTE", 60.0),
        target_require_green=env_bool("TARGET_REQUIRE_GREEN", False),

        min_btc_minus_target_change_pct=env_float("MIN_BTC_MINUS_TARGET_CHANGE_PCT", 0.0015),

        top_n_per_trigger=env_int("TOP_N_PER_TRIGGER", 5),
    )


# ==========================================================
# Grid config
# Only key levers
# ==========================================================
GRID_CONFIG: Dict[str, List[Any]] = {
    "btc_change_pct_min": [0.0040, 0.0050, 0.0060],
    "btc_min_vq": [500000.0, 1000000.0, 2000000.0],
    "btc_min_delta_ratio": [0.05, 0.10],
    "btc_min_close_pos": [0.70, 0.80],

    "target_max_change_pct": [0.0035, 0.0045, 0.0060],
    "target_max_range_pct": [0.0080, 0.0100, 0.0130],
    "target_min_buy_ratio": [0.50, 0.55],
    "target_min_delta_ratio": [0.00, 0.05],
    "min_btc_minus_target_change_pct": [0.0010, 0.0015, 0.0025],

    "top_n_per_trigger": [3, 5],
}


# ==========================================================
# Helpers
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
            v_base,
            v_quote,
            trades_count,

            taker_buy_quote,
            taker_sell_quote,
            buy_ratio_quote,
            sell_ratio_quote,
            taker_delta_quote,
            taker_delta_ratio_quote,

            change_pct,
            range_pct,
            body_pct,
            upper_wick_pct,
            lower_wick_pct,
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
                v_base=float(r["v_base"] or 0.0),
                v_quote=float(r["v_quote"] or 0.0),
                trades_count=int(r["trades_count"] or 0),

                taker_buy_quote=float(r["taker_buy_quote"] or 0.0),
                taker_sell_quote=float(r["taker_sell_quote"] or 0.0),
                buy_ratio_quote=float(r["buy_ratio_quote"] or 0.0),
                sell_ratio_quote=float(r["sell_ratio_quote"] or 0.0),
                taker_delta_quote=float(r["taker_delta_quote"] or 0.0),
                taker_delta_ratio_quote=float(r["taker_delta_ratio_quote"] or 0.0),

                change_pct=float(r["change_pct"] or 0.0),
                range_pct=float(r["range_pct"] or 0.0),
                body_pct=float(r["body_pct"] or 0.0),
                upper_wick_pct=float(r["upper_wick_pct"] or 0.0),
                lower_wick_pct=float(r["lower_wick_pct"] or 0.0),
                close_pos_in_range=float(r["close_pos_in_range"] or 0.0),
                avg_trade_quote=float(r["avg_trade_quote"] or 0.0),

                is_green=bool(r["is_green"]),
                is_red=bool(r["is_red"]),
            )
        )
    return out


# ==========================================================
# Trigger / target logic
# ==========================================================
def btc_trigger_ok(c: Candle, cfg: Config) -> bool:
    if cfg.btc_require_green and not c.is_green:
        return False
    if c.change_pct < cfg.btc_change_pct_min:
        return False
    if c.v_quote < cfg.btc_min_vq:
        return False
    if c.buy_ratio_quote < cfg.btc_min_buy_ratio:
        return False
    if c.taker_delta_ratio_quote < cfg.btc_min_delta_ratio:
        return False
    if c.close_pos_in_range < cfg.btc_min_close_pos:
        return False
    if c.body_pct < cfg.btc_min_body_pct:
        return False
    if c.upper_wick_pct > cfg.btc_max_upper_wick_pct:
        return False
    if c.avg_trade_quote < cfg.btc_min_avg_trade_quote:
        return False
    return True


def target_candidate_ok(btc: Candle, alt: Candle, cfg: Config) -> bool:
    if cfg.target_require_green and not alt.is_green:
        return False
    if alt.change_pct > cfg.target_max_change_pct:
        return False
    if alt.range_pct > cfg.target_max_range_pct:
        return False
    if alt.body_pct > cfg.target_max_body_pct:
        return False
    if alt.upper_wick_pct > cfg.target_max_upper_wick_pct:
        return False
    if alt.buy_ratio_quote < cfg.target_min_buy_ratio:
        return False
    if alt.taker_delta_ratio_quote < cfg.target_min_delta_ratio:
        return False
    if alt.close_pos_in_range < cfg.target_min_close_pos:
        return False
    if alt.avg_trade_quote < cfg.target_min_avg_trade_quote:
        return False

    # laggard filter: BTC must outperform target by minimum margin
    if (btc.change_pct - alt.change_pct) < cfg.min_btc_minus_target_change_pct:
        return False

    return True


def rank_target(btc: Candle, alt: Candle) -> float:
    # Prefer:
    # - stronger target buy pressure
    # - decent close
    # - decent liquidity
    # - but still lagging BTC
    lag = max(0.0, btc.change_pct - alt.change_pct)
    return (
        lag * 4.0
        + alt.buy_ratio_quote * 1.5
        + alt.taker_delta_ratio_quote * 2.0
        + alt.close_pos_in_range * 0.7
        + min(alt.avg_trade_quote / 200.0, 2.0) * 0.3
        - alt.upper_wick_pct * 50.0
        - alt.range_pct * 10.0
    )


# ==========================================================
# Forward evaluation
# ==========================================================
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
# Core evaluation
# ==========================================================
def evaluate_config(
    cfg: Config,
    histories: Dict[str, List[Candle]],
) -> Tuple[int, float, float, float, float, int, int]:
    btc_hist = histories.get(cfg.btc_symbol, [])
    if not btc_hist:
        return 0, 0.0, 0.0, 0.0, 0.0, 0, 0

    target_symbols = cfg.symbols[: max(0, cfg.max_targets_per_trigger)]

    idx_by_ts: Dict[str, Dict[datetime, int]] = {}
    for sym in target_symbols:
        hist = histories.get(sym, [])
        idx_by_ts[sym] = {c.ts: i for i, c in enumerate(hist)}

    profit_samples: List[float] = []
    drawdown_samples: List[float] = []

    btc_triggers = 0
    entries = 0
    cooldown_left = 0

    for btc in btc_hist:
        if cooldown_left > 0:
            cooldown_left -= 1
            continue

        if not btc_trigger_ok(btc, cfg):
            continue

        btc_triggers += 1
        ts = btc.ts

        ranked_candidates: List[Tuple[float, str, int]] = []

        for sym in target_symbols:
            hist = histories.get(sym, [])
            if not hist:
                continue

            target_idx = idx_by_ts[sym].get(ts)
            if target_idx is None:
                continue

            alt = hist[target_idx]

            if not target_candidate_ok(btc, alt, cfg):
                continue

            ranked_candidates.append((rank_target(btc, alt), sym, target_idx))

        if not ranked_candidates:
            cooldown_left = cfg.cooldown_candles
            continue

        ranked_candidates.sort(key=lambda x: x[0], reverse=True)
        selected = ranked_candidates[: cfg.top_n_per_trigger]

        for _, sym, target_idx in selected:
            hist = histories[sym]
            fwd = compute_forward_stats(hist, target_idx, cfg.forward_bars)
            if fwd is None:
                continue

            entries += 1
            profit_samples.append(fwd["profit_to_max_pct"])
            drawdown_samples.append(fwd["drawdown_to_min_pct"])

        cooldown_left = cfg.cooldown_candles

    valid_forward_samples = len(profit_samples)
    avg_profit_max = avg(profit_samples) if profit_samples else 0.0
    avg_drawdown_min = avg(drawdown_samples) if drawdown_samples else 0.0
    median_profit_max = median(profit_samples) if profit_samples else 0.0
    median_drawdown_min = median(drawdown_samples) if drawdown_samples else 0.0

    return (
        valid_forward_samples,
        avg_profit_max,
        avg_drawdown_min,
        median_profit_max,
        median_drawdown_min,
        btc_triggers,
        entries,
    )


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
        all_symbols = sorted(set([cfg.btc_symbol] + [s.upper() for s in cfg.symbols]))

        for sym in all_symbols:
            histories[sym] = await fetch_symbol_history(pool, cfg, sym)

        for combo_cfg, combo_updates in iter_grid_configs(cfg):
            (
                valid_forward_samples,
                avg_profit_max,
                avg_drawdown_min,
                median_profit_max,
                median_drawdown_min,
                btc_triggers,
                entries,
            ) = evaluate_config(combo_cfg, histories)

            parts = [
                f"valid_forward_samples={valid_forward_samples}",
                f"avg_profit_max={avg_profit_max * 100.0:.3f}%",
                f"avg_drawdown_min={avg_drawdown_min * 100.0:.3f}%",
                f"median_profit_max={median_profit_max * 100.0:.3f}%",
                f"median_drawdown_min={median_drawdown_min * 100.0:.3f}%",
                f"btc_triggers={btc_triggers}",
                f"entries={entries}",
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
