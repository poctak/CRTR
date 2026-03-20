#!/usr/bin/env python3
# progressive_entry_replay_grid_search_v3.py
# ------------------------------------------------------------
# Improved historical replay / dry-run grid search
#
# Goal:
# - test a more selective BTC-pump -> laggard-entry strategy
# - does NOT create intents
# - does NOT place orders
# - prints ONLY one line per parameter combination
#
# Improvements:
# - narrower, smarter grid based on previous findings
# - stronger laggard logic
# - optional target "not dead" filter
# - first-hit TP/SL evaluation
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
    v_quote: float
    trades_count: int
    buy_ratio_quote: float
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

    forward_bars: int
    max_targets_per_trigger: int

    # BTC trigger
    btc_change_pct_min: float
    btc_min_vq: float
    btc_min_delta_ratio: float
    btc_min_close_pos: float
    btc_require_green: bool

    # Target filters
    target_max_change_pct: float
    target_max_range_pct: float
    target_min_buy_ratio: float
    target_min_delta_ratio: float
    target_min_close_pos: float
    target_min_avg_trade_quote: float
    target_require_green: bool

    # Laggard filters
    min_btc_minus_target_change_pct: float
    max_target_change_vs_btc_ratio: float

    # Selection
    top_n_per_trigger: int

    # First-hit evaluation
    tp_pct: float
    sl_pct: float


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

        forward_bars=env_int("FORWARD_BARS", 12),
        max_targets_per_trigger=env_int("MAX_TARGETS_PER_TRIGGER", 50),

        btc_change_pct_min=env_float("BTC_CHANGE_PCT_MIN", 0.0050),
        btc_min_vq=env_float("BTC_MIN_VQ", 500000.0),
        btc_min_delta_ratio=env_float("BTC_MIN_DELTA_RATIO", 0.05),
        btc_min_close_pos=env_float("BTC_MIN_CLOSE_POS", 0.70),
        btc_require_green=env_bool("BTC_REQUIRE_GREEN", True),

        target_max_change_pct=env_float("TARGET_MAX_CHANGE_PCT", 0.0045),
        target_max_range_pct=env_float("TARGET_MAX_RANGE_PCT", 0.0080),
        target_min_buy_ratio=env_float("TARGET_MIN_BUY_RATIO", 0.50),
        target_min_delta_ratio=env_float("TARGET_MIN_DELTA_RATIO", 0.00),
        target_min_close_pos=env_float("TARGET_MIN_CLOSE_POS", 0.45),
        target_min_avg_trade_quote=env_float("TARGET_MIN_AVG_TRADE_QUOTE", 50.0),
        target_require_green=env_bool("TARGET_REQUIRE_GREEN", False),

        min_btc_minus_target_change_pct=env_float("MIN_BTC_MINUS_TARGET_CHANGE_PCT", 0.0025),
        max_target_change_vs_btc_ratio=env_float("MAX_TARGET_CHANGE_VS_BTC_RATIO", 0.60),

        top_n_per_trigger=env_int("TOP_N_PER_TRIGGER", 3),

        tp_pct=env_float("TP_PCT", 0.0040),
        sl_pct=env_float("SL_PCT", 0.0040),
    )


# ==========================================================
# Focused grid: only the parameters that mattered most
# ==========================================================
GRID_CONFIG: Dict[str, List[Any]] = {
    "target_max_range_pct": [0.0070, 0.0080, 0.0090],
    "target_max_change_pct": [0.0035, 0.0045, 0.0055],
    "min_btc_minus_target_change_pct": [0.0025, 0.0035, 0.0045],
    "max_target_change_vs_btc_ratio": [0.50, 0.60, 0.70],
    "top_n_per_trigger": [2, 3],
    "target_min_buy_ratio": [0.50, 0.55],
    "tp_pct": [0.0040, 0.0050],
    "sl_pct": [0.0040, 0.0050],
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
    for combo in product(*(GRID_CONFIG[k] for k in keys)):
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
    return [
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
            upper_wick_pct=float(r["upper_wick_pct"] or 0.0),
            lower_wick_pct=float(r["lower_wick_pct"] or 0.0),
            close_pos_in_range=float(r["close_pos_in_range"] or 0.0),
            avg_trade_quote=float(r["avg_trade_quote"] or 0.0),
            is_green=bool(r["is_green"]),
            is_red=bool(r["is_red"]),
        )
        for r in rows
    ]


# ==========================================================
# Logic
# ==========================================================
def btc_trigger_ok(c: Candle, cfg: Config) -> bool:
    if cfg.btc_require_green and not c.is_green:
        return False
    if c.change_pct < cfg.btc_change_pct_min:
        return False
    if c.v_quote < cfg.btc_min_vq:
        return False
    if c.taker_delta_ratio_quote < cfg.btc_min_delta_ratio:
        return False
    if c.close_pos_in_range < cfg.btc_min_close_pos:
        return False
    return True


def target_candidate_ok(btc: Candle, alt: Candle, cfg: Config) -> bool:
    if cfg.target_require_green and not alt.is_green:
        return False
    if alt.change_pct > cfg.target_max_change_pct:
        return False
    if alt.range_pct > cfg.target_max_range_pct:
        return False
    if alt.buy_ratio_quote < cfg.target_min_buy_ratio:
        return False
    if alt.taker_delta_ratio_quote < cfg.target_min_delta_ratio:
        return False
    if alt.close_pos_in_range < cfg.target_min_close_pos:
        return False
    if alt.avg_trade_quote < cfg.target_min_avg_trade_quote:
        return False

    lag_abs = btc.change_pct - alt.change_pct
    if lag_abs < cfg.min_btc_minus_target_change_pct:
        return False

    # stronger laggard normalization
    if btc.change_pct <= 0:
        return False
    if alt.change_pct > btc.change_pct * cfg.max_target_change_vs_btc_ratio:
        return False

    return True


def rank_target(btc: Candle, alt: Candle) -> float:
    lag_abs = max(0.0, btc.change_pct - alt.change_pct)
    lag_ratio = lag_abs / max(abs(btc.change_pct), 1e-9)

    return (
        lag_ratio * 5.0
        + lag_abs * 100.0
        + alt.buy_ratio_quote * 1.5
        + alt.taker_delta_ratio_quote * 2.0
        + alt.close_pos_in_range * 0.6
        + min(alt.avg_trade_quote / 150.0, 2.0) * 0.3
        - alt.range_pct * 12.0
        - alt.upper_wick_pct * 40.0
    )


# ==========================================================
# Evaluation helpers
# ==========================================================
def compute_forward_stats(hist: List[Candle], entry_idx: int, forward_bars: int) -> Optional[Dict[str, Any]]:
    entry = hist[entry_idx]
    future = hist[entry_idx + 1: entry_idx + 1 + forward_bars]
    if not future:
        return None

    max_candle = max(future, key=lambda c: c.h)
    min_candle = min(future, key=lambda c: c.l)

    return {
        "profit_to_max_pct": pct_change(entry.c, max_candle.h),
        "drawdown_to_min_pct": pct_change(entry.c, min_candle.l),
    }


def compute_first_hit(hist: List[Candle], entry_idx: int, forward_bars: int, tp_pct: float, sl_pct: float) -> str:
    entry = hist[entry_idx]
    tp_price = entry.c * (1.0 + tp_pct)
    sl_price = entry.c * (1.0 - sl_pct)

    future = hist[entry_idx + 1: entry_idx + 1 + forward_bars]
    if not future:
        return "NO_DATA"

    for c in future:
        hit_tp = c.h >= tp_price
        hit_sl = c.l <= sl_price

        if hit_tp and hit_sl:
            # conservative assumption
            return "BOTH"
        if hit_tp:
            return "TP"
        if hit_sl:
            return "SL"

    return "NONE"


# ==========================================================
# Core evaluation
# ==========================================================
def evaluate_config(cfg: Config, histories: Dict[str, List[Candle]]) -> Tuple[int, float, float, float, float, int, int, float, float]:
    btc_hist = histories.get(cfg.btc_symbol, [])
    if not btc_hist:
        return 0, 0.0, 0.0, 0.0, 0.0, 0, 0, 0.0, 0.0

    target_symbols = cfg.symbols[: max(0, cfg.max_targets_per_trigger)]
    idx_by_ts: Dict[str, Dict[datetime, int]] = {
        sym: {c.ts: i for i, c in enumerate(histories.get(sym, []))}
        for sym in target_symbols
    }

    profit_samples: List[float] = []
    drawdown_samples: List[float] = []

    btc_triggers = 0
    entries = 0

    tp_first = 0
    sl_first = 0
    both_hit = 0
    first_hit_samples = 0

    for btc in btc_hist:
        if not btc_trigger_ok(btc, cfg):
            continue

        btc_triggers += 1
        ranked: List[Tuple[float, str, int]] = []

        for sym in target_symbols:
            hist = histories.get(sym, [])
            idx = idx_by_ts[sym].get(btc.ts)
            if idx is None:
                continue

            alt = hist[idx]
            if not target_candidate_ok(btc, alt, cfg):
                continue

            ranked.append((rank_target(btc, alt), sym, idx))

        if not ranked:
            continue

        ranked.sort(key=lambda x: x[0], reverse=True)
        selected = ranked[: cfg.top_n_per_trigger]

        for _, sym, idx in selected:
            hist = histories[sym]

            fwd = compute_forward_stats(hist, idx, cfg.forward_bars)
            if fwd is None:
                continue

            entries += 1
            profit_samples.append(fwd["profit_to_max_pct"])
            drawdown_samples.append(fwd["drawdown_to_min_pct"])

            hit = compute_first_hit(hist, idx, cfg.forward_bars, cfg.tp_pct, cfg.sl_pct)
            if hit in ("TP", "SL", "BOTH", "NONE"):
                first_hit_samples += 1
            if hit == "TP":
                tp_first += 1
            elif hit == "SL":
                sl_first += 1
            elif hit == "BOTH":
                both_hit += 1

    valid_forward_samples = len(profit_samples)
    avg_profit_max = avg(profit_samples) if profit_samples else 0.0
    avg_drawdown_min = avg(drawdown_samples) if drawdown_samples else 0.0
    median_profit_max = median(profit_samples) if profit_samples else 0.0
    median_drawdown_min = median(drawdown_samples) if drawdown_samples else 0.0

    tp_first_winrate = (tp_first / first_hit_samples) if first_hit_samples else 0.0
    sl_first_winrate = (sl_first / first_hit_samples) if first_hit_samples else 0.0

    return (
        valid_forward_samples,
        avg_profit_max,
        avg_drawdown_min,
        median_profit_max,
        median_drawdown_min,
        btc_triggers,
        entries,
        tp_first_winrate,
        sl_first_winrate,
    )


# ==========================================================
# Run grid
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
                tp_first_winrate,
                sl_first_winrate,
            ) = evaluate_config(combo_cfg, histories)

            parts = [
                f"valid_forward_samples={valid_forward_samples}",
                f"avg_profit_max={avg_profit_max * 100.0:.3f}%",
                f"avg_drawdown_min={avg_drawdown_min * 100.0:.3f}%",
                f"median_profit_max={median_profit_max * 100.0:.3f}%",
                f"median_drawdown_min={median_drawdown_min * 100.0:.3f}%",
                f"tp_first_winrate={tp_first_winrate * 100.0:.1f}%",
                f"sl_first_winrate={sl_first_winrate * 100.0:.1f}%",
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
