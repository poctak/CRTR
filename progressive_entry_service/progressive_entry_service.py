#!/usr/bin/env python3
# progressive_entry_replay_grid_search.py
# ------------------------------------------------------------
# Historical replay / dry-run grid search version
#
# Purpose:
# - replays historical DB candles for progressive_entry_service logic
# - iterates selected "important" params across predefined values
# - prints ONLY one line per combination
#
# Strategy logic:
# - BTC closed candle must pass trigger
# - on the SAME ts, target symbols are checked
# - targets with too large own candle are skipped
# - no DB writes, no real orders, dry-run only
#
# Output example:
# valid_forward_samples=182 | avg_profit_max=1.243% | avg_drawdown_min=-0.918% |
# btc_triggers=27 | entries=182 | btc_pump_pct_th=0.006 | ...
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

    # Poll-like logic
    cooldown_candles: int
    max_targets_per_trigger: int

    # BTC trigger
    btc_pump_pct_th: float
    btc_min_vq: float
    btc_require_green: bool

    # Target filters
    target_max_pump_pct: float
    target_max_range_pct: float
    target_require_green: bool

    # Replay options
    forward_bars: int


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

        cooldown_candles=env_int("COOLDOWN_CANDLES", 1),
        max_targets_per_trigger=env_int("MAX_TARGETS_PER_TRIGGER", 50),

        btc_pump_pct_th=env_float("BTC_PUMP_PCT_TH", 0.008),
        btc_min_vq=env_float("BTC_MIN_VQ", 0.0),
        btc_require_green=env_bool("BTC_REQUIRE_GREEN", True),

        target_max_pump_pct=env_float("TARGET_MAX_PUMP_PCT", 0.007),
        target_max_range_pct=env_float("TARGET_MAX_RANGE_PCT", 0.012),
        target_require_green=env_bool("TARGET_REQUIRE_GREEN", False),

        forward_bars=env_int("FORWARD_BARS", 12),
    )


# ==========================================================
# Grid config
# Vybral jsem parametry, které budou mít největší dopad:
# - BTC trigger citlivost
# - BTC min quote volume
# - target anti-overextension filtry
# - cooldown
# ==========================================================
GRID_CONFIG: Dict[str, List[Any]] = {
    "btc_pump_pct_th": [0.0050, 0.0065, 0.0080],
    "btc_min_vq": [0.0, 500000.0, 1500000.0],
    "btc_require_green": [True],

    "target_max_pump_pct": [0.0050, 0.0070, 0.0090],
    "target_max_range_pct": [0.0090, 0.0120, 0.0160],
    "target_require_green": [False, True],

    "cooldown_candles": [0, 1, 2],
}


# ==========================================================
# Helpers
# ==========================================================
def pct_change(o: float, c: float) -> float:
    if o <= 0:
        return 0.0
    return (c - o) / o


def pct_range(o: float, h: float, l: float) -> float:
    if o <= 0:
        return 0.0
    return (h - l) / o


def avg(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


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
            trades_count
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
            )
        )
    return out


# ==========================================================
# Forward stats
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
) -> Tuple[int, float, float, int, int]:
    btc_hist = histories.get(cfg.btc_symbol, [])
    if not btc_hist:
        return 0, 0.0, 0.0, 0, 0

    target_symbols = cfg.symbols[: max(0, cfg.max_targets_per_trigger)]

    # Fast lookup by ts for each target
    idx_by_ts: Dict[str, Dict[datetime, int]] = {}
    for sym in target_symbols:
        hist = histories.get(sym, [])
        idx_by_ts[sym] = {c.ts: i for i, c in enumerate(hist)}

    profit_samples: List[float] = []
    drawdown_samples: List[float] = []

    btc_triggers = 0
    entries = 0
    cooldown_left = 0

    # Historical replay of "new closed BTC candle detected"
    for btc in btc_hist:
        if cooldown_left > 0:
            cooldown_left -= 1
            continue

        o = btc.o
        c = btc.c
        vq = btc.v_quote
        p = pct_change(o, c)

        # BTC trigger checks
        if cfg.btc_require_green and c <= o:
            continue

        if vq < cfg.btc_min_vq:
            continue

        if p < cfg.btc_pump_pct_th:
            continue

        # Triggered
        btc_triggers += 1
        ts = btc.ts

        for sym in target_symbols:
            hist = histories.get(sym, [])
            if not hist:
                continue

            target_idx = idx_by_ts[sym].get(ts)
            if target_idx is None:
                continue

            cc = hist[target_idx]

            tpump = pct_change(cc.o, cc.c)
            trange = pct_range(cc.o, cc.h, cc.l)

            if cfg.target_require_green and cc.c <= cc.o:
                continue

            if tpump >= cfg.target_max_pump_pct:
                continue

            if trange >= cfg.target_max_range_pct:
                continue

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

    return valid_forward_samples, avg_profit_max, avg_drawdown_min, btc_triggers, entries


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
                btc_triggers,
                entries,
            ) = evaluate_config(combo_cfg, histories)

            parts = [
                f"valid_forward_samples={valid_forward_samples}",
                f"avg_profit_max={avg_profit_max * 100.0:.3f}%",
                f"avg_drawdown_min={avg_drawdown_min * 100.0:.3f}%",
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
