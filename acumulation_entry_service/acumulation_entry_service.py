#!/usr/bin/env python3
# accumulation_precision_replay_grid.py
# ------------------------------------------------------------
# Historical replay / dry-run grid search
# Strategy: PRECISION accumulation
#
# FIXED / IMPROVED:
# - multi-TP replay
# - TP1 partial exit
# - TP2 for remaining size
# - trailing activates only after activation threshold
# - no lookahead in trailing logic
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


def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    return int(v) if v else default


def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    try:
        return float(v) if v else default
    except Exception:
        return default


def env_str(name: str, default: str) -> str:
    v = os.getenv(name, "").strip()
    return v if v else default


def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")


def env_list(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default).strip()
    out: List[str] = []
    for x in raw.split(","):
        x = x.strip().upper()
        if x:
            out.append(x)
    return out


@dataclass
class Config:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    candles_table: str
    symbols: List[str]

    start_ts: Optional[str]
    end_ts: Optional[str]
    max_bars_per_symbol: int

    anchor_symbol: str
    btc_symbol: str
    btc_lookback_bars: int
    btc_kill_dump_pct: float
    btc_kill_pump_pct: float

    alt_green_on: float
    alt_green_off: float
    alt_green_min_alts: int
    alt_green_move: float
    alt_exclude_symbols: List[str]

    leader_symbols: List[str]
    leader_min_green: int
    leader_green_move: float

    lookback_bars: int
    recent_bars_for_signal: int

    acc_range_max_pct: float
    acc_support_touches_min: int
    acc_support_touches_max: int
    support_touch_tolerance_pct: float
    support_defense_close_min_pct: float
    touch_buy_ratio_min: float

    compression_recent_bars: int
    compression_prev_bars: int
    compression_factor_max: float

    require_higher_lows: bool
    higher_lows_bars: int
    higher_lows_min_count: int

    sweep_lookback_bars: int
    sweep_below_support_pct: float
    sweep_reclaim_close_above_support: bool

    min_bounce_from_support_pct: float
    resistance_touch_tolerance_pct: float
    resistance_tests_min: int

    min_quote_volume_sum: float
    volume_dryup_touch_vs_avg_max: float

    allow_multiple_signals_per_symbol: bool
    cooldown_bars_after_signal: int
    forward_bars: int

    # replay pnl
    sl_pct: float
    tp1_pct: float
    tp2_pct: float
    tp1_size: float
    trailing_activation_pct: float
    trailing_after_activation_pct: float
    fee_roundtrip_pct: float


def load_config() -> Config:
    return Config(
        db_host=env_str("DB_HOST", "db"),
        db_port=env_int("DB_PORT", 5432),
        db_name=env_str("DB_NAME", "pumpdb"),
        db_user=env_str("DB_USER", "pumpuser"),
        db_password=env_str("DB_PASSWORD", ""),

        candles_table=env_str("CANDLES_TABLE", "public.candles_5m"),
        symbols=env_list("SYMBOLS", ""),

        start_ts=env_str("START_TS", "") or None,
        end_ts=env_str("END_TS", "") or None,
        max_bars_per_symbol=env_int("MAX_BARS_PER_SYMBOL", 50000),

        anchor_symbol=env_str("ANCHOR_SYMBOL", "BTCUSDC").upper(),
        btc_symbol=env_str("BTC_SYMBOL", "BTCUSDC").upper(),
        btc_lookback_bars=env_int("BTC_LOOKBACK_BARS", 3),
        btc_kill_dump_pct=env_float("BTC_KILL_DUMP_PCT", -0.008),
        btc_kill_pump_pct=env_float("BTC_KILL_PUMP_PCT", 0.012),

        alt_green_on=env_float("ALT_GREEN_ON", 0.62),
        alt_green_off=env_float("ALT_GREEN_OFF", 0.55),
        alt_green_min_alts=env_int("ALT_GREEN_MIN_ALTS", 30),
        alt_green_move=env_float("ALT_GREEN_MOVE", 0.002),
        alt_exclude_symbols=env_list(
            "ALT_EXCLUDE_SYMBOLS",
            "BTCUSDC,USDCUSDT,FDUSDUSDC,TUSDUSDC,USDPUSDC"
        ),

        leader_symbols=env_list(
            "LEADER_SYMBOLS",
            "ETHUSDC,SOLUSDC,BNBUSDC,XRPUSDC,ADAUSDC,DOGEUSDC,LINKUSDC,AVAXUSDC"
        ),
        leader_min_green=env_int("LEADER_MIN_GREEN", 5),
        leader_green_move=env_float("LEADER_GREEN_MOVE", 0.002),

        lookback_bars=env_int("ACC_LOOKBACK_BARS", 24),
        recent_bars_for_signal=env_int("ACC_RECENT_BARS_FOR_SIGNAL", 3),

        acc_range_max_pct=env_float("ACC_RANGE_MAX_PCT", 0.025),
        acc_support_touches_min=env_int("ACC_SUPPORT_TOUCHES_MIN", 3),
        acc_support_touches_max=env_int("ACC_SUPPORT_TOUCHES_MAX", 4),
        support_touch_tolerance_pct=env_float("SUPPORT_TOUCH_TOLERANCE_PCT", 0.0035),
        support_defense_close_min_pct=env_float("SUPPORT_DEFENSE_CLOSE_MIN_PCT", 0.0015),
        touch_buy_ratio_min=env_float("TOUCH_BUY_RATIO_MIN", 0.55),

        compression_recent_bars=env_int("COMPRESSION_RECENT_BARS", 6),
        compression_prev_bars=env_int("COMPRESSION_PREV_BARS", 6),
        compression_factor_max=env_float("COMPRESSION_FACTOR_MAX", 0.90),

        require_higher_lows=env_bool("REQUIRE_HIGHER_LOWS", False),
        higher_lows_bars=env_int("HIGHER_LOWS_BARS", 5),
        higher_lows_min_count=env_int("HIGHER_LOWS_MIN_COUNT", 3),

        sweep_lookback_bars=env_int("SWEEP_LOOKBACK_BARS", 3),
        sweep_below_support_pct=env_float("SWEEP_BELOW_SUPPORT_PCT", 0.003),
        sweep_reclaim_close_above_support=env_bool("SWEEP_RECLAIM_CLOSE_ABOVE_SUPPORT", True),

        min_bounce_from_support_pct=env_float("MIN_BOUNCE_FROM_SUPPORT_PCT", 0.005),
        resistance_touch_tolerance_pct=env_float("RESISTANCE_TOUCH_TOLERANCE_PCT", 0.004),
        resistance_tests_min=env_int("RESISTANCE_TESTS_MIN", 2),

        min_quote_volume_sum=env_float("MIN_QUOTE_VOLUME_SUM", 50000.0),
        volume_dryup_touch_vs_avg_max=env_float("VOLUME_DRYUP_TOUCH_VS_AVG_MAX", 1.05),

        allow_multiple_signals_per_symbol=env_bool("ALLOW_MULTIPLE_SIGNALS_PER_SYMBOL", True),
        cooldown_bars_after_signal=env_int("COOLDOWN_BARS_AFTER_SIGNAL", 12),
        forward_bars=env_int("FORWARD_BARS", 72),

        sl_pct=env_float("SL_PCT", 0.007),
        tp1_pct=env_float("TP1_PCT", 0.006),
        tp2_pct=env_float("TP2_PCT", 0.010),
        tp1_size=env_float("TP1_SIZE", 0.50),
        trailing_activation_pct=env_float("TRAILING_ACTIVATION_PCT", 0.009),
        trailing_after_activation_pct=env_float("TRAILING_AFTER_ACTIVATION_PCT", 0.008),
        fee_roundtrip_pct=env_float("FEE_ROUNDTRIP_PCT", 0.0015),
    )


GRID_CONFIG: Dict[str, List[Any]] = {
    "acc_range_max_pct": [0.022, 0.025],
    "support_touch_tolerance_pct": [0.0030, 0.0035],
    "touch_buy_ratio_min": [0.55, 0.58],
    "min_bounce_from_support_pct": [0.005, 0.006],
    "resistance_tests_min": [2, 3],
    "acc_support_touches_min": [3, 4],
    "compression_factor_max": [0.85, 0.90],
    "support_defense_close_min_pct": [0.0015, 0.0020],
}


@dataclass
class Candle:
    ts: datetime
    o: float
    h: float
    l: float
    c: float
    v_quote: float
    buy_ratio_quote: float
    change_pct: float
    range_pct: float
    close_pos_in_range: float
    is_green: bool


def pct_change(a: float, b: float) -> float:
    if a <= 0:
        return 0.0
    return (b / a) - 1.0


def avg(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def safe_ratio(a: float, b: float) -> float:
    return a / b if b > 0 else 0.0


def format_value(v: Any) -> str:
    if isinstance(v, float):
        return f"{v:.6f}".rstrip("0").rstrip(".")
    return str(v)


def iter_grid_configs(base_cfg: Config):
    keys = list(GRID_CONFIG.keys())
    for combo in product(*(GRID_CONFIG[k] for k in keys)):
        updates = dict(zip(keys, combo))
        yield replace(base_cfg, **updates), updates


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
            ts, o, h, l, c,
            v_quote, buy_ratio_quote, change_pct,
            range_pct, close_pos_in_range, is_green
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
            buy_ratio_quote=float(r["buy_ratio_quote"] or 0.0),
            change_pct=float(r["change_pct"] or 0.0),
            range_pct=float(r["range_pct"] or 0.0),
            close_pos_in_range=float(r["close_pos_in_range"] or 0.0),
            is_green=bool(r["is_green"]),
        )
        for r in rows
    ]


def btc_kill_switch(delta: float, cfg: Config) -> bool:
    return delta <= cfg.btc_kill_dump_pct or delta >= cfg.btc_kill_pump_pct


def compute_btc_change_from_history(btc_hist: List[Candle], idx: int, cfg: Config) -> Optional[float]:
    if idx < cfg.btc_lookback_bars:
        return None
    return pct_change(btc_hist[idx - cfg.btc_lookback_bars].c, btc_hist[idx].c)


def breadth_hysteresis(prev_risk_on: bool, ratio: float, cfg: Config) -> bool:
    if not prev_risk_on:
        return ratio >= cfg.alt_green_on
    return ratio > cfg.alt_green_off


def compute_breadth_for_ts(
    ts: datetime,
    histories: Dict[str, List[Candle]],
    idx_by_ts: Dict[str, Dict[datetime, int]],
    cfg: Config,
    prev_risk_on: bool,
) -> Tuple[bool, float, int, int, bool]:
    exclude = set(s.upper() for s in cfg.alt_exclude_symbols)
    universe = [s.upper() for s in cfg.symbols if s.upper() not in exclude]

    green_metric = 0.0
    total_metric = 0.0
    leader_green = 0
    leader_total = 0
    leader_set = set(s.upper() for s in cfg.leader_symbols)
    available = 0

    for sym in universe:
        hist = histories.get(sym)
        if not hist:
            continue
        idx = idx_by_ts.get(sym, {}).get(ts)
        if idx is None:
            continue

        c = hist[idx]
        available += 1
        total_metric += c.v_quote

        if c.c >= c.o * (1.0 + cfg.alt_green_move):
            green_metric += c.v_quote

        if sym in leader_set:
            leader_total += 1
            if c.c >= c.o * (1.0 + cfg.leader_green_move):
                leader_green += 1

    ratio = safe_ratio(green_metric, total_metric)
    risk_on = breadth_hysteresis(prev_risk_on, ratio, cfg) if available >= cfg.alt_green_min_alts else False
    leader_ok = leader_green >= cfg.leader_min_green
    return risk_on, ratio, leader_green, leader_total, leader_ok


def detect_higher_lows(candles: List[Candle], cfg: Config) -> Tuple[bool, int]:
    if len(candles) < cfg.higher_lows_bars:
        return False, 0
    lows = [c.l for c in candles[-cfg.higher_lows_bars:]]
    inc = sum(1 for i in range(1, len(lows)) if lows[i] >= lows[i - 1])
    return inc >= cfg.higher_lows_min_count, inc


def detect_compression(candles: List[Candle], cfg: Config) -> Tuple[bool, float, float]:
    need = cfg.compression_recent_bars + cfg.compression_prev_bars
    if len(candles) < need:
        return False, 0.0, 0.0

    recent = candles[-cfg.compression_recent_bars:]
    prev = candles[-need:-cfg.compression_recent_bars]
    recent_avg = avg([safe_ratio(c.h - c.l, c.c if c.c > 0 else 1.0) for c in recent])
    prev_avg = avg([safe_ratio(c.h - c.l, c.c if c.c > 0 else 1.0) for c in prev])

    if prev_avg <= 0:
        return False, recent_avg, prev_avg
    return recent_avg <= prev_avg * cfg.compression_factor_max, recent_avg, prev_avg


def detect_sweep(candles: List[Candle], support: float, cfg: Config) -> Tuple[bool, Optional[Dict[str, Any]]]:
    if len(candles) < cfg.sweep_lookback_bars:
        return False, None

    check = candles[-cfg.sweep_lookback_bars:]
    threshold = support * (1.0 - cfg.sweep_below_support_pct)

    for c in reversed(check):
        below = c.l < threshold
        reclaimed = (c.c > support) if cfg.sweep_reclaim_close_above_support else True
        if below and reclaimed:
            return True, {"ts": c.ts.isoformat(), "low": c.l, "close": c.c}
    return False, None


def analyze_symbol(candles: List[Candle], cfg: Config) -> Tuple[Optional[Dict[str, Any]], str]:
    if len(candles) < cfg.lookback_bars:
        return None, "not_enough_candles"

    win = candles[-cfg.lookback_bars:]
    last = win[-1]

    support = min(c.l for c in win)
    resistance = max(c.h for c in win)
    if support <= 0:
        return None, "invalid_support"

    range_pct = safe_ratio(resistance - support, support)
    if range_pct > cfg.acc_range_max_pct:
        return None, "range_too_wide"

    touch_tol_abs = support * cfg.support_touch_tolerance_pct
    support_touches = [c for c in win if abs(c.l - support) <= touch_tol_abs]
    touches = len(support_touches)

    if touches < cfg.acc_support_touches_min:
        return None, "support_touches_low"
    if touches > cfg.acc_support_touches_max:
        return None, "support_touches_high"

    defense_count = sum(1 for c in support_touches if c.c >= support * (1.0 + cfg.support_defense_close_min_pct))
    support_defense_ratio = safe_ratio(defense_count, touches)
    touch_buy_ratio = avg([c.buy_ratio_quote for c in support_touches]) if support_touches else 0.0

    if support_defense_ratio < 0.50:
        return None, "support_defense_low"
    if touch_buy_ratio < cfg.touch_buy_ratio_min:
        return None, "touch_buy_ratio_low"

    total_vq = sum(c.v_quote for c in win)
    if total_vq < cfg.min_quote_volume_sum:
        return None, "volume_sum_low"

    avg_vq = avg([c.v_quote for c in win])
    touch_avg_vq = avg([c.v_quote for c in support_touches]) if support_touches else 0.0
    volume_dryup_ok = touch_avg_vq <= avg_vq * cfg.volume_dryup_touch_vs_avg_max

    compression_ok, recent_rng, prev_rng = detect_compression(win, cfg)
    hl_ok, hl_count = detect_higher_lows(win, cfg)
    if cfg.require_higher_lows and not hl_ok:
        return None, "higher_lows_failed"

    sweep_ok, sweep_info = detect_sweep(win, support, cfg)

    bounce_pct = safe_ratio(last.c - support, support)
    if bounce_pct < cfg.min_bounce_from_support_pct and not sweep_ok:
        return None, "bounce_too_small"

    rtol_abs = resistance * cfg.resistance_touch_tolerance_pct
    resistance_tests = sum(1 for c in win if abs(c.h - resistance) <= rtol_abs)
    if resistance_tests < cfg.resistance_tests_min:
        return None, "resistance_tests_low"

    recent_ok = any(
        abs(c.l - support) <= touch_tol_abs or (c.l < support * (1.0 - cfg.sweep_below_support_pct))
        for c in win[-cfg.recent_bars_for_signal:]
    )
    if not recent_ok:
        return None, "no_recent_touch_or_sweep"

    score = 1
    score += 1 if compression_ok else 0
    score += 1 if volume_dryup_ok else 0
    score += 1 if hl_ok else 0
    score += 1 if sweep_ok else 0
    score += 1 if support_defense_ratio >= 0.66 else 0
    score += 1 if resistance_tests >= max(cfg.resistance_tests_min, 2) else 0

    return {
        "last_close": last.c,
        "score": score,
        "range_pct": range_pct,
        "touches": touches,
        "bounce_pct": bounce_pct,
        "support_defense_ratio": support_defense_ratio,
        "compression_ok": compression_ok,
        "compression_recent_avg": recent_rng,
        "compression_prev_avg": prev_rng,
        "higher_lows_ok": hl_ok,
        "higher_lows_count": hl_count,
        "sweep_detected": sweep_ok,
        "sweep_info": sweep_info,
        "volume_sum_quote": total_vq,
        "volume_avg_quote": avg_vq,
        "volume_touch_avg_quote": touch_avg_vq,
        "volume_dryup_ok": volume_dryup_ok,
        "resistance_tests": resistance_tests,
    }, "ok"


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


def compute_trade_pnl_multi_tp(
    hist: List[Candle],
    entry_idx: int,
    forward_bars: int,
    sl_pct: float,
    tp1_pct: float,
    tp2_pct: float,
    tp1_size: float,
    trailing_activation_pct: float,
    trailing_after_activation_pct: float,
    fee_roundtrip_pct: float,
) -> Optional[Tuple[float, str]]:
    entry = hist[entry_idx]
    entry_price = entry.c
    future = hist[entry_idx + 1: entry_idx + 1 + forward_bars]

    if not future:
        return None

    tp1_size = max(0.0, min(1.0, tp1_size))
    rem_size = 1.0 - tp1_size

    sl_price = entry_price * (1.0 - sl_pct)
    tp1_price = entry_price * (1.0 + tp1_pct)
    tp2_price = entry_price * (1.0 + tp2_pct)
    trailing_activation_price = entry_price * (1.0 + trailing_activation_pct)

    realized_pnl = 0.0
    remaining_size = 1.0

    tp1_hit = False
    trailing_armed = False
    highest_after_activation = 0.0
    active_trailing_stop: Optional[float] = None

    outcome = "TIME_NO_TP1"

    for c in future:
        if not tp1_hit:
            hit_sl = c.l <= sl_price
            hit_tp1 = c.h >= tp1_price

            if hit_sl and hit_tp1:
                realized_pnl += remaining_size * ((sl_price / entry_price) - 1.0)
                outcome = "SL_BEFORE_TP1"
                remaining_size = 0.0
                break

            if hit_sl:
                realized_pnl += remaining_size * ((sl_price / entry_price) - 1.0)
                outcome = "SL_BEFORE_TP1"
                remaining_size = 0.0
                break

            if hit_tp1:
                if tp1_size > 0.0:
                    realized_pnl += tp1_size * ((tp1_price / entry_price) - 1.0)

                remaining_size = rem_size
                tp1_hit = True

                if remaining_size <= 0.0:
                    outcome = "TP1_FULL"
                    break

                # trailing ještě není aktivní automaticky
                if c.h >= trailing_activation_price:
                    trailing_armed = True
                    highest_after_activation = c.h
                    active_trailing_stop = highest_after_activation * (1.0 - trailing_after_activation_pct)
                else:
                    trailing_armed = False
                    highest_after_activation = 0.0
                    active_trailing_stop = None

                outcome = "TIME_AFTER_TP1"
                continue

        else:
            # 1) trailing stop z minulé candle
            hit_trail = trailing_armed and active_trailing_stop is not None and c.l <= active_trailing_stop
            hit_tp2 = c.h >= tp2_price

            if hit_trail and hit_tp2:
                realized_pnl += remaining_size * ((active_trailing_stop / entry_price) - 1.0)
                outcome = "TRAIL_AFTER_TP1"
                remaining_size = 0.0
                break

            if hit_trail:
                realized_pnl += remaining_size * ((active_trailing_stop / entry_price) - 1.0)
                outcome = "TRAIL_AFTER_TP1"
                remaining_size = 0.0
                break

            if hit_tp2:
                realized_pnl += remaining_size * ((tp2_price / entry_price) - 1.0)
                outcome = "TP2"
                remaining_size = 0.0
                break

            # 2) trailing activation / update až po vyhodnocení candle
            if not trailing_armed:
                if c.h >= trailing_activation_price:
                    trailing_armed = True
                    highest_after_activation = c.h
                    active_trailing_stop = highest_after_activation * (1.0 - trailing_after_activation_pct)
            else:
                highest_after_activation = max(highest_after_activation, c.h)
                active_trailing_stop = highest_after_activation * (1.0 - trailing_after_activation_pct)

            outcome = "TIME_AFTER_TP1"

    if remaining_size > 0.0:
        exit_price = future[-1].c
        realized_pnl += remaining_size * ((exit_price / entry_price) - 1.0)
        outcome = "TIME_AFTER_TP1" if tp1_hit else "TIME_NO_TP1"

    realized_pnl -= fee_roundtrip_pct
    return realized_pnl, outcome


def evaluate_config(
    cfg: Config,
    histories: Dict[str, List[Candle]],
) -> Tuple[int, float, float, float, float, int, float, float, float, Dict[str, int]]:
    btc_hist = histories.get(cfg.btc_symbol.upper(), [])
    if not btc_hist:
        return 0, 0.0, 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0, {}

    idx_by_ts: Dict[str, Dict[datetime, int]] = {
        sym.upper(): {c.ts: i for i, c in enumerate(hist)}
        for sym, hist in histories.items()
    }

    profit_samples: List[float] = []
    drawdown_samples: List[float] = []
    pnl_samples: List[float] = []

    signals = 0
    signal_counts = {s.upper(): 0 for s in cfg.symbols}
    cooldown_until_idx = {s.upper(): -1 for s in cfg.symbols}
    prev_risk_on = False

    outcomes: Dict[str, int] = {}

    for idx in range(cfg.lookback_bars - 1, len(btc_hist)):
        ts = btc_hist[idx].ts
        btc_delta = compute_btc_change_from_history(btc_hist, idx, cfg)
        if btc_delta is None or btc_kill_switch(btc_delta, cfg):
            continue

        risk_on, _, _, _, leader_ok = compute_breadth_for_ts(ts, histories, idx_by_ts, cfg, prev_risk_on)
        prev_risk_on = risk_on
        if not risk_on or not leader_ok:
            continue

        exclude_set = set(s.upper() for s in cfg.alt_exclude_symbols)

        for sym in cfg.symbols:
            sym = sym.upper()
            if sym == cfg.btc_symbol.upper() or sym in exclude_set or sym not in histories:
                continue

            if not cfg.allow_multiple_signals_per_symbol and signal_counts[sym] > 0:
                continue

            hist = histories[sym]
            sym_idx = idx_by_ts[sym].get(ts)
            if sym_idx is None:
                continue
            if sym_idx <= cooldown_until_idx[sym]:
                continue
            if sym_idx + 1 >= len(hist):
                continue
            if sym_idx + 1 < cfg.lookback_bars:
                continue

            candles = hist[:sym_idx + 1]
            setup, _ = analyze_symbol(candles, cfg)
            if not setup:
                continue

            signals += 1
            signal_counts[sym] += 1
            cooldown_until_idx[sym] = sym_idx + cfg.cooldown_bars_after_signal

            fwd = compute_forward_stats(hist, sym_idx, cfg.forward_bars)
            if fwd:
                profit_samples.append(fwd["profit_to_max_pct"])
                drawdown_samples.append(fwd["drawdown_to_min_pct"])

            pnl_result = compute_trade_pnl_multi_tp(
                hist=hist,
                entry_idx=sym_idx,
                forward_bars=cfg.forward_bars,
                sl_pct=cfg.sl_pct,
                tp1_pct=cfg.tp1_pct,
                tp2_pct=cfg.tp2_pct,
                tp1_size=cfg.tp1_size,
                trailing_activation_pct=cfg.trailing_activation_pct,
                trailing_after_activation_pct=cfg.trailing_after_activation_pct,
                fee_roundtrip_pct=cfg.fee_roundtrip_pct,
            )
            if pnl_result is not None:
                pnl, outcome = pnl_result
                pnl_samples.append(pnl)
                outcomes[outcome] = outcomes.get(outcome, 0) + 1

    valid_forward_samples = len(profit_samples)
    avg_profit_max = avg(profit_samples) if profit_samples else 0.0
    avg_drawdown_min = avg(drawdown_samples) if drawdown_samples else 0.0
    median_profit_max = median(profit_samples) if profit_samples else 0.0
    median_drawdown_min = median(drawdown_samples) if drawdown_samples else 0.0

    total_net_pct = sum(pnl_samples) if pnl_samples else 0.0
    avg_net_trade = avg(pnl_samples) if pnl_samples else 0.0
    win_rate = safe_ratio(len([p for p in pnl_samples if p > 0]), len(pnl_samples))

    return (
        valid_forward_samples,
        avg_profit_max,
        avg_drawdown_min,
        median_profit_max,
        median_drawdown_min,
        signals,
        total_net_pct,
        avg_net_trade,
        win_rate,
        outcomes,
    )


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
        all_symbols = sorted(set([s.upper() for s in cfg.symbols] + [cfg.btc_symbol.upper()]))

        for sym in all_symbols:
            histories[sym] = await fetch_symbol_history(pool, cfg, sym)

        for combo_cfg, combo_updates in iter_grid_configs(cfg):
            (
                valid_forward_samples,
                avg_profit_max,
                avg_drawdown_min,
                median_profit_max,
                median_drawdown_min,
                signals,
                total_net_pct,
                avg_net_trade,
                win_rate,
                outcomes,
            ) = evaluate_config(combo_cfg, histories)

            outcome_parts = []
            for k in [
                "SL_BEFORE_TP1",
                "TP1_FULL",
                "TP2",
                "TRAIL_AFTER_TP1",
                "TIME_NO_TP1",
                "TIME_AFTER_TP1",
            ]:
                outcome_parts.append(f"{k}={outcomes.get(k, 0)}")

            parts = [
                f"valid_forward_samples={valid_forward_samples}",
                f"avg_profit_max={avg_profit_max * 100.0:.3f}%",
                f"avg_drawdown_min={avg_drawdown_min * 100.0:.3f}%",
                f"median_profit_max={median_profit_max * 100.0:.3f}%",
                f"median_drawdown_min={median_drawdown_min * 100.0:.3f}%",
                f"signals={signals}",
                f"avg_net_trade={avg_net_trade * 100.0:.3f}%",
                f"total_net_pct={total_net_pct * 100.0:.3f}%",
                f"win_rate={win_rate * 100.0:.2f}%",
                f"sl_pct={combo_cfg.sl_pct * 100.0:.3f}%",
                f"tp1_pct={combo_cfg.tp1_pct * 100.0:.3f}%",
                f"tp2_pct={combo_cfg.tp2_pct * 100.0:.3f}%",
                f"tp1_size={combo_cfg.tp1_size:.2f}",
                f"trailing_activation_pct={combo_cfg.trailing_activation_pct * 100.0:.3f}%",
                f"trailing_after_activation_pct={combo_cfg.trailing_after_activation_pct * 100.0:.3f}%",
                f"fee_roundtrip_pct={combo_cfg.fee_roundtrip_pct * 100.0:.3f}%",
            ]

            for k in GRID_CONFIG.keys():
                parts.append(f"{k}={format_value(combo_updates[k])}")

            parts.extend(outcome_parts)
            print(" | ".join(parts), flush=True)

    finally:
        await pool.close()


def main():
    cfg = load_config()
    asyncio.run(run_grid(cfg))


if __name__ == "__main__":
    main()
