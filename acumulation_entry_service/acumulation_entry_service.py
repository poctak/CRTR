#!/usr/bin/env python3
# accumulation_entry_service.py
# ------------------------------------------------------------
# Detects ACCUM_PHASE on candles stored in Postgres/Timescale
# and creates REAL trade intents into public.trade_intents.
#
# Output:
#   - writes BUY intents into public.trade_intents
#
# Notes:
#   - intended for executor_service.py
#   - writes status=NEW
#   - supports MARKET or LIMIT
#   - for MARKET, limit_price is used only as reference price placeholder
#   - support_price is filled from detected support
#   - idempotent insert uses ON CONFLICT (symbol, ts) DO NOTHING
# ------------------------------------------------------------

import os
import json
import time
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)


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
    items: List[str] = []
    for x in (v or "").split(","):
        x = x.strip().upper()
        if x:
            items.append(x)
    return items


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

    # tables / symbols
    candles_table: str
    trade_intents_table: str
    symbols: List[str]

    # polling
    poll_sec: int
    max_symbols_per_cycle: int

    # market regime
    anchor_symbol: str
    btc_symbol: str
    btc_lookback_bars: int
    btc_kill_dump_pct: float
    btc_kill_pump_pct: float

    # breadth
    alt_green_on: float
    alt_green_off: float
    alt_green_min_alts: int
    alt_green_cache_sec: float
    alt_green_move: float
    alt_exclude_symbols: List[str]

    # leaders
    leader_symbols: List[str]
    leader_min_green: int
    leader_green_move: float

    # local setup
    lookback_bars: int
    recent_bars_for_signal: int

    # structure
    acc_range_max_pct: float
    acc_support_touches_min: int
    acc_support_touches_max: int
    support_touch_tolerance_pct: float
    support_defense_close_min_pct: float
    touch_buy_ratio_min: float

    # compression
    compression_recent_bars: int
    compression_prev_bars: int
    compression_factor_max: float

    # higher lows
    require_higher_lows: bool
    higher_lows_bars: int
    higher_lows_min_count: int

    # sweep
    sweep_lookback_bars: int
    sweep_below_support_pct: float
    sweep_reclaim_close_above_support: bool

    # bounce / resistance
    min_bounce_from_support_pct: float
    resistance_touch_tolerance_pct: float
    resistance_tests_min: int

    # volume
    min_quote_volume_sum: float
    volume_dryup_touch_vs_avg_max: float

    # intent
    signal_name: str
    signal_source: str
    quote_amount: float
    entry_mode: str
    intent_status: str
    intent_side: str


def load_config() -> Config:
    return Config(
        # DB
        db_host=env_str("DB_HOST", "db"),
        db_port=env_int("DB_PORT", 5432),
        db_name=env_str("DB_NAME", "pumpdb"),
        db_user=env_str("DB_USER", "pumpuser"),
        db_password=env_str("DB_PASSWORD", ""),

        # tables / symbols
        candles_table=env_str("CANDLES_TABLE", "public.candles_5m"),
        trade_intents_table=env_str("TRADE_INTENTS_TABLE", "public.trade_intents"),
        symbols=env_list("SYMBOLS", ""),

        # polling
        poll_sec=env_int("POLL_SEC", 5),
        max_symbols_per_cycle=env_int("MAX_SYMBOLS_PER_CYCLE", 300),

        # regime
        anchor_symbol=env_str("ANCHOR_SYMBOL", "BTCUSDC"),
        btc_symbol=env_str("BTC_SYMBOL", "BTCUSDC"),
        btc_lookback_bars=env_int("BTC_LOOKBACK_BARS", 3),
        btc_kill_dump_pct=env_float("BTC_KILL_DUMP_PCT", -0.008),
        btc_kill_pump_pct=env_float("BTC_KILL_PUMP_PCT", 0.012),

        # breadth
        alt_green_on=env_float("ALT_GREEN_ON", 0.62),
        alt_green_off=env_float("ALT_GREEN_OFF", 0.55),
        alt_green_min_alts=env_int("ALT_GREEN_MIN_ALTS", 30),
        alt_green_cache_sec=env_float("ALT_GREEN_CACHE_SEC", 15.0),
        alt_green_move=env_float("ALT_GREEN_MOVE", 0.002),
        alt_exclude_symbols=env_list(
            "ALT_EXCLUDE_SYMBOLS",
            "BTCUSDC,USDCUSDT,FDUSDUSDC,TUSDUSDC,USDPUSDC"
        ),

        # leaders
        leader_symbols=env_list(
            "LEADER_SYMBOLS",
            "ETHUSDC,SOLUSDC,BNBUSDC,XRPUSDC,ADAUSDC,DOGEUSDC,LINKUSDC,AVAXUSDC"
        ),
        leader_min_green=env_int("LEADER_MIN_GREEN", 5),
        leader_green_move=env_float("LEADER_GREEN_MOVE", 0.002),

        # local setup
        lookback_bars=env_int("ACC_LOOKBACK_BARS", 24),
        recent_bars_for_signal=env_int("ACC_RECENT_BARS_FOR_SIGNAL", 3),

        # structure
        acc_range_max_pct=env_float("ACC_RANGE_MAX_PCT", 0.035),
        acc_support_touches_min=env_int("ACC_SUPPORT_TOUCHES_MIN", 2),
        acc_support_touches_max=env_int("ACC_SUPPORT_TOUCHES_MAX", 4),
        support_touch_tolerance_pct=env_float("SUPPORT_TOUCH_TOLERANCE_PCT", 0.003),
        support_defense_close_min_pct=env_float("SUPPORT_DEFENSE_CLOSE_MIN_PCT", 0.0015),
        touch_buy_ratio_min=env_float("TOUCH_BUY_RATIO_MIN", 0.52),

        # compression
        compression_recent_bars=env_int("COMPRESSION_RECENT_BARS", 6),
        compression_prev_bars=env_int("COMPRESSION_PREV_BARS", 6),
        compression_factor_max=env_float("COMPRESSION_FACTOR_MAX", 0.90),

        # higher lows
        require_higher_lows=env_bool("REQUIRE_HIGHER_LOWS", False),
        higher_lows_bars=env_int("HIGHER_LOWS_BARS", 5),
        higher_lows_min_count=env_int("HIGHER_LOWS_MIN_COUNT", 3),

        # sweep
        sweep_lookback_bars=env_int("SWEEP_LOOKBACK_BARS", 3),
        sweep_below_support_pct=env_float("SWEEP_BELOW_SUPPORT_PCT", 0.003),
        sweep_reclaim_close_above_support=env_bool("SWEEP_RECLAIM_CLOSE_ABOVE_SUPPORT", True),

        # bounce / resistance
        min_bounce_from_support_pct=env_float("MIN_BOUNCE_FROM_SUPPORT_PCT", 0.004),
        resistance_touch_tolerance_pct=env_float("RESISTANCE_TOUCH_TOLERANCE_PCT", 0.004),
        resistance_tests_min=env_int("RESISTANCE_TESTS_MIN", 1),

        # volume
        min_quote_volume_sum=env_float("MIN_QUOTE_VOLUME_SUM", 50000.0),
        volume_dryup_touch_vs_avg_max=env_float("VOLUME_DRYUP_TOUCH_VS_AVG_MAX", 1.10),

        # intent
        signal_name=env_str("ACCUM_SIGNAL_NAME", "ACCUM_PHASE"),
        signal_source=env_str("ACCUM_SIGNAL_SOURCE", "ACCUM"),
        quote_amount=env_float("QUOTE_AMOUNT", 15.0),
        entry_mode=env_str("ENTRY_MODE", "MARKET").upper(),
        intent_status=env_str("INTENT_STATUS", "NEW").upper(),
        intent_side=env_str("INTENT_SIDE", "BUY").upper(),
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


@dataclass
class BreadthState:
    risk_on: bool
    ratio: float
    green_metric: float
    total_metric: float
    anchor_ts: Optional[datetime]
    leader_green: int
    leader_total: int
    leader_ok: bool
    fetched_at: float


# ==========================================================
# Helpers
# ==========================================================
def pct_change(a: float, b: float) -> float:
    if a <= 0:
        return 0.0
    return (b / a) - 1.0


def avg(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def safe_ratio(a: float, b: float) -> float:
    return a / b if b > 0 else 0.0


# ==========================================================
# DB helpers
# ==========================================================
async def fetch_anchor_ts(pool: asyncpg.Pool, cfg: Config) -> Optional[datetime]:
    q = f"""
        SELECT MAX(ts) AS ts
        FROM {cfg.candles_table}
        WHERE symbol=$1
    """
    row = await pool.fetchrow(q, cfg.anchor_symbol)
    return row["ts"] if row and row["ts"] else None


async def fetch_recent_candles(pool: asyncpg.Pool, cfg: Config, symbol: str, limit: int) -> List[Candle]:
    q = f"""
        SELECT ts, o, h, l, c, v_quote
        FROM {cfg.candles_table}
        WHERE symbol=$1
        ORDER BY ts DESC
        LIMIT $2
    """
    rows = await pool.fetch(q, symbol, limit)

    out: List[Candle] = []
    for r in reversed(rows):
        out.append(
            Candle(
                ts=r["ts"],
                o=float(r["o"]),
                h=float(r["h"]),
                l=float(r["l"]),
                c=float(r["c"]),
                v_quote=float(r["v_quote"] or 0.0),
            )
        )
    return out


async def fetch_snapshot_rows(pool: asyncpg.Pool, cfg: Config, ts: datetime, universe: List[str]):
    q = f"""
        SELECT symbol, o, c, v_quote
        FROM {cfg.candles_table}
        WHERE ts=$1
          AND symbol = ANY($2::text[])
    """
    return await pool.fetch(q, ts, universe)


async def recent_same_intent_exists(
    pool: asyncpg.Pool,
    cfg: Config,
    symbol: str,
    ts: datetime,
) -> bool:
    q = f"""
        SELECT 1
        FROM {cfg.trade_intents_table}
        WHERE symbol=$1
          AND ts=$2
        LIMIT 1
    """
    row = await pool.fetchrow(q, symbol, ts)
    return row is not None


async def has_pending_intent(
    pool: asyncpg.Pool,
    cfg: Config,
    symbol: str,
) -> bool:
    q = f"""
        SELECT 1
        FROM {cfg.trade_intents_table}
        WHERE symbol=$1
          AND status IN ('NEW', 'SENT')
        LIMIT 1
    """
    row = await pool.fetchrow(q, symbol)
    return row is not None


async def insert_trade_intent(
    pool: asyncpg.Pool,
    cfg: Config,
    symbol: str,
    ts: datetime,
    ref_price: float,
    support_price: float,
    details: Dict[str, Any],
) -> Tuple[Optional[int], str]:
    if await recent_same_intent_exists(pool, cfg, symbol, ts):
        return None, "duplicate_ts"

    if await has_pending_intent(pool, cfg, symbol):
        return None, "pending_exists"

    meta = {
        "reason": cfg.signal_name,
        "ref_price": float(ref_price),
        "version": "2026-03-12",
        **details,
    }

    q = f"""
        INSERT INTO {cfg.trade_intents_table}(
            symbol,
            ts,
            source,
            side,
            quote_amount,
            limit_price,
            support_price,
            meta,
            status,
            created_at,
            updated_at,
            entry_mode
        )
        VALUES(
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8::jsonb,
            $9,
            NOW(),
            NOW(),
            $10
        )
        ON CONFLICT (symbol, ts) DO NOTHING
        RETURNING id
    """
    row = await pool.fetchrow(
        q,
        symbol,
        ts,
        cfg.signal_source,
        cfg.intent_side,
        float(cfg.quote_amount),
        float(ref_price),
        float(support_price),
        json.dumps(meta),
        cfg.intent_status,
        cfg.entry_mode,
    )
    if not row:
        return None, "conflict"
    return int(row["id"]), "inserted"


# ==========================================================
# Market regime
# ==========================================================
def breadth_hysteresis(prev_risk_on: bool, ratio: float, cfg: Config) -> bool:
    if not prev_risk_on:
        return ratio >= cfg.alt_green_on
    return ratio > cfg.alt_green_off


async def fetch_btc_change(pool: asyncpg.Pool, cfg: Config) -> Optional[float]:
    candles = await fetch_recent_candles(pool, cfg, cfg.btc_symbol, cfg.btc_lookback_bars + 1)
    if len(candles) < cfg.btc_lookback_bars + 1:
        return None
    return pct_change(candles[0].c, candles[-1].c)


def btc_kill_switch(delta: float, cfg: Config) -> bool:
    return delta <= cfg.btc_kill_dump_pct or delta >= cfg.btc_kill_pump_pct


async def compute_breadth(pool: asyncpg.Pool, cfg: Config, prev_state: BreadthState) -> BreadthState:
    anchor_ts = await fetch_anchor_ts(pool, cfg)
    if anchor_ts is None:
        return BreadthState(
            risk_on=False,
            ratio=0.0,
            green_metric=0.0,
            total_metric=0.0,
            anchor_ts=None,
            leader_green=0,
            leader_total=0,
            leader_ok=False,
            fetched_at=time.time(),
        )

    exclude = set(s.upper() for s in cfg.alt_exclude_symbols)
    universe = [s for s in cfg.symbols if s and s not in exclude]
    rows = await fetch_snapshot_rows(pool, cfg, anchor_ts, universe)

    green_metric = 0.0
    total_metric = 0.0
    leader_green = 0
    leader_total = 0
    leader_set = set(s.upper() for s in cfg.leader_symbols)

    for r in rows:
        sym = str(r["symbol"]).upper()
        o = float(r["o"])
        c = float(r["c"])
        vq = float(r["v_quote"] or 0.0)

        if o <= 0:
            continue

        total_metric += vq
        if c >= o * (1.0 + cfg.alt_green_move):
            green_metric += vq

        if sym in leader_set:
            leader_total += 1
            if c >= o * (1.0 + cfg.leader_green_move):
                leader_green += 1

    ratio = safe_ratio(green_metric, total_metric)

    if len(rows) >= cfg.alt_green_min_alts:
        risk_on = breadth_hysteresis(prev_state.risk_on, ratio, cfg)
    else:
        risk_on = False

    leader_ok = leader_green >= cfg.leader_min_green

    return BreadthState(
        risk_on=risk_on,
        ratio=ratio,
        green_metric=green_metric,
        total_metric=total_metric,
        anchor_ts=anchor_ts,
        leader_green=leader_green,
        leader_total=leader_total,
        leader_ok=leader_ok,
        fetched_at=time.time(),
    )


# ==========================================================
# Local setup detection
# ==========================================================
def detect_higher_lows(candles: List[Candle], cfg: Config) -> Tuple[bool, int]:
    if len(candles) < cfg.higher_lows_bars:
        return False, 0

    lows = [c.l for c in candles[-cfg.higher_lows_bars:]]
    inc = 0
    for i in range(1, len(lows)):
        if lows[i] >= lows[i - 1]:
            inc += 1
    return inc >= cfg.higher_lows_min_count, inc


def detect_compression(candles: List[Candle], cfg: Config) -> Tuple[bool, float, float]:
    need = cfg.compression_recent_bars + cfg.compression_prev_bars
    if len(candles) < need:
        return False, 0.0, 0.0

    recent = candles[-cfg.compression_recent_bars:]
    prev = candles[-need:-cfg.compression_recent_bars]

    recent_ranges = [safe_ratio(c.h - c.l, c.c if c.c > 0 else 1.0) for c in recent]
    prev_ranges = [safe_ratio(c.h - c.l, c.c if c.c > 0 else 1.0) for c in prev]

    recent_avg = avg(recent_ranges)
    prev_avg = avg(prev_ranges)

    if prev_avg <= 0:
        return False, recent_avg, prev_avg

    return recent_avg <= prev_avg * cfg.compression_factor_max, recent_avg, prev_avg


def detect_sweep(
    candles: List[Candle],
    support: float,
    cfg: Config,
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    if len(candles) < cfg.sweep_lookback_bars:
        return False, None

    check = candles[-cfg.sweep_lookback_bars:]
    threshold = support * (1.0 - cfg.sweep_below_support_pct)

    for c in reversed(check):
        below = c.l < threshold
        reclaimed = (c.c > support) if cfg.sweep_reclaim_close_above_support else True
        if below and reclaimed:
            return True, {
                "ts": c.ts.isoformat(),
                "low": c.l,
                "close": c.c,
                "threshold": threshold,
            }
    return False, None


def analyze_symbol(candles: List[Candle], cfg: Config) -> Tuple[Optional[Dict[str, Any]], str]:
    if len(candles) < cfg.lookback_bars:
        return None, f"not_enough_candles:{len(candles)}<{cfg.lookback_bars}"

    win = candles[-cfg.lookback_bars:]
    last = win[-1]

    support = min(c.l for c in win)
    resistance = max(c.h for c in win)

    if support <= 0:
        return None, "invalid_support"

    range_pct = safe_ratio(resistance - support, support)
    if range_pct > cfg.acc_range_max_pct:
        return None, f"range_too_wide:{range_pct:.4f}>{cfg.acc_range_max_pct:.4f}"

    touch_tol_abs = support * cfg.support_touch_tolerance_pct
    support_touches = [c for c in win if abs(c.l - support) <= touch_tol_abs]
    touches = len(support_touches)

    if touches < cfg.acc_support_touches_min:
        return None, f"support_touches_low:{touches}<{cfg.acc_support_touches_min}"

    if touches > cfg.acc_support_touches_max:
        return None, f"support_touches_high:{touches}>{cfg.acc_support_touches_max}"

    defense_count = sum(
        1 for c in support_touches
        if c.c >= support * (1.0 + cfg.support_defense_close_min_pct)
    )
    support_defense_ratio = safe_ratio(defense_count, touches)

    touch_buy_ratio = safe_ratio(
        sum(1 for c in support_touches if c.c > c.o),
        touches
    )

    if support_defense_ratio < 0.50:
        return None, f"support_defense_low:{support_defense_ratio:.3f}<0.500"

    if touch_buy_ratio < cfg.touch_buy_ratio_min:
        return None, f"touch_buy_ratio_low:{touch_buy_ratio:.3f}<{cfg.touch_buy_ratio_min:.3f}"

    total_vq = sum(c.v_quote for c in win)
    if total_vq < cfg.min_quote_volume_sum:
        return None, f"volume_sum_low:{total_vq:.2f}<{cfg.min_quote_volume_sum:.2f}"

    avg_vq = avg([c.v_quote for c in win])
    touch_avg_vq = avg([c.v_quote for c in support_touches])
    volume_dryup_ok = touch_avg_vq <= avg_vq * cfg.volume_dryup_touch_vs_avg_max

    compression_ok, recent_rng, prev_rng = detect_compression(win, cfg)

    hl_ok, hl_count = detect_higher_lows(win, cfg)
    if cfg.require_higher_lows and not hl_ok:
        return None, f"higher_lows_required_but_failed:{hl_count}<{cfg.higher_lows_min_count}"

    sweep_ok, sweep_info = detect_sweep(win, support, cfg)

    bounce_pct = safe_ratio(last.c - support, support)
    if bounce_pct < cfg.min_bounce_from_support_pct and not sweep_ok:
        return None, f"bounce_too_small:{bounce_pct:.4f}<{cfg.min_bounce_from_support_pct:.4f}_and_no_sweep"

    rtol_abs = resistance * cfg.resistance_touch_tolerance_pct
    resistance_tests = sum(1 for c in win if abs(c.h - resistance) <= rtol_abs)
    if resistance_tests < cfg.resistance_tests_min:
        return None, f"resistance_tests_low:{resistance_tests}<{cfg.resistance_tests_min}"

    recent_ok = any(
        abs(c.l - support) <= touch_tol_abs or (c.l < support * (1.0 - cfg.sweep_below_support_pct))
        for c in win[-cfg.recent_bars_for_signal:]
    )
    if not recent_ok:
        return None, f"no_recent_touch_or_sweep:last_{cfg.recent_bars_for_signal}_bars"

    score = 0
    score += 1
    score += 1 if compression_ok else 0
    score += 1 if volume_dryup_ok else 0
    score += 1 if hl_ok else 0
    score += 1 if sweep_ok else 0
    score += 1 if support_defense_ratio >= 0.66 else 0
    score += 1 if resistance_tests >= max(cfg.resistance_tests_min, 2) else 0

    return {
        "support": support,
        "support_price": support,
        "low": last.l,
        "range_low": support,
        "range_high": resistance,
        "resistance": resistance,
        "range_pct": range_pct,
        "support_touches": touches,
        "support_defense_ratio": support_defense_ratio,
        "touch_buy_ratio": touch_buy_ratio,
        "compression_ok": compression_ok,
        "compression_recent_avg": recent_rng,
        "compression_prev_avg": prev_rng,
        "higher_lows_ok": hl_ok,
        "higher_lows_count": hl_count,
        "sweep_detected": sweep_ok,
        "sweep_info": sweep_info,
        "bounce_pct": bounce_pct,
        "resistance_tests": resistance_tests,
        "volume_sum_quote": total_vq,
        "volume_touch_avg_quote": touch_avg_vq,
        "volume_avg_quote": avg_vq,
        "volume_dryup_ok": volume_dryup_ok,
        "last_close": last.c,
        "last_low": last.l,
        "window_from": win[0].ts.isoformat(),
        "window_to": win[-1].ts.isoformat(),
        "score": score,
    }, "ok"


# ==========================================================
# Main loop
# ==========================================================
async def run(cfg: Config):
    if not cfg.symbols:
        raise RuntimeError("SYMBOLS is empty")

    if cfg.entry_mode not in ("MARKET", "LIMIT"):
        raise RuntimeError("ENTRY_MODE must be MARKET or LIMIT")

    if cfg.intent_side != "BUY":
        raise RuntimeError("INTENT_SIDE must be BUY for executor compatibility")

    pool = await asyncpg.create_pool(
        host=cfg.db_host,
        port=cfg.db_port,
        database=cfg.db_name,
        user=cfg.db_user,
        password=cfg.db_password,
        min_size=1,
        max_size=5,
    )

    breadth = BreadthState(
        risk_on=False,
        ratio=0.0,
        green_metric=0.0,
        total_metric=0.0,
        anchor_ts=None,
        leader_green=0,
        leader_total=0,
        leader_ok=False,
        fetched_at=0.0,
    )

    logging.info(
        (
            "ACCUM ENTRY service started | db=%s@%s:%d/%s | symbols=%d | source=%s | "
            "entry_mode=%s quote_amount=%.2f status=%s side=%s | "
            "breadth ON/OFF=%.2f/%.2f min_alts=%d move=%.3f | "
            "leaders=%d min_green=%d move=%.3f | "
            "BTC kill=[<=%.3f%% or >=%.3f%%]"
        ),
        cfg.db_user, cfg.db_host, cfg.db_port, cfg.db_name,
        len(cfg.symbols),
        cfg.signal_source,
        cfg.entry_mode,
        cfg.quote_amount,
        cfg.intent_status,
        cfg.intent_side,
        cfg.alt_green_on,
        cfg.alt_green_off,
        cfg.alt_green_min_alts,
        cfg.alt_green_move,
        len(cfg.leader_symbols),
        cfg.leader_min_green,
        cfg.leader_green_move,
        cfg.btc_kill_dump_pct * 100.0,
        cfg.btc_kill_pump_pct * 100.0,
    )

    try:
        while True:
            try:
                now_ts = time.time()

                if now_ts - breadth.fetched_at >= cfg.alt_green_cache_sec:
                    breadth = await compute_breadth(pool, cfg, breadth)
                    logging.info(
                        "BREADTH | anchor=%s risk_on=%s ratio=%.3f vol=(%.2f/%.2f) leaders=%d/%d leader_ok=%s ON=%.2f OFF=%.2f MIN=%d",
                        breadth.anchor_ts.isoformat() if breadth.anchor_ts else None,
                        breadth.risk_on,
                        breadth.ratio,
                        breadth.green_metric,
                        breadth.total_metric,
                        breadth.leader_green,
                        breadth.leader_total,
                        breadth.leader_ok,
                        cfg.alt_green_on,
                        cfg.alt_green_off,
                        cfg.alt_green_min_alts,
                    )

                btc_delta = await fetch_btc_change(pool, cfg)
                if btc_delta is None:
                    logging.warning("BTC_CHANGE unavailable")
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                if btc_kill_switch(btc_delta, cfg):
                    logging.info(
                        "REGIME_BLOCKED | BTC_KILL_SWITCH | delta=%+.3f%%",
                        btc_delta * 100.0
                    )
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                if not breadth.risk_on:
                    logging.info(
                        "REGIME_BLOCKED | breadth risk_off | ratio=%.3f",
                        breadth.ratio
                    )
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                if not breadth.leader_ok:
                    logging.info(
                        "REGIME_BLOCKED | leader confirmation failed | leaders=%d/%d need>=%d",
                        breadth.leader_green,
                        breadth.leader_total,
                        cfg.leader_min_green,
                    )
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                exclude_set = set(s.upper() for s in cfg.alt_exclude_symbols)
                scan_symbols = cfg.symbols[:cfg.max_symbols_per_cycle]

                checked = 0
                skipped_excluded = 0
                skipped_btc = 0
                skipped_short_history = 0
                rejected = 0
                inserted = 0
                duplicates = 0
                pending_skips = 0

                logging.info(
                    "ACCUM_SCAN_START | symbols=%d max=%d btc_delta=%+.3f%% breadth=%.3f leaders=%d/%d",
                    len(scan_symbols),
                    cfg.max_symbols_per_cycle,
                    btc_delta * 100.0,
                    breadth.ratio,
                    breadth.leader_green,
                    breadth.leader_total,
                )

                for sym in scan_symbols:
                    sym = sym.upper()

                    if sym == cfg.btc_symbol.upper():
                        skipped_btc += 1
                        logging.debug("ACCUM_SKIP %s | reason=btc_symbol", sym)
                        continue

                    if sym in exclude_set:
                        skipped_excluded += 1
                        logging.debug("ACCUM_SKIP %s | reason=excluded_symbol", sym)
                        continue

                    checked += 1

                    candles = await fetch_recent_candles(pool, cfg, sym, cfg.lookback_bars)
                    if len(candles) < cfg.lookback_bars:
                        skipped_short_history += 1
                        logging.info(
                            "ACCUM_SKIP %s | reason=not_enough_candles got=%d need=%d",
                            sym, len(candles), cfg.lookback_bars
                        )
                        continue

                    setup, reason = analyze_symbol(candles, cfg)
                    if not setup:
                        rejected += 1
                        logging.info(
                            "ACCUM_REJECT %s | ts=%s | reason=%s",
                            sym,
                            candles[-1].ts.isoformat(),
                            reason,
                        )
                        continue

                    signal_ts = candles[-1].ts
                    ref_price = float(setup["last_close"])
                    support_price = float(setup["support_price"])

                    details = {
                        "source": cfg.signal_source,
                        "symbol": sym,
                        "ts": signal_ts.isoformat(),
                        "signal": cfg.signal_name,
                        "regime": {
                            "breadth_risk_on": breadth.risk_on,
                            "breadth_ratio": breadth.ratio,
                            "breadth_anchor_ts": breadth.anchor_ts.isoformat() if breadth.anchor_ts else None,
                            "leader_green": breadth.leader_green,
                            "leader_total": breadth.leader_total,
                            "leader_ok": breadth.leader_ok,
                            "btc_delta": btc_delta,
                        },
                        "setup": setup,
                        "support": setup["support"],
                        "support_price": setup["support_price"],
                        "low": setup["low"],
                        "range_low": setup["range_low"],
                        "range_high": setup["range_high"],
                    }

                    intent_id, result = await insert_trade_intent(
                        pool=pool,
                        cfg=cfg,
                        symbol=sym,
                        ts=signal_ts,
                        ref_price=ref_price,
                        support_price=support_price,
                        details=details,
                    )

                    if intent_id is not None:
                        inserted += 1
                        logging.warning(
                            "ACCUM_INTENT_CREATED id=%d %s | ts=%s | mode=%s quote=%.2f | ref=%.8f support=%.8f resistance=%.8f range=%.2f%% touches=%d score=%d sweep=%s breadth=%.3f leaders=%d/%d btcΔ=%+.3f%%",
                            intent_id,
                            sym,
                            signal_ts.isoformat(),
                            cfg.entry_mode,
                            cfg.quote_amount,
                            ref_price,
                            support_price,
                            setup["resistance"],
                            setup["range_pct"] * 100.0,
                            setup["support_touches"],
                            setup["score"],
                            setup["sweep_detected"],
                            breadth.ratio,
                            breadth.leader_green,
                            breadth.leader_total,
                            btc_delta * 100.0,
                        )
                    else:
                        if result == "pending_exists":
                            pending_skips += 1
                            logging.info(
                                "ACCUM_PENDING_SKIP %s | ts=%s | existing NEW/SENT intent already exists",
                                sym,
                                signal_ts.isoformat(),
                            )
                        else:
                            duplicates += 1
                            logging.info(
                                "ACCUM_INTENT_DUPLICATE %s | ts=%s | reason=%s",
                                sym,
                                signal_ts.isoformat(),
                                result,
                            )

                logging.info(
                    "ACCUM_SCAN_DONE | checked=%d inserted=%d duplicates=%d pending_skips=%d rejected=%d short_history=%d skipped_btc=%d skipped_excluded=%d",
                    checked,
                    inserted,
                    duplicates,
                    pending_skips,
                    rejected,
                    skipped_short_history,
                    skipped_btc,
                    skipped_excluded,
                )

                await asyncio.sleep(cfg.poll_sec)

            except Exception as e:
                logging.exception("LOOP_ERROR: %s", e)
                await asyncio.sleep(3)

    finally:
        await pool.close()


def main():
    cfg = load_config()
    asyncio.run(run(cfg))


if __name__ == "__main__":
    main()
