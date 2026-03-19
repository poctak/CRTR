#!/usr/bin/env python3
# accumulation_breakout_replay_service.py
# ------------------------------------------------------------
# Historical replay / dry-run version of accumulation_breakout_entry_service
#
# Purpose:
# - load already stored historical candles from public.candles_5m
# - replay them bar-by-bar from oldest to newest
# - evaluate the same setup + trigger logic
# - DO NOT insert trade intents
# - only log hypothetical intents
#
# Useful for:
# - validating PEPE / FET style patterns
# - understanding why signals fire
# - first-stage backtest before real trading
# ------------------------------------------------------------

import os
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import asyncpg


# ==========================================================
# Logging
# ==========================================================
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

    # Replay options
    allow_multiple_signals_per_symbol: bool
    cooldown_bars_after_signal: int
    debug_rejections: bool


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

        use_btc_filter=env_bool("USE_BTC_FILTER", True),
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
        trigger_close_pos_min=env_float("TRIGGER_CLOSE_POS_MIN", 0.75),
        trigger_volume_vs_setup_avg_min=env_float("TRIGGER_VOLUME_VS_SETUP_AVG_MIN", 1.8),
        trigger_buy_ratio_min=env_float("TRIGGER_BUY_RATIO_MIN", 0.55),
        trigger_delta_ratio_min=env_float("TRIGGER_DELTA_RATIO_MIN", 0.05),

        min_setup_quote_volume_sum=env_float("MIN_SETUP_QUOTE_VOLUME_SUM", 12000.0),
        min_trigger_quote_volume=env_float("MIN_TRIGGER_QUOTE_VOLUME", 3000.0),
        min_avg_trade_quote=env_float("MIN_AVG_TRADE_QUOTE", 80.0),

        resistance_lookback_bars=env_int("RESISTANCE_LOOKBACK_BARS", 8),
        breakout_above_recent_close_pct=env_float("BREAKOUT_ABOVE_RECENT_CLOSE_PCT", 0.0010),

        allow_multiple_signals_per_symbol=env_bool("ALLOW_MULTIPLE_SIGNALS_PER_SYMBOL", True),
        cooldown_bars_after_signal=env_int("COOLDOWN_BARS_AFTER_SIGNAL", 6),
        debug_rejections=env_bool("DEBUG_REJECTIONS", False),
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


def safe_ratio(a: float, b: float) -> float:
    return a / b if b > 0 else 0.0


def pct_change(a: float, b: float) -> float:
    if a <= 0:
        return 0.0
    return (b / a) - 1.0


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

    limit_sql = f"LIMIT {cfg.max_bars_per_symbol}"

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
        {limit_sql}
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
        cond_delta = c.taker_delta_ratio_quote <= cfg.absorption_delta_ratio_max
        cond_price = c.change_pct >= -cfg.absorption_max_down_move_pct
        if cond_delta and cond_price:
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

    checks = {
        "cond_change": trigger.change_pct >= cfg.trigger_change_pct_min,
        "cond_range": trigger.range_pct >= cfg.trigger_range_pct_min,
        "cond_close": trigger.close_pos_in_range >= cfg.trigger_close_pos_min,
        "cond_volume": volume_mult >= cfg.trigger_volume_vs_setup_avg_min,
        "cond_vq_abs": trigger.v_quote >= cfg.min_trigger_quote_volume,
        "cond_buy": trigger.buy_ratio_quote >= cfg.trigger_buy_ratio_min,
        "cond_delta": trigger.taker_delta_ratio_quote >= cfg.trigger_delta_ratio_min,
        "cond_break_ref": trigger.c >= recent_resistance or above_recent_close >= cfg.breakout_above_recent_close_pct,
        "cond_trade_size": trigger.avg_trade_quote >= cfg.min_avg_trade_quote,
    }

    if not checks["cond_change"]:
        return False, {}, f"trigger_change_low:{trigger.change_pct:.4f}"
    if not checks["cond_range"]:
        return False, {}, f"trigger_range_low:{trigger.range_pct:.4f}"
    if not checks["cond_close"]:
        return False, {}, f"trigger_close_pos_low:{trigger.close_pos_in_range:.3f}"
    if not checks["cond_volume"]:
        return False, {}, f"trigger_volume_mult_low:{volume_mult:.2f}"
    if not checks["cond_vq_abs"]:
        return False, {}, f"trigger_vq_low:{trigger.v_quote:.2f}"
    if not checks["cond_buy"]:
        return False, {}, f"trigger_buy_ratio_low:{trigger.buy_ratio_quote:.3f}"
    if not checks["cond_delta"]:
        return False, {}, f"trigger_delta_ratio_low:{trigger.taker_delta_ratio_quote:.3f}"
    if not checks["cond_break_ref"]:
        return False, {}, f"trigger_not_breaking_ref:recent_res={recent_resistance:.8f}"
    if not checks["cond_trade_size"]:
        return False, {}, f"trigger_avg_trade_low:{trigger.avg_trade_quote:.2f}"

    info = {
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
    }
    return True, info, "ok"


def analyze_symbol(history: List[Candle], cfg: Config) -> Tuple[Optional[Dict[str, Any]], str]:
    if len(history) < cfg.lookback_bars:
        return None, f"not_enough_candles:{len(history)}<{cfg.lookback_bars}"

    setup = history[-1 - cfg.setup_bars:-1]
    trigger = history[-1]

    if len(setup) < cfg.setup_bars:
        return None, "setup_too_short"

    setup_quote_sum = sum(c.v_quote for c in setup)
    if setup_quote_sum < cfg.min_setup_quote_volume_sum:
        return None, f"setup_quote_sum_low:{setup_quote_sum:.2f}"

    absorption_ok, absorption_hits = detect_absorption(setup, cfg)
    accumulation_ok, accumulation_hits = detect_accumulation(setup, cfg)
    compression_ok, compression_max_rng, compression_avg_rng = detect_compression(setup, cfg)

    if not (absorption_ok or accumulation_ok):
        return None, f"no_setup_pattern:abs={absorption_hits} acc={accumulation_hits}"

    if not compression_ok:
        return None, f"no_compression:max={compression_max_rng:.4f} avg={compression_avg_rng:.4f}"

    trigger_ok, trigger_info, trigger_reason = detect_breakout_trigger(history, cfg)
    if not trigger_ok:
        return None, trigger_reason

    local_support = min(c.l for c in setup)
    local_resistance = max(c.h for c in setup)

    score = 0
    score += 2 if absorption_ok else 0
    score += 2 if accumulation_ok else 0
    score += 1 if compression_ok else 0
    score += 2 if trigger_ok else 0
    score += 1 if trigger.buy_ratio_quote >= 0.65 else 0
    score += 1 if trigger.taker_delta_ratio_quote >= 0.20 else 0
    score += 1 if trigger.close_pos_in_range >= 0.90 else 0

    return {
        "support_price": local_support,
        "resistance": local_resistance,
        "setup_quote_sum": setup_quote_sum,
        "setup_avg_vq": avg([c.v_quote for c in setup]),
        "setup_avg_range_pct": avg([c.range_pct for c in setup]),
        "setup_avg_change_pct": avg([c.change_pct for c in setup]),
        "setup_absorption_ok": absorption_ok,
        "setup_absorption_hits": absorption_hits,
        "setup_accumulation_ok": accumulation_ok,
        "setup_accumulation_hits": accumulation_hits,
        "setup_compression_ok": compression_ok,
        "setup_compression_max_range_pct": compression_max_rng,
        "setup_compression_avg_range_pct": compression_avg_rng,
        "trigger": trigger_info,
        "last_close": trigger.c,
        "last_low": trigger.l,
        "window_from": history[0].ts.isoformat(),
        "window_to": history[-1].ts.isoformat(),
        "score": score,
    }, "ok"


# ==========================================================
# Replay engine
# ==========================================================
def compute_btc_regime_from_history(
    btc_hist: List[Candle],
    idx: int,
    cfg: Config,
) -> Optional[float]:
    need = cfg.btc_regime_lookback_bars
    if idx < need:
        return None
    start_c = btc_hist[idx - need].c
    end_c = btc_hist[idx].c
    return pct_change(start_c, end_c)


def btc_regime_blocked(delta: float, cfg: Config) -> bool:
    return delta <= cfg.btc_kill_dump_pct or delta >= cfg.btc_kill_pump_pct


async def run_replay(cfg: Config):
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
        logging.info(
            "REPLAY START | db=%s@%s:%d/%s | symbols=%d | table=%s | start=%s end=%s",
            cfg.db_user, cfg.db_host, cfg.db_port, cfg.db_name,
            len(cfg.symbols), cfg.candles_table, cfg.start_ts, cfg.end_ts
        )

        # load all histories
        histories: Dict[str, List[Candle]] = {}
        for sym in sorted(set([s.upper() for s in cfg.symbols] + [cfg.btc_symbol.upper()])):
            hist = await fetch_symbol_history(pool, cfg, sym)
            histories[sym] = hist
            logging.info("LOADED %s | bars=%d", sym, len(hist))

        btc_hist = histories.get(cfg.btc_symbol.upper(), [])
        if cfg.use_btc_filter and not btc_hist:
            raise RuntimeError(f"BTC history missing for {cfg.btc_symbol}")

        # per-symbol replay state
        signal_counts: Dict[str, int] = {s.upper(): 0 for s in cfg.symbols}
        cooldown_until_idx: Dict[str, int] = {s.upper(): -1 for s in cfg.symbols}

        total_checked = 0
        total_signals = 0
        total_rejected = 0
        total_blocked_btc = 0

        for sym in cfg.symbols:
            sym = sym.upper()
            hist = histories.get(sym, [])

            if len(hist) < cfg.lookback_bars:
                logging.warning("SKIP %s | not enough history (%d bars)", sym, len(hist))
                continue

            logging.info("REPLAY SYMBOL START | %s | bars=%d", sym, len(hist))

            # align BTC by index only if same 5m regularity; simple first version
            # assumes same number/order of bars for the tested period
            for idx in range(cfg.lookback_bars - 1, len(hist)):
                total_checked += 1

                if not cfg.allow_multiple_signals_per_symbol and signal_counts[sym] > 0:
                    break

                if idx <= cooldown_until_idx[sym]:
                    continue

                if cfg.use_btc_filter:
                    btc_idx = min(idx, len(btc_hist) - 1)
                    btc_delta = compute_btc_regime_from_history(btc_hist, btc_idx, cfg)
                    if btc_delta is None:
                        continue
                    if btc_regime_blocked(btc_delta, cfg):
                        total_blocked_btc += 1
                        continue
                else:
                    btc_delta = None

                history_slice = hist[:idx + 1]
                setup, reason = analyze_symbol(history_slice, cfg)

                if not setup:
                    total_rejected += 1
                    if cfg.debug_rejections:
                        logging.info(
                            "REJECT %s | ts=%s | reason=%s",
                            sym,
                            history_slice[-1].ts.isoformat(),
                            reason,
                        )
                    continue

                signal_counts[sym] += 1
                total_signals += 1
                cooldown_until_idx[sym] = idx + cfg.cooldown_bars_after_signal

                logging.warning(
                    "REPLAY_INTENT %s | ts=%s | close=%.8f support=%.8f resistance=%.8f | score=%d | abs=%s(%d) acc=%s(%d) comp=%s | trig_chg=%.3f%% trig_rng=%.3f%% trig_close=%.2f trig_vmult=%.2f trig_buy=%.3f trig_delta=%.3f btc_delta=%s",
                    sym,
                    history_slice[-1].ts.isoformat(),
                    setup["last_close"],
                    setup["support_price"],
                    setup["resistance"],
                    setup["score"],
                    setup["setup_absorption_ok"],
                    setup["setup_absorption_hits"],
                    setup["setup_accumulation_ok"],
                    setup["setup_accumulation_hits"],
                    setup["setup_compression_ok"],
                    setup["trigger"]["trigger_change_pct"] * 100.0,
                    setup["trigger"]["trigger_range_pct"] * 100.0,
                    setup["trigger"]["trigger_close_pos_in_range"],
                    setup["trigger"]["trigger_volume_mult_vs_setup_avg"],
                    setup["trigger"]["trigger_buy_ratio_quote"],
                    setup["trigger"]["trigger_delta_ratio_quote"],
                    (f"{btc_delta * 100.0:+.3f}%" if btc_delta is not None else "disabled"),
                )

            logging.info(
                "REPLAY SYMBOL DONE | %s | signals=%d",
                sym,
                signal_counts[sym]
            )

        logging.info(
            "REPLAY DONE | checked=%d signals=%d rejected=%d btc_blocked=%d",
            total_checked,
            total_signals,
            total_rejected,
            total_blocked_btc,
        )

        for sym in cfg.symbols:
            logging.info("SUMMARY %s | signals=%d", sym.upper(), signal_counts[sym.upper()])

    finally:
        await pool.close()


def main():
    cfg = load_config()
    asyncio.run(run_replay(cfg))


if __name__ == "__main__":
    main()
