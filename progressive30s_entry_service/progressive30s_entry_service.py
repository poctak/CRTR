#!/usr/bin/env python3
# pattern_replay_service.py
# ------------------------------------------------------------
# Replays historical candles from Postgres / Timescale and logs
# hypothetical BUY intents for simple pre-pump patterns.
#
# This version:
# - is less strict than previous one
# - should produce some signals again
# - supports one-shot replay OR loop replay with sleep
# - DOES NOT place any real orders
#
# Logs only:
#   REPLAY START
#   REPLAY_INTENT
#   REPLAY SUMMARY
#   REPLAY DONE
# ------------------------------------------------------------

import os
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from statistics import mean
from typing import Dict, List, Optional, Tuple

import asyncpg


# ==========================================================
# ENV helpers
# ==========================================================
def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    return int(v) if v else default


def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    return float(v) if v else default


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
    if not raw:
        return []
    return [x.strip().upper() for x in raw.split(",") if x.strip()]


def env_optional_iso(name: str) -> Optional[datetime]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


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
    start_ts: Optional[datetime]
    end_ts: Optional[datetime]
    limit_symbols: int

    forward_bars: int

    setup_bars: int
    absorption_bars: int
    accumulation_bars: int
    compression_recent_bars: int
    compression_prev_bars: int

    min_score: int
    max_distance_from_support_pct: float

    require_strong_pattern: bool
    strong_absorption_min_hits: int
    strong_accumulation_min_hits: int

    trigger_change_pct_min: float
    trigger_change_pct_max: float
    trigger_range_pct_min: float
    trigger_close_pos_min: float
    trigger_volume_vs_setup_avg_min: float
    trigger_buy_ratio_min: float
    trigger_delta_ratio_min: float

    absorption_touch_tolerance_pct: float
    absorption_min_buy_ratio: float
    absorption_min_delta_ratio: float
    absorption_min_close_pos: float

    accumulation_touch_tolerance_pct: float
    accumulation_max_range_pct: float
    accumulation_min_defended_hits: int
    accumulation_min_green_hits: int
    accumulation_max_touch_volume_vs_avg: float

    compression_factor_max: float

    oneshot: bool
    replay_sleep_sec: int
    log_level: str


def load_config() -> Config:
    return Config(
        db_host=env_str("DB_HOST", "db"),
        db_port=env_int("DB_PORT", 5432),
        db_name=env_str("DB_NAME", "pumpdb"),
        db_user=env_str("DB_USER", "pumpuser"),
        db_password=env_str("DB_PASSWORD", ""),
        candles_table=env_str("CANDLES_TABLE", "public.candles_5m"),

        symbols=env_list("SYMBOLS"),
        start_ts=env_optional_iso("REPLAY_START_TS"),
        end_ts=env_optional_iso("REPLAY_END_TS"),
        limit_symbols=env_int("LIMIT_SYMBOLS", 500),

        forward_bars=env_int("FORWARD_BARS", 72),

        setup_bars=env_int("SETUP_BARS", 24),
        absorption_bars=env_int("ABSORPTION_BARS", 6),
        accumulation_bars=env_int("ACCUMULATION_BARS", 12),
        compression_recent_bars=env_int("COMPRESSION_RECENT_BARS", 6),
        compression_prev_bars=env_int("COMPRESSION_PREV_BARS", 6),

        # Mírnější než minule
        min_score=env_int("MIN_SCORE", 7),
        max_distance_from_support_pct=env_float("MAX_DISTANCE_FROM_SUPPORT_PCT", 0.0105),  # 1.05%

        # Už ne tak brutální
        require_strong_pattern=env_bool("REQUIRE_STRONG_PATTERN", False),
        strong_absorption_min_hits=env_int("STRONG_ABSORPTION_MIN_HITS", 2),
        strong_accumulation_min_hits=env_int("STRONG_ACCUMULATION_MIN_HITS", 2),

        # Tohle byl hlavní problém -> povoleno z 0.45% na 0.30%
        trigger_change_pct_min=env_float("TRIGGER_CHANGE_PCT_MIN", 0.0030),   # 0.30%
        trigger_change_pct_max=env_float("TRIGGER_CHANGE_PCT_MAX", 0.0110),   # 1.10%
        trigger_range_pct_min=env_float("TRIGGER_RANGE_PCT_MIN", 0.0040),     # 0.40%
        trigger_close_pos_min=env_float("TRIGGER_CLOSE_POS_MIN", 0.82),
        trigger_volume_vs_setup_avg_min=env_float("TRIGGER_VOLUME_VS_SETUP_AVG_MIN", 2.20),
        trigger_buy_ratio_min=env_float("TRIGGER_BUY_RATIO_MIN", 0.62),
        trigger_delta_ratio_min=env_float("TRIGGER_DELTA_RATIO_MIN", 0.20),

        absorption_touch_tolerance_pct=env_float("ABSORPTION_TOUCH_TOLERANCE_PCT", 0.0035),
        absorption_min_buy_ratio=env_float("ABSORPTION_MIN_BUY_RATIO", 0.58),
        absorption_min_delta_ratio=env_float("ABSORPTION_MIN_DELTA_RATIO", 0.15),
        absorption_min_close_pos=env_float("ABSORPTION_MIN_CLOSE_POS", 0.52),

        accumulation_touch_tolerance_pct=env_float("ACCUMULATION_TOUCH_TOLERANCE_PCT", 0.0040),
        accumulation_max_range_pct=env_float("ACCUMULATION_MAX_RANGE_PCT", 0.022),
        accumulation_min_defended_hits=env_int("ACCUMULATION_MIN_DEFENDED_HITS", 2),
        accumulation_min_green_hits=env_int("ACCUMULATION_MIN_GREEN_HITS", 1),
        accumulation_max_touch_volume_vs_avg=env_float("ACCUMULATION_MAX_TOUCH_VOLUME_VS_AVG", 1.20),

        compression_factor_max=env_float("COMPRESSION_FACTOR_MAX", 0.90),

        # Důležité proti restart loopům
        oneshot=env_bool("ONESHOT", True),
        replay_sleep_sec=env_int("REPLAY_SLEEP_SEC", 3600),

        log_level=env_str("LOG_LEVEL", "WARNING").upper(),
    )


# ==========================================================
# Logging
# ==========================================================
def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.WARNING),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logging.getLogger().setLevel(getattr(logging, level, logging.WARNING))


# ==========================================================
# Model
# ==========================================================
@dataclass
class Candle:
    symbol: str
    ts: datetime
    o: float
    h: float
    l: float
    c: float
    v_quote: float
    trades_count: int
    buy_ratio_quote: float
    taker_delta_ratio_quote: float

    @property
    def change_pct(self) -> float:
        return pct_change(self.o, self.c)

    @property
    def range_pct(self) -> float:
        if self.o <= 0:
            return 0.0
        return (self.h - self.l) / self.o

    @property
    def is_green(self) -> bool:
        return self.c > self.o

    @property
    def close_pos_in_range(self) -> float:
        rng = self.h - self.l
        if rng <= 0:
            return 0.5
        return max(0.0, min(1.0, (self.c - self.l) / rng))


# ==========================================================
# Helpers
# ==========================================================
def pct_change(a: float, b: float) -> float:
    if a <= 0:
        return 0.0
    return (b - a) / a


def safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0


def avg(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


# ==========================================================
# DB
# ==========================================================
async def fetch_all_candles(pool: asyncpg.Pool, cfg: Config, symbol: str) -> List[Candle]:
    where = ["symbol = $1"]
    params: List[object] = [symbol]

    if cfg.start_ts:
        where.append(f"ts >= ${len(params) + 1}")
        params.append(cfg.start_ts)

    if cfg.end_ts:
        where.append(f"ts <= ${len(params) + 1}")
        params.append(cfg.end_ts)

    sql = f"""
        SELECT
            symbol,
            ts,
            o, h, l, c,
            v_quote,
            COALESCE(trades_count, 0) AS trades_count,
            COALESCE(buy_ratio_quote, 0) AS buy_ratio_quote,
            COALESCE(taker_delta_ratio_quote, 0) AS taker_delta_ratio_quote
        FROM {cfg.candles_table}
        WHERE {" AND ".join(where)}
        ORDER BY ts ASC
    """

    rows = await pool.fetch(sql, *params)
    out: List[Candle] = []
    for r in rows:
        out.append(
            Candle(
                symbol=str(r["symbol"]).upper(),
                ts=r["ts"],
                o=float(r["o"]),
                h=float(r["h"]),
                l=float(r["l"]),
                c=float(r["c"]),
                v_quote=float(r["v_quote"] or 0.0),
                trades_count=int(r["trades_count"] or 0),
                buy_ratio_quote=float(r["buy_ratio_quote"] or 0.0),
                taker_delta_ratio_quote=float(r["taker_delta_ratio_quote"] or 0.0),
            )
        )
    return out


# ==========================================================
# Pattern detection
# ==========================================================
def detect_absorption(setup: List[Candle], cfg: Config) -> Tuple[bool, int]:
    if len(setup) < cfg.absorption_bars:
        return False, 0

    recent = setup[-cfg.absorption_bars:]
    support = min(c.l for c in setup)
    tol = support * cfg.absorption_touch_tolerance_pct

    hits = 0
    for c in recent:
        near_support = abs(c.l - support) <= tol
        if not near_support:
            continue

        if (
            c.buy_ratio_quote >= cfg.absorption_min_buy_ratio
            and c.taker_delta_ratio_quote >= cfg.absorption_min_delta_ratio
            and c.close_pos_in_range >= cfg.absorption_min_close_pos
        ):
            hits += 1

    return hits >= cfg.strong_absorption_min_hits, hits


def detect_accumulation(setup: List[Candle], cfg: Config) -> Tuple[bool, int]:
    if len(setup) < cfg.accumulation_bars:
        return False, 0

    recent = setup[-cfg.accumulation_bars:]
    support = min(c.l for c in recent)
    resistance = max(c.h for c in recent)

    range_pct = safe_div(resistance - support, support)
    if range_pct > cfg.accumulation_max_range_pct:
        return False, 0

    tol = support * cfg.accumulation_touch_tolerance_pct
    avg_vq = avg([c.v_quote for c in recent])

    defended_hits = 0
    green_hits = 0
    total_hits = 0

    for c in recent:
        near_support = abs(c.l - support) <= tol
        if not near_support:
            continue

        total_hits += 1
        if c.close_pos_in_range >= 0.50:
            defended_hits += 1
        if c.is_green:
            green_hits += 1

        if avg_vq > 0 and c.v_quote > avg_vq * cfg.accumulation_max_touch_volume_vs_avg:
            return False, total_hits

    ok = (
        defended_hits >= cfg.accumulation_min_defended_hits
        and green_hits >= cfg.accumulation_min_green_hits
        and total_hits >= cfg.strong_accumulation_min_hits
    )
    return ok, total_hits


def detect_compression(setup: List[Candle], cfg: Config) -> bool:
    need = cfg.compression_recent_bars + cfg.compression_prev_bars
    if len(setup) < need:
        return False

    recent = setup[-cfg.compression_recent_bars:]
    prev = setup[-need:-cfg.compression_recent_bars]

    recent_avg = avg([c.range_pct for c in recent])
    prev_avg = avg([c.range_pct for c in prev])

    if prev_avg <= 0:
        return False

    return recent_avg <= prev_avg * cfg.compression_factor_max


def compute_score(
    absorption_ok: bool,
    absorption_hits: int,
    accumulation_ok: bool,
    accumulation_hits: int,
    compression_ok: bool,
    trigger: Candle,
    trigger_vmult: float,
    cfg: Config,
) -> int:
    score = 0

    if absorption_ok:
        score += 2
    elif absorption_hits >= 1:
        score += 1

    if accumulation_ok:
        score += 2
    elif accumulation_hits >= 1:
        score += 1

    if compression_ok:
        score += 1

    if trigger.close_pos_in_range >= cfg.trigger_close_pos_min:
        score += 1
    if trigger_vmult >= cfg.trigger_volume_vs_setup_avg_min:
        score += 1
    if trigger.buy_ratio_quote >= cfg.trigger_buy_ratio_min:
        score += 1
    if trigger.taker_delta_ratio_quote >= cfg.trigger_delta_ratio_min:
        score += 1

    return score


def evaluate_signal(
    symbol: str,
    candles: List[Candle],
    idx: int,
    cfg: Config,
) -> Tuple[bool, Optional[Dict[str, object]], Optional[str]]:
    if idx < cfg.setup_bars:
        return False, None, "not_enough_history"

    trigger = candles[idx]
    setup = candles[idx - cfg.setup_bars:idx]

    support = min(c.l for c in setup)
    resistance = max(c.h for c in setup)
    dist_support = safe_div(trigger.c - support, support)
    setup_avg_vq = avg([c.v_quote for c in setup])
    trigger_vmult = safe_div(trigger.v_quote, setup_avg_vq)

    if trigger.change_pct < cfg.trigger_change_pct_min:
        return False, None, "trigger_change_too_small"
    if trigger.change_pct > cfg.trigger_change_pct_max:
        return False, None, "trigger_change_too_big"
    if trigger.range_pct < cfg.trigger_range_pct_min:
        return False, None, "trigger_range_too_small"
    if trigger.close_pos_in_range < cfg.trigger_close_pos_min:
        return False, None, "trigger_close_pos_low"
    if trigger_vmult < cfg.trigger_volume_vs_setup_avg_min:
        return False, None, "trigger_volume_mult_low"
    if trigger.buy_ratio_quote < cfg.trigger_buy_ratio_min:
        return False, None, "trigger_buy_ratio_low"
    if trigger.taker_delta_ratio_quote < cfg.trigger_delta_ratio_min:
        return False, None, "trigger_delta_ratio_low"
    if dist_support > cfg.max_distance_from_support_pct:
        return False, None, "too_far_from_support"

    absorption_ok, absorption_hits = detect_absorption(setup, cfg)
    accumulation_ok, accumulation_hits = detect_accumulation(setup, cfg)
    compression_ok = detect_compression(setup, cfg)

    if not compression_ok:
        return False, None, "compression_failed"

    strong_pattern = (
        (absorption_ok and absorption_hits >= cfg.strong_absorption_min_hits)
        or
        (accumulation_ok and accumulation_hits >= cfg.strong_accumulation_min_hits)
    )

    if cfg.require_strong_pattern and not strong_pattern:
        return False, None, "strong_pattern_missing"

    score = compute_score(
        absorption_ok=absorption_ok,
        absorption_hits=absorption_hits,
        accumulation_ok=accumulation_ok,
        accumulation_hits=accumulation_hits,
        compression_ok=compression_ok,
        trigger=trigger,
        trigger_vmult=trigger_vmult,
        cfg=cfg,
    )

    if score < cfg.min_score:
        return False, None, "score_too_low"

    return True, {
        "symbol": symbol,
        "ts": trigger.ts,
        "close": trigger.c,
        "support": support,
        "resistance": resistance,
        "dist_support_pct": dist_support,
        "score": score,
        "absorption_ok": absorption_ok,
        "absorption_hits": absorption_hits,
        "accumulation_ok": accumulation_ok,
        "accumulation_hits": accumulation_hits,
        "compression_ok": compression_ok,
        "trigger_change_pct": trigger.change_pct,
        "trigger_range_pct": trigger.range_pct,
        "trigger_close_pos": trigger.close_pos_in_range,
        "trigger_volume_mult": trigger_vmult,
        "trigger_buy_ratio": trigger.buy_ratio_quote,
        "trigger_delta_ratio": trigger.taker_delta_ratio_quote,
    }, None


# ==========================================================
# Forward evaluation
# ==========================================================
def forward_stats(candles: List[Candle], idx: int, forward_bars: int) -> Optional[Dict[str, object]]:
    start = idx + 1
    end = min(len(candles), idx + 1 + forward_bars)

    future = candles[start:end]
    if not future:
        return None

    entry = candles[idx].c
    future_max_candle = max(future, key=lambda x: x.h)
    future_min_candle = min(future, key=lambda x: x.l)

    return {
        "future_max": future_max_candle.h,
        "future_max_ts": future_max_candle.ts,
        "future_min": future_min_candle.l,
        "future_min_ts": future_min_candle.ts,
        "profit_max": pct_change(entry, future_max_candle.h),
        "drawdown_min": pct_change(entry, future_min_candle.l),
        "future_bars": len(future),
    }


# ==========================================================
# Replay symbol
# ==========================================================
async def replay_symbol(cfg: Config, pool: asyncpg.Pool, symbol: str) -> Dict[str, object]:
    candles = await fetch_all_candles(pool, cfg, symbol)

    result: Dict[str, object] = {
        "checked": 0,
        "signals": 0,
        "profit_values": [],
        "drawdown_values": [],
        "rejects": {},
    }

    if len(candles) < cfg.setup_bars + 2:
        return result

    for idx in range(cfg.setup_bars, len(candles)):
        result["checked"] += 1

        ok, signal, reject_reason = evaluate_signal(symbol, candles, idx, cfg)
        if not ok:
            rejects: Dict[str, int] = result["rejects"]  # type: ignore[assignment]
            rejects[reject_reason or "unknown"] = rejects.get(reject_reason or "unknown", 0) + 1
            continue

        result["signals"] += 1
        fwd = forward_stats(candles, idx, cfg.forward_bars)

        if fwd:
            result["profit_values"].append(fwd["profit_max"])
            result["drawdown_values"].append(fwd["drawdown_min"])

            logging.warning(
                "REPLAY_INTENT %s | ts=%s | close=%.8f support=%.8f resistance=%.8f | "
                "dist_support=%.3f%% | score=%d | abs=%s(%d) acc=%s(%d) comp=%s | "
                "trig_chg=%.3f%% trig_rng=%.3f%% trig_close=%.2f trig_vmult=%.2f trig_buy=%.3f trig_delta=%.3f | "
                "future_max=%.8f @ %s future_min=%.8f @ %s | "
                "profit_max=%.3f%% drawdown_min=%.3f%% | future_bars=%d",
                signal["symbol"],
                signal["ts"].isoformat(),
                signal["close"],
                signal["support"],
                signal["resistance"],
                float(signal["dist_support_pct"]) * 100.0,
                signal["score"],
                signal["absorption_ok"],
                signal["absorption_hits"],
                signal["accumulation_ok"],
                signal["accumulation_hits"],
                signal["compression_ok"],
                float(signal["trigger_change_pct"]) * 100.0,
                float(signal["trigger_range_pct"]) * 100.0,
                signal["trigger_close_pos"],
                signal["trigger_volume_mult"],
                signal["trigger_buy_ratio"],
                signal["trigger_delta_ratio"],
                fwd["future_max"],
                fwd["future_max_ts"].isoformat(),
                fwd["future_min"],
                fwd["future_min_ts"].isoformat(),
                float(fwd["profit_max"]) * 100.0,
                float(fwd["drawdown_min"]) * 100.0,
                fwd["future_bars"],
            )
        else:
            logging.warning(
                "REPLAY_INTENT %s | ts=%s | close=%.8f support=%.8f resistance=%.8f | "
                "dist_support=%.3f%% | score=%d | abs=%s(%d) acc=%s(%d) comp=%s | "
                "trig_chg=%.3f%% trig_rng=%.3f%% trig_close=%.2f trig_vmult=%.2f trig_buy=%.3f trig_delta=%.3f | future_data=missing",
                signal["symbol"],
                signal["ts"].isoformat(),
                signal["close"],
                signal["support"],
                signal["resistance"],
                float(signal["dist_support_pct"]) * 100.0,
                signal["score"],
                signal["absorption_ok"],
                signal["absorption_hits"],
                signal["accumulation_ok"],
                signal["accumulation_hits"],
                signal["compression_ok"],
                float(signal["trigger_change_pct"]) * 100.0,
                float(signal["trigger_range_pct"]) * 100.0,
                signal["trigger_close_pos"],
                signal["trigger_volume_mult"],
                signal["trigger_buy_ratio"],
                signal["trigger_delta_ratio"],
            )

    return result


# ==========================================================
# One replay pass
# ==========================================================
async def replay_once(cfg: Config, pool: asyncpg.Pool) -> None:
    symbols = cfg.symbols[:cfg.limit_symbols]

    logging.warning(
        "REPLAY START | symbols=%d | start=%s end=%s | forward_bars=%d | min_score=%d | max_dist_support=%.3f%%",
        len(symbols),
        cfg.start_ts.isoformat() if cfg.start_ts else None,
        cfg.end_ts.isoformat() if cfg.end_ts else None,
        cfg.forward_bars,
        cfg.min_score,
        cfg.max_distance_from_support_pct * 100.0,
    )

    total_checked = 0
    total_signals = 0
    total_profit_values: List[float] = []
    total_drawdown_values: List[float] = []
    reject_totals: Dict[str, int] = {}

    for symbol in symbols:
        res = await replay_symbol(cfg, pool, symbol)

        total_checked += int(res["checked"])
        total_signals += int(res["signals"])
        total_profit_values.extend(res["profit_values"])      # type: ignore[arg-type]
        total_drawdown_values.extend(res["drawdown_values"])  # type: ignore[arg-type]

        for k, v in res["rejects"].items():  # type: ignore[union-attr]
            reject_totals[k] = reject_totals.get(k, 0) + int(v)

    avg_profit = mean(total_profit_values) * 100.0 if total_profit_values else None
    avg_drawdown = mean(total_drawdown_values) * 100.0 if total_drawdown_values else None

    med_profit = None
    med_drawdown = None

    if total_profit_values:
        xs = sorted(total_profit_values)
        m = len(xs) // 2
        med_profit = (xs[m] if len(xs) % 2 else (xs[m - 1] + xs[m]) / 2.0) * 100.0

    if total_drawdown_values:
        xs = sorted(total_drawdown_values)
        m = len(xs) // 2
        med_drawdown = (xs[m] if len(xs) % 2 else (xs[m - 1] + xs[m]) / 2.0) * 100.0

    top_rejects = sorted(reject_totals.items(), key=lambda x: x[1], reverse=True)
    reject_str = ", ".join(f"{k}={v}" for k, v in top_rejects[:10]) if top_rejects else "none"

    logging.warning(
        "REPLAY SUMMARY | checked=%d signals=%d | avg_profit_max=%s | median_profit_max=%s | "
        "avg_drawdown_min=%s | median_drawdown_min=%s | rejects=%s",
        total_checked,
        total_signals,
        (f"{avg_profit:.3f}%" if avg_profit is not None else "n/a"),
        (f"{med_profit:.3f}%" if med_profit is not None else "n/a"),
        (f"{avg_drawdown:.3f}%" if avg_drawdown is not None else "n/a"),
        (f"{med_drawdown:.3f}%" if med_drawdown is not None else "n/a"),
        reject_str,
    )

    logging.warning(
        "REPLAY DONE | checked=%d signals=%d",
        total_checked,
        total_signals,
    )


# ==========================================================
# Main
# ==========================================================
async def main() -> None:
    cfg = load_config()
    setup_logging(cfg.log_level)

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
        if cfg.oneshot:
            await replay_once(cfg, pool)
        else:
            while True:
                await replay_once(cfg, pool)
                await asyncio.sleep(cfg.replay_sleep_sec)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
