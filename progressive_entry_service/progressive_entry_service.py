#!/usr/bin/env python3
# progressive_service.py
# ------------------------------------------------------------
# 🟢 BTC pump -> create MARKET BUY intents (no rt_signals table)
#
# DB connection style (same as data5m_service.py):
#   DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD
# ------------------------------------------------------------

import os
import json
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

import asyncpg

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v and v.strip() else default

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v and v.strip() else default

def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v.strip() if v and v.strip() else default


@dataclass
class Config:
    # DB (data5m-style)
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    btc_symbol: str
    candles_table: str
    btc_lookback_bars: int
    btc_pump_min: float
    require_btc_close_near_high: bool
    btc_close_near_high_pct: float

    universe_limit: int
    exclude_symbols_csv: str

    baseline_bars: int
    max_candle_age_min: int

    alt_vol_rel_min: float
    alt_change_min: float
    alt_strength_min: float
    alt_runup_max: float

    top_n: int
    poll_sec: int

    source: str
    side: str
    quote_amount: float

    support_signal_name: str
    support_max_age_min: int


def load_config() -> Config:
    return Config(
        # DB
        db_host=env_str("DB_HOST", "db"),
        db_port=env_int("DB_PORT", 5432),
        db_name=env_str("DB_NAME", "pumpdb"),
        db_user=env_str("DB_USER", "pumpuser"),
        db_password=env_str("DB_PASSWORD", ""),

        btc_symbol=env_str("BTC_SYMBOL", "BTCUSDC"),
        candles_table=env_str("CANDLES_TABLE", "public.candles_5m"),
        btc_lookback_bars=env_int("BTC_LOOKBACK_BARS", 3),
        btc_pump_min=env_float("BTC_PUMP_MIN", 0.006),
        require_btc_close_near_high=env_int("BTC_CLOSE_NEAR_HIGH", 1) == 1,
        btc_close_near_high_pct=env_float("BTC_CLOSE_NEAR_HIGH_PCT", 0.20),

        universe_limit=env_int("UNIVERSE_LIMIT", 80),
        exclude_symbols_csv=env_str("EXCLUDE_SYMBOLS", "BTCUSDC"),

        baseline_bars=env_int("VOL_BASELINE_BARS", 24),
        max_candle_age_min=env_int("MAX_CANDLE_AGE_MIN", 15),

        alt_vol_rel_min=env_float("ALT_VOL_REL_MIN", 1.4),
        alt_change_min=env_float("ALT_CHANGE_MIN", 0.000),
        alt_strength_min=env_float("ALT_STRENGTH_MIN", 0.70),
        alt_runup_max=env_float("ALT_RUNUP_MAX", 0.010),

        top_n=env_int("TOP_N", 3),
        poll_sec=env_int("POLL_SEC", 2),

        source=env_str("INTENT_SOURCE", "PROGRESSIVE_PUMP"),
        side=env_str("INTENT_SIDE", "BUY"),
        quote_amount=env_float("ENTRY_QUOTE_AMOUNT", 10.0),

        support_signal_name=env_str("SUPPORT_SIGNAL_NAME", "ACCUM_PHASE"),
        support_max_age_min=env_int("SUPPORT_MAX_AGE_MIN", 240),
    )


# ---------- BTC helpers ----------
async def fetch_btc_rows(pool: asyncpg.Pool, cfg: Config):
    q = f"""
        SELECT ts, o, h, l, c
        FROM {cfg.candles_table}
        WHERE symbol=$1
        ORDER BY ts DESC
        LIMIT $2
    """
    return await pool.fetch(q, cfg.btc_symbol, cfg.btc_lookback_bars + 1)


def pct_change(rows) -> Optional[float]:
    if len(rows) < 2:
        return None
    c_now = float(rows[0]["c"])
    c_then = float(rows[-1]["c"])
    if c_then <= 0:
        return None
    return (c_now / c_then) - 1.0


def close_near_high(rows, pct: float) -> bool:
    if not rows:
        return False
    h = float(rows[0]["h"])
    l = float(rows[0]["l"])
    c = float(rows[0]["c"])
    rng = max(1e-12, h - l)
    return c >= (h - pct * rng)


# ---------- Universe selection ----------
async def fetch_universe_symbols(pool: asyncpg.Pool, cfg: Config) -> List[str]:
    q = f"""
        WITH last AS (
          SELECT DISTINCT ON (symbol)
                 symbol, ts, v_quote
          FROM {cfg.candles_table}
          WHERE ts >= NOW() - INTERVAL '30 minutes'
          ORDER BY symbol, ts DESC
        )
        SELECT symbol
        FROM last
        ORDER BY v_quote DESC
        LIMIT $1
    """
    rows = await pool.fetch(q, cfg.universe_limit)
    excluded = {x.strip() for x in cfg.exclude_symbols_csv.split(",") if x.strip()}
    return [str(r["symbol"]) for r in rows if str(r["symbol"]) not in excluded]


# ---------- Support (optional) ----------
async def fetch_latest_support(pool: asyncpg.Pool, cfg: Config, symbol: str) -> Optional[float]:
    q = """
        SELECT details
        FROM public.accum_signals
        WHERE symbol=$1
          AND signal=$2
          AND ts >= NOW() - ($3::int * INTERVAL '1 minute')
        ORDER BY ts DESC
        LIMIT 1
    """
    row = await pool.fetchrow(q, symbol, cfg.support_signal_name, cfg.support_max_age_min)
    if not row or row["details"] is None:
        return None
    d = dict(row["details"])
    for k in ("support", "support_price", "supportPrice"):
        if k in d and d[k] is not None:
            try:
                return float(d[k])
            except Exception:
                pass
    return None


# ---------- Features ----------
async def fetch_symbol_candles(pool: asyncpg.Pool, cfg: Config, symbol: str):
    limit = cfg.baseline_bars + 2
    q = f"""
        SELECT ts, o,h,l,c, v_quote
        FROM {cfg.candles_table}
        WHERE symbol=$1
        ORDER BY ts DESC
        LIMIT $2
    """
    return await pool.fetch(q, symbol, limit)


def candle_strength(h: float, l: float, c: float) -> float:
    rng = max(1e-12, h - l)
    return (c - l) / rng  # 0..1


def score(volume_rel: float, strength: float, runup: float) -> float:
    return (volume_rel * strength) / (1.0 + max(0.0, runup))


# ---------- Intents ----------
async def active_intent_exists(pool: asyncpg.Pool, symbol: str, source: str) -> bool:
    q = """
        SELECT 1
        FROM public.trade_intents
        WHERE symbol=$1 AND source=$2 AND status IN ('NEW','SENT')
        LIMIT 1
    """
    return (await pool.fetchrow(q, symbol, source)) is not None


async def create_market_intent(
    pool: asyncpg.Pool,
    *,
    symbol: str,
    ts,
    source: str,
    side: str,
    quote_amount: float,
    ref_price: float,          # goes into limit_price (NOT NULL)
    support_price: Optional[float],
    meta: Dict[str, Any],
) -> int:
    q = """
        INSERT INTO public.trade_intents
            (symbol, ts, source, side, quote_amount, limit_price, support_price, meta, status, entry_mode)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, 'NEW', 'MARKET')
        RETURNING id
    """
    row = await pool.fetchrow(
        q, symbol, ts, source, side, quote_amount, ref_price, support_price, json.dumps(meta)
    )
    return int(row["id"])


async def run(cfg: Config):
    pool = await asyncpg.create_pool(
        host=cfg.db_host,
        port=cfg.db_port,
        database=cfg.db_name,
        user=cfg.db_user,
        password=cfg.db_password,
        min_size=1,
        max_size=10
    )
    logging.info(
        "PROGRESSIVE started | db=%s@%s:%d/%s | source=%s | btc=%s pump_min=%+.3f%% | universe_limit=%d top_n=%d",
        cfg.db_user, cfg.db_host, cfg.db_port, cfg.db_name,
        cfg.source, cfg.btc_symbol, cfg.btc_pump_min * 100.0, cfg.universe_limit, cfg.top_n
    )

    try:
        while True:
            try:
                btc_rows = await fetch_btc_rows(pool, cfg)
                btc_delta = pct_change(btc_rows)
                if btc_delta is None:
                    logging.warning("BTC_CHANGE unavailable (need more candles)")
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                if btc_delta < cfg.btc_pump_min:
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                if cfg.require_btc_close_near_high and not close_near_high(btc_rows, cfg.btc_close_near_high_pct):
                    logging.info("BTC_PUMP_WEAK_CLOSE | Δ=%+.3f%% | skip", btc_delta * 100.0)
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                symbols = await fetch_universe_symbols(pool, cfg)
                if not symbols:
                    logging.info("BTC_PUMP | Δ=%+.3f%% | universe empty", btc_delta * 100.0)
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                candidates: List[Dict[str, Any]] = []

                for sym in symbols:
                    rows = await fetch_symbol_candles(pool, cfg, sym)
                    if len(rows) < 3:
                        continue

                    ts0 = rows[0]["ts"]
                    o0 = float(rows[0]["o"])
                    h0 = float(rows[0]["h"])
                    l0 = float(rows[0]["l"])
                    c0 = float(rows[0]["c"])
                    vq0 = float(rows[0]["v_quote"])

                    c1 = float(rows[1]["c"])
                    if c1 <= 0:
                        continue
                    change_5m = (c0 / c1) - 1.0

                    baseline = [float(r["v_quote"]) for r in rows[2:]]
                    base_avg = sum(baseline) / max(1, len(baseline))
                    volume_rel = (vq0 / base_avg) if base_avg > 0 else 0.0

                    strength = candle_strength(h0, l0, c0)

                    support = await fetch_latest_support(pool, cfg, sym)
                    if support and support > 0:
                        runup = (c0 / support) - 1.0
                    else:
                        if l0 <= 0:
                            continue
                        runup = (c0 / l0) - 1.0

                    if volume_rel < cfg.alt_vol_rel_min:
                        continue
                    if change_5m < cfg.alt_change_min:
                        continue
                    if strength < cfg.alt_strength_min:
                        continue
                    if runup > cfg.alt_runup_max:
                        continue

                    candidates.append({
                        "symbol": sym,
                        "ts": ts0,
                        "ref_price": c0,  # goes into limit_price to satisfy NOT NULL
                        "support": support,
                        "volume_rel": volume_rel,
                        "strength": strength,
                        "change_5m": change_5m,
                        "runup": runup,
                        "score": score(volume_rel, strength, runup),
                    })

                if not candidates:
                    logging.info("BTC_PUMP | Δ=%+.3f%% | no candidates passed filters", btc_delta * 100.0)
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                candidates.sort(key=lambda x: x["score"], reverse=True)
                top = candidates[: cfg.top_n]

                for c in top:
                    sym = c["symbol"]
                    if await active_intent_exists(pool, sym, cfg.source):
                        continue

                    meta = {
                        "regime": "BTC_PUMP",
                        "btc_change": btc_delta,
                        "score": c["score"],
                        "ref_price_note": "limit_price stores reference close for MARKET (executor should ignore for pricing)",
                        "features": {
                            "volume_rel": c["volume_rel"],
                            "strength": c["strength"],
                            "change_5m": c["change_5m"],
                            "runup": c["runup"],
                            "support": c["support"],
                        },
                        "thresholds": {
                            "ALT_VOL_REL_MIN": cfg.alt_vol_rel_min,
                            "ALT_CHANGE_MIN": cfg.alt_change_min,
                            "ALT_STRENGTH_MIN": cfg.alt_strength_min,
                            "ALT_RUNUP_MAX": cfg.alt_runup_max,
                            "BTC_PUMP_MIN": cfg.btc_pump_min,
                        },
                    }

                    intent_id = await create_market_intent(
                        pool,
                        symbol=sym,
                        ts=c["ts"],
                        source=cfg.source,
                        side=cfg.side,
                        quote_amount=cfg.quote_amount,
                        ref_price=c["ref_price"],
                        support_price=c["support"],
                        meta=meta,
                    )

                    logging.warning(
                        "INTENT_NEW_MARKET | id=%d %s | quote=%.2f | btcΔ=%+.3f%% | score=%.3f | vol=%.2fx strength=%.2f runup=%.2f%% ref=%.8f",
                        intent_id, sym, cfg.quote_amount, btc_delta * 100.0, c["score"],
                        c["volume_rel"], c["strength"], c["runup"] * 100.0, c["ref_price"]
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
