#!/usr/bin/env python3
# accumulation_entry_service.py
# ------------------------------------------------------------
# Purpose:
#   Neutral-regime entry service for ACCUM signals:
#   🔵 When BTC is "calm/neutral" -> place LIMIT entry near low/support.
#   🔴 When BTC is dumping or pumping -> DO NOT create intents here.
#
# Reads:
#   - BTC candles from candles table (same schema as alts)
#   - ACCUM signals from public.acum_signals (produced by accumulation_service)
#
# Writes:
#   - public.trade_intents with entry_mode='LIMIT'
#
# Notes / assumptions (adjust SQL if your schema differs):
#   candles table:
#     public.candles_5m(symbol text, ts timestamptz, o double, h double, l double, c double, v double, buy_ratio double)
#     (You can point to a different table via CANDLES_TABLE env.)
#
#   acum_signals table:
#     public.acum_signals(id bigserial, symbol text, tf text, signal_time timestamptz,
#                         support double, low double, status text, created_at timestamptz)
#     status expected: 'NEW' -> will be 'USED' after intent creation
#
#   trade_intents table:
#     public.trade_intents(id bigserial, symbol text, source text, quote_qty double,
#                          entry_mode text, limit_price double, status text,
#                          created_at timestamptz default now(), updated_at timestamptz default now(),
#                          meta jsonb)
#     status: 'PENDING' when created
#
#   Uniqueness:
#     We avoid duplicates by checking existing PENDING intent per symbol+source.
#     If you have a unique constraint, keep it; otherwise this script is still safe.
# ------------------------------------------------------------

import os
import json
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple

import asyncpg


# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# -----------------------------
# Env helpers
# -----------------------------
def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v and v.strip() else default

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v and v.strip() else default

def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v.strip() if v and v.strip() else default


# -----------------------------
# Config
# -----------------------------
@dataclass
class Config:
    db_dsn: str

    # BTC regime
    btc_symbol: str
    candles_table: str
    tf_minutes: int
    btc_lookback_bars: int  # for change calc (e.g., 15m -> 3 bars on 5m)
    btc_neutral_min: float  # e.g. -0.004 (-0.4%)
    btc_neutral_max: float  # e.g. +0.006 (+0.6%)

    # Polling
    poll_sec: int
    max_signals_per_cycle: int

    # Entry sizing
    quote_qty: float   # e.g. 10 USDC
    limit_offset_pct: float  # e.g. 0.0 => use low/support as-is; >0 => place slightly above low

    # Bookkeeping
    source_name: str


def load_config() -> Config:
    return Config(
        db_dsn=env_str("DB_DSN", "postgresql://pumpuser:pumpsecret@db:5432/pumpdb"),

        btc_symbol=env_str("BTC_SYMBOL", "BTCUSDC"),
        candles_table=env_str("CANDLES_TABLE", "public.candles_5m"),
        tf_minutes=env_int("TF_MINUTES", 5),

        btc_lookback_bars=env_int("BTC_LOOKBACK_BARS", 3),  # 3 bars of 5m = 15m
        btc_neutral_min=env_float("BTC_NEUTRAL_MIN", -0.004),
        btc_neutral_max=env_float("BTC_NEUTRAL_MAX", 0.006),

        poll_sec=env_int("POLL_SEC", 2),
        max_signals_per_cycle=env_int("MAX_SIGNALS_PER_CYCLE", 20),

        quote_qty=env_float("ENTRY_QUOTE_QTY", 10.0),
        limit_offset_pct=env_float("LIMIT_OFFSET_PCT", 0.0000),

        source_name=env_str("INTENT_SOURCE", "ACCUM_NEUTRAL"),
    )


# -----------------------------
# DB helpers
# -----------------------------
async def fetch_btc_change(pool: asyncpg.Pool, cfg: Config) -> Optional[float]:
    """
    Returns BTC percent change over cfg.btc_lookback_bars (close_now / close_then - 1).
    Uses candles_table for cfg.btc_symbol.
    """
    q = f"""
        SELECT ts, c
        FROM {cfg.candles_table}
        WHERE symbol = $1
        ORDER BY ts DESC
        LIMIT $2
    """
    rows = await pool.fetch(q, cfg.btc_symbol, cfg.btc_lookback_bars + 1)
    if len(rows) < cfg.btc_lookback_bars + 1:
        return None
    c_now = float(rows[0]["c"])
    c_then = float(rows[-1]["c"])
    if c_then <= 0:
        return None
    return (c_now / c_then) - 1.0


def btc_is_neutral(btc_change: float, cfg: Config) -> bool:
    return cfg.btc_neutral_min <= btc_change <= cfg.btc_neutral_max


async def fetch_new_accum_signals(pool: asyncpg.Pool, cfg: Config):
    """
    Reads NEW signals. Adjust column names if needed.
    """
    q = """
        SELECT id, symbol, tf, signal_time, support, low
        FROM public.acum_signals
        WHERE status = 'NEW'
        ORDER BY signal_time ASC
        LIMIT $1
    """
    return await pool.fetch(q, cfg.max_signals_per_cycle)


async def pending_intent_exists(pool: asyncpg.Pool, symbol: str, source: str) -> bool:
    q = """
        SELECT 1
        FROM public.trade_intents
        WHERE symbol = $1 AND source = $2 AND status IN ('PENDING','SENT')
        LIMIT 1
    """
    r = await pool.fetchrow(q, symbol, source)
    return r is not None


async def create_limit_intent(
    pool: asyncpg.Pool,
    cfg: Config,
    symbol: str,
    limit_price: float,
    meta: Dict[str, Any],
) -> Optional[int]:
    """
    Writes a trade_intent (LIMIT).
    """
    q = """
        INSERT INTO public.trade_intents(symbol, source, quote_qty, entry_mode, limit_price, status, meta)
        VALUES ($1, $2, $3, 'LIMIT', $4, 'PENDING', $5::jsonb)
        RETURNING id
    """
    row = await pool.fetchrow(q, symbol, cfg.source_name, cfg.quote_qty, limit_price, json.dumps(meta))
    return int(row["id"]) if row else None


async def mark_signal_used(pool: asyncpg.Pool, signal_id: int):
    q = """
        UPDATE public.acum_signals
        SET status = 'USED'
        WHERE id = $1
    """
    await pool.execute(q, signal_id)


# -----------------------------
# Main loop
# -----------------------------
async def run(cfg: Config):
    pool = await asyncpg.create_pool(dsn=cfg.db_dsn, min_size=1, max_size=5)

    logging.info("ACCUM_ENTRY started | candles_table=%s btc=%s neutral=[%.4f..%.4f] lookback_bars=%d",
                 cfg.candles_table, cfg.btc_symbol, cfg.btc_neutral_min, cfg.btc_neutral_max, cfg.btc_lookback_bars)

    try:
        while True:
            try:
                btc_change = await fetch_btc_change(pool, cfg)
                if btc_change is None:
                    logging.warning("BTC_CHANGE unavailable yet (not enough candles)")
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                if not btc_is_neutral(btc_change, cfg):
                    # Do nothing in non-neutral regime (pump/dump handled by progressive-service or skip).
                    logging.info("BTC_NOT_NEUTRAL | Δ=%+.3f%% | skip ACCUM intents", btc_change * 100.0)
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                signals = await fetch_new_accum_signals(pool, cfg)
                if not signals:
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                for s in signals:
                    sid = int(s["id"])
                    sym = str(s["symbol"])
                    support = float(s["support"]) if s["support"] is not None else None
                    low = float(s["low"]) if s["low"] is not None else None

                    if await pending_intent_exists(pool, sym, cfg.source_name):
                        await mark_signal_used(pool, sid)
                        logging.info("SKIP_DUPLICATE | %s signal_id=%d (pending intent exists)", sym, sid)
                        continue

                    base_price = low if low and low > 0 else (support if support and support > 0 else None)
                    if not base_price:
                        logging.warning("SKIP_NO_PRICE | %s signal_id=%d (low/support missing)", sym, sid)
                        await mark_signal_used(pool, sid)
                        continue

                    limit_price = base_price * (1.0 + cfg.limit_offset_pct)
                    meta = {
                        "regime": "BTC_NEUTRAL",
                        "btc_change": btc_change,
                        "signal_id": sid,
                        "signal_tf": s["tf"],
                        "signal_time": s["signal_time"].isoformat() if s["signal_time"] else None,
                        "support": support,
                        "low": low,
                        "limit_offset_pct": cfg.limit_offset_pct,
                    }

                    intent_id = await create_limit_intent(pool, cfg, sym, limit_price, meta)
                    await mark_signal_used(pool, sid)

                    logging.warning(
                        "INTENT_NEW_LIMIT | id=%s %s | quote=%.2f | limit=%.8f | btcΔ=%+.3f%%",
                        intent_id, sym, cfg.quote_qty, limit_price, btc_change * 100.0
                    )

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
