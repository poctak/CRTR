#!/usr/bin/env python3
# progressive_entry_service.py
# ------------------------------------------------------------
# 🟢 BTC pump -> create MARKET BUY intents (idempotent)
#
# Reads CLOSED 5m BTC candle from DB (public.candles_5m by default)
# and when BTC pump condition is met, creates MARKET trade intents
# for configured SYMBOLS at the SAME candle ts.
#
# FIX:
# - Idempotent insert into trade_intents using:
#     ON CONFLICT (symbol, ts) DO NOTHING
#   so duplicate signals/retries/parallel runs won't crash.
#
# DB connection style matches your data5m_service.py:
#   DB_HOST, DB_NAME, DB_USER, DB_PASSWORD + asyncpg.create_pool
# ------------------------------------------------------------

import os
import json
import asyncio
import logging
import time
from datetime import datetime
from typing import List, Optional, Dict, Any

import asyncpg


# ==========================================================
# ENV helpers (same style)
# ==========================================================
def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    return int(v) if v else default

def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    return float(v) if v else default

def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")

def parse_symbols(env_name: str, allow_empty: bool = False) -> List[str]:
    raw = os.getenv(env_name, "").strip()
    if not raw:
        if allow_empty:
            return []
        raise RuntimeError(f"{env_name} is empty")
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


# ==========================================================
# CONFIG
# ==========================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# DB (same pattern as data5m_service.py)
DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "pumpdb")
DB_USER = os.getenv("DB_USER", "pumpuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Tables
CANDLES_TABLE = os.getenv("CANDLES_TABLE", "public.candles_5m").strip()
TRADE_INTENTS_TABLE = os.getenv("TRADE_INTENTS_TABLE", "public.trade_intents").strip()

# Strategy inputs
BTC_SYMBOL = os.getenv("BTC_SYMBOL", "BTCUSDC").strip().upper()
SYMBOLS = parse_symbols("SYMBOLS")  # targets (alts)
SYMBOLS = [s for s in SYMBOLS if s != BTC_SYMBOL]  # safety

# Polling
POLL_SEC = env_float("POLL_SEC", 2.0)
COOLDOWN_CANDLES = env_int("COOLDOWN_CANDLES", 1)

# Trigger thresholds
BTC_PUMP_PCT_TH = env_float("BTC_PUMP_PCT_TH", 0.008)   # 0.8%
BTC_MIN_VQ = env_float("BTC_MIN_VQ", 0.0)
BTC_REQUIRE_GREEN = env_bool("BTC_REQUIRE_GREEN", True)

# Intent settings
QUOTE_AMOUNT = env_float("QUOTE_AMOUNT", 10.0)
INTENT_STATUS = os.getenv("INTENT_STATUS", "NEW").strip()
INTENT_SOURCE = os.getenv("INTENT_SOURCE", "PROGRESSIVE").strip()

# Limits / debug
MAX_TARGETS_PER_TRIGGER = env_int("MAX_TARGETS_PER_TRIGGER", 50)
DEBUG = env_bool("DEBUG", False)


# ==========================================================
# SQL
# ==========================================================
SQL_GET_LATEST = f"""
SELECT symbol, ts, o, h, l, c, v_base, v_quote, trades_count
FROM {CANDLES_TABLE}
WHERE symbol = $1
ORDER BY ts DESC
LIMIT 1;
"""

SQL_GET_AT_TS = f"""
SELECT symbol, ts, o, h, l, c, v_base, v_quote, trades_count
FROM {CANDLES_TABLE}
WHERE symbol = $1
  AND ts = $2
LIMIT 1;
"""

# Idempotent insert (fix for uq_trade_intents_symbol_ts on (symbol, ts))
SQL_INSERT_INTENT = f"""
INSERT INTO {TRADE_INTENTS_TABLE}(
  symbol, ts,
  source,
  entry_mode,
  limit_price,
  quote_amount,
  status,
  meta,
  created_at,
  updated_at
)
VALUES(
  $1, $2,
  $3,
  'MARKET',
  $4,
  $5,
  $6,
  $7::jsonb,
  NOW(),
  NOW()
)
ON CONFLICT (symbol, ts) DO NOTHING
RETURNING id;
"""


# ==========================================================
# Helpers
# ==========================================================
def pump_pct(o: float, c: float) -> float:
    if o <= 0:
        return 0.0
    return (c - o) / o

def pick_targets() -> List[str]:
    return SYMBOLS[: max(0, MAX_TARGETS_PER_TRIGGER)]


async def create_market_intent(
    pool: asyncpg.Pool,
    symbol: str,
    ts,
    ref_price: float,
    meta_extra: Optional[Dict[str, Any]] = None,
) -> Optional[int]:
    meta = {
        "reason": "BTC_PUMP",
        "ref_price": float(ref_price),
        "version": "2026-03-03",
    }
    if meta_extra:
        meta.update(meta_extra)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            SQL_INSERT_INTENT,
            symbol,
            ts,
            INTENT_SOURCE,
            float(ref_price),   # limit_price placeholder (NOT NULL)
            float(QUOTE_AMOUNT),
            INTENT_STATUS,
            json.dumps(meta),
        )
    if not row:
        return None
    return int(row["id"])


# ==========================================================
# Main loop
# ==========================================================
async def main() -> None:
    logging.info(
        "Starting progressive_entry_service | btc=%s | targets=%d | pump_th=%.3f%% | min_vq=%.2f | green=%s | candles=%s | intents=%s",
        BTC_SYMBOL, len(SYMBOLS), BTC_PUMP_PCT_TH * 100.0, BTC_MIN_VQ, BTC_REQUIRE_GREEN,
        CANDLES_TABLE, TRADE_INTENTS_TABLE
    )

    pool = await asyncpg.create_pool(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        min_size=1, max_size=5
    )
    logging.info("DB pool created | host=%s db=%s user=%s", DB_HOST, DB_NAME, DB_USER)

    last_btc_ts = None
    cooldown_left = 0

    while True:
        try:
            # 1) latest BTC candle
            async with pool.acquire() as conn:
                btc = await conn.fetchrow(SQL_GET_LATEST, BTC_SYMBOL)

            if not btc:
                logging.warning("No BTC candles yet | symbol=%s table=%s", BTC_SYMBOL, CANDLES_TABLE)
                await asyncio.sleep(POLL_SEC)
                continue

            btc_ts = btc["ts"]

            if last_btc_ts is None:
                last_btc_ts = btc_ts
                logging.info("Bootstrap last_btc_ts=%s (waiting for next close)", btc_ts)
                await asyncio.sleep(POLL_SEC)
                continue

            if btc_ts == last_btc_ts:
                await asyncio.sleep(POLL_SEC)
                continue

            # New closed BTC candle detected
            last_btc_ts = btc_ts

            o = float(btc["o"])
            c = float(btc["c"])
            vq = float(btc["v_quote"] or 0.0)
            p = pump_pct(o, c)

            if DEBUG:
                logging.info(
                    "BTC_CANDLE ts=%s o=%.6f c=%.6f pump=%.3f%% vq=%.2f",
                    btc_ts, o, c, p * 100.0, vq
                )

            if cooldown_left > 0:
                cooldown_left -= 1
                logging.info("COOLDOWN active (%d candles left)", cooldown_left)
                continue

            # 2) trigger checks
            if BTC_REQUIRE_GREEN and c <= o:
                logging.info("NO_TRIGGER btc_not_green | ts=%s o=%.6f c=%.6f", btc_ts, o, c)
                continue

            if vq < BTC_MIN_VQ:
                logging.info("NO_TRIGGER btc_vq_low | ts=%s vq=%.2f < %.2f", btc_ts, vq, BTC_MIN_VQ)
                continue

            if p < BTC_PUMP_PCT_TH:
                logging.info(
                    "NO_TRIGGER pump_too_small | ts=%s pump=%.3f%% < %.3f%%",
                    btc_ts, p * 100.0, BTC_PUMP_PCT_TH * 100.0
                )
                continue

            # 3) triggered -> intents
            targets = pick_targets()
            if not targets:
                logging.warning("TRIGGERED but no targets (SYMBOLS empty after filtering)")
                cooldown_left = COOLDOWN_CANDLES
                continue

            logging.warning(
                "TRIGGER BTC_PUMP | ts=%s pump=%.3f%% vq=%.2f -> targets=%d",
                btc_ts, p * 100.0, vq, len(targets)
            )

            created = 0
            existed = 0
            missing = 0
            errors = 0

            # Pull ref prices from candle closes at same ts (recommended with uq(symbol,ts))
            for sym in targets:
                try:
                    async with pool.acquire() as conn:
                        cc = await conn.fetchrow(SQL_GET_AT_TS, sym, btc_ts)

                    if not cc:
                        missing += 1
                        if DEBUG:
                            logging.info("SKIP %s | no candle at ts=%s", sym, btc_ts)
                        continue

                    ref_price = float(cc["c"])

                    intent_id = await create_market_intent(
                        pool,
                        symbol=sym,
                        ts=btc_ts,
                        ref_price=ref_price,
                        meta_extra={
                            "btc_ts": str(btc_ts),
                            "btc_pump_pct": float(p),
                            "btc_vq": float(vq),
                        },
                    )

                    if intent_id is None:
                        existed += 1
                        logging.info("INTENT_EXISTS %s | ts=%s ref=%.6f", sym, btc_ts, ref_price)
                    else:
                        created += 1
                        logging.warning("INTENT_CREATED id=%d %s | ts=%s ref=%.6f", intent_id, sym, btc_ts, ref_price)

                except Exception as e:
                    errors += 1
                    logging.exception("INTENT_ERROR %s | ts=%s | %s", sym, btc_ts, e)

            logging.warning(
                "TRIGGER_SUMMARY | ts=%s created=%d existed=%d missing_candle=%d errors=%d",
                btc_ts, created, existed, missing, errors
            )

            cooldown_left = COOLDOWN_CANDLES

        except Exception as e:
            logging.exception("LOOP_ERROR: %s | sleeping 3s", e)
            await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
