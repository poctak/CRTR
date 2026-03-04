#!/usr/bin/env python3
# exit_service.py
# ------------------------------------------------------------
# Watches OPEN positions and triggers close requests (SL/TP/TIME) based on 1m closed klines.
#
# KEY FIX (this version):
# - When requesting close (OPEN -> CLOSING), we also write the EXIT into public.trade_log
#   by UPDATING the existing entry row created by executor (matched by entry_order_id).
#   This keeps ONE row per trade with entry_source/entry_mode/intent_id preserved.
#
# Requires in public.trade_log (recommended):
#   entry_order_id BIGINT UNIQUE
#   entry_source TEXT, entry_mode TEXT, intent_id BIGINT
#
# Requires in positions_open:
#   entry_order_id BIGINT (already in your schema from executor upsert)
#   close_reason, close_requested_at, close_ref_price columns (as you already use)
# ------------------------------------------------------------

import os
import json
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Set

import asyncpg
import websockets

# ==========================================================
# LOGGING
# ==========================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# ==========================================================
# CONSTANTS / ENV
# ==========================================================
BINANCE_WS = "wss://stream.binance.com:9443/stream?streams="

def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    return int(v) if v else default

DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "pumpdb")
DB_USER = os.getenv("DB_USER", "pumpuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# time-based stop (minutes)
TIME_STOP_MIN = env_int("TIME_STOP_MINUTES", 100)

# How often to refresh OPEN symbols (seconds)
REFRESH_OPEN_SYMBOLS_SEC = env_int("REFRESH_OPEN_SYMBOLS_SEC", 10)

# Kline TF for exits (fixed to 1m here)
EXIT_TF = os.getenv("EXIT_TF", "1m").strip()

# ==========================================================
# DATA MODELS
# ==========================================================
@dataclass
class Candle:
    t: datetime   # open time (Binance kline 't')
    o: float
    h: float
    l: float
    c: float
    vq: float

@dataclass
class DBPosition:
    symbol: str
    entry_time: datetime
    entry_price: float
    sl: float
    tp: float
    status: str
    entry_order_id: Optional[int]  # important for trade_log update


# ==========================================================
# BINANCE WS URL
# ==========================================================
def ws_url_for_symbols(symbols: List[str], tf: str) -> str:
    # Binance multiplex: /stream?streams=symbol@kline_1m/symbol2@kline_1m
    streams = "/".join([f"{s.lower()}@kline_{tf}" for s in symbols])
    return BINANCE_WS + streams


# ==========================================================
# DB
# ==========================================================
async def init_db_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        min_size=1,
        max_size=5,
        command_timeout=60,
    )

async def db_get_open_symbols(pool: asyncpg.Pool) -> List[str]:
    sql = """
    SELECT symbol
    FROM positions_open
    WHERE status='OPEN'
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql)
    return [r["symbol"].strip().upper() for r in rows if r["symbol"]]

async def db_get_open_position(pool: asyncpg.Pool, symbol: str) -> Optional[DBPosition]:
    sql = """
    SELECT symbol, entry_time, entry_price, sl, tp, status, entry_order_id
    FROM positions_open
    WHERE symbol=$1 AND status='OPEN'
    LIMIT 1
    """
    async with pool.acquire() as conn:
        r = await conn.fetchrow(sql, symbol)
    if not r:
        return None
    return DBPosition(
        symbol=str(r["symbol"]),
        entry_time=r["entry_time"],
        entry_price=float(r["entry_price"]),
        sl=float(r["sl"]),
        tp=float(r["tp"]),
        status=str(r["status"]),
        entry_order_id=(int(r["entry_order_id"]) if r["entry_order_id"] is not None else None),
    )


# ----------------------------------------------------------
# Trade log update (EXIT) - updates the existing entry row
# ----------------------------------------------------------
UPDATE_TRADE_LOG_EXIT_BY_ENTRY_ORDER_ID_SQL = """
UPDATE public.trade_log
SET
  exit_time  = $2,
  exit_price = $3,
  reason     = $4,
  pnl_pct    = $5,
  created_at = NOW()
WHERE entry_order_id = $1
"""

def pnl_pct(entry_price: float, exit_price: float) -> float:
    if entry_price <= 0:
        return 0.0
    return (exit_price / entry_price - 1.0) * 100.0


async def db_request_close(pool: asyncpg.Pool, pos: DBPosition, reason: str, ref_price: float) -> bool:
    """
    Atomically switches OPEN -> CLOSING and stores close metadata.
    Additionally writes EXIT to trade_log by updating the entry row (matched by entry_order_id),
    so the final trade_log row keeps entry_source/entry_mode/intent_id.

    Returns True only if it really updated 1 row (i.e., you got the lock).
    """
    sql_pos = """
    UPDATE positions_open
    SET
        status='CLOSING',
        close_reason=$2,
        close_requested_at=NOW(),
        close_ref_price=$3,
        updated_at=NOW()
    WHERE symbol=$1 AND status='OPEN'
    """

    async with pool.acquire() as conn:
        async with conn.transaction():
            res = await conn.execute(sql_pos, pos.symbol, reason, ref_price)
            ok = res.endswith("1")
            if not ok:
                return False

            # Best-effort: update trade_log only if we know entry_order_id
            if pos.entry_order_id is not None:
                # For SL/TP we use the trigger price as exit_price.
                # For TIME we use candle close price.
                exit_time = datetime.now(timezone.utc)
                exit_price = float(ref_price)

                await conn.execute(
                    UPDATE_TRADE_LOG_EXIT_BY_ENTRY_ORDER_ID_SQL,
                    pos.entry_order_id,
                    exit_time,
                    exit_price,
                    reason,  # 'TP' / 'SL' / 'TIME'
                    pnl_pct(pos.entry_price, exit_price),
                )
            else:
                logging.warning(
                    "CLOSE requested but entry_order_id is NULL -> trade_log exit update skipped | %s",
                    pos.symbol,
                )

    return True


# ==========================================================
# EXIT LOGIC
# ==========================================================
async def handle_exit(pool: asyncpg.Pool, pos: DBPosition, candle: Candle) -> None:
    # SL
    if candle.l <= pos.sl:
        if await db_request_close(pool, pos, "SL", pos.sl):
            logging.warning("🏁 CLOSE REQUEST SL %s ref=%.10f", pos.symbol, pos.sl)
        return

    # TP
    if candle.h >= pos.tp:
        if await db_request_close(pool, pos, "TP", pos.tp):
            logging.warning("🏁 CLOSE REQUEST TP %s ref=%.10f", pos.symbol, pos.tp)
        return

    # TIME
    held_min = (candle.t - pos.entry_time).total_seconds() / 60.0
    if held_min >= TIME_STOP_MIN:
        if await db_request_close(pool, pos, "TIME", candle.c):
            logging.info("🏁 CLOSE REQUEST TIME %s held_min=%.1f ref=%.10f", pos.symbol, held_min, candle.c)
        return


# ==========================================================
# WS LOOP (dynamic subscribe to OPEN positions)
# ==========================================================
async def ws_loop_for_open_positions(pool: asyncpg.Pool) -> None:
    current: Set[str] = set()

    while True:
        try:
            open_syms = set(await db_get_open_symbols(pool))

            # 1) idle if nothing OPEN
            if not open_syms:
                if current:
                    logging.info("No OPEN positions -> stopping WS subscription.")
                    current = set()
                await asyncio.sleep(REFRESH_OPEN_SYMBOLS_SEC)
                continue

            # 2) reconnect if set changed
            if open_syms != current:
                current = open_syms
                url = ws_url_for_symbols(sorted(current), EXIT_TF)
                logging.info("Subscribing to %d OPEN symbols: %s", len(current), ",".join(sorted(current)))
                logging.info("WS URL streams count=%d tf=%s", len(current), EXIT_TF)

                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    logging.info("Connected to Binance (EXIT service)")

                    last_refresh = asyncio.get_event_loop().time()

                    async for message in ws:
                        # periodic refresh of open symbols -> reconnect if changed
                        now = asyncio.get_event_loop().time()
                        if now - last_refresh >= REFRESH_OPEN_SYMBOLS_SEC:
                            new_set = set(await db_get_open_symbols(pool))
                            last_refresh = now
                            if new_set != current:
                                logging.info("OPEN symbols changed -> reconnecting WS")
                                break

                        payload = json.loads(message)
                        k = payload.get("data", {}).get("k", {})
                        if not k:
                            continue
                        if not bool(k.get("x")):
                            continue  # only closed candles

                        symbol = (k.get("s") or "").upper()
                        if symbol not in current:
                            continue

                        candle = Candle(
                            t=datetime.fromtimestamp(int(k["t"]) / 1000.0, tz=timezone.utc),
                            o=float(k["o"]),
                            h=float(k["h"]),
                            l=float(k["l"]),
                            c=float(k["c"]),
                            vq=float(k["q"]),
                        )

                        pos = await db_get_open_position(pool, symbol)
                        if pos:
                            await handle_exit(pool, pos, candle)

                continue

            # 3) no change -> sleep and re-check
            await asyncio.sleep(REFRESH_OPEN_SYMBOLS_SEC)

        except Exception as e:
            logging.exception("WS loop error: %s | reconnect in 5s...", e)
            await asyncio.sleep(5)


# ==========================================================
# MAIN
# ==========================================================
async def main() -> None:
    logging.info(
        "Starting exit_service | tf=%s | time_stop=%dmin | refresh_open_syms=%ds",
        EXIT_TF, TIME_STOP_MIN, REFRESH_OPEN_SYMBOLS_SEC
    )
    pool = await init_db_pool()
    await ws_loop_for_open_positions(pool)

if __name__ == "__main__":
    asyncio.run(main())
