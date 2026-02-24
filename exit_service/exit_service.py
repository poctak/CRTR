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

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ==========================================================
# CONSTANTS / ENV
# ==========================================================

BINANCE_WS = "wss://stream.binance.com:9443/stream?streams="

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v and v.strip() else default

DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "pumpdb")
DB_USER = os.getenv("DB_USER", "pumpuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

TIME_STOP_MIN = env_int("TIME_STOP_MINUTES", 100)

# Jak často se má DB kontrolovat na změny OPEN symbolů (sekundy)
REFRESH_OPEN_SYMBOLS_SEC = env_int("REFRESH_OPEN_SYMBOLS_SEC", 10)

# ==========================================================
# DATA MODELS
# ==========================================================

@dataclass
class Candle:
    t: datetime
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

# ==========================================================
# BINANCE WS URL
# ==========================================================

def ws_url_for_symbols(symbols: List[str]) -> str:
    # Binance multiplex: /stream?streams=symbol@kline_1m/symbol2@kline_1m
    streams = "/".join([f"{s.lower()}@kline_1m" for s in symbols])
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
    SELECT symbol, entry_time, entry_price, sl, tp, status
    FROM positions_open
    WHERE symbol=$1 AND status='OPEN'
    LIMIT 1
    """
    async with pool.acquire() as conn:
        r = await conn.fetchrow(sql, symbol)
    if not r:
        return None
    return DBPosition(
        symbol=r["symbol"],
        entry_time=r["entry_time"],
        entry_price=float(r["entry_price"]),
        sl=float(r["sl"]),
        tp=float(r["tp"]),
        status=r["status"],
    )

async def db_request_close(pool: asyncpg.Pool, symbol: str, reason: str, ref_price: float) -> bool:
    """
    Atomicky přepne OPEN -> CLOSING a uloží close metadata.
    Vrací True jen pokud to opravdu přepnulo 1 řádek (tj. získal jsi lock).
    """
    sql = """
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
        res = await conn.execute(sql, symbol, reason, ref_price)
    return res.endswith("1")

# ==========================================================
# EXIT LOGIC (Variant A)
# ==========================================================

async def handle_exit(pool: asyncpg.Pool, pos: DBPosition, candle: Candle) -> None:
    # SL
    if candle.l <= pos.sl:
        if await db_request_close(pool, pos.symbol, "SL", pos.sl):
            logging.warning("🏁 CLOSE REQUEST SL %s ref=%.10f", pos.symbol, pos.sl)
        return

    # TP
    if candle.h >= pos.tp:
        if await db_request_close(pool, pos.symbol, "TP", pos.tp):
            logging.warning("🏁 CLOSE REQUEST TP %s ref=%.10f", pos.symbol, pos.tp)
        return

    # TIME
    held_min = (candle.t - pos.entry_time).total_seconds() / 60.0
    if held_min >= TIME_STOP_MIN:
        if await db_request_close(pool, pos.symbol, "TIME", candle.c):
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

            # 1) Když není nic OPEN, neotvírej WS (idle)
            if not open_syms:
                if current:
                    logging.info("No OPEN positions -> stopping WS subscription.")
                    current = set()
                await asyncio.sleep(REFRESH_OPEN_SYMBOLS_SEC)
                continue

            # 2) Když se změnil set OPEN symbolů, otevři WS na nové streamy
            if open_syms != current:
                current = open_syms
                url = ws_url_for_symbols(sorted(current))
                logging.info("Subscribing to %d OPEN symbols: %s", len(current), ",".join(sorted(current)))
                logging.info("WS URL streams count=%d", len(current))

                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    logging.info("Connected to Binance (EXIT service)")

                    last_refresh = asyncio.get_event_loop().time()

                    async for message in ws:
                        # Periodicky kontroluj změnu OPEN symbolů a případně reconnect
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

                # po WS close pokračujeme loopem a znovu načteme open symboly
                continue

            # 3) Set se nezměnil -> jen počkej a zkontroluj znovu
            await asyncio.sleep(REFRESH_OPEN_SYMBOLS_SEC)

        except Exception as e:
            logging.error("WS loop error: %s | reconnect in 5s...", e)
            await asyncio.sleep(5)

# ==========================================================
# MAIN
# ==========================================================

async def main() -> None:
    pool = await init_db_pool()
    await ws_loop_for_open_positions(pool)

if __name__ == "__main__":
    asyncio.run(main())
