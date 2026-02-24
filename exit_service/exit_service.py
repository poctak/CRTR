import os
import json
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Set

import asyncpg
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

BINANCE_WS = "wss://stream.binance.com:9443/stream?streams="

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v and v.strip() else default

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v and v.strip() else default

DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "pumpdb")
DB_USER = os.getenv("DB_USER", "pumpuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

TIME_STOP_MIN = env_int("TIME_STOP_MINUTES", 100)
COOLDOWN_AFTER_SL_MIN = env_int("COOLDOWN_AFTER_SL_MINUTES", 15)   # zatím nepoužito (jen ponecháno)
COOLDOWN_AFTER_TIME_MIN = env_int("COOLDOWN_AFTER_TIME_MINUTES", 5) # zatím nepoužito (jen ponecháno)
FEE_ROUNDTRIP_PCT = env_float("FEE_ROUNDTRIP_PCT", 0.15)

# Jak často kontrolovat DB, jestli se změnily OPEN symboly
REFRESH_OPEN_SYMBOLS_SEC = env_int("REFRESH_OPEN_SYMBOLS_SEC", 10)

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

def ws_url_for_symbols(symbols: List[str]) -> str:
    streams = "/".join([f"{s.lower()}@kline_1m" for s in symbols])
    return BINANCE_WS + streams

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

async def db_try_lock_position(pool: asyncpg.Pool, symbol: str) -> bool:
    sql = """
    UPDATE positions_open
    SET status='CLOSING', updated_at=NOW()
    WHERE symbol=$1 AND status='OPEN'
    """
    async with pool.acquire() as conn:
        res = await conn.execute(sql, symbol)
    return res.endswith("1")

async def db_delete_position(pool: asyncpg.Pool, symbol: str) -> None:
    sql = "DELETE FROM positions_open WHERE symbol=$1"
    async with pool.acquire() as conn:
        await conn.execute(sql, symbol)

async def insert_trade_log(
    pool: asyncpg.Pool,
    symbol: str,
    entry_time: datetime,
    exit_time: datetime,
    entry_price: float,
    exit_price: float,
    reason: str,
) -> None:
    gross_pct = (exit_price - entry_price) / entry_price * 100.0 if entry_price > 0 else 0.0
    net_pct = gross_pct - FEE_ROUNDTRIP_PCT

    sql = """
    INSERT INTO trade_log(symbol, entry_time, exit_time, entry_price, exit_price, reason, pnl_pct)
    VALUES ($1,$2,$3,$4,$5,$6,$7)
    """
    async with pool.acquire() as conn:
        await conn.execute(sql, symbol, entry_time, exit_time, entry_price, exit_price, reason, net_pct)

async def handle_exit(pool: asyncpg.Pool, pos: DBPosition, candle: Candle) -> None:
    # SL
    if candle.l <= pos.sl:
        if await db_try_lock_position(pool, pos.symbol):
            await insert_trade_log(pool, pos.symbol, pos.entry_time, candle.t, pos.entry_price, pos.sl, "SL")
            await db_delete_position(pool, pos.symbol)
            logging.warning(f"🏁 EXIT SL {pos.symbol}")
        return

    # TP
    if candle.h >= pos.tp:
        if await db_try_lock_position(pool, pos.symbol):
            await insert_trade_log(pool, pos.symbol, pos.entry_time, candle.t, pos.entry_price, pos.tp, "TP")
            await db_delete_position(pool, pos.symbol)
            logging.warning(f"🏁 EXIT TP {pos.symbol}")
        return

    # TIME
    held_min = (candle.t - pos.entry_time).total_seconds() / 60.0
    if held_min >= TIME_STOP_MIN:
        if await db_try_lock_position(pool, pos.symbol):
            await insert_trade_log(pool, pos.symbol, pos.entry_time, candle.t, pos.entry_price, candle.c, "TIME")
            await db_delete_position(pool, pos.symbol)
            logging.info(f"🏁 EXIT TIME {pos.symbol}")
        return

async def ws_loop_for_open_positions(pool: asyncpg.Pool) -> None:
    current: Set[str] = set()

    while True:
        try:
            open_syms = set(await db_get_open_symbols(pool))

            # když není nic open, jen čekej a neotvírej WS
            if not open_syms:
                if current:
                    logging.info("No OPEN positions -> stopping WS subscription.")
                    current = set()
                await asyncio.sleep(REFRESH_OPEN_SYMBOLS_SEC)
                continue

            # když se změnily OPEN symboly, reconnectni WS na nový set
            if open_syms != current:
                current = open_syms
                url = ws_url_for_symbols(sorted(current))
                logging.info("Subscribing to %d OPEN symbols: %s", len(current), ",".join(sorted(current)))
                logging.info("WS URL streams count=%d", len(current))

                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    logging.info("Connected to Binance (EXIT service)")

                    # současně budeme občas kontrolovat, jestli se OPEN set nezměnil
                    last_refresh = asyncio.get_event_loop().time()

                    async for message in ws:
                        now = asyncio.get_event_loop().time()
                        if now - last_refresh >= REFRESH_OPEN_SYMBOLS_SEC:
                            # forcing reconnect by breaking out if set changed
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
                        # WS by URL should only send these, but keep it safe:
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

                # po opuštění async for / WS contextu se loop vrátí nahoru a znovu načte OPEN symboly
                continue

            # když se set nezměnil, jen chvíli počkej a zkontroluj znovu
            await asyncio.sleep(REFRESH_OPEN_SYMBOLS_SEC)

        except Exception as e:
            logging.error(f"WS loop error: {e} | reconnect in 5s...")
            await asyncio.sleep(5)

async def main():
    pool = await init_db_pool()
    await ws_loop_for_open_positions(pool)

if __name__ == "__main__":
    asyncio.run(main())
