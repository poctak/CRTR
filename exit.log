#!/usr/bin/env python3
import os
import json
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Set, Dict

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

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v and v.strip() else default
    except Exception:
        return default

DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "pumpdb")
DB_USER = os.getenv("DB_USER", "pumpuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

TIME_STOP_MIN = env_int("TIME_STOP_MINUTES", 100)

# Jak často se má DB kontrolovat na změny OPEN symbolů (sekundy)
REFRESH_OPEN_SYMBOLS_SEC = env_int("REFRESH_OPEN_SYMBOLS_SEC", 10)

# Throttle DB read for a symbol (seconds) – ať nečteme pozici z DB na každý tick
POS_CACHE_SEC = env_float("POS_CACHE_SEC", 2.0)

# Cooldown po úspěšném CLOSE REQUEST – ať to neposíláme opakovaně
CLOSE_REQUEST_COOLDOWN_SEC = env_float("CLOSE_REQUEST_COOLDOWN_SEC", 15.0)

# ==========================================================
# DATA MODELS
# ==========================================================

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
    # Binance multiplex:
    # /stream?streams=btcusdc@aggTrade/ethusdc@aggTrade/...
    streams = "/".join([f"{s.lower()}@aggTrade" for s in symbols])
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
    Vrací True jen pokud to opravdu přepnulo 1 řádek.
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
# EXIT LOGIC (tick-based via aggTrade)
# ==========================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

async def maybe_close(
    pool: asyncpg.Pool,
    pos: DBPosition,
    reason: str,
    ref_price: float,
    close_cooldown: Dict[str, float],
) -> None:
    """
    Sends close request at most once per cooldown interval per symbol.
    """
    now_ts = asyncio.get_event_loop().time()
    last = close_cooldown.get(pos.symbol, 0.0)
    if now_ts - last < CLOSE_REQUEST_COOLDOWN_SEC:
        return

    if await db_request_close(pool, pos.symbol, reason, ref_price):
        close_cooldown[pos.symbol] = now_ts
        if reason in ("SL", "TP"):
            logging.warning("🏁 CLOSE REQUEST %s %s ref=%.10f", reason, pos.symbol, ref_price)
        else:
            logging.info("🏁 CLOSE REQUEST %s %s ref=%.10f", reason, pos.symbol, ref_price)

async def handle_tick_exit(
    pool: asyncpg.Pool,
    pos: DBPosition,
    last_price: float,
    event_time: datetime,
    close_cooldown: Dict[str, float],
) -> None:
    # SL: tick price <= sl
    if last_price <= pos.sl:
        await maybe_close(pool, pos, "SL", pos.sl, close_cooldown)
        return

    # TP: tick price >= tp
    if last_price >= pos.tp:
        await maybe_close(pool, pos, "TP", pos.tp, close_cooldown)
        return

    # TIME: now - entry_time >= TIME_STOP_MIN
    held_min = (utc_now() - pos.entry_time).total_seconds() / 60.0
    if held_min >= TIME_STOP_MIN:
        # ref price as last seen tick
        await maybe_close(pool, pos, "TIME", float(last_price), close_cooldown)
        return

# ==========================================================
# WS LOOP (dynamic subscribe to OPEN positions)
# ==========================================================

async def ws_loop_for_open_positions(pool: asyncpg.Pool) -> None:
    current: Set[str] = set()

    # per-symbol position cache: symbol -> (DBPosition|None, fetched_at_loop_time)
    pos_cache: Dict[str, tuple[Optional[DBPosition], float]] = {}

    # per-symbol cooldown for close request
    close_cooldown: Dict[str, float] = {}

    while True:
        try:
            open_syms = set(await db_get_open_symbols(pool))

            # 1) Když není nic OPEN, neotvírej WS (idle)
            if not open_syms:
                if current:
                    logging.info("No OPEN positions -> stopping WS subscription.")
                    current = set()
                    pos_cache.clear()
                    close_cooldown.clear()
                await asyncio.sleep(REFRESH_OPEN_SYMBOLS_SEC)
                continue

            # 2) Když se změnil set OPEN symbolů, otevři WS na nové streamy
            if open_syms != current:
                current = open_syms
                url = ws_url_for_symbols(sorted(current))
                logging.info("Subscribing to %d OPEN symbols (aggTrade): %s", len(current), ",".join(sorted(current)))
                logging.info("WS URL streams count=%d", len(current))

                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    logging.info("Connected to Binance (EXIT service, aggTrade)")

                    last_refresh = asyncio.get_event_loop().time()

                    async for message in ws:
                        # Periodicky kontroluj změnu OPEN symbolů a případně reconnect
                        now_loop = asyncio.get_event_loop().time()
                        if now_loop - last_refresh >= REFRESH_OPEN_SYMBOLS_SEC:
                            new_set = set(await db_get_open_symbols(pool))
                            last_refresh = now_loop
                            if new_set != current:
                                logging.info("OPEN symbols changed -> reconnecting WS")
                                break

                        payload = json.loads(message)
                        data = payload.get("data", {})
                        if not data:
                            continue

                        # aggTrade payload:
                        #  s = symbol, p = price (string), T = trade time (ms)
                        symbol = (data.get("s") or "").upper()
                        if not symbol or symbol not in current:
                            continue

                        try:
                            last_price = float(data.get("p"))
                        except Exception:
                            continue

                        t_ms = data.get("T")
                        try:
                            event_time = datetime.fromtimestamp(int(t_ms) / 1000.0, tz=timezone.utc) if t_ms else utc_now()
                        except Exception:
                            event_time = utc_now()

                        # --- position cache throttle ---
                        cached = pos_cache.get(symbol)
                        need_fetch = True
                        if cached:
                            _, fetched_at = cached
                            if (now_loop - fetched_at) <= POS_CACHE_SEC:
                                need_fetch = False

                        if need_fetch:
                            pos = await db_get_open_position(pool, symbol)
                            pos_cache[symbol] = (pos, now_loop)
                        else:
                            pos = cached[0] if cached else None

                        if not pos:
                            continue

                        await handle_tick_exit(pool, pos, last_price, event_time, close_cooldown)

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
