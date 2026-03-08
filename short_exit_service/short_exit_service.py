#!/usr/bin/env python3
import os
import json
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Set, Dict, Tuple

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

def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v.strip() if v and v.strip() else default

def env_list(name: str, default: str = "") -> List[str]:
    v = os.getenv(name, default)
    items: List[str] = []
    for x in (v or "").split(","):
        x = x.strip().upper()
        if x:
            items.append(x)
    return items

DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "pumpdb")
DB_USER = os.getenv("DB_USER", "pumpuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

POSITIONS_TABLE = env_str("POSITIONS_OPEN_SHORT_TABLE", "public.positions_open_short")
RUNNER_TABLE = env_str("EXIT_RUNNER_SHORT_TABLE", "public.exit_runner_state_short")
CANDLES_TABLE = env_str("CANDLES_SHORT_TABLE", "public.candles_5m_short")

TIME_STOP_MIN = env_int("SHORT_TIME_STOP_MINUTES", 100)
REFRESH_OPEN_SYMBOLS_SEC = env_int("SHORT_REFRESH_OPEN_SYMBOLS_SEC", 10)
POS_CACHE_SEC = env_float("SHORT_POS_CACHE_SEC", 2.0)
CLOSE_REQUEST_COOLDOWN_SEC = env_float("SHORT_CLOSE_REQUEST_COOLDOWN_SEC", 15.0)

# --- Breadth / regime (red ratio for short runner) ---
ANCHOR_SYMBOL = env_str("SHORT_ANCHOR_SYMBOL", "BTCUSDT")

ALT_RED_ON = env_float("ALT_RED_ON", 0.62)
ALT_RED_OFF = env_float("ALT_RED_OFF", 0.55)
ALT_RED_MIN_ALTS = env_int("ALT_RED_MIN_ALTS", 30)
ALT_RED_CACHE_SEC = env_float("ALT_RED_CACHE_SEC", 15.0)

ALT_UNIVERSE_SYMBOLS = env_list("SHORT_ALT_UNIVERSE_SYMBOLS", "")
ALT_EXCLUDE_SYMBOLS = set(env_list(
    "SHORT_ALT_EXCLUDE_SYMBOLS",
    "BTCUSDT,USDTUSDT,FDUSDUSDT,TUSDUSDT,USDPUSDT"
))

# --- Trailing runner settings for short ---
TRAIL_PCT = env_float("SHORT_TRAIL_PCT", 0.006)
RUNNER_STATE_CACHE_SEC = env_float("SHORT_RUNNER_STATE_CACHE_SEC", 1.0)

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

@dataclass
class RunnerState:
    symbol: str
    runner_enabled: bool
    activated_at: Optional[datetime]
    activated_price: Optional[float]
    best_price: Optional[float]   # for short = lowest seen price since activation
    trail_price: Optional[float]  # for short = bounce threshold
    trail_pct: Optional[float]

# ==========================================================
# BINANCE WS URL
# ==========================================================
def ws_url_for_symbols(symbols: List[str]) -> str:
    streams = "/".join([f"{s.lower()}@aggTrade" for s in symbols])
    return BINANCE_WS + streams

# ==========================================================
# TIME
# ==========================================================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

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

async def db_ensure_runner_table(pool: asyncpg.Pool) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {RUNNER_TABLE} (
      symbol              TEXT PRIMARY KEY,
      runner_enabled      BOOLEAN NOT NULL DEFAULT FALSE,

      activated_at        TIMESTAMPTZ,
      activated_price     DOUBLE PRECISION,

      best_price          DOUBLE PRECISION,
      trail_price         DOUBLE PRECISION,

      trail_pct           DOUBLE PRECISION,

      updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS ix_exit_runner_state_short_enabled
      ON {RUNNER_TABLE} (runner_enabled);
    """
    async with pool.acquire() as conn:
        await conn.execute(sql)

async def db_get_open_symbols(pool: asyncpg.Pool) -> List[str]:
    sql = f"""
    SELECT symbol
    FROM {POSITIONS_TABLE}
    WHERE status='OPEN'
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql)
    return [r["symbol"].strip().upper() for r in rows if r["symbol"]]

async def db_get_open_position(pool: asyncpg.Pool, symbol: str) -> Optional[DBPosition]:
    sql = f"""
    SELECT symbol, entry_time, entry_price, sl, tp, status
    FROM {POSITIONS_TABLE}
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
    sql = f"""
    UPDATE {POSITIONS_TABLE}
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

# --- Runner state DB helpers ---
async def db_get_runner_state(pool: asyncpg.Pool, symbol: str) -> Optional[RunnerState]:
    sql = f"""
    SELECT symbol, runner_enabled, activated_at, activated_price, best_price, trail_price, trail_pct
    FROM {RUNNER_TABLE}
    WHERE symbol=$1
    LIMIT 1
    """
    async with pool.acquire() as conn:
        r = await conn.fetchrow(sql, symbol)
    if not r:
        return None
    return RunnerState(
        symbol=r["symbol"],
        runner_enabled=bool(r["runner_enabled"]),
        activated_at=r["activated_at"],
        activated_price=float(r["activated_price"]) if r["activated_price"] is not None else None,
        best_price=float(r["best_price"]) if r["best_price"] is not None else None,
        trail_price=float(r["trail_price"]) if r["trail_price"] is not None else None,
        trail_pct=float(r["trail_pct"]) if r["trail_pct"] is not None else None,
    )

async def db_upsert_runner_activate(pool: asyncpg.Pool, symbol: str, price: float, trail_pct: float) -> None:
    sql = f"""
    INSERT INTO {RUNNER_TABLE} (symbol, runner_enabled, activated_at, activated_price, best_price, trail_price, trail_pct, updated_at)
    VALUES ($1, TRUE, NOW(), $2, $2, $3, $4, NOW())
    ON CONFLICT (symbol) DO UPDATE SET
        runner_enabled=TRUE,
        activated_at=COALESCE({RUNNER_TABLE}.activated_at, NOW()),
        activated_price=COALESCE({RUNNER_TABLE}.activated_price, $2),
        best_price=LEAST(COALESCE({RUNNER_TABLE}.best_price, $2), $2),
        trail_price=LEAST(COALESCE({RUNNER_TABLE}.trail_price, $3), $3),
        trail_pct=COALESCE({RUNNER_TABLE}.trail_pct, $4),
        updated_at=NOW()
    """
    trail_price = price * (1.0 + trail_pct)
    async with pool.acquire() as conn:
        await conn.execute(sql, symbol, float(price), float(trail_price), float(trail_pct))

async def db_update_runner_best(pool: asyncpg.Pool, symbol: str, best_price: float, trail_price: float) -> None:
    sql = f"""
    UPDATE {RUNNER_TABLE}
    SET best_price=$2, trail_price=$3, updated_at=NOW()
    WHERE symbol=$1
    """
    async with pool.acquire() as conn:
        await conn.execute(sql, symbol, float(best_price), float(trail_price))

async def db_delete_runner_state(pool: asyncpg.Pool, symbol: str) -> None:
    sql = f"DELETE FROM {RUNNER_TABLE} WHERE symbol=$1"
    async with pool.acquire() as conn:
        await conn.execute(sql, symbol)

# ==========================================================
# BREADTH (alt_red_ratio)
# ==========================================================
async def db_get_anchor_ts(pool: asyncpg.Pool, symbol: str) -> Optional[datetime]:
    sql = f"SELECT MAX(ts) AS ts FROM {CANDLES_TABLE} WHERE symbol=$1"
    async with pool.acquire() as conn:
        r = await conn.fetchrow(sql, symbol)
    return r["ts"] if r and r["ts"] else None

async def db_get_alt_red_ratio(pool: asyncpg.Pool, ts: datetime) -> Tuple[float, int, int]:
    universe = [s for s in ALT_UNIVERSE_SYMBOLS if s and s not in ALT_EXCLUDE_SYMBOLS] if ALT_UNIVERSE_SYMBOLS else []

    if universe:
        sql = f"""
        SELECT
          COUNT(*) FILTER (WHERE c < o) AS red,
          COUNT(*) AS total
        FROM {CANDLES_TABLE}
        WHERE ts=$1
          AND symbol = ANY($2::text[])
          AND symbol <> ALL($3::text[])
        """
        params = (ts, universe, list(ALT_EXCLUDE_SYMBOLS))
    else:
        sql = f"""
        SELECT
          COUNT(*) FILTER (WHERE c < o) AS red,
          COUNT(*) AS total
        FROM {CANDLES_TABLE}
        WHERE ts=$1
          AND symbol <> ALL($2::text[])
        """
        params = (ts, list(ALT_EXCLUDE_SYMBOLS))

    async with pool.acquire() as conn:
        r = await conn.fetchrow(sql, *params)

    red = int(r["red"] or 0)
    total = int(r["total"] or 0)
    ratio = (red / total) if total > 0 else 0.0
    return ratio, red, total

@dataclass
class BreadthCache:
    risk_on: bool
    ratio: float
    red: int
    total: int
    anchor_ts: Optional[datetime]
    fetched_at: float

def apply_hysteresis(prev_risk_on: bool, ratio: float) -> bool:
    if not prev_risk_on:
        return ratio >= ALT_RED_ON
    return ratio > ALT_RED_OFF

# ==========================================================
# EXIT LOGIC
# ==========================================================
async def maybe_close(
    pool: asyncpg.Pool,
    pos: DBPosition,
    reason: str,
    ref_price: float,
    close_cooldown: Dict[str, float],
) -> None:
    now_ts = asyncio.get_event_loop().time()
    last = close_cooldown.get(pos.symbol, 0.0)
    if now_ts - last < CLOSE_REQUEST_COOLDOWN_SEC:
        return

    if await db_request_close(pool, pos.symbol, reason, ref_price):
        close_cooldown[pos.symbol] = now_ts
        logging.warning("🏁 SHORT CLOSE REQUEST %s %s ref=%.10f", reason, pos.symbol, ref_price)

async def handle_tick_exit(
    pool: asyncpg.Pool,
    pos: DBPosition,
    last_price: float,
    risk_on: bool,
    breadth_ratio: float,
    breadth_red: int,
    breadth_total: int,
    runner: Optional[RunnerState],
    close_cooldown: Dict[str, float],
    runner_cache: Dict[str, Tuple[Optional[RunnerState], float]],
) -> None:
    # 1) SL for short => price goes UP
    if last_price >= pos.sl:
        await maybe_close(pool, pos, "SL", pos.sl, close_cooldown)
        return

    # 2) Runner trailing for short => if bounce above trail, close
    if runner and runner.runner_enabled and runner.trail_price is not None:
        if last_price >= runner.trail_price:
            await maybe_close(pool, pos, "TRAIL", float(runner.trail_price), close_cooldown)
            return

        best = runner.best_price if runner.best_price is not None else runner.activated_price
        if best is None:
            best = last_price

        if last_price < best:
            new_best = last_price
            pct = runner.trail_pct if runner.trail_pct else TRAIL_PCT
            new_trail = new_best * (1.0 + pct)
            await db_update_runner_best(pool, pos.symbol, new_best, new_trail)

            runner_cache[pos.symbol] = (
                RunnerState(
                    symbol=pos.symbol,
                    runner_enabled=True,
                    activated_at=runner.activated_at,
                    activated_price=runner.activated_price,
                    best_price=new_best,
                    trail_price=new_trail,
                    trail_pct=pct,
                ),
                asyncio.get_event_loop().time(),
            )

    # 3) TP for short => price goes DOWN
    if last_price <= pos.tp:
        if risk_on and breadth_total >= ALT_RED_MIN_ALTS:
            if not (runner and runner.runner_enabled):
                await db_upsert_runner_activate(pool, pos.symbol, last_price, TRAIL_PCT)
                logging.warning(
                    "🚀 SHORT RUNNER ACTIVATED %s | price=%.10f tp=%.10f | risk_on=True ratio=%.3f (%d/%d) | trail_pct=%.4f",
                    pos.symbol, last_price, pos.tp,
                    breadth_ratio, breadth_red, breadth_total,
                    TRAIL_PCT,
                )
                runner_cache[pos.symbol] = (
                    RunnerState(
                        symbol=pos.symbol,
                        runner_enabled=True,
                        activated_at=utc_now(),
                        activated_price=last_price,
                        best_price=last_price,
                        trail_price=last_price * (1.0 + TRAIL_PCT),
                        trail_pct=TRAIL_PCT,
                    ),
                    asyncio.get_event_loop().time(),
                )
            return
        else:
            await maybe_close(pool, pos, "TP", pos.tp, close_cooldown)
            return

    # 4) TIME stop only if runner not enabled
    if not (runner and runner.runner_enabled):
        held_min = (utc_now() - pos.entry_time).total_seconds() / 60.0
        if held_min >= TIME_STOP_MIN:
            await maybe_close(pool, pos, "TIME", float(last_price), close_cooldown)
            return

# ==========================================================
# WS LOOP
# ==========================================================
async def ws_loop_for_open_positions(pool: asyncpg.Pool) -> None:
    current: Set[str] = set()
    pos_cache: Dict[str, Tuple[Optional[DBPosition], float]] = {}
    runner_cache: Dict[str, Tuple[Optional[RunnerState], float]] = {}
    close_cooldown: Dict[str, float] = {}

    breadth_cache = BreadthCache(
        risk_on=False,
        ratio=0.0,
        red=0,
        total=0,
        anchor_ts=None,
        fetched_at=0.0,
    )

    while True:
        try:
            open_syms = set(await db_get_open_symbols(pool))

            if not open_syms:
                if current:
                    logging.info("No OPEN short positions -> stopping WS subscription.")
                    current = set()
                    pos_cache.clear()
                    runner_cache.clear()
                    close_cooldown.clear()
                await asyncio.sleep(REFRESH_OPEN_SYMBOLS_SEC)
                continue

            if open_syms != current:
                current = open_syms
                url = ws_url_for_symbols(sorted(current))
                logging.info("Subscribing to %d OPEN short symbols (aggTrade): %s", len(current), ",".join(sorted(current)))

                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    logging.info("Connected to Binance (SHORT EXIT service, aggTrade)")
                    last_refresh = asyncio.get_event_loop().time()

                    async for message in ws:
                        now_loop = asyncio.get_event_loop().time()

                        if now_loop - last_refresh >= REFRESH_OPEN_SYMBOLS_SEC:
                            new_set = set(await db_get_open_symbols(pool))
                            last_refresh = now_loop
                            if new_set != current:
                                logging.info("OPEN short symbols changed -> reconnecting WS")
                                break

                        if now_loop - breadth_cache.fetched_at >= ALT_RED_CACHE_SEC:
                            anchor_ts = await db_get_anchor_ts(pool, ANCHOR_SYMBOL)
                            if anchor_ts is None:
                                logging.warning("Short breadth: missing anchor ts for %s -> keeping previous state", ANCHOR_SYMBOL)
                            else:
                                ratio, red, total = await db_get_alt_red_ratio(pool, anchor_ts)
                                next_risk_on = apply_hysteresis(breadth_cache.risk_on, ratio) if total >= ALT_RED_MIN_ALTS else breadth_cache.risk_on
                                breadth_cache = BreadthCache(
                                    risk_on=next_risk_on,
                                    ratio=ratio,
                                    red=red,
                                    total=total,
                                    anchor_ts=anchor_ts,
                                    fetched_at=now_loop,
                                )
                                logging.info(
                                    "Short breadth @%s anchor=%s risk_on=%s ratio=%.3f (%d/%d) ON=%.2f OFF=%.2f MIN=%d",
                                    anchor_ts.isoformat(), ANCHOR_SYMBOL,
                                    breadth_cache.risk_on,
                                    ratio, red, total,
                                    ALT_RED_ON, ALT_RED_OFF, ALT_RED_MIN_ALTS
                                )

                        payload = json.loads(message)
                        data = payload.get("data", {})
                        if not data:
                            continue

                        symbol = (data.get("s") or "").upper()
                        if not symbol or symbol not in current:
                            continue

                        try:
                            last_price = float(data.get("p"))
                        except Exception:
                            continue

                        cached_pos = pos_cache.get(symbol)
                        need_fetch_pos = True
                        if cached_pos:
                            _, fetched_at = cached_pos
                            if (now_loop - fetched_at) <= POS_CACHE_SEC:
                                need_fetch_pos = False

                        if need_fetch_pos:
                            pos = await db_get_open_position(pool, symbol)
                            pos_cache[symbol] = (pos, now_loop)
                        else:
                            pos = cached_pos[0] if cached_pos else None

                        if not pos:
                            await db_delete_runner_state(pool, symbol)
                            runner_cache.pop(symbol, None)
                            continue

                        cached_runner = runner_cache.get(symbol)
                        need_fetch_runner = True
                        if cached_runner:
                            _, fetched_at = cached_runner
                            if (now_loop - fetched_at) <= RUNNER_STATE_CACHE_SEC:
                                need_fetch_runner = False

                        if need_fetch_runner:
                            runner = await db_get_runner_state(pool, symbol)
                            runner_cache[symbol] = (runner, now_loop)
                        else:
                            runner = cached_runner[0] if cached_runner else None

                        await handle_tick_exit(
                            pool=pool,
                            pos=pos,
                            last_price=last_price,
                            risk_on=breadth_cache.risk_on,
                            breadth_ratio=breadth_cache.ratio,
                            breadth_red=breadth_cache.red,
                            breadth_total=breadth_cache.total,
                            runner=runner,
                            close_cooldown=close_cooldown,
                            runner_cache=runner_cache,
                        )

                continue

            await asyncio.sleep(REFRESH_OPEN_SYMBOLS_SEC)

        except Exception as e:
            logging.error("SHORT WS loop error: %s | reconnect in 5s...", e)
            await asyncio.sleep(5)

# ==========================================================
# MAIN
# ==========================================================
async def main() -> None:
    pool = await init_db_pool()
    await db_ensure_runner_table(pool)

    logging.info(
        "SHORT EXIT service started | ANCHOR_SYMBOL=%s ON/OFF=%.2f/%.2f MIN_ALTS=%d cache=%.1fs | TRAIL_PCT=%.4f TIME_STOP_MIN=%d",
        ANCHOR_SYMBOL, ALT_RED_ON, ALT_RED_OFF, ALT_RED_MIN_ALTS, ALT_RED_CACHE_SEC,
        TRAIL_PCT, TIME_STOP_MIN
    )

    if ALT_UNIVERSE_SYMBOLS:
        logging.info("Short breadth universe: explicit (%d symbols)", len(ALT_UNIVERSE_SYMBOLS))
    else:
        logging.info("Short breadth universe: dynamic (all symbols at anchor ts, minus excludes=%d)", len(ALT_EXCLUDE_SYMBOLS))

    await ws_loop_for_open_positions(pool)

if __name__ == "__main__":
    asyncio.run(main())
