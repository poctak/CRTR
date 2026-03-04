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

# --- BTC trend settings (Variant A: read BTC candles_5m from DB) ---
BTC_SYMBOL = env_str("BTC_SYMBOL", "BTCUSDC")
BTC_TREND_LOOKBACK = env_int("BTC_TREND_LOOKBACK", 80)     # kolik posledních 5m candle číst
BTC_MA_FAST = env_int("BTC_MA_FAST", 20)
BTC_MA_SLOW = env_int("BTC_MA_SLOW", 50)
BTC_TREND_CACHE_SEC = env_float("BTC_TREND_CACHE_SEC", 15.0)  # jak často přepočítat trend

# --- Trailing runner settings ---
TRAIL_PCT = env_float("TRAIL_PCT", 0.006)                  # 0.6% trailing
RUNNER_STATE_CACHE_SEC = env_float("RUNNER_STATE_CACHE_SEC", 1.0)

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
    peak_price: Optional[float]
    trail_price: Optional[float]
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
    sql = """
    CREATE TABLE IF NOT EXISTS exit_runner_state (
      symbol              TEXT PRIMARY KEY,
      runner_enabled      BOOLEAN NOT NULL DEFAULT FALSE,

      activated_at        TIMESTAMPTZ,
      activated_price     DOUBLE PRECISION,

      peak_price          DOUBLE PRECISION,
      trail_price         DOUBLE PRECISION,

      trail_pct           DOUBLE PRECISION,

      updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS ix_exit_runner_state_enabled
      ON exit_runner_state (runner_enabled);
    """
    async with pool.acquire() as conn:
        await conn.execute(sql)

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

# --- Runner state DB helpers ---
async def db_get_runner_state(pool: asyncpg.Pool, symbol: str) -> Optional[RunnerState]:
    sql = """
    SELECT symbol, runner_enabled, activated_at, activated_price, peak_price, trail_price, trail_pct
    FROM exit_runner_state
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
        peak_price=float(r["peak_price"]) if r["peak_price"] is not None else None,
        trail_price=float(r["trail_price"]) if r["trail_price"] is not None else None,
        trail_pct=float(r["trail_pct"]) if r["trail_pct"] is not None else None,
    )

async def db_upsert_runner_activate(pool: asyncpg.Pool, symbol: str, price: float, trail_pct: float) -> None:
    sql = """
    INSERT INTO exit_runner_state (symbol, runner_enabled, activated_at, activated_price, peak_price, trail_price, trail_pct, updated_at)
    VALUES ($1, TRUE, NOW(), $2, $2, $3, $4, NOW())
    ON CONFLICT (symbol) DO UPDATE SET
        runner_enabled=TRUE,
        activated_at=COALESCE(exit_runner_state.activated_at, NOW()),
        activated_price=COALESCE(exit_runner_state.activated_price, $2),
        peak_price=GREATEST(COALESCE(exit_runner_state.peak_price, $2), $2),
        trail_price=GREATEST(COALESCE(exit_runner_state.trail_price, $3), $3),
        trail_pct=COALESCE(exit_runner_state.trail_pct, $4),
        updated_at=NOW()
    """
    trail_price = price * (1.0 - trail_pct)
    async with pool.acquire() as conn:
        await conn.execute(sql, symbol, float(price), float(trail_price), float(trail_pct))

async def db_update_runner_peak(pool: asyncpg.Pool, symbol: str, peak_price: float, trail_price: float) -> None:
    sql = """
    UPDATE exit_runner_state
    SET peak_price=$2, trail_price=$3, updated_at=NOW()
    WHERE symbol=$1
    """
    async with pool.acquire() as conn:
        await conn.execute(sql, symbol, float(peak_price), float(trail_price))

async def db_delete_runner_state(pool: asyncpg.Pool, symbol: str) -> None:
    sql = "DELETE FROM exit_runner_state WHERE symbol=$1"
    async with pool.acquire() as conn:
        await conn.execute(sql, symbol)

# --- BTC candles read ---
async def db_get_candles_close(pool: asyncpg.Pool, symbol: str, limit: int) -> List[float]:
    sql = """
    SELECT c
    FROM candles_5m
    WHERE symbol=$1
    ORDER BY ts DESC
    LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, symbol, limit)
    closes = []
    for r in rows:
        try:
            closes.append(float(r["c"]))
        except Exception:
            pass
    # rows are DESC; reverse to chronological for MA calculation
    closes.reverse()
    return closes

# ==========================================================
# BTC TREND (cached)
# ==========================================================
def sma(values: List[float], n: int) -> Optional[float]:
    if n <= 0 or len(values) < n:
        return None
    return sum(values[-n:]) / float(n)

def is_btc_uptrend_from_closes(closes: List[float]) -> bool:
    """
    Simple, robust rule:
      - SMA_fast > SMA_slow
      - and SMA_fast is rising vs previous fast SMA (1 step back)
    """
    if len(closes) < max(BTC_MA_SLOW + 2, BTC_MA_FAST + 2):
        return False

    fast_now = sma(closes, BTC_MA_FAST)
    slow_now = sma(closes, BTC_MA_SLOW)
    if fast_now is None or slow_now is None:
        return False
    if fast_now <= slow_now:
        return False

    # rising fast MA (compare with previous point)
    fast_prev = sma(closes[:-1], BTC_MA_FAST)
    if fast_prev is None:
        return True  # fallback: at least fast>slow
    return fast_now >= fast_prev

@dataclass
class BtcTrendCache:
    is_up: bool
    fetched_at: float  # loop time

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
    """
    Sends close request at most once per cooldown interval per symbol.
    """
    now_ts = asyncio.get_event_loop().time()
    last = close_cooldown.get(pos.symbol, 0.0)
    if now_ts - last < CLOSE_REQUEST_COOLDOWN_SEC:
        return

    if await db_request_close(pool, pos.symbol, reason, ref_price):
        close_cooldown[pos.symbol] = now_ts
        logging.warning("🏁 CLOSE REQUEST %s %s ref=%.10f", reason, pos.symbol, ref_price)

async def handle_tick_exit(
    pool: asyncpg.Pool,
    pos: DBPosition,
    last_price: float,
    btc_uptrend: bool,
    runner: Optional[RunnerState],
    close_cooldown: Dict[str, float],
    runner_cache: Dict[str, Tuple[Optional[RunnerState], float]],
) -> None:
    """
    Order of checks:
      1) SL always (hard protection)
      2) If runner_enabled: trailing exit
      3) TP hit:
         - if BTC uptrend -> activate runner, do NOT close
         - else -> close TP
      4) TIME stop only if runner not enabled
    """

    # 1) SL: tick price <= sl (always)
    if last_price <= pos.sl:
        await maybe_close(pool, pos, "SL", pos.sl, close_cooldown)
        return

    # 2) Trailing runner exit (if enabled)
    if runner and runner.runner_enabled and runner.trail_price is not None:
        if last_price <= runner.trail_price:
            await maybe_close(pool, pos, "TRAIL", float(runner.trail_price), close_cooldown)
            return

        # update peak/trail only if new peak
        peak = runner.peak_price if runner.peak_price is not None else runner.activated_price
        if peak is None:
            peak = last_price

        if last_price > peak:
            new_peak = last_price
            new_trail = new_peak * (1.0 - (runner.trail_pct if runner.trail_pct else TRAIL_PCT))
            await db_update_runner_peak(pool, pos.symbol, new_peak, new_trail)

            # refresh runner cache immediately (so next ticks use new trail)
            runner_new = RunnerState(
                symbol=pos.symbol,
                runner_enabled=True,
                activated_at=runner.activated_at,
                activated_price=runner.activated_price,
                peak_price=new_peak,
                trail_price=new_trail,
                trail_pct=runner.trail_pct if runner.trail_pct else TRAIL_PCT,
            )
            runner_cache[pos.symbol] = (runner_new, asyncio.get_event_loop().time())

    # 3) TP hit logic
    if last_price >= pos.tp:
        if btc_uptrend:
            # activate runner if not already
            if not (runner and runner.runner_enabled):
                await db_upsert_runner_activate(pool, pos.symbol, last_price, TRAIL_PCT)
                logging.warning(
                    "🚀 RUNNER ACTIVATED %s | price=%.10f tp=%.10f | BTC uptrend | trail_pct=%.4f",
                    pos.symbol, last_price, pos.tp, TRAIL_PCT
                )
                # update local cache quickly
                runner_cache[pos.symbol] = (
                    RunnerState(
                        symbol=pos.symbol,
                        runner_enabled=True,
                        activated_at=utc_now(),
                        activated_price=last_price,
                        peak_price=last_price,
                        trail_price=last_price * (1.0 - TRAIL_PCT),
                        trail_pct=TRAIL_PCT,
                    ),
                    asyncio.get_event_loop().time(),
                )
            # do NOT close at TP when BTC uptrend
            return
        else:
            await maybe_close(pool, pos, "TP", pos.tp, close_cooldown)
            return

    # 4) TIME stop only if runner not enabled
    runner_enabled = bool(runner and runner.runner_enabled)
    if not runner_enabled:
        held_min = (utc_now() - pos.entry_time).total_seconds() / 60.0
        if held_min >= TIME_STOP_MIN:
            await maybe_close(pool, pos, "TIME", float(last_price), close_cooldown)
            return

# ==========================================================
# WS LOOP (dynamic subscribe to OPEN positions)
# ==========================================================
async def ws_loop_for_open_positions(pool: asyncpg.Pool) -> None:
    current: Set[str] = set()

    # per-symbol position cache: symbol -> (DBPosition|None, fetched_at_loop_time)
    pos_cache: Dict[str, Tuple[Optional[DBPosition], float]] = {}

    # per-symbol runner cache: symbol -> (RunnerState|None, fetched_at_loop_time)
    runner_cache: Dict[str, Tuple[Optional[RunnerState], float]] = {}

    # per-symbol cooldown for close request
    close_cooldown: Dict[str, float] = {}

    # btc trend cache
    btc_cache = BtcTrendCache(is_up=False, fetched_at=0.0)

    while True:
        try:
            open_syms = set(await db_get_open_symbols(pool))

            # 1) Když není nic OPEN, neotvírej WS (idle)
            if not open_syms:
                if current:
                    logging.info("No OPEN positions -> stopping WS subscription.")
                    current = set()
                    pos_cache.clear()
                    runner_cache.clear()
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
                        now_loop = asyncio.get_event_loop().time()

                        # Periodicky kontroluj změnu OPEN symbolů a případně reconnect
                        if now_loop - last_refresh >= REFRESH_OPEN_SYMBOLS_SEC:
                            new_set = set(await db_get_open_symbols(pool))
                            last_refresh = now_loop
                            if new_set != current:
                                logging.info("OPEN symbols changed -> reconnecting WS")
                                break

                        # BTC trend refresh (cached)
                        if now_loop - btc_cache.fetched_at >= BTC_TREND_CACHE_SEC:
                            closes = await db_get_candles_close(pool, BTC_SYMBOL, BTC_TREND_LOOKBACK)
                            is_up = is_btc_uptrend_from_closes(closes)
                            btc_cache = BtcTrendCache(is_up=is_up, fetched_at=now_loop)
                            logging.info("BTC trend (%s) uptrend=%s", BTC_SYMBOL, is_up)

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

                        # --- position cache throttle ---
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
                            # Position no longer OPEN -> cleanup runner state (best effort)
                            await db_delete_runner_state(pool, symbol)
                            runner_cache.pop(symbol, None)
                            continue

                        # --- runner cache throttle ---
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
                            btc_uptrend=btc_cache.is_up,
                            runner=runner,
                            close_cooldown=close_cooldown,
                            runner_cache=runner_cache,
                        )

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
    await db_ensure_runner_table(pool)
    logging.info(
        "EXIT service started | BTC_SYMBOL=%s MA=%d/%d TRAIL_PCT=%.4f TIME_STOP_MIN=%d",
        BTC_SYMBOL, BTC_MA_FAST, BTC_MA_SLOW, TRAIL_PCT, TIME_STOP_MIN
    )
    await ws_loop_for_open_positions(pool)

if __name__ == "__main__":
    asyncio.run(main())
