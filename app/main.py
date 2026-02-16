# entry_service.py
import os
import json
import asyncio
import logging
from dataclasses import dataclass
from collections import deque, defaultdict
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional, Tuple

import aiohttp
import asyncpg
import websockets

# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ==========================================================
# ENV HELPERS
# ==========================================================

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v and v.strip() else default

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v and v.strip() else default

def parse_symbols() -> List[str]:
    raw = os.getenv("SYMBOLS", "").strip()
    if not raw:
        raise RuntimeError("SYMBOLS is empty. Set SYMBOLS in .env")
    return [s.strip().upper() for s in raw.split(",") if s.strip()]

# ==========================================================
# CONFIG
# ==========================================================

SYMBOLS = parse_symbols()

DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "pumpdb")
DB_USER = os.getenv("DB_USER", "pumpuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Signal params
LOOKBACK_MIN = env_int("LOOKBACK_MINUTES", 30)
VOLUME_MULT = env_float("VOLUME_MULTIPLIER", 2.8)
MIN_BODY_PCT = env_float("MIN_BODY_PCT", 0.30)
TP_R = env_float("TP_R", 2.0)

# Adaptive absolute volume (per symbol)
DAILY_VOL_WINDOW_MIN = env_int("DAILY_VOL_WINDOW_MIN", 1440)          # 24h (1m candles)
MIN_QUOTE_VOL_FLOOR = env_float("MIN_QUOTE_VOL_FLOOR_USD", 20000.0)   # never go below
MIN_QUOTE_VOL_DAILY_FRAC = env_float("MIN_QUOTE_VOL_DAILY_FRAC", 0.50) # e.g. 0.3-0.6

# Limiters
MAX_OPEN_POSITIONS = env_int("MAX_OPEN_POSITIONS", 1)    # global (across all symbols)
MAX_ENTRIES_PER_HOUR = env_int("MAX_ENTRIES_PER_HOUR", 5)

# Trend filters
EMA_PERIOD = env_int("EMA_PERIOD", 20)
TREND_TF = os.getenv("TREND_TF", "5m")

BTC_REGIME_SYMBOL = os.getenv("BTC_REGIME_SYMBOL", "BTCUSDT").upper()
BTC_REGIME_TF = os.getenv("BTC_REGIME_TF", "15m")
BTC_REGIME_EMA_PERIOD = env_int("BTC_REGIME_EMA_PERIOD", 50)

BINANCE_REST = "https://api.binance.com"
BINANCE_WS = "wss://stream.binance.com:9443/stream?streams="

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
    vq: float  # quote volume (USDT)

@dataclass
class Position:
    symbol: str
    entry_time: datetime
    entry_price: float
    sl: float
    tp: float

# ==========================================================
# STATE
# ==========================================================

# rolling quote volume history per symbol (short lookback, e.g. 30m)
vol_hist: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=LOOKBACK_MIN))

# rolling "daily" quote volume history per symbol (e.g. 24h)
vol_hist_day: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=DAILY_VOL_WINDOW_MIN))

# entry rate limit (local, in-memory)
entry_times: Deque[float] = deque()

# EMA values
ema_trend: Dict[str, float] = {}   # EMA on TREND_TF for each symbol
btc_ema: Optional[float] = None
btc_close: Optional[float] = None

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

async def insert_kline_1m(pool: asyncpg.Pool, symbol: str, candle: Candle) -> None:
    """
    Inserts into klines_1m table.
    Expected schema:
      time, symbol, open, high, low, close, volume_quote
    """
    sql = """
    INSERT INTO klines_1m(time, symbol, open, high, low, close, volume_quote)
    VALUES ($1,$2,$3,$4,$5,$6,$7)
    ON CONFLICT (time, symbol) DO NOTHING
    """
    async with pool.acquire() as conn:
        await conn.execute(sql, candle.t, symbol, candle.o, candle.h, candle.l, candle.c, candle.vq)

async def db_has_open_position(pool: asyncpg.Pool, symbol: str) -> bool:
    sql = "SELECT 1 FROM positions_open WHERE symbol=$1 AND status='OPEN' LIMIT 1"
    async with pool.acquire() as conn:
        r = await conn.fetchrow(sql, symbol)
    return r is not None

async def db_count_open_positions(pool: asyncpg.Pool) -> int:
    sql = "SELECT COUNT(*) AS n FROM positions_open WHERE status='OPEN'"
    async with pool.acquire() as conn:
        r = await conn.fetchrow(sql)
    return int(r["n"]) if r else 0

async def db_open_position(pool: asyncpg.Pool, pos: Position) -> bool:
    """
    Creates OPEN position in DB.
    Returns True if inserted, False if conflict (already exists).
    """
    sql = """
    INSERT INTO positions_open(symbol, entry_time, entry_price, sl, tp, status)
    VALUES ($1,$2,$3,$4,$5,'OPEN')
    ON CONFLICT (symbol) DO NOTHING
    """
    async with pool.acquire() as conn:
        res = await conn.execute(sql, pos.symbol, pos.entry_time, pos.entry_price, pos.sl, pos.tp)
    return res.endswith("1")  # "INSERT 0 1"

# ==========================================================
# EMA / TREND FILTER UPDATER
# ==========================================================

def calculate_ema(closes: List[float], period: int) -> float:
    k = 2 / (period + 1)
    ema = closes[0]
    for p in closes[1:]:
        ema = p * k + ema * (1 - k)
    return ema

async def fetch_klines(session: aiohttp.ClientSession, symbol: str, interval: str, limit: int) -> List[list]:
    url = f"{BINANCE_REST}/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    async with session.get(url, timeout=10) as resp:
        resp.raise_for_status()
        return await resp.json()

async def ema_updater_loop() -> None:
    """Every 60s refresh EMA for all symbols + BTC regime EMA."""
    global btc_ema, btc_close

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                tasks = [fetch_klines(session, s, TREND_TF, EMA_PERIOD + 5) for s in SYMBOLS]
                btc_task = fetch_klines(session, BTC_REGIME_SYMBOL, BTC_REGIME_TF, BTC_REGIME_EMA_PERIOD + 10)

                results = await asyncio.gather(*tasks, btc_task, return_exceptions=True)

                # BTC result is last
                btc_res = results[-1]
                if not isinstance(btc_res, Exception):
                    btc_closes = [float(k[4]) for k in btc_res]
                    btc_close = btc_closes[-1]
                    btc_ema = calculate_ema(btc_closes, BTC_REGIME_EMA_PERIOD)
                else:
                    logging.error(f"BTC EMA fetch failed: {btc_res}")

                # symbols
                for idx, s in enumerate(SYMBOLS):
                    r = results[idx]
                    if isinstance(r, Exception):
                        continue
                    closes = [float(k[4]) for k in r]
                    ema_trend[s] = calculate_ema(closes, EMA_PERIOD)

            if btc_close is not None and btc_ema is not None:
                regime = "RISK-ON" if btc_close > btc_ema else "RISK-OFF"
                logging.info(f"📈 EMA updated | BTC {regime} (close={btc_close:.2f} ema={btc_ema:.2f})")
            else:
                logging.info("📈 EMA updated")

        except Exception as e:
            logging.error(f"EMA updater error: {e}")

        await asyncio.sleep(60)

def btc_regime_allows() -> bool:
    """Allow longs only when BTC close > BTC EMA."""
    if btc_close is None or btc_ema is None:
        return False  # conservative until warmed up
    return btc_close > btc_ema

def symbol_trend_allows(symbol: str, price: float) -> bool:
    """Allow longs only when price > EMA on TREND_TF."""
    ema = ema_trend.get(symbol)
    if ema is None:
        return False
    return price > ema

# ==========================================================
# LIMITERS
# ==========================================================

def prune_entry_times(now_epoch: float) -> None:
    cutoff = now_epoch - 3600
    while entry_times and entry_times[0] < cutoff:
        entry_times.popleft()

async def can_enter(pool: asyncpg.Pool, now_epoch: float) -> bool:
    prune_entry_times(now_epoch)
    if len(entry_times) >= MAX_ENTRIES_PER_HOUR:
        return False

    open_count = await db_count_open_positions(pool)
    if open_count >= MAX_OPEN_POSITIONS:
        return False

    return True

def register_entry(now_epoch: float) -> None:
    entry_times.append(now_epoch)

# ==========================================================
# STRATEGY
# ==========================================================

def body_pct(o: float, c: float) -> float:
    return ((c - o) / o) * 100.0 if o > 0 else 0.0

def maybe_enter(symbol: str, candle: Candle) -> Optional[Tuple[float, float, float]]:
    """
    Simple volume breakout:
    - volume_quote > avg * VOLUME_MULT
    - volume_quote > adaptive_min_quote (based on rolling daily avg; never below MIN_QUOTE_VOL_FLOOR)
    - candle body % > MIN_BODY_PCT
    - candle green (close > open)
    - trend filters pass (BTC regime + symbol EMA)
    """
    hist = vol_hist[symbol]
    day_hist = vol_hist_day[symbol]
    day_hist.append(candle.vq)

    # Warm-up for short lookback avg
    if len(hist) < 10:
        hist.append(candle.vq)
        return None

    avg = sum(hist) / len(hist) if hist else 0.0
    hist.append(candle.vq)
    if avg <= 0:
        return None

    # Adaptive absolute volume threshold (per symbol)
    day_avg = (sum(day_hist) / len(day_hist)) if day_hist else 0.0
    min_quote = max(MIN_QUOTE_VOL_FLOOR, day_avg * MIN_QUOTE_VOL_DAILY_FRAC)

    vol_ok = candle.vq >= min_quote and candle.vq >= avg * VOLUME_MULT
    body_ok = body_pct(candle.o, candle.c) >= MIN_BODY_PCT
    green = candle.c > candle.o

    if not (vol_ok and body_ok and green):
        return None

    # Expert filters
    if not btc_regime_allows():
        return None
    if not symbol_trend_allows(symbol, candle.c):
        return None

    # Entry and risk
    entry = candle.c
    sl = candle.l
    risk = entry - sl
    if risk <= 0:
        return None
    tp = entry + TP_R * risk
    return entry, sl, tp

# ==========================================================
# CANDLE HANDLER
# ==========================================================

async def on_closed_candle(pool: asyncpg.Pool, symbol: str, candle: Candle) -> None:
    """
    Called for each closed 1m candle:
    - store to DB
    - generate entry if conditions pass and DB says no OPEN position for this symbol
    """
    # 1) save candle to DB
    await insert_kline_1m(pool, symbol, candle)

    # 2) entry only if no OPEN position in DB for this symbol
    if await db_has_open_position(pool, symbol):
        return

    now_epoch = candle.t.timestamp()
    if not await can_enter(pool, now_epoch):
        return

    res = maybe_enter(symbol, candle)
    if res is None:
        return

    entry, sl, tp = res
    pos = Position(symbol=symbol, entry_time=candle.t, entry_price=entry, sl=sl, tp=tp)

    inserted = await db_open_position(pool, pos)
    if inserted:
        register_entry(now_epoch)
        logging.warning(f"🟢 ENTER {symbol} entry={entry:.6f} SL={sl:.6f} TP={tp:.6f}")

# ==========================================================
# WEBSOCKET LOOP
# ==========================================================

def ws_url(symbols: List[str]) -> str:
    streams = "/".join([f"{s.lower()}@kline_1m" for s in symbols])
    return BINANCE_WS + streams

async def ws_loop(pool: asyncpg.Pool) -> None:
    url = ws_url(SYMBOLS)

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                logging.info("Connected to Binance (ENTRY service)")
                async for message in ws:
                    payload = json.loads(message)
                    k = payload.get("data", {}).get("k", {})
                    if not k:
                        continue

                    # Only closed candles
                    if not bool(k.get("x")):
                        continue

                    symbol = (k.get("s") or "").upper()
                    if symbol not in SYMBOLS:
                        continue

                    candle = Candle(
                        t=datetime.fromtimestamp(int(k["t"]) / 1000.0, tz=timezone.utc),
                        o=float(k["o"]),
                        h=float(k["h"]),
                        l=float(k["l"]),
                        c=float(k["c"]),
                        vq=float(k["q"]),
                    )

                    await on_closed_candle(pool, symbol, candle)

        except Exception as e:
            logging.error(f"WS error: {e} | reconnect in 5s...")
            await asyncio.sleep(5)

# ==========================================================
# MAIN
# ==========================================================

async def main():
    pool = await init_db_pool()

    # Start EMA updater (trend + BTC regime)
    asyncio.create_task(ema_updater_loop())

    # Start websocket loop
    await ws_loop(pool)

if __name__ == "__main__":
    asyncio.run(main())
