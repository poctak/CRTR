import os
import json
import time
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
# MIN_QUOTE_VOL = env_float("MIN_QUOTE_VOL_USD", 150000.0)
MIN_BODY_PCT = env_float("MIN_BODY_PCT", 0.30)
# Adaptive absolute volume floor (per symbol)
DAILY_VOL_WINDOW_MIN = env_int("DAILY_VOL_WINDOW_MIN", 1440)          # 24h rolling window of 1m candles
MIN_QUOTE_VOL_FLOOR = env_float("MIN_QUOTE_VOL_FLOOR_USD", 20000.0)   # never go below this
MIN_QUOTE_VOL_DAILY_FRAC = env_float("MIN_QUOTE_VOL_DAILY_FRAC", 0.50) # 50% of rolling daily avg


# Risk/exit params
TP_R = env_float("TP_R", 2.0)
TIME_STOP_MIN = env_int("TIME_STOP_MINUTES", 7)
MAX_OPEN_POSITIONS = env_int("MAX_OPEN_POSITIONS", 1)
MAX_ENTRIES_PER_HOUR = env_int("MAX_ENTRIES_PER_HOUR", 5)
COOLDOWN_AFTER_SL_MIN = env_int("COOLDOWN_AFTER_SL_MINUTES", 15)
COOLDOWN_AFTER_TIME_MIN = env_int("COOLDOWN_AFTER_TIME_MINUTES", 5)

# Costs (used only for PnL logging here)
FEE_ROUNDTRIP_PCT = env_float("FEE_ROUNDTRIP_PCT", 0.15)

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

# rolling quote volume history per symbol (to compute average)
vol_hist: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=LOOKBACK_MIN))
# rolling 24h quote volume history per symbol (for adaptive MIN_QUOTE_VOL)
vol_hist_day: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=DAILY_VOL_WINDOW_MIN))


# open positions (global limit)
positions: Dict[str, Position] = {}

# entry rate limit
entry_times: Deque[float] = deque()
cooldown_until_epoch: float = 0.0

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
    Expected schema (your table):
      time, symbol, open, high, low, close, volume_quote
    """
    sql = """
    INSERT INTO klines_1m(time, symbol, open, high, low, close, volume_quote)
    VALUES ($1,$2,$3,$4,$5,$6,$7)
    ON CONFLICT (time, symbol) DO NOTHING
    """
    async with pool.acquire() as conn:
        await conn.execute(sql, candle.t, symbol, candle.o, candle.h, candle.l, candle.c, candle.vq)

async def insert_trade_log(
    pool: asyncpg.Pool,
    symbol: str,
    entry_time: datetime,
    exit_time: datetime,
    entry_price: float,
    exit_price: float,
    reason: str,
) -> None:
    """
    Inserts into trade_log table.
    This matches the simplified trade_log you created earlier:
      symbol, entry_time, exit_time, entry_price, exit_price, reason, pnl_pct, created_at
    """
    gross_pct = (exit_price - entry_price) / entry_price * 100.0 if entry_price > 0 else 0.0
    net_pct = gross_pct - FEE_ROUNDTRIP_PCT

    sql = """
    INSERT INTO trade_log(symbol, entry_time, exit_time, entry_price, exit_price, reason, pnl_pct)
    VALUES ($1,$2,$3,$4,$5,$6,$7)
    """
    async with pool.acquire() as conn:
        await conn.execute(sql, symbol, entry_time, exit_time, entry_price, exit_price, reason, net_pct)

# ==========================================================
# EMA / TREND FILTER UPDATER
# ==========================================================

def calculate_ema(closes: List[float], period: int) -> float:
    """Standard EMA calculation."""
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
                # fetch trend TF klines for all symbols
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

def can_enter(now_epoch: float) -> bool:
    global cooldown_until_epoch

    if now_epoch < cooldown_until_epoch:
        return False

    if len(positions) >= MAX_OPEN_POSITIONS:
        return False

    prune_entry_times(now_epoch)
    if len(entry_times) >= MAX_ENTRIES_PER_HOUR:
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
    - volume_quote > MIN_QUOTE_VOL
    - candle body % > MIN_BODY_PCT
    - candle green (close > open)
    - trend filters pass
    """
    hist = vol_hist[symbol]
    # update rolling "daily" volume history (24h window)
    day_hist = vol_hist_day[symbol]
    day_hist.append(candle.vq)

    if len(hist) < 10:
        hist.append(candle.vq)
        return None

    avg = sum(hist) / len(hist) if hist else 0.0
    hist.append(candle.vq)
    if avg <= 0:
        return None

    # adaptive absolute volume threshold based on rolling daily avg
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

async def manage_position(pool: asyncpg.Pool, symbol: str, candle: Candle) -> None:
    """
    Exit logic:
    - SL if candle low <= SL
    - TP if candle high >= TP
    - TIME exit after TIME_STOP_MIN
    Cooldown after SL/TIME.
    Writes to trade_log.
    """
    global cooldown_until_epoch

    pos = positions.get(symbol)
    if not pos:
        return

    # SL
    if candle.l <= pos.sl:
        await insert_trade_log(pool, symbol, pos.entry_time, candle.t, pos.entry_price, pos.sl, "SL")
        positions.pop(symbol, None)
        cooldown_until_epoch = candle.t.timestamp() + COOLDOWN_AFTER_SL_MIN * 60
        logging.warning(f"🏁 EXIT SL {symbol} | cooldown {COOLDOWN_AFTER_SL_MIN}m")
        return

    # TP
    if candle.h >= pos.tp:
        await insert_trade_log(pool, symbol, pos.entry_time, candle.t, pos.entry_price, pos.tp, f"TP_{TP_R}R")
        positions.pop(symbol, None)
        logging.warning(f"🏁 EXIT TP {symbol}")
        return

    # TIME
    held_min = (candle.t - pos.entry_time).total_seconds() / 60.0
    if held_min >= TIME_STOP_MIN:
        await insert_trade_log(pool, symbol, pos.entry_time, candle.t, pos.entry_price, candle.c, "TIME")
        positions.pop(symbol, None)
        cooldown_until_epoch = candle.t.timestamp() + COOLDOWN_AFTER_TIME_MIN * 60
        logging.info(f"🏁 EXIT TIME {symbol} | cooldown {COOLDOWN_AFTER_TIME_MIN}m")

async def on_closed_candle(pool: asyncpg.Pool, symbol: str, candle: Candle) -> None:
    """
    Called for each closed 1m candle:
    - store to DB
    - manage existing position
    - generate entry if conditions pass
    """
    # 1) save candle to DB
    await insert_kline_1m(pool, symbol, candle)

    # 2) manage exits
    await manage_position(pool, symbol, candle)

    # 3) if no position in this symbol and global limiter allows, check entry
    if symbol not in positions:
        now_epoch = candle.t.timestamp()
        if can_enter(now_epoch):
            res = maybe_enter(symbol, candle)
            if res is not None:
                entry, sl, tp = res
                positions[symbol] = Position(symbol=symbol, entry_time=candle.t, entry_price=entry, sl=sl, tp=tp)
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
                logging.info("Connected to Binance")
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
