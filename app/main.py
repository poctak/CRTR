import os
import json
import time
import asyncio
import logging
from dataclasses import dataclass
from collections import deque, defaultdict
from datetime import datetime, timezone
from typing import Deque, Dict, List, Tuple, Optional

import aiohttp
import asyncpg
import websockets

# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ==========================================================
# ENV HELPERS
# ==========================================================

def env_int(name: str, default: int) -> int:
    """Načte int z ENV proměnné."""
    v = os.getenv(name)
    return int(v) if v and v.strip() else default

def env_float(name: str, default: float) -> float:
    """Načte float z ENV proměnné."""
    v = os.getenv(name)
    return float(v) if v and v.strip() else default

def parse_symbols() -> List[str]:
    """Načte seznam symbolů z ENV (oddělené čárkou)."""
    raw = os.getenv("SYMBOLS", "")
    return [s.strip().upper() for s in raw.split(",") if s.strip()]

# ==========================================================
# KONFIGURACE Z ENV
# ==========================================================

SYMBOLS = parse_symbols()

DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "pumpdb")
DB_USER = os.getenv("DB_USER", "pumpuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pumppassword")

LOOKBACK_MIN = env_int("LOOKBACK_MINUTES", 30)
VOLUME_MULT = env_float("VOLUME_MULTIPLIER", 2.8)
MIN_QUOTE_VOL = env_float("MIN_QUOTE_VOL_USD", 150000.0)

TP_R = env_float("TP_R", 2.0)
TIME_STOP_MIN = env_int("TIME_STOP_MINUTES", 7)

EMA_PERIOD = env_int("EMA_PERIOD", 20)
TREND_TF = os.getenv("TREND_TF", "5m")

BTC_REGIME_SYMBOL = os.getenv("BTC_REGIME_SYMBOL", "BTCUSDT")
BTC_REGIME_TF = os.getenv("BTC_REGIME_TF", "15m")
BTC_REGIME_EMA_PERIOD = env_int("BTC_REGIME_EMA_PERIOD", 50)

MAX_OPEN_POSITIONS = env_int("MAX_OPEN_POSITIONS", 1)
COOLDOWN_AFTER_SL_MIN = env_int("COOLDOWN_AFTER_SL_MINUTES", 15)

FEE_ROUNDTRIP_PCT = env_float("FEE_ROUNDTRIP_PCT", 0.15)

# ==========================================================
# DATA MODELY
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
class Position:
    symbol: str
    entry_time: datetime
    entry_price: float
    sl: float
    tp: float

# ==========================================================
# STAV STRATEGIE
# ==========================================================

vol_hist: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=LOOKBACK_MIN))
positions: Dict[str, Position] = {}
cooldown_until = 0

ema_5m: Dict[str, float] = {}
btc_ema: Optional[float] = None
btc_close: Optional[float] = None

# ==========================================================
# EMA VÝPOČET
# ==========================================================

def calculate_ema(closes: List[float], period: int) -> float:
    """Vypočítá EMA z listu cen."""
    k = 2 / (period + 1)
    ema = closes[0]
    for p in closes[1:]:
        ema = p * k + ema * (1 - k)
    return ema

# ==========================================================
# TREND FILTR
# ==========================================================

def trend_allows(symbol: str, price: float) -> bool:
    """Kontrola EMA trendu."""
    if symbol not in ema_5m:
        return False
    return price > ema_5m[symbol]

def btc_regime_allows() -> bool:
    """Obchodujeme jen když BTC je nad EMA."""
    if btc_ema is None or btc_close is None:
        return False
    return btc_close > btc_ema

# ==========================================================
# DATABASE
# ==========================================================

async def init_db():
    return await asyncpg.create_pool(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

# ==========================================================
# STRATEGIE LOGIKA
# ==========================================================

def create_signal(symbol: str, candle: Candle):
    """Detekce volume breakout."""
    history = vol_hist[symbol]

    if len(history) < 10:
        history.append(candle.vq)
        return

    avg_vol = sum(history) / len(history)
    history.append(candle.vq)

    if candle.vq > avg_vol * VOLUME_MULT and candle.vq > MIN_QUOTE_VOL:
        if symbol not in positions:
            if trend_allows(symbol, candle.c) and btc_regime_allows():
                enter_trade(symbol, candle)

def enter_trade(symbol: str, candle: Candle):
    """Otevření pozice."""
    global cooldown_until

    if time.time() < cooldown_until:
        return

    if len(positions) >= MAX_OPEN_POSITIONS:
        return

    entry = candle.c
    sl = candle.l
    risk = entry - sl
    tp = entry + TP_R * risk

    positions[symbol] = Position(
        symbol=symbol,
        entry_time=candle.t,
        entry_price=entry,
        sl=sl,
        tp=tp
    )

    logging.warning(f"ENTER {symbol} entry={entry} SL={sl} TP={tp}")

def manage_position(symbol: str, candle: Candle):
    """Správa otevřené pozice."""
    global cooldown_until

    if symbol not in positions:
        return

    pos = positions[symbol]

    if candle.l <= pos.sl:
        logging.warning(f"STOP LOSS {symbol}")
        cooldown_until = time.time() + COOLDOWN_AFTER_SL_MIN * 60
        del positions[symbol]
        return

    if candle.h >= pos.tp:
        logging.warning(f"TAKE PROFIT {symbol}")
        del positions[symbol]
        return

    held_minutes = (candle.t - pos.entry_time).total_seconds() / 60
    if held_minutes >= TIME_STOP_MIN:
        logging.warning(f"TIME EXIT {symbol}")
        del positions[symbol]

# ==========================================================
# WEBSOCKET LOOP
# ==========================================================

async def ws_loop():
    url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(
        [f"{s.lower()}@kline_1m" for s in SYMBOLS]
    )

    async with websockets.connect(url) as ws:
        logging.info("Connected to Binance")

        async for message in ws:
            data = json.loads(message)
            k = data["data"]["k"]

            if not k["x"]:
                continue  # jen uzavřené svíčky

            symbol = k["s"]
            candle = Candle(
                t=datetime.fromtimestamp(k["t"]/1000, tz=timezone.utc),
                o=float(k["o"]),
                h=float(k["h"]),
                l=float(k["l"]),
                c=float(k["c"]),
                vq=float(k["q"])
            )

            manage_position(symbol, candle)
            create_signal(symbol, candle)

# ==========================================================
# MAIN
# ==========================================================

if __name__ == "__main__":
    asyncio.run(ws_loop())

