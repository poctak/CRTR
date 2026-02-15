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
MIN_BODY_PCT = env_float("MIN_BODY_PCT", 0.30)

# Adaptive absolute volume floor (per symbol)
DAILY_VOL_WINDOW_MIN = env_int("DAILY_VOL_WINDOW_MIN", 1440)          # 24h rolling window of 1m candles
MIN_QUOTE_VOL_FLOOR = env_float("MIN_QUOTE_VOL_FLOOR_USD", 20000.0)   # never go below this
MIN_QUOTE_VOL_DAILY_FRAC = env_float("MIN_QUOTE_VOL_DAILY_FRAC", 0.50) # 50% of rolling daily avg

# Risk/exit params (base target)
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

# Orderflow / orderbook exits
OBI_HOLD = env_float("OBI_HOLD", 0.12)          # if TP reached and OBI >= this => HOLD (ride)
OBI_EXIT = env_float("OBI_EXIT", -0.02)         # if riding and OBI <= this => EXIT
TRAIL_FROM_PEAK_PCT = env_float("TRAIL_FROM_PEAK_PCT", 0.008)  # 0.8% trailing from peak
MOVE_SL_TO_BE_ON_TP = env_int("MOVE_SL_TO_BE_ON_TP", 1)        # 1 => move SL to entry when TP first hit
MAX_SPREAD_PCT = env_float("MAX_SPREAD_PCT", 0.0035)           # 0.35% (guard on micro illiquid)

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
    risk: float
    tp_reached: bool = False
    peak: float = 0.0
    last_mid: float = 0.0
    monitor_task: Optional[asyncio.Task] = None

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

# Per-symbol latest OBI (only needed for open position monitor, but we store anyway)
latest_obi: Dict[str, Optional[float]] = defaultdict(lambda: None)

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
    global btc_ema, btc_close

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                tasks = [fetch_klines(session, s, TREND_TF, EMA_PERIOD + 5) for s in SYMBOLS]
                btc_task = fetch_klines(session, BTC_REGIME_SYMBOL, BTC_REGIME_TF, BTC_REGIME_EMA_PERIOD + 10)

                results = await asyncio.gather(*tasks, btc_task, return_exceptions=True)

                btc_res = results[-1]
                if not isinstance(btc_res, Exception):
                    btc_closes = [float(k[4]) for k in btc_res]
                    btc_close = btc_closes[-1]
                    btc_ema = calculate_ema(btc_closes, BTC_REGIME_EMA_PERIOD)
                else:
                    logging.error(f"BTC EMA fetch failed: {btc_res}")

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
    if btc_close is None or btc_ema is None:
        return False
    return btc_close > btc_ema

def symbol_trend_allows(symbol: str, price: float) -> bool:
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
# ORDERFLOW HELPERS
# ==========================================================

def safe_mid(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    if bid is None or ask is None:
        return None
    return (bid + ask) / 2.0

def spread_pct(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    if bid is None or ask is None:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    return (ask - bid) / mid

def compute_obi(bids, asks) -> Optional[float]:
    try:
        bid_vol = 0.0
        ask_vol = 0.0
        for p, q in bids:
            bid_vol += float(q)
        for p, q in asks:
            ask_vol += float(q)
        denom = bid_vol + ask_vol
        if denom <= 0:
            return None
        return (bid_vol - ask_vol) / denom
    except Exception:
        return None

# ==========================================================
# STRATEGY
# ==========================================================

def body_pct(o: float, c: float) -> float:
    return ((c - o) / o) * 100.0 if o > 0 else 0.0

def maybe_enter(symbol: str, candle: Candle) -> Optional[Tuple[float, float, float, float]]:
    """
    Simple volume breakout:
    - volume_quote > avg * VOLUME_MULT
    - volume_quote > adaptive MIN_QUOTE_VOL
    - candle body % > MIN_BODY_PCT
    - candle green (close > open)
    - trend filters pass
    """
    hist = vol_hist[symbol]
    day_hist = vol_hist_day[symbol]
    day_hist.append(candle.vq)

    if len(hist) < 10:
        hist.append(candle.vq)
        return None

    avg = sum(hist) / len(hist) if hist else 0.0
    hist.append(candle.vq)
    if avg <= 0:
        return None

    day_avg = (sum(day_hist) / len(day_hist)) if day_hist else 0.0
    min_quote = max(MIN_QUOTE_VOL_FLOOR, day_avg * MIN_QUOTE_VOL_DAILY_FRAC)

    vol_ok = candle.vq >= min_quote and candle.vq >= avg * VOLUME_MULT
    body_ok = body_pct(candle.o, candle.c) >= MIN_BODY_PCT
    green = candle.c > candle.o

    if not (vol_ok and body_ok and green):
        return None

    if not btc_regime_allows():
        return None
    if not symbol_trend_allows(symbol, candle.c):
        return None

    entry = candle.c
    sl = candle.l
    risk = entry - sl
    if risk <= 0:
        return None
    tp = entry + TP_R * risk
    return entry, sl, tp, risk

async def exit_position(pool: asyncpg.Pool, symbol: str, exit_time: datetime, exit_price: float, reason: str) -> None:
    global cooldown_until_epoch

    pos = positions.get(symbol)
    if not pos:
        return

    await insert_trade_log(pool, symbol, pos.entry_time, exit_time, pos.entry_price, exit_price, reason)

    # cooldown rules preserved
    if reason.startswith("SL") or reason.startswith("TRAIL_SL"):
        cooldown_until_epoch = exit_time.timestamp() + COOLDOWN_AFTER_SL_MIN * 60
        logging.warning(f"🏁 EXIT {reason} {symbol} | cooldown {COOLDOWN_AFTER_SL_MIN}m")
    elif reason.startswith("TIME"):
        cooldown_until_epoch = exit_time.timestamp() + COOLDOWN_AFTER_TIME_MIN * 60
        logging.info(f"🏁 EXIT {reason} {symbol} | cooldown {COOLDOWN_AFTER_TIME_MIN}m")
    else:
        logging.warning(f"🏁 EXIT {reason} {symbol}")

    # stop monitor task if running
    if pos.monitor_task and not pos.monitor_task.done():
        pos.monitor_task.cancel()

    positions.pop(symbol, None)

async def monitor_position_loop(pool: asyncpg.Pool, symbol: str) -> None:
    """
    Live exit monitor for an open position:
    - uses bookTicker for mid price
    - uses depth20@100ms for OBI
    - TP is NOT immediate: at TP touch, decide hold/exit based on OBI,
      then ride with trailing SL + OBI flip exit.
    """
    sym = symbol.lower()
    url = f"wss://stream.binance.com:9443/stream?streams={sym}@bookTicker/{sym}@depth20@100ms"

    bid: Optional[float] = None
    ask: Optional[float] = None
    obi: Optional[float] = None

    backoff = 1.0
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                logging.info(f"📡 Monitor connected {symbol}")
                backoff = 1.0

                async for message in ws:
                    pos = positions.get(symbol)
                    if not pos:
                        return  # position closed elsewhere

                    payload = json.loads(message)
                    data = payload.get("data", {})
                    event = data.get("e")

                    if event == "bookTicker":
                        bid = float(data.get("b"))
                        ask = float(data.get("a"))
                    elif event == "depthUpdate":
                        obi = compute_obi(data.get("b", []), data.get("a", []))
                        latest_obi[symbol] = obi

                    mid = safe_mid(bid, ask)
                    if mid is None:
                        continue

                    sp = spread_pct(bid, ask)
                    if sp is not None and sp > MAX_SPREAD_PCT:
                        # avoid exits/logic on garbage spreads (micro illiquid moments)
                        continue

                    pos.last_mid = mid
                    if pos.peak <= 0:
                        pos.peak = mid
                    if mid > pos.peak:
                        pos.peak = mid

                    now_dt = datetime.now(timezone.utc)

                    # TIME exit (based on wall time)
                    held_min = (now_dt - pos.entry_time).total_seconds() / 60.0
                    if held_min >= TIME_STOP_MIN:
                        await exit_position(pool, symbol, now_dt, mid, "TIME")
                        return

                    # SL (hard)
                    if mid <= pos.sl:
                        await exit_position(pool, symbol, now_dt, pos.sl, "SL")
                        return

                    # If TP not reached yet, handle TP touch
                    if not pos.tp_reached and mid >= pos.tp:
                        current_obi = obi if obi is not None else latest_obi.get(symbol)

                        # If OBI strong => HOLD and switch to ride mode
                        if current_obi is not None and current_obi >= OBI_HOLD:
                            pos.tp_reached = True
                            logging.warning(
                                f"🟡 TP touched {symbol} mid={mid:.6f} OBI={current_obi:.3f} => HOLD (ride)"
                            )
                            if MOVE_SL_TO_BE_ON_TP == 1 and pos.sl < pos.entry_price:
                                pos.sl = pos.entry_price
                                logging.info(f"🟡 {symbol} SL moved to BE {pos.sl:.6f}")
                        else:
                            # If OBI not strong => exit on TP
                            reason = f"TP_{TP_R}R"
                            if current_obi is not None:
                                reason = f"{reason}_OBI{current_obi:.3f}"
                            await exit_position(pool, symbol, now_dt, pos.tp, reason)
                            return

                    # Ride mode: trailing + OBI flip
                    if pos.tp_reached:
                        # trailing stop from peak
                        trail_sl = pos.peak * (1.0 - TRAIL_FROM_PEAK_PCT)
                        if trail_sl > pos.sl:
                            pos.sl = trail_sl

                        # OBI weakness exit
                        current_obi = obi if obi is not None else latest_obi.get(symbol)
                        if current_obi is not None and current_obi <= OBI_EXIT:
                            await exit_position(pool, symbol, now_dt, mid, f"OBI_WEAK({current_obi:.3f})")
                            return

        except asyncio.CancelledError:
            return
        except Exception as e:
            logging.error(f"Monitor WS error {symbol}: {e} | reconnect in {backoff:.1f}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.8, 30.0)

async def on_closed_candle(pool: asyncpg.Pool, symbol: str, candle: Candle) -> None:
    """
    Called for each closed 1m candle:
    - store to DB
    - if no open position and limiter allows, check entry
    """
    await insert_kline_1m(pool, symbol, candle)

    # ENTRY only (exits handled live by monitor task)
    if symbol not in positions:
        now_epoch = candle.t.timestamp()
        if can_enter(now_epoch):
            res = maybe_enter(symbol, candle)
            if res is not None:
                entry, sl, tp, risk = res
                pos = Position(
                    symbol=symbol,
                    entry_time=candle.t,
                    entry_price=entry,
                    sl=sl,
                    tp=tp,
                    risk=risk,
                    tp_reached=False,
                    peak=entry,
                    last_mid=entry,
                )
                positions[symbol] = pos
                register_entry(now_epoch)
                logging.warning(f"🟢 ENTER {symbol} entry={entry:.6f} SL={sl:.6f} TP={tp:.6f}")

                # start live monitor for this open position
                pos.monitor_task = asyncio.create_task(monitor_position_loop(pool, symbol))

# ==========================================================
# WEBSOCKET LOOP (1m klines for all symbols)
# ==========================================================

def ws_url(symbols: List[str]) -> str:
    streams = "/".join([f"{s.lower()}@kline_1m" for s in symbols])
    return BINANCE_WS + streams

async def ws_loop(pool: asyncpg.Pool) -> None:
    url = ws_url(SYMBOLS)

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                logging.info("Connected to Binance (klines)")
                async for message in ws:
                    payload = json.loads(message)
                    k = payload.get("data", {}).get("k", {})
                    if not k:
                        continue

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
            logging.error(f"WS error (klines): {e} | reconnect in 5s...")
            await asyncio.sleep(5)

# ==========================================================
# MAIN
# ==========================================================

async def main():
    pool = await init_db_pool()

    # Start EMA updater (trend + BTC regime)
    asyncio.create_task(ema_updater_loop())

    # Start websocket loop (klines)
    await ws_loop(pool)

if __name__ == "__main__":
    asyncio.run(main())
