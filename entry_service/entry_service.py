# entry_service.py
import os
import json
import asyncio
import logging
import time
from dataclasses import dataclass
from collections import deque, defaultdict
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional, Tuple

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
    v = os.getenv(name)
    return int(v) if v and v.strip() else default

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v and v.strip() else default

def parse_symbols() -> List[str]:
    raw = os.getenv("SYMBOLS", "").strip()
    if not raw:
        raise RuntimeError("SYMBOLS is empty")
    return [s.strip().upper() for s in raw.split(",") if s.strip()]

# ==========================================================
# CONFIG
# ==========================================================

SYMBOLS = parse_symbols()

DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "pumpdb")
DB_USER = os.getenv("DB_USER", "pumpuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

MAX_OPEN_POSITIONS = env_int("MAX_OPEN_POSITIONS", 1)
MAX_ENTRIES_PER_HOUR = env_int("MAX_ENTRIES_PER_HOUR", 5)
TP_R = env_float("TP_R", 2.0)

# Realtime params
RT_WINDOW_SEC = env_int("RT_WINDOW_SEC", 10)
RT_MIN_QUOTE_VOL_10S = env_float("RT_MIN_QUOTE_VOL_10S", 150000.0)
RT_MIN_PRICE_CHANGE_10S = env_float("RT_MIN_PRICE_CHANGE_10S", 0.004)  # 0.4%
RT_MIN_BUY_RATIO_10S = env_float("RT_MIN_BUY_RATIO_10S", 0.65)
RT_MIN_IMBALANCE = env_float("RT_MIN_IMBALANCE", 1.4)
RT_COOLDOWN_SEC = env_int("RT_COOLDOWN_SEC", 30)
RT_SL_PCT = env_float("RT_SL_PCT", 0.003)  # 0.3% stop for early entries

DEPTH_TOP_N = env_int("DEPTH_TOP_N", 10)
RT_NEAR_WALL_PCT = env_float("RT_NEAR_WALL_PCT", 0.0015)  # 0.15%
RT_WALL_MULT = env_float("RT_WALL_MULT", 3.0)

# Logging throttle
METRICS_LOG_EVERY_SEC = env_float("METRICS_LOG_EVERY_SEC", 1.0)

BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream?streams="

# ==========================================================
# MODELS
# ==========================================================

@dataclass
class Position:
    symbol: str
    entry_time: datetime
    entry_price: float
    sl: float
    tp: float

@dataclass
class TradePoint:
    ts_ms: int
    price: float
    quote: float
    taker_buy: bool

@dataclass
class BookState:
    ts_ms: int
    bids: List[Tuple[float, float]]
    asks: List[Tuple[float, float]]

# ==========================================================
# STATE
# ==========================================================

entry_times: Deque[float] = deque()

rt_trades: Dict[str, Deque[TradePoint]] = defaultdict(deque)
rt_book: Dict[str, Optional[BookState]] = defaultdict(lambda: None)
rt_last_price: Dict[str, float] = {}
rt_last_signal_ts: Dict[str, float] = defaultdict(lambda: 0.0)

# per-symbol log throttle
rt_last_metrics_log_ts: Dict[str, float] = defaultdict(lambda: 0.0)

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
    sql = """
    INSERT INTO positions_open(symbol, entry_time, entry_price, sl, tp, status)
    VALUES ($1,$2,$3,$4,$5,'OPEN')
    ON CONFLICT (symbol) DO NOTHING
    """
    async with pool.acquire() as conn:
        res = await conn.execute(sql, pos.symbol, pos.entry_time, pos.entry_price, pos.sl, pos.tp)
    return res.endswith("1")

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

    if await db_count_open_positions(pool) >= MAX_OPEN_POSITIONS:
        return False

    return True

def register_entry(now_epoch: float) -> None:
    entry_times.append(now_epoch)

# ==========================================================
# REALTIME METRICS
# ==========================================================

def _prune_trades(sym: str, now_ms: int) -> None:
    cutoff = now_ms - RT_WINDOW_SEC * 1000
    dq = rt_trades[sym]
    while dq and dq[0].ts_ms < cutoff:
        dq.popleft()

def _near_sell_wall(sym: str, price: float) -> bool:
    book = rt_book[sym]
    if book is None or not book.asks or price <= 0:
        return False

    asks = book.asks[:DEPTH_TOP_N]
    levels = [p * q for p, q in asks if q > 0]
    if not levels:
        return False

    avg_level = sum(levels) / len(levels)
    max_level = max(levels)

    best_ask = asks[0][0]
    dist = (best_ask - price) / price  # relative distance

    return (dist <= RT_NEAR_WALL_PCT) and (avg_level > 0) and (max_level >= RT_WALL_MULT * avg_level)

def compute_metrics(sym: str) -> Optional[dict]:
    dq = rt_trades[sym]
    book = rt_book[sym]
    price = rt_last_price.get(sym)

    if not dq or book is None or price is None:
        return None

    total_quote = sum(t.quote for t in dq)
    if total_quote <= 0:
        return None

    taker_buy_quote = sum(t.quote for t in dq if t.taker_buy)
    buy_ratio = taker_buy_quote / total_quote

    p0 = dq[0].price
    price_change = (price - p0) / p0 if p0 > 0 else 0.0

    bids = book.bids[:DEPTH_TOP_N]
    asks = book.asks[:DEPTH_TOP_N]

    bid_notional = sum(p * q for p, q in bids)
    ask_notional = sum(p * q for p, q in asks)
    imbalance = (bid_notional / ask_notional) if ask_notional > 0 else 999.0

    near_wall = _near_sell_wall(sym, price)

    return {
        "price": price,
        "vol_10s": total_quote,
        "buy_ratio": buy_ratio,
        "price_change": price_change,
        "imbalance": imbalance,
        "near_sell_wall": near_wall,
    }

# ==========================================================
# REALTIME ENTRY
# ==========================================================

async def maybe_enter(pool: asyncpg.Pool, sym: str, m: dict, now_ts: float) -> None:
    if now_ts - rt_last_signal_ts[sym] < RT_COOLDOWN_SEC:
        return

    if await db_has_open_position(pool, sym):
        return

    if not await can_enter(pool, now_ts):
        return

    if not (
        m["vol_10s"] >= RT_MIN_QUOTE_VOL_10S
        and m["price_change"] >= RT_MIN_PRICE_CHANGE_10S
        and m["buy_ratio"] >= RT_MIN_BUY_RATIO_10S
        and m["imbalance"] >= RT_MIN_IMBALANCE
        and not m["near_sell_wall"]
    ):
        return

    entry = m["price"]
    sl = entry * (1.0 - RT_SL_PCT)
    risk = entry - sl
    if risk <= 0:
        return
    tp = entry + TP_R * risk

    pos = Position(
        symbol=sym,
        entry_time=datetime.fromtimestamp(now_ts, tz=timezone.utc),
        entry_price=entry,
        sl=sl,
        tp=tp,
    )

    inserted = await db_open_position(pool, pos)
    if inserted:
        register_entry(now_ts)
        rt_last_signal_ts[sym] = now_ts
        logging.warning(
            f"⚡ ENTER_RT {sym} "
            f"price={entry:.6f} SL={sl:.6f} TP={tp:.6f} "
            f"Δ{RT_WINDOW_SEC}s={m['price_change']*100:.2f}% "
            f"vol={m['vol_10s']:.0f} buy={m['buy_ratio']:.2f} "
            f"imbal={m['imbalance']:.2f} wall={'Y' if m['near_sell_wall'] else 'N'}"
        )

# ==========================================================
# WEBSOCKET
# ==========================================================

def ws_url(symbols: List[str]) -> str:
    streams = []
    for s in symbols:
        sym = s.lower()
        streams.append(f"{sym}@aggTrade")
        streams.append(f"{sym}@depth@250ms")
    return BINANCE_WS_BASE + "/".join(streams)

def should_log_metrics(sym: str, now_ts: float) -> bool:
    last = rt_last_metrics_log_ts[sym]
    if now_ts - last >= METRICS_LOG_EVERY_SEC:
        rt_last_metrics_log_ts[sym] = now_ts
        return True
    return False

async def realtime_loop(pool: asyncpg.Pool) -> None:
    url = ws_url(SYMBOLS)

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                logging.info("Connected to Binance (REALTIME ENTRY ONLY + throttled metrics logs)")

                async for msg in ws:
                    payload = json.loads(msg)
                    stream = payload.get("stream", "").lower()
                    data = payload.get("data", {})

                    now_ms = int(time.time() * 1000)
                    now_ts = now_ms / 1000.0

                    if stream.endswith("@aggtrade"):
                        sym = data.get("s", "").upper()
                        if sym not in SYMBOLS:
                            continue

                        price = float(data["p"])
                        qty = float(data["q"])
                        quote = price * qty
                        ts_ms = int(data["T"])
                        maker = bool(data["m"])
                        taker_buy = (maker is False)

                        rt_last_price[sym] = price
                        rt_trades[sym].append(TradePoint(ts_ms=ts_ms, price=price, quote=quote, taker_buy=taker_buy))
                        _prune_trades(sym, now_ms)

                        m = compute_metrics(sym)
                        if m:
                            if should_log_metrics(sym, now_ts):
                                logging.info(
                                    f"METRICS {sym} "
                                    f"price={m['price']:.6f} "
                                    f"Δ{RT_WINDOW_SEC}s={m['price_change']*100:.2f}% "
                                    f"vol={m['vol_10s']:.0f} "
                                    f"buy={m['buy_ratio']:.2f} "
                                    f"imbal={m['imbalance']:.2f} "
                                    f"wall={'Y' if m['near_sell_wall'] else 'N'}"
                                )
                            await maybe_enter(pool, sym, m, now_ts)

                    elif "@depth" in stream:
                        sym = data.get("s", "").upper()
                        if sym not in SYMBOLS:
                            continue

                        bids = [(float(p), float(q)) for p, q in data.get("b", []) if float(q) > 0]
                        asks = [(float(p), float(q)) for p, q in data.get("a", []) if float(q) > 0]

                        bids.sort(key=lambda x: x[0], reverse=True)
                        asks.sort(key=lambda x: x[0])

                        rt_book[sym] = BookState(
                            ts_ms=int(data.get("E", now_ms)),
                            bids=bids,
                            asks=asks,
                        )

        except Exception as e:
            logging.error(f"WS error: {e} | reconnecting in 5s")
            await asyncio.sleep(5)

# ==========================================================
# MAIN
# ==========================================================

async def main():
    pool = await init_db_pool()
    await realtime_loop(pool)

if __name__ == "__main__":
    asyncio.run(main())
