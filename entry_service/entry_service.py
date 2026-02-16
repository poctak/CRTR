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
    syms = [s.strip().upper() for s in raw.split(",") if s.strip()]
    if not syms:
        raise RuntimeError("SYMBOLS is empty after parsing")
    return syms

# ==========================================================
# CONFIG
# ==========================================================

SYMBOLS = parse_symbols()
if len(SYMBOLS) != 1:
    logging.warning("DEBUG build expects 1 symbol for full verbose logs. Current SYMBOLS=%s", SYMBOLS)

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
RT_SL_PCT = env_float("RT_SL_PCT", 0.003)  # 0.3%

DEPTH_TOP_N = env_int("DEPTH_TOP_N", 10)
RT_NEAR_WALL_PCT = env_float("RT_NEAR_WALL_PCT", 0.0015)  # 0.15%
RT_WALL_MULT = env_float("RT_WALL_MULT", 3.0)

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
        logging.info("LIMITER | MAX_ENTRIES_PER_HOUR reached: %d", len(entry_times))
        return False

    open_count = await db_count_open_positions(pool)
    if open_count >= MAX_OPEN_POSITIONS:
        logging.info("LIMITER | MAX_OPEN_POSITIONS reached: %d", open_count)
        return False

    return True

def register_entry(now_epoch: float) -> None:
    entry_times.append(now_epoch)
    logging.info("LIMITER | register_entry | entries_last_hour=%d", len(entry_times))

# ==========================================================
# REALTIME METRICS
# ==========================================================

def _prune_trades(sym: str, now_ms: int) -> None:
    dq = rt_trades[sym]
    before = len(dq)
    cutoff = now_ms - RT_WINDOW_SEC * 1000
    while dq and dq[0].ts_ms < cutoff:
        dq.popleft()
    after = len(dq)
    if before != after:
        logging.info("PRUNE %s | before=%d after=%d cutoff_ms=%d", sym, before, after, cutoff)

def _near_sell_wall(sym: str, price: float) -> bool:
    book = rt_book[sym]
    if book is None or not book.asks or price <= 0:
        logging.info("WALL %s | cannot check (book_missing=%s asks_empty=%s price=%.6f)",
                     sym, book is None, (book is not None and not book.asks), price if price else -1.0)
        return False

    asks = book.asks[:DEPTH_TOP_N]
    levels = [p * q for p, q in asks if q > 0]
    if not levels:
        logging.info("WALL %s | asks levels empty after filtering", sym)
        return False

    avg_level = sum(levels) / len(levels)
    max_level = max(levels)

    best_ask = asks[0][0]
    dist = (best_ask - price) / price  # relative distance

    near = dist <= RT_NEAR_WALL_PCT
    big = (avg_level > 0) and (max_level >= RT_WALL_MULT * avg_level)

    logging.info(
        "WALL %s | best_ask=%.6f price=%.6f dist=%.5f%% near=%s avg_lvl=%.2f max_lvl=%.2f big=%s",
        sym, best_ask, price, dist * 100.0, near, avg_level, max_level, big
    )

    return near and big

def compute_metrics(sym: str, now_ts: float) -> Optional[dict]:
    dq = rt_trades[sym]
    book = rt_book[sym]
    price = rt_last_price.get(sym)

    logging.info(
        "COMPUTE START %s | trades=%d | has_book=%s | has_price=%s",
        sym, len(dq), book is not None, price is not None
    )

    if not dq:
        logging.info("COMPUTE STOP %s | no trades in window", sym)
        return None
    if book is None:
        logging.info("COMPUTE STOP %s | no orderbook yet", sym)
        return None
    if price is None:
        logging.info("COMPUTE STOP %s | no last price", sym)
        return None

    total_quote = sum(t.quote for t in dq)
    taker_buy_quote = sum(t.quote for t in dq if t.taker_buy)

    logging.info(
        "FLOW %s | total_quote=%.2f | taker_buy_quote=%.2f | trades=%d",
        sym, total_quote, taker_buy_quote, len(dq)
    )

    if total_quote <= 0:
        logging.info("COMPUTE STOP %s | total_quote<=0", sym)
        return None

    buy_ratio = taker_buy_quote / total_quote

    p0 = dq[0].price
    price_change = (price - p0) / p0 if p0 > 0 else 0.0

    logging.info(
        "PRICE %s | p0=%.6f | now=%.6f | Δ%ss=%.4f%%",
        sym, p0, price, RT_WINDOW_SEC, price_change * 100.0
    )

    bids = book.bids[:DEPTH_TOP_N]
    asks = book.asks[:DEPTH_TOP_N]

    bid_notional = sum(p * q for p, q in bids)
    ask_notional = sum(p * q for p, q in asks)
    imbalance = (bid_notional / ask_notional) if ask_notional > 0 else 999.0

    best_bid = bids[0][0] if bids else 0.0
    best_ask = asks[0][0] if asks else 0.0

    logging.info(
        "BOOK %s | best_bid=%.6f best_ask=%.6f | bid_notional=%.2f ask_notional=%.2f imbalance=%.2f | topN=%d",
        sym, best_bid, best_ask, bid_notional, ask_notional, imbalance, DEPTH_TOP_N
    )

    near_wall = _near_sell_wall(sym, price)

    logging.info(
        "COMPUTE DONE %s | vol=%.0f buy=%.3f imbal=%.3f wall=%s",
        sym, total_quote, buy_ratio, imbalance, near_wall
    )

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
    logging.info("CHECK ENTRY %s", sym)

    if now_ts - rt_last_signal_ts[sym] < RT_COOLDOWN_SEC:
        logging.info("ENTRY BLOCKED %s | cooldown (%.2fs remaining)",
                     sym, RT_COOLDOWN_SEC - (now_ts - rt_last_signal_ts[sym]))
        return

    if await db_has_open_position(pool, sym):
        logging.info("ENTRY BLOCKED %s | already open position in DB", sym)
        return

    if not await can_enter(pool, now_ts):
        logging.info("ENTRY BLOCKED %s | limiter", sym)
        return

    if m["vol_10s"] < RT_MIN_QUOTE_VOL_10S:
        logging.info("ENTRY BLOCKED %s | volume too low (%.0f < %.0f)",
                     sym, m["vol_10s"], RT_MIN_QUOTE_VOL_10S)
        return

    if m["price_change"] < RT_MIN_PRICE_CHANGE_10S:
        logging.info("ENTRY BLOCKED %s | price_change too low (%.4f%% < %.4f%%)",
                     sym, m["price_change"] * 100.0, RT_MIN_PRICE_CHANGE_10S * 100.0)
        return

    if m["buy_ratio"] < RT_MIN_BUY_RATIO_10S:
        logging.info("ENTRY BLOCKED %s | buy_ratio too low (%.3f < %.3f)",
                     sym, m["buy_ratio"], RT_MIN_BUY_RATIO_10S)
        return

    if m["imbalance"] < RT_MIN_IMBALANCE:
        logging.info("ENTRY BLOCKED %s | imbalance too low (%.3f < %.3f)",
                     sym, m["imbalance"], RT_MIN_IMBALANCE)
        return

    if m["near_sell_wall"]:
        logging.info("ENTRY BLOCKED %s | near sell wall", sym)
        return

    logging.info("ENTRY PASSED ALL FILTERS %s", sym)

    entry = m["price"]
    sl = entry * (1.0 - RT_SL_PCT)
    risk = entry - sl
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
            "⚡ ENTER_RT %s | entry=%.6f SL=%.6f TP=%.6f | Δ%ss=%.2f%% vol=%.0f buy=%.2f imbal=%.2f wall=%s",
            sym, entry, sl, tp,
            RT_WINDOW_SEC, m["price_change"] * 100.0, m["vol_10s"], m["buy_ratio"], m["imbalance"],
            "Y" if m["near_sell_wall"] else "N"
        )
    else:
        logging.info("ENTRY FAILED %s | DB insert conflict (already exists)", sym)

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

async def realtime_loop(pool: asyncpg.Pool) -> None:
    url = ws_url(SYMBOLS)

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                logging.info("Connected to Binance (REALTIME ENTRY ONLY | FULL VERBOSE DEBUG)")

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

                        maker = bool(data["m"])   # True = buyer is maker => sell-initiated
                        taker_buy = (maker is False)

                        logging.info(
                            "TRADE %s | price=%.6f | qty=%.6f | quote=%.2f | taker_buy=%s | T=%d",
                            sym, price, qty, quote, taker_buy, ts_ms
                        )

                        rt_last_price[sym] = price
                        rt_trades[sym].append(TradePoint(ts_ms=ts_ms, price=price, quote=quote, taker_buy=taker_buy))
                        _prune_trades(sym, now_ms)

                        m = compute_metrics(sym, now_ts)
                        if m is not None:
                            logging.info(
                                "METRICS %s | price=%.6f Δ%ss=%.2f%% vol=%.0f buy=%.2f imbal=%.2f wall=%s",
                                sym, m["price"], RT_WINDOW_SEC, m["price_change"] * 100.0, m["vol_10s"],
                                m["buy_ratio"], m["imbalance"], "Y" if m["near_sell_wall"] else "N"
                            )
                            await maybe_enter(pool, sym, m, now_ts)
                        else:
                            logging.info("METRICS %s | None (insufficient data yet)", sym)

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

                        # show top few levels to understand what we receive (diffs)
                        topb = ", ".join([f"{p:.6f}:{q:.4f}" for p, q in bids[:3]])
                        topa = ", ".join([f"{p:.6f}:{q:.4f}" for p, q in asks[:3]])
                        logging.info("DEPTH %s | bids_top3=[%s] asks_top3=[%s] (diff update)", sym, topb, topa)

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
