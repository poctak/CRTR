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

import aiohttp
import asyncpg
import websockets

# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
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
    logging.warning("FULL VERBOSE DEBUG is best with 1 symbol. Current SYMBOLS=%s", SYMBOLS)

DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "pumpdb")
DB_USER = os.getenv("DB_USER", "pumpuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

BINANCE_REST = "https://api.binance.com"
BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream?streams="

# Risk / limits
MAX_OPEN_POSITIONS = env_int("MAX_OPEN_POSITIONS", 1)
MAX_ENTRIES_PER_HOUR = env_int("MAX_ENTRIES_PER_HOUR", 5)
TP_R = env_float("TP_R", 2.0)

# Realtime window
RT_WINDOW_SEC = env_int("RT_WINDOW_SEC", 10)

# Entry thresholds (price/orderflow)
RT_MIN_PRICE_CHANGE_10S = env_float("RT_MIN_PRICE_CHANGE_10S", 0.004)  # 0.4%
RT_MIN_BUY_RATIO_10S = env_float("RT_MIN_BUY_RATIO_10S", 0.65)
RT_MIN_IMBALANCE = env_float("RT_MIN_IMBALANCE", 1.4)
RT_COOLDOWN_SEC = env_int("RT_COOLDOWN_SEC", 30)
RT_SL_PCT = env_float("RT_SL_PCT", 0.003)  # 0.3%

# ==========================================================
# PRO VOLUME (adaptive baseline + z-score)
# ==========================================================
# baseline history length (in seconds). We store 1 sample per RT_VOL_UPDATE_EVERY_SEC.
RT_VOL_BASELINE_SEC = env_int("RT_VOL_BASELINE_SEC", 120)           # 2 minutes baseline
RT_VOL_UPDATE_EVERY_SEC = env_float("RT_VOL_UPDATE_EVERY_SEC", 1.0) # store baseline sample once per second

RT_MIN_VOL_ABS = env_float("RT_MIN_VOL_ABS", 1000.0)               # absolute floor (avoid micro-noise)
RT_VOL_Z_TH = env_float("RT_VOL_Z_TH", 2.5)                        # z-score threshold
RT_VOL_MULT = env_float("RT_VOL_MULT", 3.0)                        # vol_10s / mean threshold

# Orderbook params
DEPTH_SNAPSHOT_LIMIT = env_int("DEPTH_SNAPSHOT_LIMIT", 1000)  # 100, 500, 1000 allowed
DEPTH_TOP_N = env_int("DEPTH_TOP_N", 10)
RT_NEAR_WALL_PCT = env_float("RT_NEAR_WALL_PCT", 0.0015)  # 0.15%
RT_WALL_MULT = env_float("RT_WALL_MULT", 3.0)

# Verbose logs
LOG_DEPTH_UPDATES = env_int("LOG_DEPTH_UPDATES", 1)      # 1=log each depth update (very noisy)
LOG_BOOK_TOP_LEVELS = env_int("LOG_BOOK_TOP_LEVELS", 1)  # log top3 bids/asks after updates

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

# ==========================================================
# STATE
# ==========================================================

entry_times: Deque[float] = deque()

# trades rolling window
rt_trades: Dict[str, Deque[TradePoint]] = defaultdict(deque)
rt_last_price: Dict[str, float] = {}
rt_last_signal_ts: Dict[str, float] = defaultdict(lambda: 0.0)

# volume baseline per symbol (stores vol_10s samples)
vol_baseline: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=max(10, int(RT_VOL_BASELINE_SEC / max(RT_VOL_UPDATE_EVERY_SEC, 0.1)))))
vol_last_update_ts: Dict[str, float] = defaultdict(lambda: 0.0)

# ==========================================================
# ORDERBOOK (snapshot + diff)
# ==========================================================

class OrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.last_update_id: Optional[int] = None
        self.bids: Dict[float, float] = {}  # price -> qty
        self.asks: Dict[float, float] = {}  # price -> qty
        self.ready: bool = False

    def reset(self) -> None:
        self.last_update_id = None
        self.bids.clear()
        self.asks.clear()
        self.ready = False

    def apply_snapshot(self, last_update_id: int, bids: List[List[str]], asks: List[List[str]]) -> None:
        self.reset()
        self.last_update_id = int(last_update_id)

        for p_str, q_str in bids:
            p = float(p_str); q = float(q_str)
            if q > 0:
                self.bids[p] = q

        for p_str, q_str in asks:
            p = float(p_str); q = float(q_str)
            if q > 0:
                self.asks[p] = q

        self.ready = True
        logging.info("BOOK SNAPSHOT %s | lastUpdateId=%s | bids=%d asks=%d",
                     self.symbol, self.last_update_id, len(self.bids), len(self.asks))

    def _apply_side(self, side: Dict[float, float], updates: List[List[str]]) -> None:
        for p_str, q_str in updates:
            p = float(p_str); q = float(q_str)
            if q == 0.0:
                side.pop(p, None)
            else:
                side[p] = q

    def apply_diff(self, U: int, u: int, b: List[List[str]], a: List[List[str]]) -> Tuple[bool, str]:
        if not self.ready or self.last_update_id is None:
            return False, "not_ready"

        last_id = self.last_update_id

        if u <= last_id:
            return True, f"ignored_old u={u} last={last_id}"

        if U <= last_id + 1 <= u:
            self._apply_side(self.bids, b)
            self._apply_side(self.asks, a)
            self.last_update_id = u
            return True, f"applied U={U} u={u} new_last={u}"

        return False, f"out_of_sync U={U} u={u} last={last_id}"

    def top_n(self, n: int) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        bids_sorted = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        asks_sorted = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        return bids_sorted, asks_sorted

    def best_bid_ask(self) -> Tuple[float, float]:
        best_bid = max(self.bids.keys()) if self.bids else 0.0
        best_ask = min(self.asks.keys()) if self.asks else 0.0
        return best_bid, best_ask

orderbooks: Dict[str, OrderBook] = {s: OrderBook(s) for s in SYMBOLS}
resync_lock: Dict[str, asyncio.Lock] = {s: asyncio.Lock() for s in SYMBOLS}

async def fetch_depth_snapshot(session: aiohttp.ClientSession, symbol: str) -> dict:
    url = f"{BINANCE_REST}/api/v3/depth?symbol={symbol}&limit={DEPTH_SNAPSHOT_LIMIT}"
    logging.info("FETCH SNAPSHOT %s | %s", symbol, url)
    async with session.get(url, timeout=10) as resp:
        resp.raise_for_status()
        return await resp.json()

async def resync_orderbook(session: aiohttp.ClientSession, symbol: str) -> None:
    async with resync_lock[symbol]:
        data = await fetch_depth_snapshot(session, symbol)
        last_update_id = int(data["lastUpdateId"])
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        orderbooks[symbol].apply_snapshot(last_update_id, bids, asks)

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
# METRICS HELPERS
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

def _near_sell_wall(sym: str, price: float, asks_top: List[Tuple[float, float]]) -> bool:
    if not asks_top or price <= 0:
        logging.info("WALL %s | cannot check (asks_empty=%s price=%.6f)",
                     sym, not bool(asks_top), price if price else -1.0)
        return False

    levels = [p * q for p, q in asks_top if q > 0]
    if not levels:
        logging.info("WALL %s | asks levels empty after filtering", sym)
        return False

    avg_level = sum(levels) / len(levels)
    max_level = max(levels)

    best_ask = asks_top[0][0]
    dist = (best_ask - price) / price

    near = dist <= RT_NEAR_WALL_PCT
    big = (avg_level > 0) and (max_level >= RT_WALL_MULT * avg_level)

    logging.info(
        "WALL %s | best_ask=%.6f price=%.6f dist=%.5f%% near=%s avg_lvl=%.2f max_lvl=%.2f big=%s",
        sym, best_ask, price, dist * 100.0, near, avg_level, max_level, big
    )
    return near and big

def _update_volume_baseline(sym: str, now_ts: float, vol_10s: float) -> None:
    last = vol_last_update_ts[sym]
    if now_ts - last < RT_VOL_UPDATE_EVERY_SEC:
        return
    vol_last_update_ts[sym] = now_ts
    vol_baseline[sym].append(float(vol_10s))

def _mean_std(values: List[float]) -> Tuple[float, float]:
    # simple population std (enough for thresholding)
    n = len(values)
    if n <= 1:
        return (values[0] if n == 1 else 0.0), 0.0
    mean = sum(values) / n
    var = sum((x - mean) ** 2 for x in values) / n
    std = var ** 0.5
    return mean, std

def _volume_anomaly(sym: str, vol_10s: float) -> Tuple[float, float, float, float]:
    """
    Returns: (mean, std, rel_mult, z)
    rel_mult = vol_10s / mean (if mean>0 else 0)
    z = (vol_10s - mean) / std (if std>0 else 0)
    """
    hist = list(vol_baseline[sym])
    if len(hist) < 20:
        # not enough history yet -> treat as no anomaly
        mean, std = _mean_std(hist) if hist else (0.0, 0.0)
        rel = (vol_10s / mean) if mean > 0 else 0.0
        z = ((vol_10s - mean) / std) if std > 0 else 0.0
        return mean, std, rel, z

    mean, std = _mean_std(hist)
    rel = (vol_10s / mean) if mean > 0 else 0.0
    z = ((vol_10s - mean) / std) if std > 0 else 0.0
    return mean, std, rel, z

# ==========================================================
# METRICS
# ==========================================================

def compute_metrics(sym: str, now_ts: float) -> Optional[dict]:
    dq = rt_trades[sym]
    price = rt_last_price.get(sym)
    ob = orderbooks[sym]

    logging.info(
        "COMPUTE START %s | trades=%d | has_price=%s | book_ready=%s lastUpdateId=%s | baseline_n=%d",
        sym, len(dq), price is not None, ob.ready, ob.last_update_id, len(vol_baseline[sym])
    )

    if not dq:
        logging.info("COMPUTE STOP %s | no trades in window", sym)
        return None
    if price is None:
        logging.info("COMPUTE STOP %s | no last price", sym)
        return None
    if not ob.ready:
        logging.info("COMPUTE STOP %s | orderbook not ready", sym)
        return None

    total_quote = sum(t.quote for t in dq)
    taker_buy_quote = sum(t.quote for t in dq if t.taker_buy)

    logging.info(
        "FLOW %s | vol_10s=%.2f | taker_buy_quote=%.2f | trades=%d",
        sym, total_quote, taker_buy_quote, len(dq)
    )
    if total_quote <= 0:
        logging.info("COMPUTE STOP %s | vol_10s<=0", sym)
        return None

    buy_ratio = taker_buy_quote / total_quote

    p0 = dq[0].price
    price_change = (price - p0) / p0 if p0 > 0 else 0.0
    logging.info(
        "PRICE %s | p0=%.6f | now=%.6f | Δ%ss=%.4f%%",
        sym, p0, price, RT_WINDOW_SEC, price_change * 100.0
    )

    bids_top, asks_top = ob.top_n(DEPTH_TOP_N)
    best_bid, best_ask = ob.best_bid_ask()

    bid_notional = sum(p * q for p, q in bids_top)
    ask_notional = sum(p * q for p, q in asks_top)
    imbalance = (bid_notional / ask_notional) if ask_notional > 0 else 0.0

    logging.info(
        "BOOK %s | best_bid=%.6f best_ask=%.6f | bid_notional=%.2f ask_notional=%.2f imbalance=%.2f | topN=%d",
        sym, best_bid, best_ask, bid_notional, ask_notional, imbalance, DEPTH_TOP_N
    )

    near_wall = _near_sell_wall(sym, price, asks_top)

    # ---- PRO VOLUME baseline / anomaly ----
    _update_volume_baseline(sym, now_ts, total_quote)
    mean, std, rel_mult, z = _volume_anomaly(sym, total_quote)

    logging.info(
        "VOL_ANOM %s | vol_10s=%.0f | mean=%.0f std=%.0f | rel=%.2fx | z=%.2f | abs_floor=%.0f",
        sym, total_quote, mean, std, rel_mult, z, RT_MIN_VOL_ABS
    )

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
        "vol_mean": mean,
        "vol_std": std,
        "vol_rel": rel_mult,
        "vol_z": z,
        "baseline_n": len(vol_baseline[sym]),
    }

# ==========================================================
# ENTRY
# ==========================================================

async def maybe_enter(pool: asyncpg.Pool, sym: str, m: dict, now_ts: float) -> None:
    logging.info("CHECK ENTRY %s", sym)

    if now_ts - rt_last_signal_ts[sym] < RT_COOLDOWN_SEC:
        logging.info(
            "ENTRY BLOCKED %s | cooldown (%.2fs remaining)",
            sym, RT_COOLDOWN_SEC - (now_ts - rt_last_signal_ts[sym])
        )
        return

    if await db_has_open_position(pool, sym):
        logging.info("ENTRY BLOCKED %s | already open position in DB", sym)
        return

    if not await can_enter(pool, now_ts):
        logging.info("ENTRY BLOCKED %s | limiter", sym)
        return

    # ---- PRO VOLUME filter ----
    if m["vol_10s"] < RT_MIN_VOL_ABS:
        logging.info("ENTRY BLOCKED %s | abs volume too low (%.0f < %.0f)",
                     sym, m["vol_10s"], RT_MIN_VOL_ABS)
        return

    # baseline must be warmed up
    if m["baseline_n"] < 20:
        logging.info("ENTRY BLOCKED %s | baseline not warmed (n=%d < 20)",
                     sym, m["baseline_n"])
        return

    if m["vol_z"] < RT_VOL_Z_TH:
        logging.info("ENTRY BLOCKED %s | vol z too low (%.2f < %.2f)",
                     sym, m["vol_z"], RT_VOL_Z_TH)
        return

    if m["vol_rel"] < RT_VOL_MULT:
        logging.info("ENTRY BLOCKED %s | vol rel too low (%.2fx < %.2fx)",
                     sym, m["vol_rel"], RT_VOL_MULT)
        return

    # price / orderflow filters
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
            "⚡ ENTER_RT %s | entry=%.6f SL=%.6f TP=%.6f | "
            "Δ%ss=%.2f%% vol=%.0f buy=%.2f imbal=%.2f wall=%s | rel=%.2fx z=%.2f (mean=%.0f std=%.0f)",
            sym, entry, sl, tp,
            RT_WINDOW_SEC, m["price_change"] * 100.0, m["vol_10s"], m["buy_ratio"], m["imbalance"],
            "Y" if m["near_sell_wall"] else "N",
            m["vol_rel"], m["vol_z"], m["vol_mean"], m["vol_std"]
        )
    else:
        logging.info("ENTRY FAILED %s | DB insert conflict (already exists)", sym)

# ==========================================================
# WEBSOCKET
# ==========================================================

def ws_url(symbols: List[str]) -> str:
    streams: List[str] = []
    for s in symbols:
        sym = s.lower()
        streams.append(f"{sym}@aggTrade")
        streams.append(f"{sym}@depth@100ms")
    return BINANCE_WS_BASE + "/".join(streams)

async def realtime_loop(pool: asyncpg.Pool) -> None:
    url = ws_url(SYMBOLS)

    async with aiohttp.ClientSession() as session:
        # initial snapshots before WS
        for s in SYMBOLS:
            await resync_orderbook(session, s)

        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    logging.info("Connected to Binance (ENTRY ONLY | ORDERBOOK SNAPSHOT+DIFF | PRO VOLUME | FULL VERBOSE DEBUG)")

                    async for msg in ws:
                        stream = ""
                        try:
                            payload = json.loads(msg)
                            stream = (payload.get("stream") or "").lower()
                            data = payload.get("data") or {}

                            now_ms = int(time.time() * 1000)
                            now_ts = now_ms / 1000.0

                            # aggTrade
                            if stream.endswith("@aggtrade"):
                                sym = (data.get("s") or "").upper()
                                if sym not in SYMBOLS:
                                    continue

                                price = float(data["p"])
                                qty = float(data["q"])
                                quote = price * qty
                                ts_ms = int(data["T"])

                                maker = bool(data["m"])
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
                                        "METRICS %s | price=%.6f Δ%ss=%.2f%% vol=%.0f buy=%.2f imbal=%.2f wall=%s | rel=%.2fx z=%.2f (n=%d)",
                                        sym, m["price"], RT_WINDOW_SEC, m["price_change"] * 100.0, m["vol_10s"],
                                        m["buy_ratio"], m["imbalance"], "Y" if m["near_sell_wall"] else "N",
                                        m["vol_rel"], m["vol_z"], m["baseline_n"]
                                    )
                                    await maybe_enter(pool, sym, m, now_ts)
                                else:
                                    logging.info("METRICS %s | None (insufficient data yet)", sym)

                            # depthUpdate
                            elif "@depth" in stream:
                                sym = (data.get("s") or "").upper()
                                if sym not in SYMBOLS:
                                    continue

                                U = int(data.get("U"))
                                u = int(data.get("u"))
                                b = data.get("b", [])
                                a = data.get("a", [])

                                ok, reason = orderbooks[sym].apply_diff(U, u, b, a)
                                if LOG_DEPTH_UPDATES:
                                    logging.info("DEPTH %s | %s", sym, reason)

                                if not ok:
                                    logging.warning("DEPTH %s | %s -> RESYNC", sym, reason)
                                    await resync_orderbook(session, sym)
                                    continue

                                if LOG_BOOK_TOP_LEVELS:
                                    bids_top, asks_top = orderbooks[sym].top_n(3)
                                    topb = ", ".join([f"{p:.6f}:{q:.4f}" for p, q in bids_top])
                                    topa = ", ".join([f"{p:.6f}:{q:.4f}" for p, q in asks_top])
                                    bb, ba = orderbooks[sym].best_bid_ask()
                                    logging.info(
                                        "BOOKTOP %s | best_bid=%.6f best_ask=%.6f | bids_top3=[%s] asks_top3=[%s]",
                                        sym, bb, ba, topb, topa
                                    )

                        except Exception as inner:
                            logging.error("Message handling error (stream=%s): %s", stream, inner)

            except Exception as e:
                logging.error("WS error: %s | reconnecting in 5s", e)
                await asyncio.sleep(5)

# ==========================================================
# MAIN
# ==========================================================

async def main():
    pool = await init_db_pool()
    await realtime_loop(pool)

if __name__ == "__main__":
    asyncio.run(main())
