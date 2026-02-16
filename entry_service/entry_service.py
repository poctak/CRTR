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

# Price / orderflow thresholds
RT_MIN_PRICE_CHANGE_10S = env_float("RT_MIN_PRICE_CHANGE_10S", 0.004)  # 0.4%
RT_MIN_BUY_RATIO_10S = env_float("RT_MIN_BUY_RATIO_10S", 0.65)
RT_MIN_IMBALANCE = env_float("RT_MIN_IMBALANCE", 1.4)
RT_COOLDOWN_SEC = env_int("RT_COOLDOWN_SEC", 30)
RT_SL_PCT = env_float("RT_SL_PCT", 0.003)  # 0.3%

# ==========================================================
# PRO VOLUME (robust MAD + dynamic abs floor per coin + 2-phase)
# ==========================================================

RT_VOL_BASELINE_SEC = env_int("RT_VOL_BASELINE_SEC", 120)            # baseline horizon
RT_VOL_UPDATE_EVERY_SEC = env_float("RT_VOL_UPDATE_EVERY_SEC", 1.0)  # baseline sampling rate

# Warmup for gating (counts ALL samples incl zeros)
RT_BASELINE_WARMUP_N = env_int("RT_BASELINE_WARMUP_N", 60)           # 1Hz => ~60s

# Robust stats warmup (counts only NON-ZERO samples)
RT_STATS_MIN_N = env_int("RT_STATS_MIN_N", 30)

RT_VOL_MAD_Z_TH = env_float("RT_VOL_MAD_Z_TH", 6.0)                  # robust z threshold via MAD
RT_VOL_REL_TH = env_float("RT_VOL_REL_TH", 3.0)                      # vol_10s / median threshold

RT_ABS_FLOOR_GLOBAL_MIN = env_float("RT_ABS_FLOOR_GLOBAL_MIN", 200.0)  # absolute minimum across all coins
RT_ABS_FLOOR_PCTL = env_float("RT_ABS_FLOOR_PCTL", 0.25)               # percentile of baseline_pos
RT_ABS_FLOOR_MULT_MED = env_float("RT_ABS_FLOOR_MULT_MED", 0.6)         # multiplier of median baseline_pos

RT_ARM_WINDOW_SEC = env_float("RT_ARM_WINDOW_SEC", 3.0)              # confirm time window after ARM
RT_CONFIRM_MIN_VOL_FRAC = env_float("RT_CONFIRM_MIN_VOL_FRAC", 0.6)  # confirm vol must keep at least this fraction of ARM vol_10s

# ==========================================================
# ORDERBOOK (snapshot + diff) + WALL (anti-spoof)
# ==========================================================

DEPTH_SNAPSHOT_LIMIT = env_int("DEPTH_SNAPSHOT_LIMIT", 100)  # 100, 500, 1000 allowed
DEPTH_TOP_N = env_int("DEPTH_TOP_N", 10)

RT_NEAR_WALL_PCT = env_float("RT_NEAR_WALL_PCT", 0.0015)  # 0.15%
RT_WALL_MULT = env_float("RT_WALL_MULT", 3.0)

# Anti-spoof wall: require persistence at ~same price for X seconds
RT_WALL_PERSIST_SEC = env_float("RT_WALL_PERSIST_SEC", 1.5)
RT_WALL_PRICE_EPS_PCT = env_float("RT_WALL_PRICE_EPS_PCT", 0.0005)  # 0.05% tolerance

# Verbose logs (careful with many symbols)
LOG_DEPTH_UPDATES = env_int("LOG_DEPTH_UPDATES", 0)      # 1=log each depth update
LOG_BOOK_TOP_LEVELS = env_int("LOG_BOOK_TOP_LEVELS", 0)  # 1=log top levels after updates

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
class ArmState:
    armed_at_ts: float
    arm_price: float
    arm_vol_10s: float

@dataclass
class WallTracker:
    first_seen_ts: float
    last_seen_ts: float
    wall_price: float

# ==========================================================
# STATE
# ==========================================================

entry_times: Deque[float] = deque()

# trades rolling window
rt_trades: Dict[str, Deque[TradePoint]] = defaultdict(deque)
rt_last_price: Dict[str, float] = {}
rt_last_signal_ts: Dict[str, float] = defaultdict(lambda: 0.0)

# baseline samples
_baseline_maxlen = max(10, int(RT_VOL_BASELINE_SEC / max(RT_VOL_UPDATE_EVERY_SEC, 0.1)))

# counts ALL samples incl zeros => for warmup & "true inactivity" context
vol_baseline_all: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=_baseline_maxlen))

# counts only non-zero samples => used for robust stats & abs_floor
vol_baseline_pos: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=_baseline_maxlen))

vol_last_update_ts: Dict[str, float] = defaultdict(lambda: 0.0)

# 2-phase ARM state
armed: Dict[str, Optional[ArmState]] = defaultdict(lambda: None)

# wall persistence tracker
wall_tracker: Dict[str, Optional[WallTracker]] = defaultdict(lambda: None)

# ==========================================================
# ORDERBOOK IMPLEMENTATION
# ==========================================================

class OrderBook:
    """
    Maintains a local orderbook using:
      - REST snapshot (lastUpdateId)
      - WS depth diff updates (U/u, bids/asks)
    """
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
            p = float(p_str)
            q = float(q_str)
            if q > 0:
                self.bids[p] = q

        for p_str, q_str in asks:
            p = float(p_str)
            q = float(q_str)
            if q > 0:
                self.asks[p] = q

        self.ready = True
        logging.info(
            "BOOK SNAPSHOT %s | lastUpdateId=%s | bids=%d asks=%d",
            self.symbol, self.last_update_id, len(self.bids), len(self.asks)
        )

    def _apply_side(self, side: Dict[float, float], updates: List[List[str]]) -> None:
        for p_str, q_str in updates:
            p = float(p_str)
            q = float(q_str)
            if q == 0.0:
                side.pop(p, None)
            else:
                side[p] = q

    def apply_diff(self, U: int, u: int, b: List[List[str]], a: List[List[str]]) -> Tuple[bool, str]:
        """
        Returns (ok, reason).
        Sequencing rules:
          - if u <= lastUpdateId: ignore
          - if U <= lastUpdateId+1 <= u: apply
          - else: out of sync -> resync needed
        """
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
# ROBUST STATS HELPERS
# ==========================================================

def _median(xs: List[float]) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return s[mid]
    return 0.5 * (s[mid - 1] + s[mid])

def _percentile(xs: List[float], p: float) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    p = max(0.0, min(1.0, p))
    idx = int(round(p * (len(s) - 1)))
    return s[idx]

def _mad(xs: List[float], med: float) -> float:
    if not xs:
        return 0.0
    dev = [abs(x - med) for x in xs]
    return _median(dev)

def _robust_z_mad(x: float, med: float, mad: float) -> float:
    denom = 1.4826 * mad
    if denom <= 0:
        return 0.0
    return (x - med) / denom

def _dynamic_abs_floor_from_pos(sym: str) -> float:
    hist = list(vol_baseline_pos[sym])
    if len(hist) < 10:
        return RT_ABS_FLOOR_GLOBAL_MIN
    med = _median(hist)
    pctl = _percentile(hist, RT_ABS_FLOOR_PCTL)
    return max(RT_ABS_FLOOR_GLOBAL_MIN, pctl, RT_ABS_FLOOR_MULT_MED * med)

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

def _near_sell_wall_raw(sym: str, price: float, asks_top: List[Tuple[float, float]]) -> Tuple[bool, float]:
    """
    Returns (is_wall_raw, wall_price_level).
    raw means: near best ask AND big sell liquidity concentration in topN.
    """
    if not asks_top or price <= 0:
        logging.info("WALL %s | cannot check (asks_empty=%s price=%.6f)",
                     sym, not bool(asks_top), price if price else -1.0)
        return False, 0.0

    levels = [p * q for p, q in asks_top if q > 0]
    if not levels:
        logging.info("WALL %s | asks levels empty after filtering", sym)
        return False, 0.0

    avg_level = sum(levels) / len(levels)
    max_level = max(levels)

    best_ask = asks_top[0][0]
    dist = (best_ask - price) / price

    near = dist <= RT_NEAR_WALL_PCT
    big = (avg_level > 0) and (max_level >= RT_WALL_MULT * avg_level)

    logging.info(
        "WALL_RAW %s | best_ask=%.6f price=%.6f dist=%.5f%% near=%s avg_lvl=%.2f max_lvl=%.2f big=%s",
        sym, best_ask, price, dist * 100.0, near, avg_level, max_level, big
    )
    return (near and big), best_ask

def _wall_persistent(sym: str, now_ts: float, raw: bool, wall_price: float) -> bool:
    """
    Anti-spoof: wall must persist near same price level for RT_WALL_PERSIST_SEC.
    """
    tr = wall_tracker[sym]

    if not raw:
        if tr is not None:
            logging.info(
                "WALL_ASSESS %s | raw=False -> reset tracker (was price=%.6f age=%.2fs)",
                sym, tr.wall_price, (now_ts - tr.first_seen_ts)
            )
        wall_tracker[sym] = None
        return False

    # raw=True
    if tr is None:
        wall_tracker[sym] = WallTracker(first_seen_ts=now_ts, last_seen_ts=now_ts, wall_price=wall_price)
        logging.info("WALL_ASSESS %s | raw=True -> start tracker price=%.6f", sym, wall_price)
        return False

    eps = (RT_WALL_PRICE_EPS_PCT * tr.wall_price) if tr.wall_price > 0 else 0.0
    same_level = abs(wall_price - tr.wall_price) <= eps

    if not same_level:
        logging.info(
            "WALL_ASSESS %s | raw=True but moved (old=%.6f new=%.6f eps=%.8f) -> reset tracker",
            sym, tr.wall_price, wall_price, eps
        )
        wall_tracker[sym] = WallTracker(first_seen_ts=now_ts, last_seen_ts=now_ts, wall_price=wall_price)
        return False

    tr.last_seen_ts = now_ts
    age = now_ts - tr.first_seen_ts
    stable = age >= RT_WALL_PERSIST_SEC

    logging.info(
        "WALL_ASSESS %s | raw=True same_level price=%.6f age=%.2fs stable=%s (need>=%.2fs)",
        sym, tr.wall_price, age, stable, RT_WALL_PERSIST_SEC
    )
    return stable

# ==========================================================
# BASELINE SAMPLER (ALL + POS)
# ==========================================================

def _update_volume_baselines(sym: str, now_ts: float, vol_10s: float) -> None:
    last = vol_last_update_ts[sym]
    if now_ts - last < RT_VOL_UPDATE_EVERY_SEC:
        return
    vol_last_update_ts[sym] = now_ts

    v = float(vol_10s)
    vol_baseline_all[sym].append(v)
    if v > 0:
        vol_baseline_pos[sym].append(v)

async def baseline_sampler_loop() -> None:
    """
    Samples vol_10s per symbol every RT_VOL_UPDATE_EVERY_SEC,
    independent of incoming trades.

    - baseline_all: includes zeros -> warmup + true inactivity
    - baseline_pos: non-zero only -> robust stats + abs floor
    """
    while True:
        try:
            now_ts = time.time()
            now_ms = int(now_ts * 1000)

            for sym in SYMBOLS:
                _prune_trades(sym, now_ms)
                dq = rt_trades[sym]
                vol_10s = sum(t.quote for t in dq) if dq else 0.0
                _update_volume_baselines(sym, now_ts, vol_10s)

        except Exception as e:
            logging.error("baseline_sampler_loop error: %s", e)

        await asyncio.sleep(max(0.1, RT_VOL_UPDATE_EVERY_SEC))

# ==========================================================
# METRICS
# ==========================================================

def compute_metrics(sym: str, now_ts: float) -> Optional[dict]:
    dq = rt_trades[sym]
    price = rt_last_price.get(sym)
    ob = orderbooks[sym]

    all_n = len(vol_baseline_all[sym])
    pos_n = len(vol_baseline_pos[sym])

    logging.info(
        "COMPUTE START %s | trades=%d | has_price=%s | book_ready=%s lastUpdateId=%s | baseline_all_n=%d baseline_pos_n=%d",
        sym, len(dq), price is not None, ob.ready, ob.last_update_id, all_n, pos_n
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

    # wall (anti-spoof)
    wall_raw, wall_price = _near_sell_wall_raw(sym, price, asks_top)
    near_wall = _wall_persistent(sym, now_ts, wall_raw, wall_price)

    # robust volume stats + dynamic abs floor (from NON-ZERO baseline)
    hist_pos = list(vol_baseline_pos[sym])

    abs_floor = _dynamic_abs_floor_from_pos(sym)

    stats_ready = (len(hist_pos) >= RT_STATS_MIN_N)
    if not stats_ready:
        vol_med = _median(hist_pos) if hist_pos else 0.0
        vol_mad = _mad(hist_pos, vol_med) if hist_pos else 0.0
        vol_rel = 0.0
        vol_z_mad = 0.0

        logging.info(
            "VOL_PRO %s | stats_not_ready pos_n=%d < %d | abs_floor(sym)=%.0f | (med=%.0f mad=%.0f)",
            sym, len(hist_pos), RT_STATS_MIN_N, abs_floor, vol_med, vol_mad
        )
    else:
        vol_med = _median(hist_pos)
        vol_mad = _mad(hist_pos, vol_med)
        vol_rel = (total_quote / vol_med) if vol_med > 0 else 0.0
        vol_z_mad = _robust_z_mad(total_quote, vol_med, vol_mad)

        logging.info(
            "VOL_PRO %s | vol_10s=%.0f | med=%.0f mad=%.0f | rel=%.2fx | z_mad=%.2f | abs_floor(sym)=%.0f | pos_n=%d",
            sym, total_quote, vol_med, vol_mad, vol_rel, vol_z_mad, abs_floor, len(hist_pos)
        )

    logging.info(
        "COMPUTE DONE %s | vol=%.0f buy=%.3f imbal=%.3f wall=%s (raw=%s) | baseline_all_n=%d baseline_pos_n=%d",
        sym, total_quote, buy_ratio, imbalance, near_wall, wall_raw, all_n, pos_n
    )

    return {
        "price": price,
        "vol_10s": total_quote,
        "buy_ratio": buy_ratio,
        "price_change": price_change,
        "imbalance": imbalance,
        "near_sell_wall": near_wall,     # anti-spoofed
        "wall_raw": wall_raw,            # debug

        "abs_floor": abs_floor,

        "stats_ready": stats_ready,
        "vol_med": vol_med,
        "vol_mad": vol_mad,
        "vol_rel": vol_rel,
        "vol_z_mad": vol_z_mad,

        "baseline_all_n": all_n,
        "baseline_pos_n": pos_n,
    }

# ==========================================================
# ENTRY (2-PHASE: ARM -> CONFIRM)
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

    # warm-up (ALL samples incl zeros)
    if m["baseline_all_n"] < RT_BASELINE_WARMUP_N:
        logging.info(
            "ENTRY BLOCKED %s | baseline not warmed (all_n=%d < %d)",
            sym, m["baseline_all_n"], RT_BASELINE_WARMUP_N
        )
        return

    # dynamic abs floor per coin
    if m["vol_10s"] < m["abs_floor"]:
        logging.info(
            "ENTRY BLOCKED %s | vol below dynamic abs_floor (%.0f < %.0f)",
            sym, m["vol_10s"], m["abs_floor"]
        )
        return

    # robust stats readiness (NON-ZERO baseline)
    if not m["stats_ready"]:
        logging.info(
            "ENTRY BLOCKED %s | stats not ready (pos_n=%d < %d)",
            sym, m["baseline_pos_n"], RT_STATS_MIN_N
        )
        return

    st = armed[sym]

    # ARM phase
    if st is None:
        vol_anom_ok = (m["vol_z_mad"] >= RT_VOL_MAD_Z_TH) and (m["vol_rel"] >= RT_VOL_REL_TH)
        if not vol_anom_ok:
            logging.info(
                "ENTRY BLOCKED %s | vol anomaly not met (z_mad=%.2f<%.2f OR rel=%.2fx<%.2fx)",
                sym, m["vol_z_mad"], RT_VOL_MAD_Z_TH, m["vol_rel"], RT_VOL_REL_TH
            )
            return

        armed[sym] = ArmState(
            armed_at_ts=now_ts,
            arm_price=m["price"],
            arm_vol_10s=m["vol_10s"],
        )
        logging.warning(
            "⚑ ARM %s | price=%.6f vol_10s=%.0f | rel=%.2fx z_mad=%.2f | waiting %.1fs for confirm",
            sym, m["price"], m["vol_10s"], m["vol_rel"], m["vol_z_mad"], RT_ARM_WINDOW_SEC
        )
        return

    # CONFIRM phase
    age = now_ts - st.armed_at_ts
    if age > RT_ARM_WINDOW_SEC:
        logging.info("DISARM %s | arm expired (age=%.2fs > %.2fs)", sym, age, RT_ARM_WINDOW_SEC)
        armed[sym] = None
        return

    if m["vol_10s"] < RT_CONFIRM_MIN_VOL_FRAC * st.arm_vol_10s:
        logging.info(
            "ENTRY BLOCKED %s | confirm vol faded (%.0f < %.0f)",
            sym, m["vol_10s"], RT_CONFIRM_MIN_VOL_FRAC * st.arm_vol_10s
        )
        return

    if m["price_change"] < RT_MIN_PRICE_CHANGE_10S:
        logging.info(
            "ENTRY BLOCKED %s | price_change too low (%.4f%% < %.4f%%)",
            sym, m["price_change"] * 100.0, RT_MIN_PRICE_CHANGE_10S * 100.0
        )
        return

    if m["buy_ratio"] < RT_MIN_BUY_RATIO_10S:
        logging.info("ENTRY BLOCKED %s | buy_ratio too low (%.3f < %.3f)", sym, m["buy_ratio"], RT_MIN_BUY_RATIO_10S)
        return

    if m["imbalance"] < RT_MIN_IMBALANCE:
        logging.info("ENTRY BLOCKED %s | imbalance too low (%.3f < %.3f)", sym, m["imbalance"], RT_MIN_IMBALANCE)
        return

    if m["near_sell_wall"]:
        logging.info("ENTRY BLOCKED %s | near sell wall (anti-spoofed) raw=%s", sym, m.get("wall_raw", False))
        return

    logging.info("ENTRY PASSED ALL FILTERS %s", sym)

    # clear arm before DB work to avoid duplicates
    armed[sym] = None

    entry = m["price"]
    sl = entry * (1.0 - RT_SL_PCT)
    risk = entry - sl
    if risk <= 0:
        logging.info("ENTRY BLOCKED %s | invalid risk (entry=%.6f sl=%.6f)", sym, entry, sl)
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
            "⚡ ENTER_RT %s | entry=%.6f SL=%.6f TP=%.6f | "
            "Δ%ss=%.2f%% vol=%.0f buy=%.2f imbal=%.2f wall=%s(raw=%s) | "
            "rel=%.2fx z_mad=%.2f (med=%.0f mad=%.0f) abs_floor=%.0f | pos_n=%d all_n=%d",
            sym, entry, sl, tp,
            RT_WINDOW_SEC, m["price_change"] * 100.0, m["vol_10s"], m["buy_ratio"], m["imbalance"],
            m["near_sell_wall"], m.get("wall_raw", False),
            m["vol_rel"], m["vol_z_mad"], m["vol_med"], m["vol_mad"], m["abs_floor"],
            m["baseline_pos_n"], m["baseline_all_n"]
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
                    logging.info("Connected to Binance (ENTRY ONLY | SNAPSHOT+DIFF | PRO baseline_all+pos | ARM/CONFIRM | ANTI-SPOOF WALL)")

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

                                maker = bool(data["m"])  # True = buyer is maker => sell-initiated
                                taker_buy = (maker is False)

                                logging.info(
                                    "TRADE %s | price=%.6f | qty=%.6f | quote=%.2f | taker_buy=%s | T=%d",
                                    sym, price, qty, quote, taker_buy, ts_ms
                                )

                                rt_last_price[sym] = price
                                rt_trades[sym].append(
                                    TradePoint(ts_ms=ts_ms, price=price, quote=quote, taker_buy=taker_buy)
                                )
                                _prune_trades(sym, now_ms)

                                m = compute_metrics(sym, now_ts)
                                if m is not None:
                                    logging.info(
                                        "METRICS %s | price=%.6f Δ%ss=%.2f%% vol=%.0f buy=%.2f imbal=%.2f "
                                        "wall=%s(raw=%s) | abs_floor=%.0f | stats_ready=%s rel=%.2fx z_mad=%.2f | pos_n=%d all_n=%d",
                                        sym, m["price"], RT_WINDOW_SEC, m["price_change"] * 100.0, m["vol_10s"],
                                        m["buy_ratio"], m["imbalance"],
                                        "Y" if m["near_sell_wall"] else "N", "Y" if m.get("wall_raw") else "N",
                                        m["abs_floor"],
                                        m["stats_ready"], m["vol_rel"], m["vol_z_mad"],
                                        m["baseline_pos_n"], m["baseline_all_n"],
                                    )
                                    await maybe_enter(pool, sym, m, now_ts)
                                else:
                                    logging.info("METRICS %s | None (insufficient data yet)", sym)

                            # depthUpdate (diff)
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
                                    wall_tracker[sym] = None
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
    logging.info("Starting entry_service | symbols=%s", SYMBOLS)
    pool = await init_db_pool()

    # baseline sampler so warmup works even for sparse-trade coins
    asyncio.create_task(baseline_sampler_loop())

    await realtime_loop(pool)

if __name__ == "__main__":
    asyncio.run(main())
