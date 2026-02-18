# volume_entry_service.py
import os
import json
import asyncio
import logging
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
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ==========================================================
# ENV HELPERS
# ==========================================================
def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v.strip() if v and v.strip() else default

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
DB_HOST = env_str("DB_HOST", "db")
DB_NAME = env_str("DB_NAME", "pumpdb")
DB_USER = env_str("DB_USER", "pumpuser")
DB_PASSWORD = env_str("DB_PASSWORD", "pumpuser")

SYMBOLS = parse_symbols()

KLINE_INTERVAL = env_str("KLINE_INTERVAL", "5m")  # should be 5m

# Lookbacks
ANOM_LOOKBACK = env_int("ANOM_LOOKBACK", 20)  # baseline candles for volume anomaly
MAX_CACHE = env_int("MAX_CACHE", 200)          # cached candles per symbol

# Entry rules
MAX_MOVE_PCT = env_float("MAX_MOVE_PCT", 0.02)        # <= 2% move over 3 candles
MIN_MOVE_PCT = env_float("MIN_MOVE_PCT", 0.002)       # avoid micro-noise entries
MAX_UPPER_WICK_PCT = env_float("MAX_UPPER_WICK_PCT", 0.003)

# Volume anomaly thresholds (either condition is sufficient)
VOL_REL_TH = env_float("VOL_REL_TH", 2.5)       # v / median >= 2.5x
VOL_ZMAD_TH = env_float("VOL_ZMAD_TH", 6.0)     # |v-med|/mad >= 6

# Risk management (kept simple; exit-service handles closing)
RT_SL_PCT = env_float("RT_SL_PCT", 0.003)       # e.g. 0.3%
TP_R = env_float("TP_R", 1.0)                   # e.g. 1R

# Safety / throttling
COOLDOWN_SEC = env_int("COOLDOWN_SEC", 60 * 30)  # per symbol cooldown
CREATE_TABLES = env_int("CREATE_TABLES", 1)      # creates candles_5m if not exists

# ==========================================================
# DATA TYPES
# ==========================================================
@dataclass
class Candle:
    t: datetime
    o: float
    h: float
    l: float
    c: float
    v_base: float
    v_quote: float
    n_trades: Optional[int] = None

# ==========================================================
# STATS HELPERS
# ==========================================================
def median(xs: List[float]) -> float:
    xs2 = sorted(xs)
    n = len(xs2)
    if n == 0:
        return 0.0
    mid = n // 2
    if n % 2 == 1:
        return xs2[mid]
    return 0.5 * (xs2[mid - 1] + xs2[mid])

def mad(xs: List[float], med: float) -> float:
    dev = [abs(x - med) for x in xs]
    return median(dev)

def compute_anom_stats(vols: List[float], v: float) -> Tuple[float, float, float, float]:
    """
    returns: med, mad, rel, z_mad
    """
    med = median(vols)
    m = mad(vols, med)
    rel = (v / med) if med > 0 else 0.0
    z = (abs(v - med) / m) if m > 1e-12 else (999.0 if v != med else 0.0)
    return med, m, rel, z

# ==========================================================
# BINANCE WS
# ==========================================================
def ws_url(symbols: List[str], interval: str) -> str:
    streams = "/".join([f"{s.lower()}@kline_{interval}" for s in symbols])
    return f"wss://stream.binance.com:9443/stream?streams={streams}"

# ==========================================================
# GLOBAL STATE (in-memory)
# ==========================================================
candles: Dict[str, Deque[Candle]] = defaultdict(lambda: deque(maxlen=MAX_CACHE))
last_entry_ts: Dict[str, float] = defaultdict(lambda: 0.0)

# ==========================================================
# DB
# ==========================================================
async def db_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        min_size=1,
        max_size=5,
        command_timeout=30,
    )

async def ensure_candles_table(pool: asyncpg.Pool) -> None:
    """
    Creates only candles_5m (timeseries). Does NOT touch positions_open or trades tables.
    """
    if CREATE_TABLES == 0:
        logging.info("CREATE_TABLES=0 -> skipping candles_5m DDL")
        return

    sql = """
    CREATE EXTENSION IF NOT EXISTS timescaledb;

    CREATE TABLE IF NOT EXISTS candles_5m (
      symbol        TEXT        NOT NULL,
      ts            TIMESTAMPTZ NOT NULL,
      o             DOUBLE PRECISION NOT NULL,
      h             DOUBLE PRECISION NOT NULL,
      l             DOUBLE PRECISION NOT NULL,
      c             DOUBLE PRECISION NOT NULL,
      v_base        DOUBLE PRECISION NOT NULL,
      v_quote       DOUBLE PRECISION NOT NULL,
      trades_count  INTEGER,
      PRIMARY KEY (symbol, ts)
    );
    """
    async with pool.acquire() as conn:
        await conn.execute(sql)
        await conn.execute("SELECT create_hypertable('candles_5m','ts', if_not_exists => TRUE);")
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_candles_5m_symbol_ts_desc
            ON candles_5m (symbol, ts DESC);
        """)
    logging.info("candles_5m ensured")

async def upsert_candle(pool: asyncpg.Pool, sym: str, c: Candle) -> None:
    sql = """
    INSERT INTO candles_5m(symbol, ts, o, h, l, c, v_base, v_quote, trades_count)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
    ON CONFLICT(symbol, ts) DO UPDATE SET
      o=EXCLUDED.o, h=EXCLUDED.h, l=EXCLUDED.l, c=EXCLUDED.c,
      v_base=EXCLUDED.v_base, v_quote=EXCLUDED.v_quote,
      trades_count=EXCLUDED.trades_count;
    """
    async with pool.acquire() as conn:
        await conn.execute(
            sql, sym, c.t, c.o, c.h, c.l, c.c, c.v_base, c.v_quote, c.n_trades
        )

# ----------------------------------------------------------
# EXISTING TABLE INTEGRATION (matches your schema)
# positions_open(symbol PK, entry_time, entry_price, sl, tp, status, opened_at, updated_at)
# ----------------------------------------------------------
async def has_open_position(pool: asyncpg.Pool, sym: str) -> bool:
    sql = """
    SELECT 1
    FROM positions_open
    WHERE symbol = $1
      AND status = 'OPEN'
    LIMIT 1;
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, sym)
        return row is not None

async def insert_open_position(pool: asyncpg.Pool, sym: str, entry_time: datetime, entry_price: float, sl: float, tp: float) -> bool:
    """
    Inserts a new OPEN position for symbol (symbol is PRIMARY KEY).
    Returns True if inserted, False if already exists.
    """
    sql = """
    INSERT INTO positions_open(symbol, entry_time, entry_price, sl, tp, status, opened_at, updated_at)
    VALUES ($1, $2, $3, $4, $5, 'OPEN', NOW(), NOW())
    ON CONFLICT (symbol) DO NOTHING;
    """
    async with pool.acquire() as conn:
        res = await conn.execute(sql, sym, entry_time, entry_price, sl, tp)
        # asyncpg returns string like "INSERT 0 1" or "INSERT 0 0"
        return res.endswith("1")

# ==========================================================
# ENTRY SIGNAL
# ==========================================================
def is_green(c: Candle) -> bool:
    return c.c > c.o

def upper_wick_pct(c: Candle) -> float:
    if c.c <= 0:
        return 1.0
    return max(0.0, (c.h - c.c) / c.c)

def check_entry(sym: str, dq: Deque[Candle]) -> Tuple[bool, str]:
    """
    Conditions:
      - last 3 closed candles are green
      - each of them has anomalous volume vs preceding ANOM_LOOKBACK candles
      - move from open(c1) -> close(c3) <= MAX_MOVE_PCT and >= MIN_MOVE_PCT
      - upper wick on c3 <= MAX_UPPER_WICK_PCT
      - breakout: close(c3) > max high of previous ANOM_LOOKBACK candles (excluding the last 3)
    """
    need = ANOM_LOOKBACK + 3 + 1
    if len(dq) < need:
        return False, "not_enough_candles"

    c1, c2, c3 = dq[-3], dq[-2], dq[-1]

    if not (is_green(c1) and is_green(c2) and is_green(c3)):
        return False, "not_all_green"

    move = (c3.c - c1.o) / c1.o if c1.o > 0 else 0.0
    if move <= 0:
        return False, "move_nonpositive"
    if move > MAX_MOVE_PCT:
        return False, f"move_too_big({move:.4f})"
    if move < MIN_MOVE_PCT:
        return False, f"move_too_small({move:.4f})"

    uw = upper_wick_pct(c3)
    if uw > MAX_UPPER_WICK_PCT:
        return False, f"upper_wick_too_big({uw:.4f})"

    # breakout check
    lookback = list(dq)[-(3 + ANOM_LOOKBACK):-3]  # previous ANOM_LOOKBACK, excluding last 3
    max_high = max(x.h for x in lookback)
    if c3.c <= max_high:
        return False, "no_breakout"

    # volume anomaly checks for each of the 3 candles vs its own preceding baseline window
    details = []
    for i, cc in enumerate([c1, c2, c3], start=1):
        # find baseline window ending right before cc
        cc_idx = len(dq) - (4 - i)  # 1-based-ish position of cc in dq
        base = list(dq)[cc_idx - 1 - ANOM_LOOKBACK : cc_idx - 1]
        vols = [x.v_quote for x in base]

        med, m, rel, z = compute_anom_stats(vols, cc.v_quote)
        ok = (rel >= VOL_REL_TH) or (z >= VOL_ZMAD_TH)
        if not ok:
            return False, f"vol_not_anom(c{i}) rel={rel:.2f} z={z:.2f} med={med:.1f} mad={m:.1f}"

        details.append(f"c{i}:rel={rel:.2f} z={z:.2f}")

    return True, f"OK move={move:.4f} uw={uw:.4f} break>{max_high:.6f} " + " ".join(details)

def compute_sl_tp(entry: float) -> Tuple[float, float]:
    sl = entry * (1.0 - RT_SL_PCT)
    risk = entry - sl
    if risk <= 0:
        sl = entry * 0.999
        risk = entry - sl
    tp = entry + TP_R * risk
    return sl, tp

# ==========================================================
# EVENT HANDLER
# ==========================================================
async def on_closed_candle(pool: asyncpg.Pool, sym: str, c: Candle) -> None:
    # Persist candle
    await upsert_candle(pool, sym, c)

    # Update in-memory
    dq = candles[sym]
    dq.append(c)

    # Cooldown
    now = c.t.timestamp()
    if now - last_entry_ts[sym] < COOLDOWN_SEC:
        return

    # Avoid duplicate position
    if await has_open_position(pool, sym):
        return

    ok, why = check_entry(sym, dq)
    if not ok:
        logging.info("NO_ENTRY %s | %s", sym, why)
        return

    entry = c.c
    sl, tp = compute_sl_tp(entry)

    inserted = await insert_open_position(pool, sym, c.t, entry, sl, tp)
    if not inserted:
        # someone else inserted (race / duplicate)
        return

    last_entry_ts[sym] = now
    logging.warning("🟢 ENTER_5M %s entry=%.8f SL=%.8f TP=%.8f | %s", sym, entry, sl, tp, why)

# ==========================================================
# WS LOOP
# ==========================================================
async def ws_loop(pool: asyncpg.Pool) -> None:
    url = ws_url(SYMBOLS, KLINE_INTERVAL)

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                logging.info("Connected to Binance (klines %s) | symbols=%d", KLINE_INTERVAL, len(SYMBOLS))

                async for message in ws:
                    payload = json.loads(message)
                    k = payload.get("data", {}).get("k", {})
                    if not k:
                        continue

                    # only closed candles
                    if not bool(k.get("x")):
                        continue

                    sym = (k.get("s") or "").upper()
                    if sym not in SYMBOLS:
                        continue

                    t = datetime.fromtimestamp(int(k["t"]) / 1000.0, tz=timezone.utc)

                    candle = Candle(
                        t=t,
                        o=float(k["o"]),
                        h=float(k["h"]),
                        l=float(k["l"]),
                        c=float(k["c"]),
                        v_base=float(k["v"]),
                        v_quote=float(k["q"]),
                        n_trades=int(k["n"]) if k.get("n") is not None else None,
                    )

                    await on_closed_candle(pool, sym, candle)

        except Exception as e:
            logging.exception("WS error: %s", e)
            await asyncio.sleep(3)

# ==========================================================
# MAIN
# ==========================================================
async def main() -> None:
    pool = await db_pool()
    await ensure_candles_table(pool)
    await ws_loop(pool)

if __name__ == "__main__":
    asyncio.run(main())
