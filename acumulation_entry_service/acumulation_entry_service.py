# accumulation_service.py
import os
import json
import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional

import asyncpg
import websockets

# ----------------------------
# ENV helpers
# ----------------------------
def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    return int(v) if v else default

def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    return float(v) if v else default

def parse_symbols() -> List[str]:
    raw = os.getenv("SYMBOLS", "").strip()
    if not raw:
        raise RuntimeError("SYMBOLS is empty")
    return [s.strip().upper() for s in raw.split(",") if s.strip()]

def tf_to_minutes(tf: str) -> int:
    tf = tf.lower().strip()
    if tf.endswith("m"):
        return int(tf[:-1])
    if tf.endswith("h"):
        return int(tf[:-1]) * 60
    raise ValueError(f"Unsupported TF={tf}")

def safe_div(a: float, b: float) -> float:
    return a / b if b and b > 0 else 0.0

# ----------------------------
# CONFIG (defaults are OK)
# ----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
HEARTBEAT_EVERY = env_int("HEARTBEAT_EVERY", 50)  # heartbeat per N closed candles processed
DEBUG_EVERY = env_int("DEBUG_EVERY", 10)          # per-candle debug per N candles

TF = os.getenv("TF", "5m").strip()

ACC_WIN_H = env_int("ACC_WIN_H", 6)
ACC_BASE_H = env_int("ACC_BASE_H", 24)

ACC_RANGE_PCT_MAX = env_float("ACC_RANGE_PCT_MAX", 0.03)
ACC_VOL_REL_MIN = env_float("ACC_VOL_REL_MIN", 1.10)
ACC_VOL_REL_MAX = env_float("ACC_VOL_REL_MAX", 1.80)
ACC_BUY_RATIO_MIN = env_float("ACC_BUY_RATIO_MIN", 0.55)

ACC_SUPPORT_TOUCH_EPS = env_float("ACC_SUPPORT_TOUCH_EPS", 0.002)
ACC_SUPPORT_TOUCHES_MIN = env_int("ACC_SUPPORT_TOUCHES_MIN", 3)
ACC_BUY_RATIO_TOUCH_MIN = env_float("ACC_BUY_RATIO_TOUCH_MIN", 0.55)

ACC_SWING_LOOKBACK = env_int("ACC_SWING_LOOKBACK", 72)
ACC_SWING_MIN_COUNT = env_int("ACC_SWING_MIN_COUNT", 3)

SYMBOLS = parse_symbols()

TF_MIN = tf_to_minutes(TF)
if TF_MIN <= 0 or (60 % TF_MIN) != 0:
    raise RuntimeError(f"TF must divide an hour cleanly for this implementation. Got TF={TF} ({TF_MIN}min)")

CANDLES_PER_H = 60 // TF_MIN
WIN_N = ACC_WIN_H * CANDLES_PER_H
BASE_N = ACC_BASE_H * CANDLES_PER_H

# ----------------------------
# LOGGING
# ----------------------------
logging.basicConfig(
    level=logging.INFO,  # baseline, we set final level below
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# ----------------------------
# DB
# ----------------------------
DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "pumpdb")
DB_USER = os.getenv("DB_USER", "pumpuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS candles_5m_ac (
  symbol TEXT NOT NULL,
  open_time TIMESTAMPTZ NOT NULL,
  close_time TIMESTAMPTZ NOT NULL,
  o DOUBLE PRECISION NOT NULL,
  h DOUBLE PRECISION NOT NULL,
  l DOUBLE PRECISION NOT NULL,
  c DOUBLE PRECISION NOT NULL,
  v_base DOUBLE PRECISION NOT NULL,
  v_quote DOUBLE PRECISION NOT NULL,
  taker_buy_base DOUBLE PRECISION NOT NULL,
  taker_buy_quote DOUBLE PRECISION NOT NULL,
  trade_count INT NOT NULL,
  PRIMARY KEY(symbol, open_time)
);

CREATE INDEX IF NOT EXISTS idx_candles_5m_ac_symbol_time
  ON candles_5m_ac(symbol, open_time DESC);

CREATE TABLE IF NOT EXISTS accum_features_5m (
  symbol TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  win_h INT NOT NULL,
  base_h INT NOT NULL,

  range_pct DOUBLE PRECISION NOT NULL,
  vol_rel DOUBLE PRECISION NOT NULL,
  buy_ratio_win DOUBLE PRECISION NOT NULL,
  support_price DOUBLE PRECISION NOT NULL,
  support_touches INT NOT NULL,
  buy_ratio_on_touches DOUBLE PRECISION NOT NULL,

  swing_lows_found INT NOT NULL,
  higher_lows BOOL NOT NULL,

  is_accum BOOL NOT NULL,
  reason TEXT NOT NULL,

  PRIMARY KEY(symbol, ts)
);

CREATE INDEX IF NOT EXISTS idx_accum_features_5m_symbol_ts
  ON accum_features_5m(symbol, ts DESC);

CREATE TABLE IF NOT EXISTS accum_signals (
  id BIGSERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  signal TEXT NOT NULL,
  details JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_accum_signals_symbol_ts
  ON accum_signals(symbol, ts DESC);
"""

INSERT_CANDLE_SQL = """
INSERT INTO candles_5m_ac(
  symbol, open_time, close_time,
  o,h,l,c,
  v_base, v_quote,
  taker_buy_base, taker_buy_quote,
  trade_count
)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
ON CONFLICT(symbol, open_time) DO UPDATE SET
  close_time=EXCLUDED.close_time,
  o=EXCLUDED.o, h=EXCLUDED.h, l=EXCLUDED.l, c=EXCLUDED.c,
  v_base=EXCLUDED.v_base, v_quote=EXCLUDED.v_quote,
  taker_buy_base=EXCLUDED.taker_buy_base, taker_buy_quote=EXCLUDED.taker_buy_quote,
  trade_count=EXCLUDED.trade_count
"""

SELECT_LAST_N_SQL = """
SELECT open_time, close_time, o,h,l,c, v_quote, taker_buy_quote
FROM candles_5m_ac
WHERE symbol=$1
ORDER BY open_time DESC
LIMIT $2
"""

UPSERT_FEATURE_SQL = """
INSERT INTO accum_features_5m(
  symbol, ts, win_h, base_h,
  range_pct, vol_rel, buy_ratio_win,
  support_price, support_touches, buy_ratio_on_touches,
  swing_lows_found, higher_lows,
  is_accum, reason
)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
ON CONFLICT(symbol, ts) DO UPDATE SET
  range_pct=EXCLUDED.range_pct,
  vol_rel=EXCLUDED.vol_rel,
  buy_ratio_win=EXCLUDED.buy_ratio_win,
  support_price=EXCLUDED.support_price,
  support_touches=EXCLUDED.support_touches,
  buy_ratio_on_touches=EXCLUDED.buy_ratio_on_touches,
  swing_lows_found=EXCLUDED.swing_lows_found,
  higher_lows=EXCLUDED.higher_lows,
  is_accum=EXCLUDED.is_accum,
  reason=EXCLUDED.reason
"""

INSERT_SIGNAL_SQL = """
INSERT INTO accum_signals(symbol, ts, signal, details)
VALUES($1,$2,$3,$4::jsonb)
"""

# ----------------------------
# Runtime counters (heartbeat)
# ----------------------------
CANDLE_COUNTER = 0
LAST_CLOSE_TIME: Optional[datetime] = None
DB_LAT_MS_SUM = 0.0
DB_LAT_MS_N = 0

# ----------------------------
# Core logic
# ----------------------------
@dataclass
class Row:
    open_time: datetime
    close_time: datetime
    o: float
    h: float
    l: float
    c: float
    vq: float
    tbq: float

def compute_swing_lows(lows: List[float]) -> List[Tuple[int, float]]:
    """
    Simple fractal swing low:
      low[i] is swing low if low[i] < low[i-1] and low[i] < low[i+1]
    Returns list of (idx, low) in chronological order (idx increasing).
    """
    swings: List[Tuple[int, float]] = []
    for i in range(1, len(lows) - 1):
        if lows[i] < lows[i - 1] and lows[i] < lows[i + 1]:
            swings.append((i, lows[i]))
    return swings

def is_higher_lows(swings: List[Tuple[int, float]], min_count: int = 3) -> Tuple[bool, int]:
    if len(swings) < min_count:
        return False, len(swings)
    last = swings[-min_count:]
    vals = [v for _, v in last]
    ok = vals[0] < vals[1] < vals[2]
    return ok, len(swings)

def detect_accum(rows_chron: List[Row]) -> Tuple[bool, Dict, str]:
    """
    Detect accumulation phase:
      - range in last WIN_N is tight
      - volume in last WIN_N is mildly elevated vs BASE_N
      - buy ratio in WIN_N is mildly > 0.5
      - support is "defended": repeated touches near min low with decent buy ratio
      - higher lows present (simple swing-low fractal)
    """
    if len(rows_chron) < max(WIN_N, BASE_N // 4):
        return False, {}, f"not_enough_data(n={len(rows_chron)}, need~{WIN_N})"

    win = rows_chron[-WIN_N:] if len(rows_chron) >= WIN_N else rows_chron
    base = rows_chron[-BASE_N:] if len(rows_chron) >= BASE_N else rows_chron

    max_h = max(r.h for r in win)
    min_l = min(r.l for r in win)
    mid = (max_h + min_l) / 2.0
    range_pct = safe_div((max_h - min_l), mid)

    avg_vq_win = sum(r.vq for r in win) / max(1, len(win))
    avg_vq_base = sum(r.vq for r in base) / max(1, len(base))
    vol_rel = safe_div(avg_vq_win, avg_vq_base)

    sum_tbq_win = sum(r.tbq for r in win)
    sum_vq_win = sum(r.vq for r in win)
    buy_ratio_win = safe_div(sum_tbq_win, sum_vq_win)

    support_price = min_l
    eps = ACC_SUPPORT_TOUCH_EPS
    touch_rows = [r for r in win if r.l <= support_price * (1.0 + eps)]
    support_touches = len(touch_rows)
    buy_ratio_on_touches = safe_div(sum(r.tbq for r in touch_rows), sum(r.vq for r in touch_rows))

    lookback = win[-min(len(win), ACC_SWING_LOOKBACK):]
    lows = [r.l for r in lookback]
    swings = compute_swing_lows(lows)
    higher_lows, swing_found = is_higher_lows(swings, ACC_SWING_MIN_COUNT)

    feats = {
        "range_pct": range_pct,
        "vol_rel": vol_rel,
        "buy_ratio_win": buy_ratio_win,
        "support_price": support_price,
        "support_touches": support_touches,
        "buy_ratio_on_touches": buy_ratio_on_touches,
        "swing_lows_found": swing_found,
        "higher_lows": higher_lows,
        "win_max_h": max_h,
        "win_min_l": min_l,
        "avg_vq_win": avg_vq_win,
        "avg_vq_base": avg_vq_base,
    }

    if range_pct > ACC_RANGE_PCT_MAX:
        return False, feats, f"range_too_wide({range_pct:.4f})"

    if not (ACC_VOL_REL_MIN <= vol_rel <= ACC_VOL_REL_MAX):
        return False, feats, f"vol_rel_out_of_band({vol_rel:.3f})"

    if buy_ratio_win < ACC_BUY_RATIO_MIN:
        return False, feats, f"buy_ratio_low({buy_ratio_win:.3f})"

    if support_touches < ACC_SUPPORT_TOUCHES_MIN:
        return False, feats, f"not_enough_support_touches({support_touches})"

    if buy_ratio_on_touches < ACC_BUY_RATIO_TOUCH_MIN:
        return False, feats, f"support_not_defended(buy_touch={buy_ratio_on_touches:.3f})"

    if not higher_lows:
        return False, feats, f"no_higher_lows(swings={swing_found})"

    return True, feats, "ACCUM_PHASE"

# ----------------------------
# Binance WS
# ----------------------------
def ws_url(symbols: List[str], tf: str) -> str:
    streams = "/".join([f"{s.lower()}@kline_{tf}" for s in symbols])
    return f"wss://stream.binance.com:9443/stream?streams={streams}"

async def init_db(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(CREATE_SQL)
    logging.info("DB init OK (tables ensured)")

async def on_closed_kline(pool: asyncpg.Pool, symbol: str, k: dict) -> None:
    global CANDLE_COUNTER, LAST_CLOSE_TIME, DB_LAT_MS_SUM, DB_LAT_MS_N

    open_time = datetime.fromtimestamp(int(k["t"]) / 1000.0, tz=timezone.utc)
    close_time = datetime.fromtimestamp(int(k["T"]) / 1000.0, tz=timezone.utc)

    o = float(k["o"]); h = float(k["h"]); l = float(k["l"]); c = float(k["c"])
    v_base = float(k["v"])
    v_quote = float(k["q"])
    taker_buy_base = float(k["V"])
    taker_buy_quote = float(k["Q"])
    trade_count = int(k["n"])

    t0 = time.perf_counter()
    async with pool.acquire() as conn:
        # 1) store candle
        await conn.execute(
            INSERT_CANDLE_SQL,
            symbol, open_time, close_time,
            o, h, l, c,
            v_base, v_quote, taker_buy_base, taker_buy_quote,
            trade_count
        )

        # 2) fetch recent candles for analysis
        need = max(BASE_N, WIN_N, ACC_SWING_LOOKBACK)
        rows = await conn.fetch(SELECT_LAST_N_SQL, symbol, need)

    db_ms = (time.perf_counter() - t0) * 1000.0
    DB_LAT_MS_SUM += db_ms
    DB_LAT_MS_N += 1

    # convert newest->oldest into chronological
    rows_chron: List[Row] = []
    for r in reversed(rows):
        rows_chron.append(Row(
            open_time=r["open_time"],
            close_time=r["close_time"],
            o=float(r["o"]), h=float(r["h"]), l=float(r["l"]), c=float(r["c"]),
            vq=float(r["v_quote"]),
            tbq=float(r["taker_buy_quote"]),
        ))

    ok, feats, reason = detect_accum(rows_chron)

    # write features + signals (separate DB acquire to avoid holding it too long)
    t1 = time.perf_counter()
    async with pool.acquire() as conn:
        if feats:
            await conn.execute(
                UPSERT_FEATURE_SQL,
                symbol, close_time, ACC_WIN_H, ACC_BASE_H,
                float(feats["range_pct"]),
                float(feats["vol_rel"]),
                float(feats["buy_ratio_win"]),
                float(feats["support_price"]),
                int(feats["support_touches"]),
                float(feats["buy_ratio_on_touches"]),
                int(feats["swing_lows_found"]),
                bool(feats["higher_lows"]),
                bool(ok),
                reason
            )

        if ok:
            details = {
                "tf": TF,
                "tf_min": TF_MIN,
                "win_h": ACC_WIN_H,
                "base_h": ACC_BASE_H,
                # core metrics
                "range_pct": feats["range_pct"],
                "vol_rel": feats["vol_rel"],
                "buy_ratio_win": feats["buy_ratio_win"],
                "support_price": feats["support_price"],
                "support_touches": feats["support_touches"],
                "buy_ratio_on_touches": feats["buy_ratio_on_touches"],
                "higher_lows": feats["higher_lows"],
                "swing_lows_found": feats["swing_lows_found"],
                # helpful debug context
                "win_max_h": feats["win_max_h"],
                "win_min_l": feats["win_min_l"],
                "avg_vq_win": feats["avg_vq_win"],
                "avg_vq_base": feats["avg_vq_base"],
            }
            await conn.execute(
                INSERT_SIGNAL_SQL,
                symbol, close_time, "ACCUM_PHASE", json.dumps(details)
            )

    db_ms2 = (time.perf_counter() - t1) * 1000.0
    DB_LAT_MS_SUM += db_ms2
    DB_LAT_MS_N += 1

    # Counters + heartbeat + debug
    CANDLE_COUNTER += 1
    LAST_CLOSE_TIME = close_time

    # Per-candle debug (every DEBUG_EVERY)
    if feats and (CANDLE_COUNTER % DEBUG_EVERY == 0):
        logging.info(
            "AC_DEBUG %s | close=%s | reason=%s | range=%.2f%% vol_rel=%.2fx buy=%.2f "
            "touches=%d touch_buy=%.2f support=%.6f higher_lows=%s swings=%d | db=%.1fms",
            symbol,
            close_time.isoformat(),
            reason,
            feats["range_pct"] * 100.0,
            feats["vol_rel"],
            feats["buy_ratio_win"],
            feats["support_touches"],
            feats["buy_ratio_on_touches"],
            feats["support_price"],
            feats["higher_lows"],
            feats["swing_lows_found"],
            (db_ms + db_ms2),
        )

    # Signal log (always)
    if ok:
        logging.warning(
            "🟦 ACCUM_PHASE %s | tf=%s range=%.2f%% vol_rel=%.2fx buy=%.2f touches=%d touch_buy=%.2f "
            "support=%.6f | win=[%.6f..%.6f] avgVq(win/base)=%.0f/%.0f",
            symbol, TF,
            feats["range_pct"] * 100.0,
            feats["vol_rel"],
            feats["buy_ratio_win"],
            feats["support_touches"],
            feats["buy_ratio_on_touches"],
            feats["support_price"],
            feats["win_min_l"],
            feats["win_max_h"],
            feats["avg_vq_win"],
            feats["avg_vq_base"],
        )

    # Heartbeat
    if CANDLE_COUNTER % HEARTBEAT_EVERY == 0:
        avg_db = (DB_LAT_MS_SUM / DB_LAT_MS_N) if DB_LAT_MS_N else 0.0
        logging.info(
            "HEARTBEAT | candles=%d | last_close=%s | symbols=%d | tf=%s | avg_db=%.1fms",
            CANDLE_COUNTER,
            (LAST_CLOSE_TIME.isoformat() if LAST_CLOSE_TIME else "n/a"),
            len(SYMBOLS),
            TF,
            avg_db,
        )

def _chunk_symbols(symbols: List[str], chunk_size: int) -> List[List[str]]:
    return [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]

async def ws_loop(pool: asyncpg.Pool) -> None:
    # Binance has limits for streams per connection; 20 is fine, but we keep chunking configurable.
    chunk_size = env_int("WS_SYMBOLS_PER_CONN", 40)
    groups = _chunk_symbols(SYMBOLS, chunk_size)

    logging.info("WS groups=%d (chunk_size=%d)", len(groups), chunk_size)

    while True:
        try:
            tasks = []
            for idx, g in enumerate(groups, start=1):
                url = ws_url(g, TF)
                tasks.append(asyncio.create_task(ws_loop_one(pool, url, idx, len(g))))
            await asyncio.gather(*tasks)
        except Exception as e:
            logging.exception("Top-level WS supervisor error: %s | restarting in 3s", e)
            await asyncio.sleep(3)

async def ws_loop_one(pool: asyncpg.Pool, url: str, group_idx: int, n_symbols: int) -> None:
    while True:
        try:
            logging.info("Connecting WS group=%d symbols=%d | %s", group_idx, n_symbols, url)
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                logging.info("Connected WS group=%d symbols=%d", group_idx, n_symbols)
                async for message in ws:
                    payload = json.loads(message)
                    k = payload.get("data", {}).get("k", {})
                    if not k:
                        continue
                    if not bool(k.get("x")):
                        continue  # only closed candles

                    symbol = (k.get("s") or "").upper()
                    # No need to check membership; stream already filtered by URL.
                    await on_closed_kline(pool, symbol, k)

        except Exception as e:
            logging.exception("WS group=%d error: %s | reconnecting in 3s", group_idx, e)
            await asyncio.sleep(3)

async def main() -> None:
    logging.info(
        "Starting accumulation_service | tf=%s (%dmin) | win=%dh (%d candles) | base=%dh (%d candles) | symbols=%d",
        TF, TF_MIN, ACC_WIN_H, WIN_N, ACC_BASE_H, BASE_N, len(SYMBOLS)
    )
    logging.info(
        "Thresholds | range<=%.2f%% | vol_rel=[%.2f..%.2f] | buy_ratio>=%.2f | "
        "touches>=%d eps=%.3f%% touch_buy>=%.2f | swing_min=%d lookback=%d | log=%s hb=%d dbg=%d",
        ACC_RANGE_PCT_MAX * 100.0,
        ACC_VOL_REL_MIN, ACC_VOL_REL_MAX,
        ACC_BUY_RATIO_MIN,
        ACC_SUPPORT_TOUCHES_MIN,
        ACC_SUPPORT_TOUCH_EPS * 100.0,
        ACC_BUY_RATIO_TOUCH_MIN,
        ACC_SWING_MIN_COUNT,
        ACC_SWING_LOOKBACK,
        LOG_LEVEL, HEARTBEAT_EVERY, DEBUG_EVERY
    )

    pool = await asyncpg.create_pool(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        min_size=1, max_size=5
    )
    logging.info("DB pool created | host=%s db=%s user=%s", DB_HOST, DB_NAME, DB_USER)

    await init_db(pool)
    await ws_loop(pool)

if __name__ == "__main__":
    asyncio.run(main())
