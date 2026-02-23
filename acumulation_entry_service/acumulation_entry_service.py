# accumulation_service.py
# ------------------------------------------------------------
# Detects ACCUM_PHASE on Binance klines (TF, default 5m),
# stores candles + computed features into Timescale/Postgres,
# logs signals, and defines a VIRTUAL plan:
#
#   - VIRTUAL PLAN (no real orders in this service) is created EARLY when:
#       support_touches >= (ACC_SUPPORT_TOUCHES_MIN - 1),
#       and all OTHER accumulation rules are satisfied:
#         range/vol/buy_ratio/support defense/higher lows
#
# IMPORTANT (per request):
#   - Removed ALL "recent last 30 minutes" constraints
#   - Removed ALL profit constraints for creating the virtual plan
#     (profit_* is still computed/stored/logged, but NOT used to gate plan creation)
#
# NEW:
#   - Writes a "trade intent" into DB (trade_intents) for executor module
#   - Enforces "max 1 open position per symbol":
#       if positions_open has OPEN for symbol => do NOT create new intent
# ------------------------------------------------------------

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

# ==========================================================
# ENV helpers
# ==========================================================
def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    return int(v) if v else default

def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    return float(v) if v else default

def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")

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

# ==========================================================
# CONFIG
# ==========================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
HEARTBEAT_EVERY = env_int("HEARTBEAT_EVERY", 50)
DEBUG_EVERY = env_int("DEBUG_EVERY", 10)

TF = os.getenv("TF", "5m").strip()

# --- windows ---
ACC_WIN_H = env_int("ACC_WIN_H", 2)
ACC_BASE_H = env_int("ACC_BASE_H", 4)

# --- thresholds ---
ACC_RANGE_PCT_MAX = env_float("ACC_RANGE_PCT_MAX", 0.03)
ACC_VOL_REL_MIN = env_float("ACC_VOL_REL_MIN", 1.10)
ACC_VOL_REL_MAX = env_float("ACC_VOL_REL_MAX", 1.80)
ACC_BUY_RATIO_MIN = env_float("ACC_BUY_RATIO_MIN", 0.55)

ACC_SUPPORT_TOUCH_EPS = env_float("ACC_SUPPORT_TOUCH_EPS", 0.002)
ACC_SUPPORT_TOUCHES_MIN = env_int("ACC_SUPPORT_TOUCHES_MIN", 3)
ACC_BUY_RATIO_TOUCH_MIN = env_float("ACC_BUY_RATIO_TOUCH_MIN", 0.55)

ACC_SWING_LOOKBACK = env_int("ACC_SWING_LOOKBACK", 36)
ACC_SWING_MIN_COUNT = env_int("ACC_SWING_MIN_COUNT", 2)  # you said: 2

# --- breakdown confirm (Variant B) ---
ACC_BREAK_CONFIRM_CANDLES = env_int("ACC_BREAK_CONFIRM_CANDLES", 2)
ACC_BREAK_EPS = env_float("ACC_BREAK_EPS", 0.0)

# --- profit zone (computed/stored/logged) ---
ACC_PROFIT_PCT = env_float("ACC_PROFIT_PCT", 0.007)              # support + 0.7%
ACC_ENTRY_OFFSET_PCT = env_float("ACC_ENTRY_OFFSET_PCT", 0.001)  # support + 0.1%
ACC_PROFIT_HITS_MIN = env_int("ACC_PROFIT_HITS_MIN", 3)          # kept for logging/features

# --- virtual plan behavior ---
ACC_PLACE_VIRTUAL_ORDERS = env_bool("ACC_PLACE_VIRTUAL_ORDERS", True)
ACC_SIMULATE_FILLS = env_bool("ACC_SIMULATE_FILLS", False)

# --- real trade intents (executor will send orders) ---
ACC_REAL_TRADE_USDT = env_float("ACC_REAL_TRADE_USDT", 10.0)
ACC_WRITE_INTENTS = env_bool("ACC_WRITE_INTENTS", True)

SYMBOLS = parse_symbols()

TF_MIN = tf_to_minutes(TF)
if TF_MIN <= 0 or (60 % TF_MIN) != 0:
    raise RuntimeError(f"TF must divide an hour cleanly. Got TF={TF} ({TF_MIN}min)")

CANDLES_PER_H = 60 // TF_MIN
WIN_N = ACC_WIN_H * CANDLES_PER_H
BASE_N = ACC_BASE_H * CANDLES_PER_H

# ==========================================================
# LOGGING
# ==========================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# ==========================================================
# DB
# ==========================================================
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

  profit_low DOUBLE PRECISION NOT NULL,
  profit_high DOUBLE PRECISION NOT NULL,
  profit_hits INT NOT NULL,

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

-- trade intents for executor (real orders)
CREATE TABLE IF NOT EXISTS trade_intents (
  id BIGSERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL DEFAULT 'ACCUM',
  side TEXT NOT NULL DEFAULT 'BUY',
  quote_amount DOUBLE PRECISION NOT NULL,
  limit_price DOUBLE PRECISION NOT NULL,
  support_price DOUBLE PRECISION,
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'NEW',      -- NEW | SENT | FILLED | CANCELLED | ERROR
  order_id BIGINT,
  client_order_id TEXT,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_trade_intents_symbol_ts
  ON trade_intents(symbol, ts);

-- only one pending intent per symbol (prevents spam)
CREATE UNIQUE INDEX IF NOT EXISTS uq_trade_intents_symbol_pending
ON trade_intents(symbol)
WHERE status IN ('NEW','SENT');

CREATE INDEX IF NOT EXISTS idx_trade_intents_status_created
  ON trade_intents(status, created_at);

-- ensure columns exist (executor uses these)
ALTER TABLE positions_open
ADD COLUMN IF NOT EXISTS qty DOUBLE PRECISION;

ALTER TABLE positions_open
ADD COLUMN IF NOT EXISTS entry_order_id BIGINT;
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
  profit_low, profit_high, profit_hits,
  is_accum, reason
)
VALUES($1,$2,$3,$4,
       $5,$6,$7,
       $8,$9,$10,
       $11,$12,
       $13,$14,$15,
       $16,$17)
ON CONFLICT(symbol, ts) DO UPDATE SET
  range_pct=EXCLUDED.range_pct,
  vol_rel=EXCLUDED.vol_rel,
  buy_ratio_win=EXCLUDED.buy_ratio_win,
  support_price=EXCLUDED.support_price,
  support_touches=EXCLUDED.support_touches,
  buy_ratio_on_touches=EXCLUDED.buy_ratio_on_touches,
  swing_lows_found=EXCLUDED.swing_lows_found,
  higher_lows=EXCLUDED.higher_lows,
  profit_low=EXCLUDED.profit_low,
  profit_high=EXCLUDED.profit_high,
  profit_hits=EXCLUDED.profit_hits,
  is_accum=EXCLUDED.is_accum,
  reason=EXCLUDED.reason
"""

INSERT_SIGNAL_SQL = """
INSERT INTO accum_signals(symbol, ts, signal, details)
VALUES($1,$2,$3,$4::jsonb)
"""

INSERT_INTENT_SQL = """
INSERT INTO trade_intents(
  symbol, ts, source, side, quote_amount, limit_price, support_price, meta, status
)
VALUES($1,$2,'ACCUM','BUY',$3,$4,$5,$6::jsonb,'NEW')
ON CONFLICT(symbol, ts) DO NOTHING
"""

HAS_OPEN_POSITION_SQL = """
SELECT 1 FROM positions_open WHERE symbol=$1 AND status='OPEN' LIMIT 1
"""

# ==========================================================
# Runtime counters
# ==========================================================
CANDLE_COUNTER = 0
LAST_CLOSE_TIME: Optional[datetime] = None
DB_LAT_MS_SUM = 0.0
DB_LAT_MS_N = 0

# ==========================================================
# Core
# ==========================================================
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
    swings: List[Tuple[int, float]] = []
    for i in range(1, len(lows) - 1):
        if lows[i] < lows[i - 1] and lows[i] < lows[i + 1]:
            swings.append((i, lows[i]))
    return swings

def is_higher_lows(swings: List[Tuple[int, float]], min_count: int) -> Tuple[bool, int]:
    if min_count <= 1:
        return True, len(swings)
    if len(swings) < min_count:
        return False, len(swings)
    last = swings[-min_count:]
    vals = [v for _, v in last]
    ok = all(vals[i] < vals[i + 1] for i in range(len(vals) - 1))
    return ok, len(swings)

def _profit_zone(support_price: float, win_max_h: float) -> Tuple[float, float, bool]:
    profit_low = support_price * (1.0 + ACC_PROFIT_PCT)
    profit_high = win_max_h
    ok = profit_low < profit_high
    return profit_low, profit_high, ok

def detect_accum(rows_chron: List[Row]) -> Tuple[bool, Dict, str]:
    """
    ACCUM_PHASE:
      - tight range in WIN
      - mild elevated volume vs BASE
      - buy ratio >= threshold
      - defended support: touches near support with decent buy ratio
      - higher lows present
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

    profit_low, profit_high, profit_ok = _profit_zone(support_price, max_h)
    profit_hits = 0
    if profit_ok:
        profit_hits = sum(1 for r in win if r.h >= profit_low)

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
        "profit_low": profit_low,
        "profit_high": profit_high,
        "profit_ok": profit_ok,
        "profit_hits": profit_hits,
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

def accum_other_rules_ok(feats: Dict) -> Tuple[bool, str]:
    """
    True pokud jsou splněná všechna pravidla pro akumulaci KROMĚ plného počtu support_touches.
    """
    if not feats:
        return False, "no_feats"

    range_pct = float(feats.get("range_pct", 0.0))
    vol_rel = float(feats.get("vol_rel", 0.0))
    buy_ratio_win = float(feats.get("buy_ratio_win", 0.0))
    buy_ratio_on_touches = float(feats.get("buy_ratio_on_touches", 0.0))
    higher_lows = bool(feats.get("higher_lows", False))

    if range_pct > ACC_RANGE_PCT_MAX:
        return False, f"range_too_wide({range_pct:.4f})"
    if not (ACC_VOL_REL_MIN <= vol_rel <= ACC_VOL_REL_MAX):
        return False, f"vol_rel_out_of_band({vol_rel:.3f})"
    if buy_ratio_win < ACC_BUY_RATIO_MIN:
        return False, f"buy_ratio_low({buy_ratio_win:.3f})"
    if buy_ratio_on_touches < ACC_BUY_RATIO_TOUCH_MIN:
        return False, f"support_not_defended(buy_touch={buy_ratio_on_touches:.3f})"
    if not higher_lows:
        return False, f"no_higher_lows(swings={int(feats.get('swing_lows_found', 0))})"
    return True, "ok"

# ==========================================================
# Breakdown validation (Variant B) + virtual state
# ==========================================================
def _support_level(support_price: float) -> float:
    return support_price * (1.0 - ACC_BREAK_EPS)

@dataclass
class BreakEpisode:
    support_level: float
    start_ts: datetime
    wick_breaches: int = 0
    close_breaches: int = 0
    consec_closes_below: int = 0

@dataclass
class VirtualPlan:
    created_ts: datetime
    support_price: float
    entry_price: float
    tp_price: float
    profit_low: float
    profit_high: float
    status: str = "OPEN"  # OPEN | FILLED | CLOSED | CANCELLED
    filled_ts: Optional[datetime] = None
    closed_ts: Optional[datetime] = None

@dataclass
class SymbolState:
    episode: Optional[BreakEpisode] = None
    plan: Optional[VirtualPlan] = None

STATE: Dict[str, SymbolState] = {}

def update_breakdown_episode(
    symbol: str,
    close_time: datetime,
    candle_o: float,
    candle_h: float,
    candle_l: float,
    candle_c: float,
    support_price: float,
) -> Tuple[Optional[str], Optional[Dict]]:
    st = STATE.setdefault(symbol, SymbolState())

    level = _support_level(support_price)
    low_below = candle_l < level
    close_below = candle_c < level
    recovered = candle_c >= level

    if low_below or close_below:
        if st.episode is None or abs(st.episode.support_level - level) / max(level, 1e-12) > 1e-6:
            st.episode = BreakEpisode(support_level=level, start_ts=close_time)

        ep = st.episode
        if low_below:
            ep.wick_breaches += 1
        if close_below:
            ep.close_breaches += 1
            ep.consec_closes_below += 1
        else:
            ep.consec_closes_below = 0

        if ep.consec_closes_below >= ACC_BREAK_CONFIRM_CANDLES:
            details = {
                "tf": TF,
                "confirm_candles": ACC_BREAK_CONFIRM_CANDLES,
                "break_eps": ACC_BREAK_EPS,
                "support_price": support_price,
                "support_level": level,
                "episode_start": ep.start_ts.isoformat(),
                "episode_wick_breaches": ep.wick_breaches,
                "episode_close_breaches": ep.close_breaches,
                "episode_consec_closes_below": ep.consec_closes_below,
                "break_candle": {"t": close_time.isoformat(), "o": candle_o, "h": candle_h, "l": candle_l, "c": candle_c},
            }
            st.episode = None
            return "ACCUM_BREAKDOWN", details

        return None, None

    if st.episode is not None and recovered:
        ep = st.episode
        details = {
            "tf": TF,
            "confirm_candles": ACC_BREAK_CONFIRM_CANDLES,
            "break_eps": ACC_BREAK_EPS,
            "support_price": support_price,
            "support_level": ep.support_level,
            "episode_start": ep.start_ts.isoformat(),
            "episode_end": close_time.isoformat(),
            "episode_wick_breaches": ep.wick_breaches,
            "episode_close_breaches": ep.close_breaches,
            "recover_candle": {"t": close_time.isoformat(), "o": candle_o, "h": candle_h, "l": candle_l, "c": candle_c},
        }
        st.episode = None
        return "ACCUM_FALSE_BREAK", details

    return None, None

def maybe_create_virtual_plan(symbol: str, close_time: datetime, feats: Dict) -> Optional[VirtualPlan]:
    """
    Create VIRTUAL plan EARLY if:
      - support_touches >= (ACC_SUPPORT_TOUCHES_MIN - 1)
      - and all other accumulation rules are satisfied (range/vol/buy/defense/higher-lows)

    IMPORTANT:
      - NO profit_ok / NO profit_hits checks (removed per request)
    """
    if not ACC_PLACE_VIRTUAL_ORDERS:
        return None

    st = STATE.setdefault(symbol, SymbolState())
    if st.plan is not None and st.plan.status in ("OPEN", "FILLED"):
        return None

    if not feats:
        return None

    early_min = max(1, ACC_SUPPORT_TOUCHES_MIN - 1)
    support_touches = int(feats.get("support_touches", 0))
    if support_touches < early_min:
        return None

    other_ok, _ = accum_other_rules_ok(feats)
    if not other_ok:
        return None

    support_price = float(feats["support_price"])
    profit_low = float(feats["profit_low"])
    profit_high = float(feats["profit_high"])

    entry_price = support_price * (1.0 + ACC_ENTRY_OFFSET_PCT)
    tp_price = profit_low  # SELL_LIMIT stays profit_low (even if zone would be "invalid")

    plan = VirtualPlan(
        created_ts=close_time,
        support_price=support_price,
        entry_price=entry_price,
        tp_price=tp_price,
        profit_low=profit_low,
        profit_high=profit_high,
        status="OPEN",
    )
    st.plan = plan
    return plan

def simulate_plan_fills(symbol: str, close_time: datetime, o: float, h: float, l: float, c: float) -> Optional[Dict]:
    if not ACC_SIMULATE_FILLS:
        return None

    st = STATE.setdefault(symbol, SymbolState())
    if st.plan is None:
        return None

    plan = st.plan
    if plan.status == "OPEN":
        if l <= plan.entry_price:
            plan.status = "FILLED"
            plan.filled_ts = close_time
            return {"event": "BUY_FILLED", "t": close_time.isoformat(), "entry_price": plan.entry_price,
                    "candle": {"o": o, "h": h, "l": l, "c": c}}

    if plan.status == "FILLED":
        if h >= plan.tp_price:
            plan.status = "CLOSED"
            plan.closed_ts = close_time
            return {"event": "TP_FILLED", "t": close_time.isoformat(), "tp_price": plan.tp_price,
                    "candle": {"o": o, "h": h, "l": l, "c": c}}

    return None

# ==========================================================
# Binance WS
# ==========================================================
def ws_url(symbols: List[str], tf: str) -> str:
    streams = "/".join([f"{s.lower()}@kline_{tf}" for s in symbols])
    return f"wss://stream.binance.com:9443/stream?streams={streams}"

async def init_db(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(CREATE_SQL)
    logging.info("DB init OK (tables ensured)")

# ==========================================================
# Per-candle handler
# ==========================================================
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
        await conn.execute(
            INSERT_CANDLE_SQL,
            symbol, open_time, close_time,
            o, h, l, c,
            v_base, v_quote, taker_buy_base, taker_buy_quote,
            trade_count
        )
        need = max(BASE_N, WIN_N, ACC_SWING_LOOKBACK)
        rows = await conn.fetch(SELECT_LAST_N_SQL, symbol, need)

    db_ms = (time.perf_counter() - t0) * 1000.0
    DB_LAT_MS_SUM += db_ms
    DB_LAT_MS_N += 1

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

    breakdown_sig = None
    breakdown_details = None
    if feats:
        breakdown_sig, breakdown_details = update_breakdown_episode(
            symbol, close_time, o, h, l, c, float(feats["support_price"])
        )

    # EARLY plan creation (even if ok == False)
    created_plan: Optional[VirtualPlan] = None
    if feats:
        created_plan = maybe_create_virtual_plan(symbol, close_time, feats)

    sim_event = simulate_plan_fills(symbol, close_time, o, h, l, c)

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
                float(feats["profit_low"]),
                float(feats["profit_high"]),
                int(feats["profit_hits"]),
                bool(ok),
                reason
            )

        if ok and feats:
            details = {
                "tf": TF,
                "tf_min": TF_MIN,
                "win_h": ACC_WIN_H,
                "base_h": ACC_BASE_H,
                "range_pct": feats["range_pct"],
                "vol_rel": feats["vol_rel"],
                "buy_ratio_win": feats["buy_ratio_win"],
                "support_price": feats["support_price"],
                "support_touches": feats["support_touches"],
                "buy_ratio_on_touches": feats["buy_ratio_on_touches"],
                "higher_lows": feats["higher_lows"],
                "swing_lows_found": feats["swing_lows_found"],
                "profit_low": feats["profit_low"],
                "profit_high": feats["profit_high"],
                "profit_hits": feats["profit_hits"],
                "win_max_h": feats["win_max_h"],
                "win_min_l": feats["win_min_l"],
                "avg_vq_win": feats["avg_vq_win"],
                "avg_vq_base": feats["avg_vq_base"],
            }
            await conn.execute(INSERT_SIGNAL_SQL, symbol, close_time, "ACCUM_PHASE", json.dumps(details))

        if breakdown_sig and breakdown_details:
            await conn.execute(INSERT_SIGNAL_SQL, symbol, close_time, breakdown_sig, json.dumps(breakdown_details))

        if created_plan is not None:
            early_min = max(1, ACC_SUPPORT_TOUCHES_MIN - 1)
            plan_details = {
                "tf": TF,
                "support_price": created_plan.support_price,
                "entry_offset_pct": ACC_ENTRY_OFFSET_PCT,
                "support_touches_min": ACC_SUPPORT_TOUCHES_MIN,
                "support_touches_early_min": early_min,
                "support_touches": int(feats.get("support_touches", 0)) if feats else None,
                "profit_low": created_plan.profit_low,
                "profit_high": created_plan.profit_high,
                "profit_hits": int(feats.get("profit_hits", 0)) if feats else None,
                "virtual_buy_limit": created_plan.entry_price,
                "virtual_sell_limit": created_plan.tp_price,
                "note": "VIRTUAL ONLY in this service. Executor may place real orders from trade_intents.",
            }
            await conn.execute(INSERT_SIGNAL_SQL, symbol, close_time, "ACCUM_VIRTUAL_PLAN", json.dumps(plan_details))

            # write real trade intent (unless position already open)
            if ACC_WRITE_INTENTS:
                has_pos = await conn.fetchrow(HAS_OPEN_POSITION_SQL, symbol)
                if has_pos:
                    # don't spam intents if already in position
                    pass
                else:
                    intent_meta = {
                        "tf": TF,
                        "tf_min": TF_MIN,
                        "reason": "EARLY_PLAN_MIN_1_TOUCHES",
                        "quote_amount_usdt": ACC_REAL_TRADE_USDT,
                        "virtual_buy_limit": created_plan.entry_price,
                        "virtual_sell_limit": created_plan.tp_price,
                    }
                    await conn.execute(
                        INSERT_INTENT_SQL,
                        symbol,
                        close_time,
                        float(ACC_REAL_TRADE_USDT),
                        float(created_plan.entry_price),
                        float(created_plan.support_price),
                        json.dumps(intent_meta),
                    )

        if sim_event is not None:
            await conn.execute(INSERT_SIGNAL_SQL, symbol, close_time, "ACCUM_VIRTUAL_SIM", json.dumps(sim_event))

    db_ms2 = (time.perf_counter() - t1) * 1000.0
    DB_LAT_MS_SUM += db_ms2
    DB_LAT_MS_N += 1

    CANDLE_COUNTER += 1
    LAST_CLOSE_TIME = close_time

    if feats and (CANDLE_COUNTER % DEBUG_EVERY == 0):
        lvl = _support_level(float(feats["support_price"]))
        logging.info(
            "AC_DEBUG %s | close=%s | reason=%s | range=%.2f%% vol_rel=%.2fx buy=%.2f "
            "touches=%d touch_buy=%.2f support=%.6f(level=%.6f) profit=[%.6f..%.6f] hits=%d | "
            "higher_lows=%s swings=%d | db=%.1fms",
            symbol,
            close_time.isoformat(),
            reason,
            feats["range_pct"] * 100.0,
            feats["vol_rel"],
            feats["buy_ratio_win"],
            feats["support_touches"],
            feats["buy_ratio_on_touches"],
            feats["support_price"],
            lvl,
            feats["profit_low"],
            feats["profit_high"],
            feats["profit_hits"],
            feats["higher_lows"],
            feats["swing_lows_found"],
            (db_ms + db_ms2),
        )

    if ok and feats:
        logging.warning(
            "🟦 ACCUM_PHASE %s | tf=%s range=%.2f%% vol_rel=%.2fx buy=%.2f "
            "touches=%d (min=%d) touch_buy=%.2f support=%.6f | profit=[%.6f..%.6f] hits=%d",
            symbol, TF,
            feats["range_pct"] * 100.0,
            feats["vol_rel"],
            feats["buy_ratio_win"],
            feats["support_touches"],
            ACC_SUPPORT_TOUCHES_MIN,
            feats["buy_ratio_on_touches"],
            feats["support_price"],
            feats["profit_low"],
            feats["profit_high"],
            feats["profit_hits"],
        )

    if created_plan is not None:
        early_min = max(1, ACC_SUPPORT_TOUCHES_MIN - 1)
        logging.warning(
            "🟩 VIRTUAL_PLAN %s | BUY_LIMIT=%.6f (support=%.6f +%.2f%%) | SELL_LIMIT=%.6f (profit_low) | "
            "touches=%d (early_min=%d full_min=%d)",
            symbol,
            created_plan.entry_price,
            created_plan.support_price,
            ACC_ENTRY_OFFSET_PCT * 100.0,
            created_plan.tp_price,
            int(feats.get("support_touches", 0)) if feats else -1,
            early_min,
            ACC_SUPPORT_TOUCHES_MIN,
        )

    if breakdown_sig == "ACCUM_FALSE_BREAK":
        logging.warning(
            "🟨 FALSE_BREAK %s | support=%.6f level=%.6f | wick_breaches=%d close_breaches=%d | episode=%s..%s",
            symbol,
            breakdown_details["support_price"],
            breakdown_details["support_level"],
            breakdown_details["episode_wick_breaches"],
            breakdown_details["episode_close_breaches"],
            breakdown_details["episode_start"],
            breakdown_details["episode_end"],
        )

    if breakdown_sig == "ACCUM_BREAKDOWN":
        logging.error(
            "🟥 BREAKDOWN_CONFIRMED %s | support=%.6f level=%.6f | confirm=%d closes_below | wick=%d close=%d",
            symbol,
            breakdown_details["support_price"],
            breakdown_details["support_level"],
            breakdown_details["confirm_candles"],
            breakdown_details["episode_wick_breaches"],
            breakdown_details["episode_close_breaches"],
        )
        st = STATE.setdefault(symbol, SymbolState())
        if st.plan is not None and st.plan.status in ("OPEN", "FILLED"):
            st.plan.status = "CANCELLED"
            logging.warning("🟫 VIRTUAL_PLAN_CANCELLED %s | reason=BREAKDOWN_CONFIRMED", symbol)

    if sim_event is not None:
        logging.warning("🧪 VIRTUAL_SIM %s | %s", symbol, json.dumps(sim_event))

    if CANDLE_COUNTER % HEARTBEAT_EVERY == 0:
        avg_db = (DB_LAT_MS_SUM / DB_LAT_MS_N) if DB_LAT_MS_N else 0.0
        early_min = max(1, ACC_SUPPORT_TOUCHES_MIN - 1)
        logging.info(
            "HEARTBEAT | candles=%d | last_close=%s | symbols=%d | tf=%s | avg_db=%.1fms | "
            "win=%dh base=%dh | entry_off=%.2f%% | touches_min=%d early_min=%d | swing_min=%d | intents=%s usdt=%.2f",
            CANDLE_COUNTER,
            (LAST_CLOSE_TIME.isoformat() if LAST_CLOSE_TIME else "n/a"),
            len(SYMBOLS),
            TF,
            avg_db,
            ACC_WIN_H,
            ACC_BASE_H,
            ACC_ENTRY_OFFSET_PCT * 100.0,
            ACC_SUPPORT_TOUCHES_MIN,
            early_min,
            ACC_SWING_MIN_COUNT,
            ACC_WRITE_INTENTS,
            ACC_REAL_TRADE_USDT,
        )

# ==========================================================
# WS loop with chunking
# ==========================================================
def _chunk_symbols(symbols: List[str], chunk_size: int) -> List[List[str]]:
    return [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]

async def ws_loop(pool: asyncpg.Pool) -> None:
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
                        continue
                    symbol = (k.get("s") or "").upper()
                    await on_closed_kline(pool, symbol, k)
        except Exception as e:
            logging.exception("WS group=%d error: %s | reconnecting in 3s", group_idx, e)
            await asyncio.sleep(3)

# ==========================================================
# main
# ==========================================================
async def main() -> None:
    early_min = max(1, ACC_SUPPORT_TOUCHES_MIN - 1)
    logging.info(
        "Starting accumulation_service | tf=%s (%dmin) | win=%dh (%d candles) | base=%dh (%d candles) | symbols=%d",
        TF, TF_MIN, ACC_WIN_H, WIN_N, ACC_BASE_H, BASE_N, len(SYMBOLS)
    )
    logging.info(
        "Thresholds | range<=%.2f%% | vol_rel=[%.2f..%.2f] | buy_ratio>=%.2f | "
        "touches>=%d (early_plan>=%d) eps=%.3f%% touch_buy>=%.2f | swing_min=%d lookback=%d | "
        "profit_pct=%.2f%% entry_off=%.2f%% | break_confirm=%d break_eps=%.4f | "
        "virtual=%s simulate=%s | intents=%s usdt=%.2f | log=%s hb=%d dbg=%d",
        ACC_RANGE_PCT_MAX * 100.0,
        ACC_VOL_REL_MIN, ACC_VOL_REL_MAX,
        ACC_BUY_RATIO_MIN,
        ACC_SUPPORT_TOUCHES_MIN,
        early_min,
        ACC_SUPPORT_TOUCH_EPS * 100.0,
        ACC_BUY_RATIO_TOUCH_MIN,
        ACC_SWING_MIN_COUNT,
        ACC_SWING_LOOKBACK,
        ACC_PROFIT_PCT * 100.0,
        ACC_ENTRY_OFFSET_PCT * 100.0,
        ACC_BREAK_CONFIRM_CANDLES,
        ACC_BREAK_EPS,
        ACC_PLACE_VIRTUAL_ORDERS,
        ACC_SIMULATE_FILLS,
        ACC_WRITE_INTENTS,
        ACC_REAL_TRADE_USDT,
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
