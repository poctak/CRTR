# order_executor_service.py
# ------------------------------------------------------------
# Reads trade_intents from DB and places REAL Binance SPOT orders.
#
# - Pulls NEW intents
# - Enforces: max 1 open position per symbol
#     if positions_open(symbol, status='OPEN') exists -> CANCEL intent
# - Places LIMIT BUY (GTC) using REST (HMAC signed)
# - Listens to UserDataStream WS (executionReport)
#     -> upserts spot_orders
#     -> inserts spot_fills
#     -> on FILLED BUY: upserts positions_open with real VWAP entry
# ------------------------------------------------------------

import os
import json
import hmac
import time
import math
import asyncio
import logging
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

import aiohttp
import asyncpg
import websockets

# ==========================================================
# ENV
# ==========================================================
def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    return int(v) if v else default

def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    return float(v) if v else default

def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name, "").strip()
    return v if v else default

LOG_LEVEL = env_str("LOG_LEVEL", "INFO").upper()

DB_HOST = env_str("DB_HOST", "db")
DB_NAME = env_str("DB_NAME", "pumpdb")
DB_USER = env_str("DB_USER", "pumpuser")
DB_PASSWORD = env_str("DB_PASSWORD", "")

BINANCE_API_KEY = env_str("BINANCE_API_KEY")
BINANCE_API_SECRET = env_str("BINANCE_API_SECRET")
BINANCE_REST = env_str("BINANCE_REST", "https://api.binance.com").rstrip("/")
BINANCE_WS_BASE = env_str("BINANCE_WS_BASE", "wss://stream.binance.com:9443/ws").rstrip("/")

POLL_EVERY_SEC = env_int("EXEC_POLL_EVERY_SEC", 2)
MAX_OPEN_ORDERS_TOTAL = env_int("MAX_OPEN_ORDERS_TOTAL", 5)
MAX_OPEN_ORDERS_PER_SYMBOL = env_int("MAX_OPEN_ORDERS_PER_SYMBOL", 1)

# placeholders (exit will be tuned later)
DEFAULT_SL_PCT = env_float("SPOT_DEFAULT_SL_PCT", 0.01)
DEFAULT_TP_PCT = env_float("SPOT_DEFAULT_TP_PCT", 0.01)

# ==========================================================
# LOGGING
# ==========================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# ==========================================================
# DB (ensure tables exist)
# ==========================================================
CREATE_SQL = """
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

CREATE UNIQUE INDEX IF NOT EXISTS uq_trade_intents_symbol_pending
ON trade_intents(symbol)
WHERE status IN ('NEW','SENT');

CREATE INDEX IF NOT EXISTS idx_trade_intents_status_created
  ON trade_intents(status, created_at);

CREATE TABLE IF NOT EXISTS spot_orders (
  id BIGSERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  type TEXT NOT NULL,
  time_in_force TEXT,
  client_order_id TEXT,
  order_id BIGINT,
  price DOUBLE PRECISION,
  orig_qty DOUBLE PRECISION,
  executed_qty DOUBLE PRECISION NOT NULL DEFAULT 0,
  cumm_quote_qty DOUBLE PRECISION NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_spot_orders_symbol_orderid
  ON spot_orders(symbol, order_id);

CREATE INDEX IF NOT EXISTS idx_spot_orders_symbol_created
  ON spot_orders(symbol, created_at DESC);

CREATE TABLE IF NOT EXISTS spot_fills (
  id BIGSERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  order_id BIGINT NOT NULL,
  trade_id BIGINT NOT NULL,
  side TEXT NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  qty DOUBLE PRECISION NOT NULL,
  quote_qty DOUBLE PRECISION NOT NULL,
  commission DOUBLE PRECISION NOT NULL,
  commission_asset TEXT NOT NULL,
  trade_time TIMESTAMPTZ NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(symbol, trade_id)
);

CREATE INDEX IF NOT EXISTS idx_spot_fills_symbol_time
  ON spot_fills(symbol, trade_time DESC);

ALTER TABLE positions_open
ADD COLUMN IF NOT EXISTS qty DOUBLE PRECISION;

ALTER TABLE positions_open
ADD COLUMN IF NOT EXISTS entry_order_id BIGINT;
"""

FETCH_NEW_INTENTS_SQL = """
SELECT id, symbol, ts, quote_amount, limit_price, support_price, meta
FROM trade_intents
WHERE status='NEW'
ORDER BY created_at ASC
LIMIT 20
"""

COUNT_OPEN_ORDERS_TOTAL_SQL = """
SELECT COUNT(*) AS n
FROM spot_orders
WHERE status IN ('NEW','PARTIALLY_FILLED')
"""

COUNT_OPEN_ORDERS_SYMBOL_SQL = """
SELECT COUNT(*) AS n
FROM spot_orders
WHERE symbol=$1 AND status IN ('NEW','PARTIALLY_FILLED')
"""

HAS_OPEN_POSITION_SQL = """
SELECT 1
FROM positions_open
WHERE symbol=$1 AND status='OPEN'
LIMIT 1
"""

MARK_INTENT_SENT_SQL = """
UPDATE trade_intents
SET status='SENT', order_id=$2, client_order_id=$3, updated_at=NOW(), error=NULL
WHERE id=$1
"""

MARK_INTENT_ERROR_SQL = """
UPDATE trade_intents
SET status='ERROR', error=$2, updated_at=NOW()
WHERE id=$1
"""

MARK_INTENT_CANCELLED_SQL = """
UPDATE trade_intents
SET status='CANCELLED', error=$2, updated_at=NOW()
WHERE id=$1
"""

UPSERT_SPOT_ORDER_SQL = """
INSERT INTO spot_orders(
  symbol, side, type, time_in_force, client_order_id, order_id,
  price, orig_qty, executed_qty, cumm_quote_qty, status
)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT(symbol, order_id) DO UPDATE SET
  executed_qty=EXCLUDED.executed_qty,
  cumm_quote_qty=EXCLUDED.cumm_quote_qty,
  status=EXCLUDED.status,
  updated_at=NOW()
"""

INSERT_FILL_SQL = """
INSERT INTO spot_fills(
  symbol, order_id, trade_id, side, price, qty, quote_qty,
  commission, commission_asset, trade_time
)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT(symbol, trade_id) DO NOTHING
"""

UPSERT_POSITION_OPEN_SQL = """
INSERT INTO positions_open(
  symbol, entry_time, entry_price, sl, tp, status, opened_at, updated_at, qty, entry_order_id
)
VALUES($1,$2,$3,$4,$5,'OPEN',NOW(),NOW(),$6,$7)
ON CONFLICT(symbol) DO UPDATE SET
  entry_time=EXCLUDED.entry_time,
  entry_price=EXCLUDED.entry_price,
  sl=EXCLUDED.sl,
  tp=EXCLUDED.tp,
  status='OPEN',
  qty=EXCLUDED.qty,
  entry_order_id=EXCLUDED.entry_order_id,
  updated_at=NOW()
"""

# ==========================================================
# Binance REST helpers
# ==========================================================
def _sign(secret: str, query: str) -> str:
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()

def _ts_ms() -> int:
    return int(time.time() * 1000)

async def _rest(
    session: aiohttp.ClientSession,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    signed: bool = False,
) -> Any:
    if params is None:
        params = {}

    headers = {"X-MBX-APIKEY": BINANCE_API_KEY} if BINANCE_API_KEY else {}

    if signed:
        params["timestamp"] = _ts_ms()
        params.setdefault("recvWindow", 5000)
        query = "&".join([f"{k}={params[k]}" for k in sorted(params.keys())])
        params["signature"] = _sign(BINANCE_API_SECRET, query)

    url = f"{BINANCE_REST}{path}"
    async with session.request(method, url, params=params, headers=headers, timeout=15) as resp:
        txt = await resp.text()
        if resp.status >= 400:
            raise RuntimeError(f"Binance REST {method} {path} failed {resp.status}: {txt}")
        return json.loads(txt) if txt and txt[0] in "{[" else txt

# ==========================================================
# Exchange filters cache
# ==========================================================
@dataclass
class SymbolFilters:
    min_notional: float
    min_qty: float
    step_size: float
    tick_size: float

FILTERS: Dict[str, SymbolFilters] = {}

def _floor_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return math.floor(x / step) * step

def _round_tick(price: float, tick: float) -> float:
    if tick <= 0:
        return price
    return math.floor(price / tick) * tick

async def load_exchange_info(session: aiohttp.ClientSession, symbols: Optional[List[str]] = None) -> None:
    data = await _rest(session, "GET", "/api/v3/exchangeInfo", signed=False)
    for s in data.get("symbols", []):
        sym = s.get("symbol", "")
        if symbols and sym not in symbols:
            continue
        if s.get("status") != "TRADING":
            continue

        min_notional = 0.0
        min_qty = 0.0
        step = 0.0
        tick = 0.0

        for f in s.get("filters", []):
            ft = f.get("filterType")
            if ft == "MIN_NOTIONAL":
                min_notional = float(f.get("minNotional", 0.0))
            elif ft == "LOT_SIZE":
                min_qty = float(f.get("minQty", 0.0))
                step = float(f.get("stepSize", 0.0))
            elif ft == "PRICE_FILTER":
                tick = float(f.get("tickSize", 0.0))

        if step <= 0 or tick <= 0:
            continue

        FILTERS[sym] = SymbolFilters(
            min_notional=min_notional,
            min_qty=min_qty,
            step_size=step,
            tick_size=tick,
        )

# ==========================================================
# Order placement
# ==========================================================
async def place_limit_buy(
    session: aiohttp.ClientSession,
    symbol: str,
    quote_amount: float,
    limit_price: float,
    client_order_id: str,
) -> Tuple[int, float, float]:
    """
    Returns: (order_id, price, orig_qty)
    """
    if symbol not in FILTERS:
        await load_exchange_info(session, symbols=[symbol])
    if symbol not in FILTERS:
        raise RuntimeError(f"Missing filters for {symbol}")

    flt = FILTERS[symbol]

    price = _round_tick(limit_price, flt.tick_size)
    if price <= 0:
        raise RuntimeError("invalid_price_after_tick_rounding")

    qty_raw = quote_amount / price
    qty = _floor_step(qty_raw, flt.step_size)

    if qty < flt.min_qty:
        raise RuntimeError(f"qty_too_small(qty={qty}, min_qty={flt.min_qty})")

    notional = qty * price
    if flt.min_notional and notional < flt.min_notional:
        raise RuntimeError(f"min_notional_fail(notional={notional:.6f}, min_notional={flt.min_notional})")

    params = {
        "symbol": symbol,
        "side": "BUY",
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": f"{qty:.16f}".rstrip("0").rstrip("."),
        "price": f"{price:.16f}".rstrip("0").rstrip("."),
        "newClientOrderId": client_order_id,
    }
    res = await _rest(session, "POST", "/api/v3/order", params=params, signed=True)
    order_id = int(res["orderId"])
    return order_id, price, qty

# ==========================================================
# User data stream
# ==========================================================
async def create_listen_key(session: aiohttp.ClientSession) -> str:
    res = await _rest(session, "POST", "/api/v3/userDataStream", signed=False)
    return res["listenKey"]

async def keepalive_listen_key(session: aiohttp.ClientSession, listen_key: str) -> None:
    await _rest(session, "PUT", "/api/v3/userDataStream", params={"listenKey": listen_key}, signed=False)

def _parse_time_ms(ms: Any) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)

async def handle_execution_report(pool: asyncpg.Pool, ev: Dict[str, Any]) -> None:
    symbol = ev.get("s")
    side = ev.get("S")
    order_id = int(ev.get("i", 0))
    client_order_id = ev.get("c")
    status = ev.get("X")
    exec_type = ev.get("x")

    price = float(ev.get("p", 0.0)) if ev.get("p") else 0.0
    orig_qty = float(ev.get("q", 0.0)) if ev.get("q") else 0.0
    executed_qty = float(ev.get("z", 0.0)) if ev.get("z") else 0.0
    cumm_quote = float(ev.get("Z", 0.0)) if ev.get("Z") else 0.0

    async with pool.acquire() as conn:
        await conn.execute(
            UPSERT_SPOT_ORDER_SQL,
            symbol, side, ev.get("o", "LIMIT"), ev.get("f", "GTC"),
            client_order_id, order_id,
            price, orig_qty, executed_qty, cumm_quote, status
        )

        if exec_type == "TRADE":
            trade_id = int(ev.get("t", 0))
            last_px = float(ev.get("L", 0.0))
            last_qty = float(ev.get("l", 0.0))
            quote_qty = last_px * last_qty
            commission = float(ev.get("n", 0.0)) if ev.get("n") else 0.0
            commission_asset = ev.get("N") or ""
            trade_time = _parse_time_ms(ev.get("T", _ts_ms()))

            await conn.execute(
                INSERT_FILL_SQL,
                symbol, order_id, trade_id, side,
                last_px, last_qty, quote_qty,
                commission, commission_asset, trade_time
            )

        if side == "BUY" and status == "FILLED" and executed_qty > 0 and cumm_quote > 0:
            entry_price = cumm_quote / executed_qty
            entry_time = _parse_time_ms(ev.get("T", _ts_ms()))
            sl = entry_price * (1.0 - DEFAULT_SL_PCT)
            tp = entry_price * (1.0 + DEFAULT_TP_PCT)

            await conn.execute(
                UPSERT_POSITION_OPEN_SQL,
                symbol, entry_time, entry_price, sl, tp,
                executed_qty, order_id
            )

            await conn.execute(
                "UPDATE trade_intents SET status='FILLED', updated_at=NOW() WHERE order_id=$1 AND status IN ('NEW','SENT')",
                order_id
            )

# ==========================================================
# Executor loops
# ==========================================================
def make_client_order_id(symbol: str, ts: datetime) -> str:
    return f"AC_{symbol}_{int(ts.timestamp())}"

async def poll_and_place(pool: asyncpg.Pool, session: aiohttp.ClientSession) -> None:
    while True:
        try:
            async with pool.acquire() as conn:
                n_total = int((await conn.fetchrow(COUNT_OPEN_ORDERS_TOTAL_SQL))["n"])
            if n_total >= MAX_OPEN_ORDERS_TOTAL:
                await asyncio.sleep(POLL_EVERY_SEC)
                continue

            async with pool.acquire() as conn:
                intents = await conn.fetch(FETCH_NEW_INTENTS_SQL)

            for it in intents:
                intent_id = int(it["id"])
                symbol = it["symbol"]
                ts = it["ts"]
                quote_amount = float(it["quote_amount"])
                limit_price = float(it["limit_price"])

                # enforce: max 1 open position per symbol
                async with pool.acquire() as conn:
                    has_pos = await conn.fetchrow(HAS_OPEN_POSITION_SQL, symbol)
                if has_pos:
                    async with pool.acquire() as conn:
                        await conn.execute(MARK_INTENT_CANCELLED_SQL, intent_id, "blocked_open_position")
                    logging.info("INTENT_CANCELLED %s | id=%d | reason=open_position", symbol, intent_id)
                    continue

                # per-symbol open order cap
                async with pool.acquire() as conn:
                    n_sym = int((await conn.fetchrow(COUNT_OPEN_ORDERS_SYMBOL_SQL, symbol))["n"])
                if n_sym >= MAX_OPEN_ORDERS_PER_SYMBOL:
                    # keep it NEW (it will retry later)
                    continue

                client_id = make_client_order_id(symbol, ts)

                try:
                    order_id, px, qty = await place_limit_buy(session, symbol, quote_amount, limit_price, client_id)
                except Exception as e:
                    logging.error("INTENT_ERROR id=%d %s | %s", intent_id, symbol, e)
                    async with pool.acquire() as conn:
                        await conn.execute(MARK_INTENT_ERROR_SQL, intent_id, str(e))
                    continue

                async with pool.acquire() as conn:
                    await conn.execute(
                        UPSERT_SPOT_ORDER_SQL,
                        symbol, "BUY", "LIMIT", "GTC",
                        client_id, order_id,
                        px, qty, 0.0, 0.0, "NEW"
                    )
                    await conn.execute(MARK_INTENT_SENT_SQL, intent_id, order_id, client_id)

                logging.warning("🟢 ORDER_SENT %s | orderId=%d BUY qty=%.8f price=%.8f quote=%.2f",
                                symbol, order_id, qty, px, quote_amount)

            await asyncio.sleep(POLL_EVERY_SEC)

        except Exception as e:
            logging.exception("poll_and_place error: %s", e)
            await asyncio.sleep(2)

async def user_stream_loop(pool: asyncpg.Pool, session: aiohttp.ClientSession) -> None:
    listen_key = await create_listen_key(session)
    logging.info("UserData listenKey created")

    async def _keepalive():
        while True:
            try:
                await asyncio.sleep(30 * 60)
                await keepalive_listen_key(session, listen_key)
                logging.info("listenKey keepalive ok")
            except Exception as e:
                logging.exception("listenKey keepalive error: %s", e)

    asyncio.create_task(_keepalive())

    url = f"{BINANCE_WS_BASE}/{listen_key}"
    while True:
        try:
            logging.info("Connecting user WS %s", url)
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                logging.info("Connected user WS")
                async for msg in ws:
                    ev = json.loads(msg)
                    if ev.get("e") == "executionReport":
                        await handle_execution_report(pool, ev)
        except Exception as e:
            logging.exception("user_stream_loop error: %s | reconnecting in 3s", e)
            await asyncio.sleep(3)

async def init_db(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(CREATE_SQL)
    logging.info("DB init OK (tables ensured)")

# ==========================================================
# main
# ==========================================================
async def main() -> None:
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        raise RuntimeError("Missing BINANCE_API_KEY / BINANCE_API_SECRET")

    pool = await asyncpg.create_pool(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        min_size=1, max_size=5
    )
    logging.info("DB pool created")

    await init_db(pool)

    async with aiohttp.ClientSession() as session:
        try:
            await load_exchange_info(session, symbols=None)
            logging.info("exchangeInfo loaded | symbols=%d", len(FILTERS))
        except Exception as e:
            logging.warning("exchangeInfo load failed (will lazy-load): %s", e)

        await asyncio.gather(
            user_stream_loop(pool, session),
            poll_and_place(pool, session),
        )

if __name__ == "__main__":
    asyncio.run(main())
