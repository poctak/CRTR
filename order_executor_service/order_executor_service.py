# order_executor_service.py
# ------------------------------------------------------------
# Reads trade_intents from DB and places REAL Binance SPOT orders.
#
# - Pulls NEW intents
# - Enforces: max 1 open position per symbol
# - Places LIMIT BUY (GTC) using REST (HMAC signed)
# - Listens to User Data Stream via WebSocket API (subscribe.signature)
#     -> upserts spot_orders
#     -> inserts spot_fills (per TRADE)
#     -> BUY FILLED:
#           - computes net_qty after fee (if fee in base asset)
#           - computes effective_quote (if fee in quote asset)
#           - entry_price = effective_quote / net_qty
#           - upserts positions_open with net qty
#           - marks trade_intents FILLED
#     -> SELL FILLED:
#           - computes net_quote proceeds fee-aware
#           - computes realized PnL in quote vs positions_open entry
#           - inserts positions_closed row
#           - reduces positions_open.qty (partial close supported)
#           - if qty near zero -> marks positions_open CLOSED
#     -> CANCELED/EXPIRED/REJECTED: updates trade_intents accordingly
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
from urllib.parse import urlencode

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
BINANCE_WS_API = env_str("BINANCE_WS_API", "wss://ws-api.binance.com:443/ws-api/v3").rstrip("/")

POLL_EVERY_SEC = env_int("EXEC_POLL_EVERY_SEC", 2)
MAX_OPEN_ORDERS_TOTAL = env_int("MAX_OPEN_ORDERS_TOTAL", 5)
MAX_OPEN_ORDERS_PER_SYMBOL = env_int("MAX_OPEN_ORDERS_PER_SYMBOL", 1)

DEFAULT_SL_PCT = env_float("SPOT_DEFAULT_SL_PCT", 0.01)
DEFAULT_TP_PCT = env_float("SPOT_DEFAULT_TP_PCT", 0.01)

# Dust threshold for closing position to zero
POS_DUST_QTY = env_float("POS_DUST_QTY", 1e-10)

# RecvWindow for signed requests (timestamp tolerance)
RECV_WINDOW = env_int("BINANCE_RECV_WINDOW", 10000)


# ==========================================================
# LOGGING
# ==========================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))


# ==========================================================
# DB schema
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

-- Realized PnL log (one row per close event; supports partial closes)
CREATE TABLE IF NOT EXISTS positions_closed (
  id BIGSERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  entry_order_id BIGINT,
  exit_order_id BIGINT NOT NULL,
  entry_time TIMESTAMPTZ,
  exit_time TIMESTAMPTZ NOT NULL,
  entry_price DOUBLE PRECISION NOT NULL,
  exit_price DOUBLE PRECISION NOT NULL,
  qty DOUBLE PRECISION NOT NULL,
  gross_quote DOUBLE PRECISION NOT NULL,
  fee_quote DOUBLE PRECISION NOT NULL,
  net_quote DOUBLE PRECISION NOT NULL,
  pnl_quote DOUBLE PRECISION NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_positions_closed_symbol_time
  ON positions_closed(symbol, exit_time DESC);
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

GET_OPEN_POSITION_SQL = """
SELECT symbol, entry_time, entry_price, qty, entry_order_id, status
FROM positions_open
WHERE symbol=$1 AND status='OPEN'
LIMIT 1
"""

UPDATE_POSITION_AFTER_SELL_SQL = """
UPDATE positions_open
SET qty=$2, status=$3, updated_at=NOW()
WHERE symbol=$1
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

MARK_INTENT_FILLED_BY_ORDER_SQL = """
UPDATE trade_intents
SET status='FILLED', updated_at=NOW(), error=NULL
WHERE order_id=$1 AND status IN ('NEW','SENT')
"""

MARK_INTENT_CANCELLED_BY_ORDER_SQL = """
UPDATE trade_intents
SET status='CANCELLED', updated_at=NOW(), error=$2
WHERE order_id=$1 AND status IN ('NEW','SENT')
"""

MARK_INTENT_ERROR_BY_ORDER_SQL = """
UPDATE trade_intents
SET status='ERROR', updated_at=NOW(), error=$2
WHERE order_id=$1 AND status IN ('NEW','SENT')
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

SUM_FILLS_FOR_ORDER_SQL = """
SELECT
  COALESCE(SUM(qty), 0) AS sum_qty,
  COALESCE(SUM(quote_qty), 0) AS sum_quote_qty,
  COALESCE(SUM(CASE WHEN commission_asset = $3 THEN commission ELSE 0 END), 0) AS comm_base,
  COALESCE(SUM(CASE WHEN commission_asset = $4 THEN commission ELSE 0 END), 0) AS comm_quote
FROM spot_fills
WHERE symbol = $1 AND order_id = $2
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

INSERT_POSITION_CLOSED_SQL = """
INSERT INTO positions_closed(
  symbol, entry_order_id, exit_order_id,
  entry_time, exit_time,
  entry_price, exit_price,
  qty,
  gross_quote, fee_quote, net_quote,
  pnl_quote
)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
"""


# ==========================================================
# Binance signing helpers
# ==========================================================
def _sign(secret: str, payload: str) -> str:
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


def _ts_ms() -> int:
    return int(time.time() * 1000)


async def _rest(
    session: aiohttp.ClientSession,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    signed: bool = False,
) -> Any:
    """
    IMPORTANT:
    For signed endpoints we MUST sign the exact URL-encoded payload that is sent.
    Otherwise Binance returns -1022 (invalid signature).
    """
    if params is None:
        params = {}

    headers = {"X-MBX-APIKEY": BINANCE_API_KEY} if BINANCE_API_KEY else {}
    url = f"{BINANCE_REST}{path}"

    if not signed:
        async with session.request(method, url, params=params, headers=headers, timeout=15) as resp:
            txt = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"Binance REST {method} {path} failed {resp.status}: {txt}")
            return json.loads(txt) if txt and txt[0] in "{[" else txt

    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        raise RuntimeError("Missing BINANCE_API_KEY / BINANCE_API_SECRET")

    params = dict(params)  # copy
    params["timestamp"] = _ts_ms()
    params.setdefault("recvWindow", RECV_WINDOW)

    qs = urlencode(params, doseq=True)  # URL-encoded string (exact)
    sig = _sign(BINANCE_API_SECRET, qs)
    body = qs + "&signature=" + sig

    async with session.request(
        method,
        url,
        data=body,
        headers={**headers, "Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    ) as resp:
        txt = await resp.text()
        if resp.status >= 400:
            raise RuntimeError(f"Binance REST {method} {path} failed {resp.status}: {txt}")
        return json.loads(txt) if txt and txt[0] in "{[" else txt


# ==========================================================
# Symbol parsing (base/quote)
# ==========================================================
KNOWN_QUOTES = [
    "USDC", "USDT", "FDUSD", "BUSD", "TUSD",
    "EUR", "TRY",
    "BTC", "ETH", "BNB",
]


def split_symbol(symbol: str) -> Tuple[str, str]:
    s = (symbol or "").upper().strip()
    for q in KNOWN_QUOTES:
        if s.endswith(q) and len(s) > len(q):
            return s[:-len(q)], q
    return s, ""


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
# Order placement (BUY)
# ==========================================================
async def place_limit_buy(
    session: aiohttp.ClientSession,
    symbol: str,
    quote_amount: float,
    limit_price: float,
    client_order_id: str,
) -> Tuple[int, float, float]:
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
# WS API user data stream
# ==========================================================
def _parse_time_ms(ms: Any) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)


def _wsapi_signature_payload(params: Dict[str, Any]) -> str:
    return "&".join([f"{k}={params[k]}" for k in sorted(params.keys())])


async def wsapi_user_stream_subscribe(ws: websockets.WebSocketClientProtocol) -> int:
    ts = _ts_ms()
    sig_params = {"apiKey": BINANCE_API_KEY, "timestamp": ts}
    signature = _sign(BINANCE_API_SECRET, _wsapi_signature_payload(sig_params))

    req = {
        "id": f"uds_{ts}",
        "method": "userDataStream.subscribe.signature",
        "params": {"apiKey": BINANCE_API_KEY, "timestamp": ts, "signature": signature},
    }
    await ws.send(json.dumps(req))

    raw = await ws.recv()
    resp = json.loads(raw)

    if resp.get("status") != 200:
        raise RuntimeError(f"WS API subscribe failed: {resp}")

    sub_id = int(resp.get("result", {}).get("subscriptionId", -1))
    if sub_id < 0:
        raise RuntimeError(f"WS API subscribe missing subscriptionId: {resp}")
    return sub_id


def _safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0


async def _compute_order_net_from_fills(
    conn: asyncpg.Connection,
    symbol: str,
    order_id: int,
    base: str,
    quote: str
) -> Tuple[float, float, float, float, float]:
    row = await conn.fetchrow(SUM_FILLS_FOR_ORDER_SQL, symbol, order_id, base, quote)
    sum_qty = float(row["sum_qty"] or 0.0)
    sum_quote_qty = float(row["sum_quote_qty"] or 0.0)
    comm_base = float(row["comm_base"] or 0.0)
    comm_quote = float(row["comm_quote"] or 0.0)
    avg_price = _safe_div(sum_quote_qty, sum_qty)
    return sum_qty, sum_quote_qty, comm_base, comm_quote, avg_price


async def handle_execution_report(pool: asyncpg.Pool, ev: Dict[str, Any]) -> None:
    symbol = (ev.get("s") or "").upper()
    side = ev.get("S")
    order_id = int(ev.get("i", 0))
    client_order_id = ev.get("c") or ""
    status = ev.get("X") or "UNKNOWN"
    exec_type = ev.get("x") or ""

    otype = ev.get("o") or "UNKNOWN"
    tif = ev.get("f")

    price = float(ev.get("p", 0.0)) if ev.get("p") else 0.0
    orig_qty = float(ev.get("q", 0.0)) if ev.get("q") else 0.0
    executed_qty_gross = float(ev.get("z", 0.0)) if ev.get("z") else 0.0
    cumm_quote_gross = float(ev.get("Z", 0.0)) if ev.get("Z") else 0.0

    base, quote = split_symbol(symbol)
    event_time = _parse_time_ms(ev.get("T", _ts_ms()))

    async with pool.acquire() as conn:
        # 1) upsert order snapshot
        await conn.execute(
            UPSERT_SPOT_ORDER_SQL,
            symbol, side, otype, tif, client_order_id, order_id,
            price, orig_qty, executed_qty_gross, cumm_quote_gross, status
        )

        # 2) record fills on TRADE
        if exec_type == "TRADE":
            trade_id = int(ev.get("t", 0))
            last_px = float(ev.get("L", 0.0))
            last_qty = float(ev.get("l", 0.0))
            quote_qty = last_px * last_qty
            commission = float(ev.get("n", 0.0)) if ev.get("n") else 0.0
            commission_asset = (ev.get("N") or "").upper()

            await conn.execute(
                INSERT_FILL_SQL,
                symbol, order_id, trade_id, side,
                last_px, last_qty, quote_qty,
                commission, commission_asset, event_time
            )

        # 3) terminal -> update trade_intents
        if status in ("CANCELED", "EXPIRED"):
            reason = f"order_{status.lower()}" if executed_qty_gross <= 0 else f"order_{status.lower()}_partial_fill(executed_qty={executed_qty_gross:.8f})"
            await conn.execute(MARK_INTENT_CANCELLED_BY_ORDER_SQL, order_id, reason)

        elif status == "REJECTED":
            reason = ev.get("r") or "order_rejected"
            await conn.execute(MARK_INTENT_ERROR_BY_ORDER_SQL, order_id, f"order_rejected({reason})")

        # 4) BUY FILLED -> fee-aware open
        if side == "BUY" and status == "FILLED":
            sum_qty, sum_quote_qty, comm_base, comm_quote, avg_px = await _compute_order_net_from_fills(
                conn, symbol, order_id, base, quote
            )

            if sum_qty <= 0 or sum_quote_qty <= 0:
                sum_qty = executed_qty_gross
                sum_quote_qty = cumm_quote_gross
                avg_px = _safe_div(sum_quote_qty, sum_qty)

            net_qty = sum_qty - comm_base
            effective_quote = sum_quote_qty + comm_quote

            if net_qty <= 0 or effective_quote <= 0:
                logging.warning(
                    "BUY FILLED but net calc invalid | %s order=%d sum_qty=%.10f comm_base=%.10f sum_quote=%.10f comm_quote=%.10f",
                    symbol, order_id, sum_qty, comm_base, sum_quote_qty, comm_quote
                )
                return

            entry_price = effective_quote / net_qty
            sl = entry_price * (1.0 - DEFAULT_SL_PCT)
            tp = entry_price * (1.0 + DEFAULT_TP_PCT)

            await conn.execute(
                UPSERT_POSITION_OPEN_SQL,
                symbol, event_time, entry_price, sl, tp,
                net_qty, order_id
            )
            await conn.execute(MARK_INTENT_FILLED_BY_ORDER_SQL, order_id)

        # 5) SELL FILLED -> fee-aware realized pnl
        if side == "SELL" and status == "FILLED":
            pos = await conn.fetchrow(GET_OPEN_POSITION_SQL, symbol)
            if not pos:
                return

            entry_time = pos["entry_time"]
            entry_price = float(pos["entry_price"] or 0.0)
            pos_qty = float(pos["qty"] or 0.0)
            entry_order_id = int(pos["entry_order_id"] or 0)

            if pos_qty <= POS_DUST_QTY or entry_price <= 0:
                return

            sum_qty, sum_quote_qty, comm_base, comm_quote, avg_px = await _compute_order_net_from_fills(
                conn, symbol, order_id, base, quote
            )

            if sum_qty <= 0 or sum_quote_qty <= 0:
                sum_qty = executed_qty_gross
                sum_quote_qty = cumm_quote_gross
                avg_px = _safe_div(sum_quote_qty, sum_qty)

            qty_close = min(pos_qty, sum_qty)
            if qty_close <= POS_DUST_QTY:
                return

            fee_quote = comm_quote + (comm_base * avg_px)

            scale = qty_close / sum_qty if sum_qty > 0 else 0.0
            gross_quote_scaled = sum_quote_qty * scale
            fee_quote_scaled = fee_quote * scale

            net_quote = gross_quote_scaled - fee_quote_scaled
            entry_cost_quote = entry_price * qty_close

            pnl_quote = net_quote - entry_cost_quote
            exit_price = _safe_div(gross_quote_scaled, qty_close)

            await conn.execute(
                INSERT_POSITION_CLOSED_SQL,
                symbol, entry_order_id, order_id,
                entry_time, event_time,
                entry_price, exit_price,
                qty_close,
                gross_quote_scaled, fee_quote_scaled, net_quote,
                pnl_quote
            )

            remaining = pos_qty - qty_close
            if remaining <= POS_DUST_QTY:
                remaining = 0.0
                new_status = "CLOSED"
            else:
                new_status = "OPEN"

            await conn.execute(UPDATE_POSITION_AFTER_SELL_SQL, symbol, remaining, new_status)


async def user_stream_loop(pool: asyncpg.Pool) -> None:
    while True:
        try:
            logging.info("Connecting WS API %s", BINANCE_WS_API)
            async with websockets.connect(
                BINANCE_WS_API,
                ping_interval=20,
                ping_timeout=20,
                max_size=None
            ) as ws:
                sub_id = await wsapi_user_stream_subscribe(ws)
                logging.info("User Data Stream subscribed | subscriptionId=%s", sub_id)

                async for msg in ws:
                    try:
                        payload = json.loads(msg)
                    except Exception:
                        continue

                    ev = payload.get("event") if isinstance(payload, dict) else None
                    if not isinstance(ev, dict):
                        continue

                    if ev.get("e") == "executionReport":
                        await handle_execution_report(pool, ev)

        except Exception as e:
            logging.exception("user_stream_loop error: %s | reconnecting in 3s", e)
            await asyncio.sleep(3)


# ==========================================================
# Executor loops
# ==========================================================
def make_client_order_id(symbol: str, ts: datetime) -> str:
    # keep it URL-safe (no spaces)
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

                async with pool.acquire() as conn:
                    has_pos = await conn.fetchrow(HAS_OPEN_POSITION_SQL, symbol)
                if has_pos:
                    async with pool.acquire() as conn:
                        await conn.execute(MARK_INTENT_CANCELLED_SQL, intent_id, "blocked_open_position")
                    logging.info("INTENT_CANCELLED %s | id=%d | reason=open_position", symbol, intent_id)
                    continue

                async with pool.acquire() as conn:
                    n_sym = int((await conn.fetchrow(COUNT_OPEN_ORDERS_SYMBOL_SQL, symbol))["n"])
                if n_sym >= MAX_OPEN_ORDERS_PER_SYMBOL:
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

                logging.warning(
                    "🟢 ORDER_SENT %s | orderId=%d BUY qty=%.8f price=%.8f quote=%.2f",
                    symbol, order_id, qty, px, quote_amount
                )

            await asyncio.sleep(POLL_EVERY_SEC)

        except Exception as e:
            logging.exception("poll_and_place error: %s", e)
            await asyncio.sleep(2)


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
            user_stream_loop(pool),
            poll_and_place(pool, session),
        )


if __name__ == "__main__":
    asyncio.run(main())
