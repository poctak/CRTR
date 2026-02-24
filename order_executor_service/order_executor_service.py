# executor_service.py
# ------------------------------------------------------------
# Reads trade_intents (source=ACCUM) from Timescale/Postgres
# and places REAL Binance LIMIT BUY orders for USDC pairs.
#
# FIXES / CHANGES:
#   - Quantity rounded DOWN to LOT_SIZE stepSize (fixes -1111)
#   - Price rounded DOWN to PRICE_FILTER tickSize
#   - Robust signature: signed requests are sent as raw URL query string (fixes -1022)
#   - IMPORTANT: DO NOT write positions_open as OPEN immediately.
#       We now poll the order status and only create/update positions_open when FILLED.
#       Until filled, we keep trade_intents.status = SENT and store order_id/client_order_id.
#
# Notes:
#   - trade_intents.quote_amount is QUOTE currency amount (USDC).
#   - This module does NOT place OCO/SL/TP on Binance; it will write SL/TP to DB only after FILLED.
# ------------------------------------------------------------

import os
import json
import time
import hmac
import hashlib
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

import aiohttp
import asyncpg
from decimal import Decimal, ROUND_DOWN


# ==========================================================
# ENV helpers
# ==========================================================
def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    return int(v) if v else default

def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    return float(v) if v else default

def env_str(name: str, default: str) -> str:
    v = os.getenv(name, "").strip()
    return v if v else default


# ==========================================================
# CONFIG
# ==========================================================
LOG_LEVEL = env_str("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

DB_HOST = env_str("DB_HOST", "db")
DB_NAME = env_str("DB_NAME", "pumpdb")
DB_USER = env_str("DB_USER", "pumpuser")
DB_PASSWORD = env_str("DB_PASSWORD", "")

# Binance
BINANCE_BASE_URL = env_str("BINANCE_BASE_URL", "https://api.binance.com").rstrip("/")
BINANCE_API_KEY = env_str("BINANCE_API_KEY", "")
BINANCE_API_SECRET = env_str("BINANCE_API_SECRET", "")

# Execution loop
POLL_EVERY_SEC = env_int("EXEC_POLL_EVERY_SEC", 2)
MAX_BATCH = env_int("EXEC_MAX_BATCH", 25)

# Quote currency
QUOTE_CCY = env_str("QUOTE_CCY", "USDC").upper()

# exchangeInfo cache
EXINFO_TTL_SEC = env_int("EXINFO_TTL_SEC", 900)  # 15 minutes

# clientOrderId prefix
CLIENT_OID_PREFIX = env_str("CLIENT_OID_PREFIX", "ACC")

# DB defaults for positions_open (used later by exit_service)
EXEC_DEFAULT_SL_PCT = env_float("EXEC_DEFAULT_SL_PCT", 0.003)  # 0.3%
EXEC_DEFAULT_TP_PCT = env_float("EXEC_DEFAULT_TP_PCT", 0.003)  # 0.3%

# Binance timing
BINANCE_RECV_WINDOW = env_int("BINANCE_RECV_WINDOW", 5000)

# Order fill polling
FILL_POLL_EVERY_SEC = env_float("FILL_POLL_EVERY_SEC", 1.0)
FILL_WAIT_MAX_SEC = env_int("FILL_WAIT_MAX_SEC", 30)  # how long to wait after sending order

# Debug signature inputs (temporary)
DEBUG_SIGN = env_str("DEBUG_SIGN", "false").lower() in ("1", "true", "yes", "y", "on")


# ==========================================================
# SQL
# ==========================================================
# Claim NEW intents and mark SENT immediately (so multiple workers don't race)
PICK_INTENTS_SQL = """
WITH picked AS (
  SELECT id
  FROM trade_intents
  WHERE status='NEW'
    AND side='BUY'
    AND source='ACCUM'
  ORDER BY created_at ASC
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
UPDATE trade_intents ti
SET status='SENT', updated_at=NOW()
FROM picked
WHERE ti.id = picked.id
RETURNING ti.*;
"""

MARK_INTENT_ERROR_SQL = """
UPDATE trade_intents
SET status='ERROR', error=$2, updated_at=NOW()
WHERE id=$1
"""

MARK_INTENT_SENT_SQL = """
UPDATE trade_intents
SET order_id=$2, client_order_id=$3, updated_at=NOW()
WHERE id=$1
"""

MARK_INTENT_FILLED_SQL = """
UPDATE trade_intents
SET status='FILLED', updated_at=NOW()
WHERE id=$1
"""

# positions_open: write only when FILLED
UPSERT_POSITION_OPEN_SQL = """
INSERT INTO positions_open(symbol, entry_time, entry_price, sl, tp, status, qty, entry_order_id, opened_at, updated_at)
VALUES($1,$2,$3,$4,$5,'OPEN',$6,$7,NOW(),NOW())
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

HAS_OPEN_POSITION_SQL = """
SELECT 1 FROM positions_open WHERE symbol=$1 AND status='OPEN' LIMIT 1
"""


# ==========================================================
# Decimal helpers
# ==========================================================
def _d(x: Any) -> Decimal:
    return Decimal(str(x))

def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step

def decimal_places(step: Decimal) -> int:
    s = format(step.normalize(), "f")
    if "." not in s:
        return 0
    return len(s.split(".")[1].rstrip("0"))

def fmt_decimal(value: Decimal, step: Decimal) -> str:
    dp = decimal_places(step)
    q = value.quantize(Decimal(10) ** -dp, rounding=ROUND_DOWN)
    return format(q, "f")


# ==========================================================
# Binance signing (robust)
# ==========================================================
def build_query_string(params: Dict[str, Any]) -> str:
    # Deterministic: sort keys, and stringify values as-is
    items = []
    for k in sorted(params.keys()):
        items.append(f"{k}={params[k]}")
    return "&".join(items)

def sign_query(query_string: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()

async def binance_public(
    session: aiohttp.ClientSession,
    method: str,
    path: str,
    params: Dict[str, Any],
) -> Tuple[int, str]:
    url = f"{BINANCE_BASE_URL}{path}"
    async with session.request(method.upper(), url, params=params) as resp:
        return resp.status, await resp.text()

async def binance_signed(
    session: aiohttp.ClientSession,
    method: str,
    path: str,
    params: Dict[str, Any],
) -> Tuple[int, str]:
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        return 401, "Missing BINANCE_API_KEY/SECRET"

    url = f"{BINANCE_BASE_URL}{path}"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

    qp = dict(params)
    qp["timestamp"] = int(time.time() * 1000)
    qp["recvWindow"] = BINANCE_RECV_WINDOW

    qs = build_query_string(qp)
    sig = sign_query(qs, BINANCE_API_SECRET)
    full_qs = f"{qs}&signature={sig}"

    if DEBUG_SIGN:
        logging.info("SIGN_DEBUG | %s %s?%s", method.upper(), path, full_qs)

    async with session.request(method.upper(), url + "?" + full_qs, headers=headers) as resp:
        return resp.status, await resp.text()


# ==========================================================
# exchangeInfo cache
# ==========================================================
@dataclass
class SymbolFilters:
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    min_notional: Decimal

_EX_CACHE: Dict[str, Tuple[float, SymbolFilters]] = {}

def _parse_filters(symbol: str, exinfo_json: Dict[str, Any]) -> SymbolFilters:
    sym = None
    for s in exinfo_json.get("symbols", []):
        if (s.get("symbol") or "").upper() == symbol.upper():
            sym = s
            break
    if not sym:
        raise RuntimeError(f"exchangeInfo missing symbol={symbol}")

    tick = Decimal("0")
    step = Decimal("0")
    min_qty = Decimal("0")
    min_notional = Decimal("0")

    for f in sym.get("filters", []):
        ft = f.get("filterType")
        if ft == "PRICE_FILTER":
            tick = _d(f.get("tickSize", "0"))
        elif ft == "LOT_SIZE":
            step = _d(f.get("stepSize", "0"))
            min_qty = _d(f.get("minQty", "0"))
        elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
            mn = f.get("minNotional")
            if mn is not None:
                min_notional = _d(mn)

    if tick <= 0 or step <= 0:
        raise RuntimeError(f"Bad filters for {symbol}: tick={tick} step={step}")

    return SymbolFilters(tick_size=tick, step_size=step, min_qty=min_qty, min_notional=min_notional)

async def get_symbol_filters(session: aiohttp.ClientSession, symbol: str) -> SymbolFilters:
    now = time.time()
    cached = _EX_CACHE.get(symbol)
    if cached and (now - cached[0]) < EXINFO_TTL_SEC:
        return cached[1]

    status, text = await binance_public(session, "GET", "/api/v3/exchangeInfo", {"symbol": symbol})
    if status != 200:
        raise RuntimeError(f"Binance exchangeInfo failed {status}: {text}")

    exj = json.loads(text)
    filt = _parse_filters(symbol, exj)
    _EX_CACHE[symbol] = (now, filt)
    return filt


# ==========================================================
# Order placement + fill polling
# ==========================================================
def build_client_order_id(intent_id: int, symbol: str) -> str:
    ts = int(time.time())
    return f"{CLIENT_OID_PREFIX}-{symbol}-{intent_id}-{ts}"

def compute_qty_price(
    quote_amount: Decimal,
    limit_price: Decimal,
    f: SymbolFilters
) -> Tuple[str, str, Decimal, Decimal]:
    px = floor_to_step(limit_price, f.tick_size)
    if px <= 0:
        raise ValueError("price<=0 after tick rounding")

    raw_qty = quote_amount / px
    qty = floor_to_step(raw_qty, f.step_size)

    if f.min_qty > 0 and qty < f.min_qty:
        raise ValueError(f"qty<{f.min_qty} after rounding (qty={qty})")

    if f.min_notional > 0:
        notional = qty * px
        if notional < f.min_notional:
            raise ValueError(f"notional<{f.min_notional} (notional={notional}, qty={qty}, px={px})")

    qty_s = fmt_decimal(qty, f.step_size)
    px_s = fmt_decimal(px, f.tick_size)
    return qty_s, px_s, qty, px

async def place_limit_buy(
    session: aiohttp.ClientSession,
    intent_id: int,
    symbol: str,
    quote_amount: float,
    limit_price: float,
) -> Tuple[int, str, str, str]:
    """
    Returns: (order_id, client_oid, qty_str, price_str)
    """
    f = await get_symbol_filters(session, symbol)
    qa = _d(quote_amount)
    lp = _d(limit_price)

    qty_s, px_s, _, _ = compute_qty_price(qa, lp, f)
    client_oid = build_client_order_id(intent_id, symbol)

    params = {
        "symbol": symbol,
        "side": "BUY",
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": qty_s,
        "price": px_s,
        "newClientOrderId": client_oid,
    }

    status, text = await binance_signed(session, "POST", "/api/v3/order", params)
    if status != 200:
        raise RuntimeError(f"Binance REST POST /api/v3/order failed {status}: {text}")

    resp = json.loads(text)
    order_id = int(resp.get("orderId"))
    return order_id, client_oid, qty_s, px_s

async def fetch_order(
    session: aiohttp.ClientSession,
    symbol: str,
    order_id: int,
) -> Dict[str, Any]:
    params = {"symbol": symbol, "orderId": order_id}
    status, text = await binance_signed(session, "GET", "/api/v3/order", params)
    if status != 200:
        raise RuntimeError(f"Binance GET /api/v3/order failed {status}: {text}")
    return json.loads(text)

def compute_avg_fill_price(executed_qty: Decimal, cum_quote_qty: Decimal) -> Decimal:
    if executed_qty <= 0:
        return Decimal("0")
    return cum_quote_qty / executed_qty

async def wait_for_fill(
    session: aiohttp.ClientSession,
    symbol: str,
    order_id: int,
    max_wait_sec: int,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Polls order until FILLED or timeout.
    Returns (filled, last_order_json).
    """
    deadline = time.time() + max_wait_sec
    last: Dict[str, Any] = {}
    while time.time() < deadline:
        last = await fetch_order(session, symbol, order_id)
        st = (last.get("status") or "").upper()
        if st == "FILLED":
            return True, last
        if st in ("CANCELED", "CANCELLED", "REJECTED", "EXPIRED"):
            return False, last
        await asyncio.sleep(FILL_POLL_EVERY_SEC)
    # timeout
    if not last:
        last = await fetch_order(session, symbol, order_id)
    return False, last


# ==========================================================
# Processing
# ==========================================================
async def process_intent(conn: asyncpg.Connection, session: aiohttp.ClientSession, intent: asyncpg.Record) -> None:
    intent_id = int(intent["id"])
    symbol = str(intent["symbol"]).upper()
    quote_amount = float(intent["quote_amount"])
    limit_price = float(intent["limit_price"])

    if not symbol.endswith(QUOTE_CCY):
        raise RuntimeError(f"Symbol {symbol} does not end with {QUOTE_CCY} (QUOTE_CCY={QUOTE_CCY})")

    # If already open, mark intent ERROR so it won't block pending unique index
    has_pos = await conn.fetchrow(HAS_OPEN_POSITION_SQL, symbol)
    if has_pos:
        await conn.execute(MARK_INTENT_ERROR_SQL, intent_id, "OPEN_POSITION_EXISTS")
        logging.warning("INTENT_SKIP id=%d %s | open position exists", intent_id, symbol)
        return

    # Place order
    order_id, client_oid, qty_s, px_s = await place_limit_buy(session, intent_id, symbol, quote_amount, limit_price)
    await conn.execute(MARK_INTENT_SENT_SQL, intent_id, order_id, client_oid)

    logging.warning(
        "INTENT_SENT id=%d %s | quote=%.2f %s | limit=%s qty=%s | order_id=%d client_oid=%s",
        intent_id, symbol, quote_amount, QUOTE_CCY, px_s, qty_s, order_id, client_oid
    )

    # Wait for fill (optional window); only then write positions_open
    filled, ordj = await wait_for_fill(session, symbol, order_id, FILL_WAIT_MAX_SEC)
    status = (ordj.get("status") or "").upper()

    if not filled:
        logging.warning(
            "INTENT_NOT_FILLED id=%d %s | order_id=%d status=%s | waited=%ds | leaving positions_open untouched",
            intent_id, symbol, order_id, status, FILL_WAIT_MAX_SEC
        )
        return

    executed_qty = _d(ordj.get("executedQty", "0"))
    cum_quote = _d(ordj.get("cummulativeQuoteQty", "0"))  # Binance field spelling
    avg_px = compute_avg_fill_price(executed_qty, cum_quote)

    if executed_qty <= 0 or avg_px <= 0:
        # Extremely defensive
        raise RuntimeError(f"FILLED but executedQty/avgPx invalid: executedQty={executed_qty} cumQuote={cum_quote}")

    # Mark intent FILLED
    await conn.execute(MARK_INTENT_FILLED_SQL, intent_id)

    # Write positions_open now with real fill numbers
    entry_time = datetime.now(timezone.utc)
    entry_price = float(avg_px)

    sl = entry_price * (1.0 - EXEC_DEFAULT_SL_PCT)
    tp = entry_price * (1.0 + EXEC_DEFAULT_TP_PCT)

    qty = float(executed_qty)

    await conn.execute(
        UPSERT_POSITION_OPEN_SQL,
        symbol, entry_time, entry_price, sl, tp, qty, order_id
    )

    logging.warning(
        "POSITION_OPENED id=%d %s | FILLED order_id=%d | avg_fill=%.10f qty=%.10f | sl=%.10f tp=%.10f",
        intent_id, symbol, order_id, entry_price, qty, sl, tp
    )


# ==========================================================
# Worker loop
# ==========================================================
async def worker_loop(pool: asyncpg.Pool) -> None:
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        intents = await conn.fetch(PICK_INTENTS_SQL, MAX_BATCH)

                        if not intents:
                            await asyncio.sleep(POLL_EVERY_SEC)
                            continue

                        for intent in intents:
                            try:
                                await process_intent(conn, session, intent)
                            except Exception as e:
                                err = str(e)
                                await conn.execute(MARK_INTENT_ERROR_SQL, int(intent["id"]), err)
                                logging.error("INTENT_ERROR id=%s %s | %s", intent["id"], intent["symbol"], err)

            except Exception as e:
                logging.exception("Worker loop error: %s | sleeping %ds", e, POLL_EVERY_SEC)
                await asyncio.sleep(POLL_EVERY_SEC)


# ==========================================================
# main
# ==========================================================
async def main() -> None:
    logging.info(
        "Starting executor_service | quote=%s | poll=%ds | batch=%d | fill_wait=%ds poll_fill=%.1fs",
        QUOTE_CCY, POLL_EVERY_SEC, MAX_BATCH, FILL_WAIT_MAX_SEC, FILL_POLL_EVERY_SEC
    )
    pool = await asyncpg.create_pool(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        min_size=1, max_size=5
    )
    logging.info("DB pool created | host=%s db=%s user=%s", DB_HOST, DB_NAME, DB_USER)
    await worker_loop(pool)

if __name__ == "__main__":
    asyncio.run(main())
