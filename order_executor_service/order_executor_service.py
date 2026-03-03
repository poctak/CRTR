#!/usr/bin/env python3
# executor_service.py
# ------------------------------------------------------------
# Reads trade_intents from Timescale/Postgres and places REAL Binance orders.
#
# Supports:
#   - LIMIT BUY  (entry_mode='LIMIT')
#   - MARKET BUY (entry_mode='MARKET') using quoteOrderQty
#
# VARIANT B (parallel fill-wait):
#   - NEW intents are picked & marked SENT quickly (short transaction)
#   - Each intent is processed in its own task:
#       * place order (LIMIT or MARKET)
#       * store order_id + client_order_id (WITHOUT touching updated_at)
#       * start a parallel wait_for_fill task (optional fast-path)
#   - Reconciler loop continuously checks SENT intents and writes positions_open once FILLED.
#
# ADDITION:
#   - Cancel LIMIT BUY orders that are not filled within EXEC_CANCEL_AFTER_SEC (default 3600s)
#   - IMPORTANT: updated_at is used as SENT timestamp for timeout measurement
#       -> we set updated_at ONLY when transitioning NEW -> SENT
#       -> we do NOT update updated_at when writing order_id/client_order_id
#
# NEW (this version):
#   - Writes an ENTRY row into public.trade_log when order becomes FILLED,
#     including: entry_source (= trade_intents.source), entry_mode, intent_id, entry_order_id.
#   - Idempotent by UNIQUE(entry_order_id) on trade_log (recommended migration below).
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
from typing import Any, Dict, Tuple, Optional, List, Set

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

# Variant B: max parallel in-flight tasks
EXEC_MAX_INFLIGHT = env_int("EXEC_MAX_INFLIGHT", 20)

# Optional: only process these sources (CSV). Empty => all sources.
# Example: EXEC_SOURCES=ACCUM_NEUTRAL,PROGRESSIVE_PUMP
EXEC_SOURCES_CSV = env_str("EXEC_SOURCES", "").strip()

# Quote currency
QUOTE_CCY = env_str("QUOTE_CCY", "USDC").upper()

# exchangeInfo cache
EXINFO_TTL_SEC = env_int("EXINFO_TTL_SEC", 900)

# clientOrderId prefix
CLIENT_OID_PREFIX = env_str("CLIENT_OID_PREFIX", "ACC")

# Defaults stored in positions_open after FILLED (for later exit_service)
EXEC_DEFAULT_SL_PCT = env_float("EXEC_DEFAULT_SL_PCT", 0.003)
EXEC_DEFAULT_TP_PCT = env_float("EXEC_DEFAULT_TP_PCT", 0.003)

# Binance timing
BINANCE_RECV_WINDOW = env_int("BINANCE_RECV_WINDOW", 5000)

# Order fill polling (optional immediate wait after send; now parallel)
FILL_POLL_EVERY_SEC = env_float("FILL_POLL_EVERY_SEC", 1.0)
FILL_WAIT_MAX_SEC = env_int("FILL_WAIT_MAX_SEC", 30)

# Reconciler
RECONCILE_EVERY_SEC = env_int("RECONCILE_EVERY_SEC", 5)
RECONCILE_BATCH = env_int("RECONCILE_BATCH", 50)

# Cancel stale LIMIT orders after N seconds (default 1 hour)
EXEC_CANCEL_AFTER_SEC = env_int("EXEC_CANCEL_AFTER_SEC", 3600)

DEBUG_SIGN = env_str("DEBUG_SIGN", "false").lower() in ("1", "true", "yes", "y", "on")


def parse_sources(csv: str) -> List[str]:
    if not csv:
        return []
    return [x.strip() for x in csv.split(",") if x.strip()]


EXEC_SOURCES: List[str] = parse_sources(EXEC_SOURCES_CSV)


# ==========================================================
# SQL
# ==========================================================
PICK_NEW_INTENTS_SQL = """
WITH picked AS (
  SELECT id
  FROM trade_intents
  WHERE status='NEW'
    AND side='BUY'
    AND (cardinality($2::text[]) = 0 OR source = ANY($2::text[]))
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

PICK_SENT_INTENTS_SQL = """
WITH picked AS (
  SELECT id
  FROM trade_intents
  WHERE status='SENT'
    AND side='BUY'
    AND order_id IS NOT NULL
    AND (cardinality($2::text[]) = 0 OR source = ANY($2::text[]))
  ORDER BY updated_at ASC
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
SELECT ti.*
FROM trade_intents ti
JOIN picked p ON p.id = ti.id;
"""

MARK_INTENT_ERROR_SQL = """
UPDATE trade_intents
SET status='ERROR', error=$2, updated_at=NOW()
WHERE id=$1
"""

# do NOT touch updated_at here (we want updated_at to stay as SENT timestamp)
MARK_INTENT_SENT_SQL = """
UPDATE trade_intents
SET order_id=$2, client_order_id=$3
WHERE id=$1
"""

MARK_INTENT_FILLED_SQL = """
UPDATE trade_intents
SET status='FILLED', updated_at=NOW()
WHERE id=$1
"""

HAS_OPEN_POSITION_SQL = """
SELECT 1 FROM positions_open WHERE symbol=$1 AND status='OPEN' LIMIT 1
"""

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

# Insert ENTRY into trade_log (idempotent by UNIQUE(entry_order_id))
INSERT_TRADE_LOG_ENTRY_SQL = """
INSERT INTO public.trade_log(
  symbol, entry_time, entry_price, reason,
  entry_source, entry_mode, intent_id, entry_order_id
)
VALUES($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (entry_order_id) DO NOTHING
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
    return "&".join([f"{k}={params[k]}" for k in sorted(params.keys())])

def sign_query(query_string: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()

async def binance_public(session: aiohttp.ClientSession, method: str, path: str, params: Dict[str, Any]) -> Tuple[int, str]:
    url = f"{BINANCE_BASE_URL}{path}"
    async with session.request(method.upper(), url, params=params) as resp:
        return resp.status, await resp.text()

async def binance_signed(session: aiohttp.ClientSession, method: str, path: str, params: Dict[str, Any]) -> Tuple[int, str]:
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
# exchangeInfo cache (LIMIT only)
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
# Orders
# ==========================================================
def build_client_order_id(intent_id: int, symbol: str) -> str:
    return f"{CLIENT_OID_PREFIX}-{symbol}-{intent_id}-{int(time.time())}"

def compute_qty_price(quote_amount: Decimal, limit_price: Decimal, f: SymbolFilters) -> Tuple[str, str]:
    px = floor_to_step(limit_price, f.tick_size)
    if px <= 0:
        raise ValueError("price<=0 after tick rounding")

    qty = floor_to_step(quote_amount / px, f.step_size)

    if f.min_qty > 0 and qty < f.min_qty:
        raise ValueError(f"qty<{f.min_qty} after rounding (qty={qty})")

    if f.min_notional > 0:
        notional = qty * px
        if notional < f.min_notional:
            raise ValueError(f"notional<{f.min_notional} (notional={notional}, qty={qty}, px={px})")

    return fmt_decimal(qty, f.step_size), fmt_decimal(px, f.tick_size)

async def place_limit_buy(session: aiohttp.ClientSession, intent_id: int, symbol: str, quote_amount: float, limit_price: float) -> Tuple[int, str, str, str]:
    f = await get_symbol_filters(session, symbol)
    qty_s, px_s = compute_qty_price(_d(quote_amount), _d(limit_price), f)
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
        raise RuntimeError(f"Binance POST /api/v3/order failed {status}: {text}")

    resp = json.loads(text)
    return int(resp["orderId"]), client_oid, qty_s, px_s

async def place_market_buy(session: aiohttp.ClientSession, intent_id: int, symbol: str, quote_amount: float) -> Tuple[int, str]:
    client_oid = build_client_order_id(intent_id, symbol)
    qoq = f"{quote_amount:.2f}"

    params = {
        "symbol": symbol,
        "side": "BUY",
        "type": "MARKET",
        "quoteOrderQty": qoq,
        "newClientOrderId": client_oid,
    }

    status, text = await binance_signed(session, "POST", "/api/v3/order", params)
    if status != 200:
        raise RuntimeError(f"Binance POST /api/v3/order (MARKET) failed {status}: {text}")

    resp = json.loads(text)
    return int(resp["orderId"]), client_oid

async def fetch_order(session: aiohttp.ClientSession, symbol: str, order_id: int) -> Dict[str, Any]:
    status, text = await binance_signed(session, "GET", "/api/v3/order", {"symbol": symbol, "orderId": order_id})
    if status != 200:
        raise RuntimeError(f"Binance GET /api/v3/order failed {status}: {text}")
    return json.loads(text)

async def cancel_order(session: aiohttp.ClientSession, symbol: str, order_id: int) -> Dict[str, Any]:
    status, text = await binance_signed(session, "DELETE", "/api/v3/order", {"symbol": symbol, "orderId": order_id})
    if status != 200:
        raise RuntimeError(f"Binance DELETE /api/v3/order failed {status}: {text}")
    return json.loads(text)

def avg_fill_price(executed_qty: Decimal, cum_quote_qty: Decimal) -> Decimal:
    return (cum_quote_qty / executed_qty) if executed_qty > 0 else Decimal("0")

async def wait_for_fill(session: aiohttp.ClientSession, symbol: str, order_id: int, max_wait_sec: int) -> Tuple[bool, Dict[str, Any]]:
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
    if not last:
        last = await fetch_order(session, symbol, order_id)
    return False, last


# ==========================================================
# Core actions
# ==========================================================
async def write_position_from_order(
    conn: asyncpg.Connection,
    intent: asyncpg.Record,
    order_id: int,
    ordj: Dict[str, Any]
) -> None:
    intent_id = int(intent["id"])
    symbol = str(intent["symbol"]).upper()
    intent_source = str(intent.get("source") or "")
    entry_mode = str(intent.get("entry_mode") or "LIMIT").upper()

    executed_qty = _d(ordj.get("executedQty", "0"))
    cum_quote = _d(ordj.get("cummulativeQuoteQty", "0"))
    st = (ordj.get("status") or "").upper()

    if st != "FILLED":
        raise RuntimeError(f"write_position_from_order called with status={st}")

    px = avg_fill_price(executed_qty, cum_quote)
    if executed_qty <= 0 or px <= 0:
        raise RuntimeError(f"FILLED but invalid executedQty/avgPx: executedQty={executed_qty} cumQuote={cum_quote}")

    entry_time = datetime.now(timezone.utc)
    entry_price = float(px)
    qty = float(executed_qty)

    sl = entry_price * (1.0 - EXEC_DEFAULT_SL_PCT)
    tp = entry_price * (1.0 + EXEC_DEFAULT_TP_PCT)

    await conn.execute(MARK_INTENT_FILLED_SQL, intent_id)
    await conn.execute(UPSERT_POSITION_OPEN_SQL, symbol, entry_time, entry_price, sl, tp, qty, order_id)

    # Write ENTRY trade_log row (idempotent by entry_order_id unique constraint)
    reason = f"ENTRY_{entry_mode}"
    await conn.execute(
        INSERT_TRADE_LOG_ENTRY_SQL,
        symbol,
        entry_time,
        entry_price,
        reason,
        intent_source,
        entry_mode,
        intent_id,
        order_id
    )

    logging.warning(
        "POSITION_OPENED id=%d %s | FILLED order_id=%d | avg_fill=%.10f qty=%.10f | source=%s mode=%s",
        intent_id, symbol, order_id, entry_price, qty, intent_source, entry_mode
    )


# ==========================================================
# NEW intents (Variant B)
# ==========================================================
async def _place_and_kickoff_fill_wait(pool: asyncpg.Pool, session: aiohttp.ClientSession, intent: asyncpg.Record) -> None:
    intent_id = int(intent["id"])
    symbol = str(intent["symbol"]).upper()
    quote_amount = float(intent["quote_amount"])
    entry_mode = str(intent.get("entry_mode") or "LIMIT").upper()
    limit_price = float(intent["limit_price"])  # always present (NOT NULL), may be reference price for MARKET

    if not symbol.endswith(QUOTE_CCY):
        async with pool.acquire() as conn:
            await conn.execute(MARK_INTENT_ERROR_SQL, intent_id, f"BAD_SYMBOL_QUOTE({QUOTE_CCY})")
        raise RuntimeError(f"Symbol {symbol} does not end with {QUOTE_CCY}")

    # check open position (fresh check in this task)
    async with pool.acquire() as conn:
        has_pos = await conn.fetchrow(HAS_OPEN_POSITION_SQL, symbol)
        if has_pos:
            await conn.execute(MARK_INTENT_ERROR_SQL, intent_id, "OPEN_POSITION_EXISTS")
            logging.warning("INTENT_SKIP id=%d %s | open position exists", intent_id, symbol)
            return

    # place order
    try:
        if entry_mode == "MARKET":
            order_id, client_oid = await place_market_buy(session, intent_id, symbol, quote_amount)
            qty_s, px_s = "?", "MKT"
        else:
            order_id, client_oid, qty_s, px_s = await place_limit_buy(session, intent_id, symbol, quote_amount, limit_price)

    except Exception as e:
        async with pool.acquire() as conn:
            await conn.execute(MARK_INTENT_ERROR_SQL, intent_id, f"PLACE_FAILED: {str(e)}")
        logging.error("INTENT_ERROR id=%d %s | place failed | %s", intent_id, symbol, str(e))
        return

    # store order_id/client_oid WITHOUT touching updated_at
    async with pool.acquire() as conn:
        await conn.execute(MARK_INTENT_SENT_SQL, intent_id, order_id, client_oid)

    if entry_mode == "MARKET":
        logging.warning(
            "INTENT_SENT id=%d %s | mode=MARKET | quote=%.2f %s | order_id=%d client_oid=%s | ref_limit_price=%.8f",
            intent_id, symbol, quote_amount, QUOTE_CCY, order_id, client_oid, limit_price
        )
    else:
        logging.warning(
            "INTENT_SENT id=%d %s | mode=LIMIT | quote=%.2f %s | limit=%s qty=%s | order_id=%d client_oid=%s",
            intent_id, symbol, quote_amount, QUOTE_CCY, px_s, qty_s, order_id, client_oid
        )

    # parallel fast-path wait
    try:
        filled, ordj = await wait_for_fill(session, symbol, order_id, FILL_WAIT_MAX_SEC)
        if filled:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await write_position_from_order(conn, intent, order_id, ordj)
        else:
            st = (ordj.get("status") or "").upper()
            logging.warning(
                "INTENT_NOT_FILLED id=%d %s | mode=%s order_id=%d status=%s | waited=%ds | will reconcile later%s",
                intent_id, symbol, entry_mode, order_id, st, FILL_WAIT_MAX_SEC,
                "" if entry_mode == "MARKET" else f" (timeout cancel after {EXEC_CANCEL_AFTER_SEC}s)"
            )
    except Exception as e:
        logging.error("FILL_WAIT_ERROR id=%d %s | order_id=%d | %s", intent_id, symbol, order_id, str(e))


async def new_intents_loop(pool: asyncpg.Pool) -> None:
    timeout = aiohttp.ClientTimeout(total=30)
    inflight_sem = asyncio.Semaphore(EXEC_MAX_INFLIGHT)
    tasks: Set[asyncio.Task] = set()

    def _done_cb(t: asyncio.Task) -> None:
        tasks.discard(t)
        try:
            _ = t.result()
        except Exception as e:
            logging.error("INTENT_TASK_ERROR | %s", str(e))
        finally:
            inflight_sem.release()

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        intents = await conn.fetch(PICK_NEW_INTENTS_SQL, MAX_BATCH, EXEC_SOURCES)

                if not intents:
                    await asyncio.sleep(POLL_EVERY_SEC)
                    continue

                for it in intents:
                    await inflight_sem.acquire()
                    t = asyncio.create_task(_place_and_kickoff_fill_wait(pool, session, it))
                    tasks.add(t)
                    t.add_done_callback(_done_cb)

                await asyncio.sleep(0)

            except Exception as e:
                logging.exception("NEW loop error: %s | sleeping %ds", e, POLL_EVERY_SEC)
                await asyncio.sleep(POLL_EVERY_SEC)


# ==========================================================
# Reconciler for SENT intents (fills + timeout cancel for LIMIT)
# ==========================================================
async def reconcile_sent_once(conn: asyncpg.Connection, session: aiohttp.ClientSession) -> None:
    intents = await conn.fetch(PICK_SENT_INTENTS_SQL, RECONCILE_BATCH, EXEC_SOURCES)
    if not intents:
        return

    now = datetime.now(timezone.utc)

    for it in intents:
        intent_id = int(it["id"])
        symbol = str(it["symbol"]).upper()
        order_id = int(it["order_id"])
        entry_mode = str(it.get("entry_mode") or "LIMIT").upper()

        sent_at = it.get("updated_at")
        age_sec: Optional[float] = None
        if isinstance(sent_at, datetime):
            if sent_at.tzinfo is None:
                sent_at = sent_at.replace(tzinfo=timezone.utc)
            age_sec = (now - sent_at).total_seconds()

        try:
            ordj = await fetch_order(session, symbol, order_id)
            st = (ordj.get("status") or "").upper()

            if st == "FILLED":
                await write_position_from_order(conn, it, order_id, ordj)
                continue

            if st in ("CANCELED", "CANCELLED", "REJECTED", "EXPIRED"):
                await conn.execute(MARK_INTENT_ERROR_SQL, intent_id, f"ORDER_{st}")
                logging.error("INTENT_ERROR id=%d %s | order_id=%d status=%s", intent_id, symbol, order_id, st)
                continue

            # Timeout cancel applies ONLY to LIMIT orders.
            if entry_mode == "LIMIT" and age_sec is not None and age_sec >= EXEC_CANCEL_AFTER_SEC:
                try:
                    _ = await cancel_order(session, symbol, order_id)
                    ordj2 = await fetch_order(session, symbol, order_id)
                    st2 = (ordj2.get("status") or "").upper()

                    await conn.execute(MARK_INTENT_ERROR_SQL, intent_id, f"TIMEOUT_CANCEL_{st2}")
                    logging.warning(
                        "INTENT_TIMEOUT_CANCEL id=%d %s | order_id=%d age=%.0fs | final_status=%s | marked ERROR",
                        intent_id, symbol, order_id, age_sec, st2
                    )
                except Exception as ce:
                    logging.error(
                        "CANCEL_ERROR id=%d %s | order_id=%d age=%.0fs | %s",
                        intent_id, symbol, order_id, age_sec, str(ce)
                    )
                continue

            logging.info(
                "RECONCILE_PENDING id=%d %s | mode=%s | order_id=%d status=%s age=%ss",
                intent_id, symbol, entry_mode, order_id, st, f"{age_sec:.0f}" if age_sec is not None else "?"
            )

        except Exception as e:
            logging.error("RECONCILE_ERROR id=%d %s | order_id=%d | %s", intent_id, symbol, order_id, str(e))


async def reconcile_loop(pool: asyncpg.Pool) -> None:
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        await reconcile_sent_once(conn, session)
                await asyncio.sleep(RECONCILE_EVERY_SEC)
            except Exception as e:
                logging.exception("RECONCILE loop error: %s | sleeping %ds", e, RECONCILE_EVERY_SEC)
                await asyncio.sleep(RECONCILE_EVERY_SEC)


# ==========================================================
# main
# ==========================================================
async def main() -> None:
    logging.info(
        "Starting executor_service (Variant B) | quote=%s | sources=%s | new_poll=%ds batch=%d inflight=%d | fill_wait=%ds | reconcile=%ds batch=%d | cancel_after=%ds",
        QUOTE_CCY,
        ",".join(EXEC_SOURCES) if EXEC_SOURCES else "<ALL>",
        POLL_EVERY_SEC, MAX_BATCH, EXEC_MAX_INFLIGHT,
        FILL_WAIT_MAX_SEC,
        RECONCILE_EVERY_SEC, RECONCILE_BATCH,
        EXEC_CANCEL_AFTER_SEC
    )

    pool = await asyncpg.create_pool(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        min_size=1, max_size=10
    )
    logging.info("DB pool created | host=%s db=%s user=%s", DB_HOST, DB_NAME, DB_USER)

    await asyncio.gather(
        new_intents_loop(pool),
        reconcile_loop(pool),
    )

if __name__ == "__main__":
    asyncio.run(main())
