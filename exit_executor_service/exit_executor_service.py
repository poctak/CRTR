import os
import time
import hmac
import json
import math
import hashlib
import asyncio
import logging
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from decimal import Decimal, ROUND_DOWN, getcontext

import aiohttp
import asyncpg

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# =========================
# DECIMAL
# =========================
getcontext().prec = 28

def d(x: Any) -> Decimal:
    return Decimal(str(x))

# =========================
# ENV
# =========================
DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "pumpdb")
DB_USER = os.getenv("DB_USER", "pumpuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

BINANCE_BASE_URL = os.getenv("BINANCE_BASE_URL", "https://api.binance.com").rstrip("/")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()
BINANCE_RECV_WINDOW = int(os.getenv("BINANCE_RECV_WINDOW", "5000"))

EXECUTOR_POLL_SEC = int(os.getenv("EXECUTOR_POLL_SEC", "2"))
ORDER_CHECK_SEC = int(os.getenv("ORDER_CHECK_SEC", "3"))
LIMIT_TTL_SEC = int(os.getenv("LIMIT_TTL_SEC", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
FALLBACK_TO_MARKET = os.getenv("FALLBACK_TO_MARKET", "1").strip().lower() in ("1", "true", "yes")
REPRICE_USING_BOOK_BID = os.getenv("REPRICE_USING_BOOK_BID", "1").strip().lower() in ("1", "true", "yes")

# reclaim stuck PROCESSING after N seconds
PROCESSING_STALE_SEC = int(os.getenv("PROCESSING_STALE_SEC", "30"))

# log heartbeat when idle
IDLE_LOG_SEC = int(os.getenv("IDLE_LOG_SEC", "20"))

# qty safety buffer for fees/rounding (0.2% default)
QTY_BUFFER_PCT = d(os.getenv("QTY_BUFFER_PCT", "0.002"))  # 0.002 = 0.2%

FEE_ROUNDTRIP_PCT = float(os.getenv("FEE_ROUNDTRIP_PCT", "0.15"))

if not BINANCE_API_KEY or not BINANCE_API_SECRET:
    logging.warning("BINANCE_API_KEY/SECRET not set -> placing signed orders will fail.")

# =========================
# BINANCE SIGNED REQUEST (fix -1022)
# =========================

def _ts_ms() -> int:
    return int(time.time() * 1000)

def _sign(query_string: str) -> str:
    return hmac.new(
        BINANCE_API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

def _normalize_param_value(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)

def _build_query(params: Dict[str, Any]) -> str:
    items = []
    for k in sorted(params.keys()):
        v = params[k]
        if v is None:
            continue
        items.append((k, _normalize_param_value(v)))
    return urllib.parse.urlencode(items, quote_via=urllib.parse.quote, safe="")

async def binance_request(
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
        if not BINANCE_API_SECRET:
            raise RuntimeError("BINANCE_API_SECRET missing for signed request")

        p = dict(params)
        p["timestamp"] = _ts_ms()
        p["recvWindow"] = BINANCE_RECV_WINDOW

        qs = _build_query(p)
        sig = _sign(qs)
        full_qs = f"{qs}&signature={sig}" if qs else f"signature={sig}"
        url = f"{BINANCE_BASE_URL}{path}?{full_qs}"

        async with session.request(method, url, headers=headers) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"Binance HTTP {resp.status} {path} -> {text}")
            try:
                return json.loads(text)
            except Exception:
                return text

    url = f"{BINANCE_BASE_URL}{path}"
    async with session.request(method, url, params=params, headers=headers) as resp:
        text = await resp.text()
        if resp.status >= 400:
            raise RuntimeError(f"Binance HTTP {resp.status} {path} -> {text}")
        try:
            return json.loads(text)
        except Exception:
            return text

# =========================
# exchangeInfo cache (tick/step + baseAsset)
# =========================

def _decimals_from_step_str(step: str) -> int:
    if "." not in step:
        return 0
    frac = step.split(".", 1)[1].rstrip("0")
    return len(frac)

def _quantize_down(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step

def _fmt_decimal_fixed(x: Decimal, decimals: int) -> str:
    if decimals <= 0:
        return str(x.quantize(Decimal("1"), rounding=ROUND_DOWN))
    q = Decimal("1").scaleb(-decimals)  # 10^-decimals
    return format(x.quantize(q, rounding=ROUND_DOWN), "f")

@dataclass
class SymbolFilters:
    base_asset: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    min_notional: Decimal
    tick_decimals: int
    step_decimals: int

EXINFO_CACHE: Dict[str, SymbolFilters] = {}
ACCOUNT_FREE_CACHE: Dict[str, Tuple[Decimal, float]] = {}  # asset -> (free, ts)

ACCOUNT_CACHE_SEC = int(os.getenv("ACCOUNT_CACHE_SEC", "2"))  # cache free balance briefly

async def load_exchange_info(session: aiohttp.ClientSession) -> None:
    data = await binance_request(session, "GET", "/api/v3/exchangeInfo", signed=False)
    symbols = data.get("symbols", [])
    for s in symbols:
        sym = (s.get("symbol") or "").upper()
        if not sym:
            continue

        base_asset = (s.get("baseAsset") or "").upper()

        tick_str = "0"
        step_str = "0"
        min_qty_str = "0"
        min_notional_str = "0"

        for f in s.get("filters", []):
            t = f.get("filterType")
            if t == "PRICE_FILTER":
                tick_str = str(f.get("tickSize", "0"))
            elif t == "LOT_SIZE":
                step_str = str(f.get("stepSize", "0"))
                min_qty_str = str(f.get("minQty", "0"))
            elif t == "MIN_NOTIONAL":
                min_notional_str = str(f.get("minNotional", "0"))

        tick_dec = _decimals_from_step_str(tick_str)
        step_dec = _decimals_from_step_str(step_str)

        EXINFO_CACHE[sym] = SymbolFilters(
            base_asset=base_asset,
            tick_size=d(tick_str),
            step_size=d(step_str),
            min_qty=d(min_qty_str),
            min_notional=d(min_notional_str),
            tick_decimals=tick_dec,
            step_decimals=step_dec,
        )

    logging.info("Loaded exchangeInfo for %d symbols", len(EXINFO_CACHE))

async def book_bid_price(session: aiohttp.ClientSession, symbol: str) -> Optional[Decimal]:
    data = await binance_request(session, "GET", "/api/v3/ticker/bookTicker", params={"symbol": symbol}, signed=False)
    bid = data.get("bidPrice")
    try:
        b = d(bid)
        return b if b > 0 else None
    except Exception:
        return None

async def get_free_balance(session: aiohttp.ClientSession, asset: str) -> Decimal:
    """
    Avoid hammering /account by caching for a short time.
    """
    now = time.time()
    cached = ACCOUNT_FREE_CACHE.get(asset)
    if cached and (now - cached[1]) <= ACCOUNT_CACHE_SEC:
        return cached[0]

    acc = await binance_request(session, "GET", "/api/v3/account", signed=True)
    free = Decimal("0")
    for b in acc.get("balances", []):
        if (b.get("asset") or "").upper() == asset:
            free = d(b.get("free", "0"))
            break

    ACCOUNT_FREE_CACHE[asset] = (free, now)
    return free

# =========================
# DB
# =========================

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

async def db_claim_one_closing(pool: asyncpg.Pool) -> Optional[asyncpg.Record]:
    """
    Claim:
      - rows not PROCESSING
      - OR PROCESSING but stale (updated_at older than PROCESSING_STALE_SEC)
    """
    sql = f"""
    WITH cte AS (
      SELECT symbol
      FROM positions_open
      WHERE status='CLOSING'
        AND exit_filled_at IS NULL
        AND (
              exit_order_status IS DISTINCT FROM 'PROCESSING'
              OR updated_at < (NOW() - INTERVAL '{PROCESSING_STALE_SEC} seconds')
            )
      ORDER BY close_requested_at NULLS LAST, updated_at
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    UPDATE positions_open p
    SET exit_order_status='PROCESSING', updated_at=NOW()
    FROM cte
    WHERE p.symbol = cte.symbol
    RETURNING p.*;
    """
    async with pool.acquire() as conn:
        return await conn.fetchrow(sql)

async def db_set_order_placed(pool: asyncpg.Pool, symbol: str, order_id: str, limit_price: float, status: str) -> None:
    sql = """
    UPDATE positions_open
    SET
      exit_order_id=$2,
      exit_order_price=$3,
      exit_order_status=$4,
      exit_order_placed_at=NOW(),
      updated_at=NOW()
    WHERE symbol=$1 AND status='CLOSING'
    """
    async with pool.acquire() as conn:
        await conn.execute(sql, symbol, order_id, limit_price, status)

async def db_set_order_status(pool: asyncpg.Pool, symbol: str, status: str) -> None:
    sql = """
    UPDATE positions_open
    SET exit_order_status=$2, updated_at=NOW()
    WHERE symbol=$1 AND status='CLOSING'
    """
    async with pool.acquire() as conn:
        await conn.execute(sql, symbol, status)

async def db_inc_retries(pool: asyncpg.Pool, symbol: str) -> int:
    sql = """
    UPDATE positions_open
    SET exit_retries = COALESCE(exit_retries,0) + 1, updated_at=NOW()
    WHERE symbol=$1 AND status='CLOSING'
    RETURNING exit_retries
    """
    async with pool.acquire() as conn:
        r = await conn.fetchrow(sql, symbol)
    return int(r["exit_retries"]) if r and r["exit_retries"] is not None else 0

async def db_set_fill(pool: asyncpg.Pool, symbol: str, fill_price: float) -> None:
    sql = """
    UPDATE positions_open
    SET
      exit_fill_price=$2,
      exit_filled_at=NOW(),
      exit_order_status='FILLED',
      updated_at=NOW()
    WHERE symbol=$1 AND status='CLOSING'
    """
    async with pool.acquire() as conn:
        await conn.execute(sql, symbol, fill_price)

async def db_load_position(pool: asyncpg.Pool, symbol: str) -> Optional[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM positions_open WHERE symbol=$1", symbol)

async def insert_trade_log(pool: asyncpg.Pool, pos: asyncpg.Record) -> None:
    entry_price = float(pos["entry_price"])
    exit_price = float(pos["exit_fill_price"])
    gross_pct = (exit_price - entry_price) / entry_price * 100.0 if entry_price > 0 else 0.0
    net_pct = gross_pct - FEE_ROUNDTRIP_PCT

    sql = """
    INSERT INTO trade_log(symbol, entry_time, exit_time, entry_price, exit_price, reason, pnl_pct)
    VALUES ($1,$2,$3,$4,$5,$6,$7)
    """
    async with pool.acquire() as conn:
        await conn.execute(
            sql,
            pos["symbol"],
            pos["entry_time"],
            pos["exit_filled_at"],
            entry_price,
            exit_price,
            (pos.get("close_reason") or "UNKNOWN"),
            net_pct,
        )

async def delete_position(pool: asyncpg.Pool, symbol: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM positions_open WHERE symbol=$1", symbol)

# =========================
# ORDER EXECUTION
# =========================

def compute_limit_price(close_ref: Decimal, bid: Optional[Decimal]) -> Decimal:
    if REPRICE_USING_BOOK_BID and bid and bid > 0:
        return bid
    return close_ref

def normalize_qty_price(symbol: str, qty: Decimal, price: Decimal) -> Tuple[str, str, float, Decimal]:
    """
    Returns:
      qty_str, price_str, price_float_for_db, qty_quantized_decimal
    """
    f = EXINFO_CACHE.get(symbol)
    if not f:
        qty_q = qty
        price_q = price
        qty_str = _fmt_decimal_fixed(qty_q, 8)
        price_str = _fmt_decimal_fixed(price_q, 8)
        return qty_str, price_str, float(price_q), qty_q

    qty_q = _quantize_down(qty, f.step_size)
    price_q = _quantize_down(price, f.tick_size)

    qty_str = _fmt_decimal_fixed(qty_q, f.step_decimals)
    price_str = _fmt_decimal_fixed(price_q, f.tick_decimals)
    return qty_str, price_str, float(price_q), qty_q

async def place_limit_sell(session: aiohttp.ClientSession, symbol: str, qty_str: str, price_str: str) -> Dict[str, Any]:
    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": qty_str,
        "price": price_str,
        "newOrderRespType": "RESULT",
    }
    return await binance_request(session, "POST", "/api/v3/order", params=params, signed=True)

async def place_market_sell(session: aiohttp.ClientSession, symbol: str, qty_str: str) -> Dict[str, Any]:
    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "MARKET",
        "quantity": qty_str,
        "newOrderRespType": "RESULT",
    }
    return await binance_request(session, "POST", "/api/v3/order", params=params, signed=True)

async def get_order(session: aiohttp.ClientSession, symbol: str, order_id: str) -> Dict[str, Any]:
    return await binance_request(session, "GET", "/api/v3/order", params={"symbol": symbol, "orderId": order_id}, signed=True)

async def cancel_order(session: aiohttp.ClientSession, symbol: str, order_id: str) -> Dict[str, Any]:
    return await binance_request(session, "DELETE", "/api/v3/order", params={"symbol": symbol, "orderId": order_id}, signed=True)

def extract_fill_price(order_resp: Dict[str, Any]) -> Optional[float]:
    try:
        executed = d(order_resp.get("executedQty", "0"))
        cum_quote = d(order_resp.get("cummulativeQuoteQty", "0"))
        if executed > 0 and cum_quote > 0:
            return float(cum_quote / executed)
    except Exception:
        pass
    return None

def clamp_qty_for_fee(qty: Decimal) -> Decimal:
    # keep a small buffer to avoid -2010 due to fee/rounding
    if QTY_BUFFER_PCT <= 0:
        return qty
    return qty * (Decimal("1") - QTY_BUFFER_PCT)

# =========================
# CORE PROCESSING
# =========================

async def process_position(pool: asyncpg.Pool, session: aiohttp.ClientSession, pos: asyncpg.Record) -> None:
    symbol = (pos["symbol"] or "").upper()
    f = EXINFO_CACHE.get(symbol)
    if not f:
        logging.error("No exchange info for %s", symbol)
        await db_set_order_status(pool, symbol, "ERROR_NO_EXINFO")
        return

    db_qty = d(pos.get("qty") or "0")
    if db_qty <= 0:
        logging.error("CLOSING %s but qty missing/invalid -> cannot execute sell", symbol)
        await db_set_order_status(pool, symbol, "ERROR_NO_QTY")
        return

    # --- balance clamp (fix -2010) ---
    free = await get_free_balance(session, f.base_asset)
    if free <= 0:
        logging.error("No free balance for %s (symbol=%s)", f.base_asset, symbol)
        await db_set_order_status(pool, symbol, "ERROR_NO_BALANCE")
        return

    qty = min(db_qty, free)
    qty = clamp_qty_for_fee(qty)

    close_ref = d(pos.get("close_ref_price") or "0")
    if close_ref <= 0:
        logging.warning("CLOSING %s close_ref_price missing/invalid, will use bid if enabled", symbol)

    # 1) place LIMIT if no order yet
    if not pos.get("exit_order_id"):
        bid = await book_bid_price(session, symbol) if REPRICE_USING_BOOK_BID else None
        base_price = close_ref if close_ref > 0 else (bid or Decimal("0"))
        raw_price = compute_limit_price(base_price, bid)

        qty_str, price_str, price_for_db, qty_q = normalize_qty_price(symbol, qty, raw_price)

        if qty_q < f.min_qty or d(qty_str) <= 0:
            logging.error("Qty too small after clamp/round: %s free=%s db=%s minQty=%s",
                          qty_str, str(free), str(db_qty), str(f.min_qty))
            await db_set_order_status(pool, symbol, "ERROR_QTY_TOO_SMALL")
            return

        logging.warning("PLACE LIMIT SELL %s qty=%s price=%s (db_qty=%s free=%s buffer=%s ref=%s bid=%s)",
                        symbol, qty_str, price_str, str(db_qty), str(free), str(QTY_BUFFER_PCT), str(close_ref), str(bid) if bid else None)

        resp = await place_limit_sell(session, symbol, qty_str, price_str)
        order_id = str(resp.get("orderId"))
        status = str(resp.get("status") or "NEW")
        await db_set_order_placed(pool, symbol, order_id, price_for_db, status)
        return

    # 2) check existing order
    order_id = str(pos.get("exit_order_id"))
    placed_at = pos.get("exit_order_placed_at")
    retries = int(pos.get("exit_retries") or 0)

    o = await get_order(session, symbol, order_id)
    status = str(o.get("status") or "")
    await db_set_order_status(pool, symbol, status)

    if status == "FILLED":
        fp = extract_fill_price(o) or float(pos.get("exit_order_price") or 0.0)
        if fp <= 0:
            fp = float(pos.get("exit_order_price") or 0.0)

        await db_set_fill(pool, symbol, fp)

        pos2 = await db_load_position(pool, symbol)
        if pos2:
            await insert_trade_log(pool, pos2)
            await delete_position(pool, symbol)

        logging.warning("✅ CLOSED %s FILLED order=%s fill=%s", symbol, order_id, fp)
        return

    if status in ("CANCELED", "REJECTED", "EXPIRED"):
        logging.warning("Order %s status=%s -> retry", symbol, status)
        retries = await db_inc_retries(pool, symbol)

        if retries > MAX_RETRIES and FALLBACK_TO_MARKET:
            # recompute qty (db/free may change)
            free = await get_free_balance(session, f.base_asset)
            qty = clamp_qty_for_fee(min(db_qty, free))
            qty_str, _, _, qty_q = normalize_qty_price(symbol, qty, Decimal("1"))
            if qty_q < f.min_qty or d(qty_str) <= 0:
                await db_set_order_status(pool, symbol, "ERROR_QTY_TOO_SMALL")
                return

            logging.warning("FALLBACK MARKET SELL %s qty=%s (retries=%s)", symbol, qty_str, retries)
            m = await place_market_sell(session, symbol, qty_str)
            fp = extract_fill_price(m) or float(pos.get("exit_order_price") or 0.0)

            await db_set_fill(pool, symbol, fp if fp > 0 else float(pos.get("exit_order_price") or 0.0))
            pos2 = await db_load_position(pool, symbol)
            if pos2:
                await insert_trade_log(pool, pos2)
                await delete_position(pool, symbol)

            logging.warning("✅ CLOSED %s MARKET fallback fill=%s", symbol, fp)
        return

    # TTL cancel/reprice
    if placed_at:
        now = datetime.now(timezone.utc)
        age = (now - placed_at).total_seconds()
        if age >= LIMIT_TTL_SEC:
            if retries >= MAX_RETRIES and FALLBACK_TO_MARKET:
                logging.warning("TTL reached. Cancel + MARKET fallback %s order=%s", symbol, order_id)
                try:
                    await cancel_order(session, symbol, order_id)
                except Exception as e:
                    logging.warning("Cancel failed %s: %s", symbol, e)

                await db_inc_retries(pool, symbol)

                free = await get_free_balance(session, f.base_asset)
                qty = clamp_qty_for_fee(min(db_qty, free))
                qty_str, _, _, qty_q = normalize_qty_price(symbol, qty, Decimal("1"))
                if qty_q < f.min_qty or d(qty_str) <= 0:
                    await db_set_order_status(pool, symbol, "ERROR_QTY_TOO_SMALL")
                    return

                m = await place_market_sell(session, symbol, qty_str)
                fp = extract_fill_price(m) or float(pos.get("exit_order_price") or 0.0)

                await db_set_fill(pool, symbol, fp if fp > 0 else float(pos.get("exit_order_price") or 0.0))
                pos2 = await db_load_position(pool, symbol)
                if pos2:
                    await insert_trade_log(pool, pos2)
                    await delete_position(pool, symbol)

                logging.warning("✅ CLOSED %s MARKET fallback fill=%s", symbol, fp)
                return

            logging.warning("TTL reached. Cancel + reprice LIMIT %s order=%s age=%.1fs retries=%s",
                            symbol, order_id, age, retries)
            try:
                await cancel_order(session, symbol, order_id)
            except Exception as e:
                logging.warning("Cancel failed %s: %s", symbol, e)

            retries2 = await db_inc_retries(pool, symbol)

            free = await get_free_balance(session, f.base_asset)
            qty = clamp_qty_for_fee(min(db_qty, free))
            bid = await book_bid_price(session, symbol) if REPRICE_USING_BOOK_BID else None
            base_price = close_ref if close_ref > 0 else (bid or Decimal("0"))
            raw_price = compute_limit_price(base_price, bid)

            qty_str, price_str, price_for_db, qty_q = normalize_qty_price(symbol, qty, raw_price)
            if qty_q < f.min_qty or d(qty_str) <= 0:
                await db_set_order_status(pool, symbol, "ERROR_QTY_TOO_SMALL")
                return

            resp = await place_limit_sell(session, symbol, qty_str, price_str)
            new_id = str(resp.get("orderId"))
            new_status = str(resp.get("status") or "NEW")
            await db_set_order_placed(pool, symbol, new_id, price_for_db, new_status)

            logging.warning("REPLACED LIMIT %s new_order=%s price=%s retries=%s bid=%s",
                            symbol, new_id, price_str, retries2, str(bid) if bid else None)
            return

# =========================
# MAIN LOOP
# =========================

async def loop(pool: asyncpg.Pool) -> None:
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        await load_exchange_info(session)

        last_idle = 0.0

        while True:
            try:
                pos = await db_claim_one_closing(pool)
                if not pos:
                    now = time.time()
                    if now - last_idle >= IDLE_LOG_SEC:
                        logging.info("Idle: no CLOSING positions")
                        last_idle = now
                    await asyncio.sleep(EXECUTOR_POLL_SEC)
                    continue

                await process_position(pool, session, pos)
                await asyncio.sleep(ORDER_CHECK_SEC)

            except Exception as e:
                logging.error("executor error: %s", e)
                await asyncio.sleep(2)

async def main():
    pool = await init_db_pool()
    await loop(pool)

if __name__ == "__main__":
    asyncio.run(main())
