#!/usr/bin/env python3
# accumulation_entry_service.py
# ------------------------------------------------------------
# 🔵 BTC neutral -> create LIMIT BUY intents based on ACCUM signals
#
# Reads:
#   public.accum_signals(id, symbol, ts, signal, details jsonb)
#     - we filter on signal name (default: 'ACCUM_PHASE')
#     - support/low are extracted from details json
#
# Writes:
#   public.trade_intents:
#     symbol, ts, source, side, quote_amount, limit_price, support_price, meta, status, entry_mode
#
# Important:
#   accum_signals has no status -> this service is idempotent by:
#     - checking for existing PENDING/SENT intents per (symbol, source)
#   and by only looking at recent signals (SIGNAL_MAX_AGE_MIN).
# ------------------------------------------------------------

import os
import json
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple, List

import asyncpg

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v and v.strip() else default

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v and v.strip() else default

def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v.strip() if v and v.strip() else default


@dataclass
class Config:
    db_dsn: str

    # BTC regime
    btc_symbol: str
    candles_table: str
    btc_lookback_bars: int
    btc_neutral_min: float
    btc_neutral_max: float

    # Signals
    signal_name: str
    signal_max_age_min: int
    max_signals_per_cycle: int

    # Polling
    poll_sec: int

    # Entry
    quote_amount: float
    limit_offset_pct: float

    # Intent fields
    source: str
    side: str


def load_config() -> Config:
    return Config(
        db_dsn=env_str("DB_DSN", "postgresql://pumpuser:pumpsecret@db:5432/pumpdb"),

        btc_symbol=env_str("BTC_SYMBOL", "BTCUSDC"),
        candles_table=env_str("CANDLES_TABLE", "public.candles_5m"),
        btc_lookback_bars=env_int("BTC_LOOKBACK_BARS", 3),  # 3x5m=15m

        btc_neutral_min=env_float("BTC_NEUTRAL_MIN", -0.004),
        btc_neutral_max=env_float("BTC_NEUTRAL_MAX", 0.006),

        signal_name=env_str("ACCUM_SIGNAL_NAME", "ACCUM_PHASE"),
        signal_max_age_min=env_int("SIGNAL_MAX_AGE_MIN", 60),
        max_signals_per_cycle=env_int("MAX_SIGNALS_PER_CYCLE", 50),

        poll_sec=env_int("POLL_SEC", 2),

        quote_amount=env_float("ENTRY_QUOTE_AMOUNT", 10.0),
        limit_offset_pct=env_float("LIMIT_OFFSET_PCT", 0.0),

        source=env_str("INTENT_SOURCE", "ACCUM_NEUTRAL"),
        side=env_str("INTENT_SIDE", "BUY"),
    )


# ---------- BTC helpers ----------
async def fetch_btc_change(pool: asyncpg.Pool, cfg: Config) -> Optional[float]:
    q = f"""
        SELECT ts, c
        FROM {cfg.candles_table}
        WHERE symbol = $1
        ORDER BY ts DESC
        LIMIT $2
    """
    rows = await pool.fetch(q, cfg.btc_symbol, cfg.btc_lookback_bars + 1)
    if len(rows) < cfg.btc_lookback_bars + 1:
        return None
    c_now = float(rows[0]["c"])
    c_then = float(rows[-1]["c"])
    if c_then <= 0:
        return None
    return (c_now / c_then) - 1.0


def btc_is_neutral(delta: float, cfg: Config) -> bool:
    return cfg.btc_neutral_min <= delta <= cfg.btc_neutral_max


# ---------- Signal helpers ----------
def _first_number(d: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for k in keys:
        if k in d and d[k] is not None:
            try:
                return float(d[k])
            except Exception:
                pass
    return None


def extract_prices(details: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """
    Tries to extract (support, low) from details JSON.
    Supports multiple key variants to match your existing logs.
    """
    # common variants we've used/seen
    support = _first_number(details, ["support", "support_price", "supportPrice"])
    low = _first_number(details, ["low", "l", "range_low", "rangeLow"])

    # Some systems store support window like win=[a..b]
    # If present, use lower bound as support-ish.
    if support is None:
        win = details.get("win") or details.get("window") or None
        # accept "x..y" string or [x,y] list
        try:
            if isinstance(win, str) and ".." in win:
                a = float(win.split("..")[0].strip("[] ()"))
                support = a
            elif isinstance(win, (list, tuple)) and len(win) >= 1:
                support = float(win[0])
        except Exception:
            pass

    return support, low


async def fetch_recent_accum_signals(pool: asyncpg.Pool, cfg: Config):
    q = """
        SELECT id, symbol, ts, signal, details
        FROM public.accum_signals
        WHERE signal = $1
          AND ts >= NOW() - ($2::int * INTERVAL '1 minute')
        ORDER BY ts ASC
        LIMIT $3
    """
    return await pool.fetch(q, cfg.signal_name, cfg.signal_max_age_min, cfg.max_signals_per_cycle)


# ---------- Intent helpers ----------
async def pending_intent_exists(pool: asyncpg.Pool, symbol: str, source: str) -> bool:
    q = """
        SELECT 1
        FROM public.trade_intents
        WHERE symbol=$1 AND source=$2 AND status IN ('PENDING','SENT')
        LIMIT 1
    """
    return (await pool.fetchrow(q, symbol, source)) is not None


async def create_limit_intent(
    pool: asyncpg.Pool,
    *,
    symbol: str,
    ts,
    source: str,
    side: str,
    quote_amount: float,
    limit_price: float,
    support_price: Optional[float],
    meta: Dict[str, Any],
) -> int:
    q = """
        INSERT INTO public.trade_intents
            (symbol, ts, source, side, quote_amount, limit_price, support_price, meta, status, entry_mode)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, 'PENDING', 'LIMIT')
        RETURNING id
    """
    row = await pool.fetchrow(
        q,
        symbol,
        ts,
        source,
        side,
        quote_amount,
        limit_price,
        support_price,
        json.dumps(meta),
    )
    return int(row["id"])


# ---------- Main loop ----------
async def run(cfg: Config):
    pool = await asyncpg.create_pool(dsn=cfg.db_dsn, min_size=1, max_size=5)
    logging.info(
        "ACCUM_ENTRY started | source=%s | signal=%s max_age=%dmin | btc=%s neutral=[%.3f%%..%.3f%%]",
        cfg.source,
        cfg.signal_name,
        cfg.signal_max_age_min,
        cfg.btc_symbol,
        cfg.btc_neutral_min * 100.0,
        cfg.btc_neutral_max * 100.0,
    )

    try:
        while True:
            try:
                btc_delta = await fetch_btc_change(pool, cfg)
                if btc_delta is None:
                    logging.warning("BTC_CHANGE unavailable (need more candles)")
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                if not btc_is_neutral(btc_delta, cfg):
                    logging.info("BTC_NOT_NEUTRAL | Δ=%+.3f%% | skip ACCUM intents", btc_delta * 100.0)
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                signals = await fetch_recent_accum_signals(pool, cfg)
                if not signals:
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                for s in signals:
                    sid = int(s["id"])
                    sym = str(s["symbol"])
                    sig_ts = s["ts"]
                    details = dict(s["details"]) if s["details"] is not None else {}

                    # idempotency guard
                    if await pending_intent_exists(pool, sym, cfg.source):
                        continue

                    support, low = extract_prices(details)
                    base = low if low and low > 0 else (support if support and support > 0 else None)
                    if not base:
                        logging.warning("SKIP_NO_PRICE | %s accum_signal_id=%d (details lacks low/support)", sym, sid)
                        continue

                    limit_price = base * (1.0 + cfg.limit_offset_pct)

                    meta = {
                        "regime": "BTC_NEUTRAL",
                        "btc_change": btc_delta,
                        "accum_signal": {
                            "id": sid,
                            "ts": sig_ts.isoformat() if sig_ts else None,
                            "signal": s["signal"],
                            "details": details,
                        },
                        "parsed": {
                            "support": support,
                            "low": low,
                            "base": base,
                            "limit_offset_pct": cfg.limit_offset_pct,
                        }
                    }

                    intent_id = await create_limit_intent(
                        pool,
                        symbol=sym,
                        ts=sig_ts,  # ts = time of signal
                        source=cfg.source,
                        side=cfg.side,
                        quote_amount=cfg.quote_amount,
                        limit_price=limit_price,
                        support_price=support,
                        meta=meta,
                    )

                    logging.warning(
                        "INTENT_NEW_LIMIT | id=%d %s | quote=%.2f | limit=%.8f | btcΔ=%+.3f%% | accum_id=%d",
                        intent_id, sym, cfg.quote_amount, limit_price, btc_delta * 100.0, sid
                    )

                await asyncio.sleep(cfg.poll_sec)

            except Exception as e:
                logging.exception("LOOP_ERROR: %s", e)
                await asyncio.sleep(3)

    finally:
        await pool.close()


def main():
    cfg = load_config()
    asyncio.run(run(cfg))


if __name__ == "__main__":
    main()
