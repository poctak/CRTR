#!/usr/bin/env python3
# accumulation_entry_service.py
# ------------------------------------------------------------
# 🔵 BTC neutral -> create LIMIT BUY intents based on ACCUM signals
# 🔴 BTC dump/pump -> do nothing (handled by progressive_service or skipped)
#
# Requires trade_intents columns:
#   symbol, ts, source, side, quote_amount, limit_price, support_price, meta, status, entry_mode
#
# Assumes:
#   - BTC candles in a table like public.candles_5m (symbol, ts, o,h,l,c,...)
#   - ACCUM signals in public.acum_signals with at least:
#       id, symbol, tf, signal_time, support, low, status ('NEW'|'USED')
# ------------------------------------------------------------

import os
import json
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any

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

    # Polling
    poll_sec: int
    max_signals_per_cycle: int

    # Entry
    quote_amount: float
    limit_offset_pct: float  # place slightly above low/support if >0

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

        poll_sec=env_int("POLL_SEC", 2),
        max_signals_per_cycle=env_int("MAX_SIGNALS_PER_CYCLE", 20),

        quote_amount=env_float("ENTRY_QUOTE_AMOUNT", 10.0),
        limit_offset_pct=env_float("LIMIT_OFFSET_PCT", 0.0),

        source=env_str("INTENT_SOURCE", "ACCUM_NEUTRAL"),
        side=env_str("INTENT_SIDE", "BUY"),
    )


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


async def fetch_new_accum_signals(pool: asyncpg.Pool, cfg: Config):
    q = """
        SELECT id, symbol, tf, signal_time, support, low
        FROM public.acum_signals
        WHERE status = 'NEW'
        ORDER BY signal_time ASC
        LIMIT $1
    """
    return await pool.fetch(q, cfg.max_signals_per_cycle)


async def mark_signal_used(pool: asyncpg.Pool, signal_id: int):
    q = "UPDATE public.acum_signals SET status='USED' WHERE id=$1"
    await pool.execute(q, signal_id)


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


async def run(cfg: Config):
    pool = await asyncpg.create_pool(dsn=cfg.db_dsn, min_size=1, max_size=5)
    logging.info(
        "ACCUM_ENTRY started | source=%s | btc=%s neutral=[%.3f%%..%.3f%%] lookback=%d bars",
        cfg.source,
        cfg.btc_symbol,
        cfg.btc_neutral_min * 100.0,
        cfg.btc_neutral_max * 100.0,
        cfg.btc_lookback_bars,
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

                signals = await fetch_new_accum_signals(pool, cfg)
                if not signals:
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                for s in signals:
                    sid = int(s["id"])
                    sym = str(s["symbol"])
                    sig_time = s["signal_time"]
                    support = float(s["support"]) if s["support"] is not None else None
                    low = float(s["low"]) if s["low"] is not None else None

                    # one intent per symbol+source while pending/sent
                    if await pending_intent_exists(pool, sym, cfg.source):
                        await mark_signal_used(pool, sid)
                        logging.info("SKIP_DUPLICATE | %s signal_id=%d (pending exists)", sym, sid)
                        continue

                    base = low if low and low > 0 else (support if support and support > 0 else None)
                    if not base:
                        await mark_signal_used(pool, sid)
                        logging.warning("SKIP_NO_PRICE | %s signal_id=%d (low/support missing)", sym, sid)
                        continue

                    limit_price = base * (1.0 + cfg.limit_offset_pct)

                    meta = {
                        "regime": "BTC_NEUTRAL",
                        "btc_change": btc_delta,
                        "signal": {
                            "id": sid,
                            "tf": s["tf"],
                            "time": sig_time.isoformat() if sig_time else None,
                            "support": support,
                            "low": low,
                        },
                        "limit_offset_pct": cfg.limit_offset_pct,
                    }

                    intent_id = await create_limit_intent(
                        pool,
                        symbol=sym,
                        ts=sig_time,  # ts = signal time
                        source=cfg.source,
                        side=cfg.side,
                        quote_amount=cfg.quote_amount,
                        limit_price=limit_price,
                        support_price=support,
                        meta=meta,
                    )

                    await mark_signal_used(pool, sid)
                    logging.warning(
                        "INTENT_NEW_LIMIT | id=%d %s | quote=%.2f | limit=%.8f | btcΔ=%+.3f%%",
                        intent_id,
                        sym,
                        cfg.quote_amount,
                        limit_price,
                        btc_delta * 100.0,
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
