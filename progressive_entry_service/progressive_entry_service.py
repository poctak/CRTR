#!/usr/bin/env python3
# progressive_entry_service.py
# ------------------------------------------------------------
# 🟢 BTC pump -> create MARKET BUY intents (idempotent)
#
# candles_5m schema (given):
#   symbol, ts, o,h,l,c,v_base,v_quote,trades_count
#
# trade_intents (assumed fields used here):
#   id (serial/bigserial)
#   symbol TEXT
#   ts TIMESTAMPTZ
#   source TEXT
#   entry_mode TEXT            -- 'MARKET'
#   limit_price DOUBLE PRECISION NOT NULL   -- reference price for MARKET
#   quote_amount DOUBLE PRECISION
#   status TEXT DEFAULT 'NEW'
#   meta JSONB
#   created_at TIMESTAMPTZ DEFAULT now()
#   updated_at TIMESTAMPTZ DEFAULT now()
#
# IMPORTANT:
# - Unique constraint exists: uq_trade_intents_symbol_ts on (symbol, ts)
# - This script uses ON CONFLICT (symbol, ts) DO NOTHING to be idempotent.
# ------------------------------------------------------------

import os
import json
import time
import signal
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple

import asyncpg


# ============================================================
# Logging
# ============================================================
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("progressive_entry")


# ============================================================
# ENV helpers
# ============================================================
def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v.strip() if v and v.strip() else default

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v and v.strip() else default

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v and v.strip() else default

def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or not v.strip():
        return default
    v = v.strip().lower()
    return v in ("1", "true", "yes", "y", "on")


# ============================================================
# Config
# ============================================================
@dataclass(frozen=True)
class Config:
    db_dsn: str

    candles_table: str
    intents_table: str

    btc_symbol: str
    symbols: List[str]          # targets to create intents for (typically alts)
    source: str

    tf_sec: int                 # expected timeframe seconds (5m = 300)
    poll_sec: float

    # Trigger rules
    pump_pct_th: float          # e.g. 0.008 = +0.8% over candle (o->c)
    min_btc_vq: float           # minimal BTC v_quote to consider pump valid
    require_green: bool         # require c > o

    # Intent params
    quote_amount: float
    intent_status: str

    # Safety / operations
    max_targets_per_trigger: int
    cooldown_candles: int       # after trigger, wait N candles before next trigger
    use_advisory_lock: bool
    advisory_lock_key: int

    # Debug
    log_debug: bool


def load_config() -> Config:
    # DB
    db_dsn = env_str("DB_DSN", env_str("DATABASE_URL", "postgresql://pumpuser:pumpsecret@db:5432/pumpdb"))

    candles_table = env_str("CANDLES_TABLE", "public.candles_5m")
    intents_table = env_str("TRADE_INTENTS_TABLE", "public.trade_intents")

    # Symbols
    btc_symbol = env_str("BTC_SYMBOL", "BTCUSDC")
    # Comma-separated. If empty, script will not create any intents.
    symbols_csv = env_str("SYMBOLS", "")
    symbols = [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]

    # Source label in DB
    source = env_str("INTENT_SOURCE", "PROGRESSIVE")

    # Timing
    tf_sec = env_int("TF_SEC", 300)  # 5m
    poll_sec = env_float("POLL_SEC", 2.0)

    # Trigger
    pump_pct_th = env_float("BTC_PUMP_PCT_TH", 0.008)   # 0.8%
    min_btc_vq = env_float("BTC_MIN_VQ", 0.0)          # no minimum by default
    require_green = env_bool("BTC_REQUIRE_GREEN", True)

    # Intent
    quote_amount = env_float("QUOTE_AMOUNT", 10.0)     # USDC
    intent_status = env_str("INTENT_STATUS", "NEW")

    # Safety
    max_targets_per_trigger = env_int("MAX_TARGETS_PER_TRIGGER", 50)
    cooldown_candles = env_int("COOLDOWN_CANDLES", 1)
    use_advisory_lock = env_bool("USE_ADVISORY_LOCK", True)
    advisory_lock_key = env_int("ADVISORY_LOCK_KEY", 778899)

    log_debug = env_bool("DEBUG", False)

    # Don't include BTC in targets by accident
    symbols = [s for s in symbols if s != btc_symbol]

    return Config(
        db_dsn=db_dsn,
        candles_table=candles_table,
        intents_table=intents_table,
        btc_symbol=btc_symbol,
        symbols=symbols,
        source=source,
        tf_sec=tf_sec,
        poll_sec=poll_sec,
        pump_pct_th=pump_pct_th,
        min_btc_vq=min_btc_vq,
        require_green=require_green,
        quote_amount=quote_amount,
        intent_status=intent_status,
        max_targets_per_trigger=max_targets_per_trigger,
        cooldown_candles=cooldown_candles,
        use_advisory_lock=use_advisory_lock,
        advisory_lock_key=advisory_lock_key,
        log_debug=log_debug,
    )


# ============================================================
# DB queries
# ============================================================
SQL_GET_LATEST_CANDLE = """
SELECT symbol, ts, o, h, l, c, v_base, v_quote, trades_count
FROM {candles_table}
WHERE symbol = $1
ORDER BY ts DESC
LIMIT 1;
"""

SQL_GET_CANDLE_AT_TS = """
SELECT symbol, ts, o, h, l, c, v_base, v_quote, trades_count
FROM {candles_table}
WHERE symbol = $1
  AND ts = $2
LIMIT 1;
"""

SQL_CREATE_MARKET_INTENT_IDEMPOTENT = """
INSERT INTO {intents_table}
    (symbol, ts, source, entry_mode, limit_price, quote_amount, status, meta, created_at, updated_at)
VALUES
    ($1, $2, $3, 'MARKET', $4, $5, $6, $7::jsonb, NOW(), NOW())
ON CONFLICT (symbol, ts) DO NOTHING
RETURNING id;
"""

SQL_TRY_ADVISORY_LOCK = "SELECT pg_try_advisory_lock($1) AS locked;"
SQL_UNLOCK_ADVISORY_LOCK = "SELECT pg_advisory_unlock($1) AS unlocked;"


# ============================================================
# Core logic
# ============================================================
def compute_pump_pct(o: float, c: float) -> float:
    if o <= 0:
        return 0.0
    return (c - o) / o


async def ensure_single_instance(conn: asyncpg.Connection, cfg: Config) -> None:
    if not cfg.use_advisory_lock:
        return
    row = await conn.fetchrow(SQL_TRY_ADVISORY_LOCK, cfg.advisory_lock_key)
    if not row or not row["locked"]:
        raise RuntimeError(
            f"Another instance holds advisory lock key={cfg.advisory_lock_key}. "
            f"Set USE_ADVISORY_LOCK=0 if you really want to run multiple instances."
        )
    log.info(f"ADVISORY_LOCK acquired key={cfg.advisory_lock_key}")


async def release_single_instance(conn: asyncpg.Connection, cfg: Config) -> None:
    if not cfg.use_advisory_lock:
        return
    try:
        row = await conn.fetchrow(SQL_UNLOCK_ADVISORY_LOCK, cfg.advisory_lock_key)
        if row and row["unlocked"]:
            log.info(f"ADVISORY_LOCK released key={cfg.advisory_lock_key}")
    except Exception as e:
        log.warning(f"ADVISORY_LOCK release failed: {e}")


async def fetch_latest_candle(conn: asyncpg.Connection, cfg: Config, symbol: str) -> Optional[asyncpg.Record]:
    q = SQL_GET_LATEST_CANDLE.format(candles_table=cfg.candles_table)
    return await conn.fetchrow(q, symbol)


async def fetch_candle_at_ts(conn: asyncpg.Connection, cfg: Config, symbol: str, ts) -> Optional[asyncpg.Record]:
    q = SQL_GET_CANDLE_AT_TS.format(candles_table=cfg.candles_table)
    return await conn.fetchrow(q, symbol, ts)


async def create_market_intent(
    conn: asyncpg.Connection,
    cfg: Config,
    symbol: str,
    ts,
    reference_price: float,
    extra_meta: Dict[str, Any],
) -> Optional[int]:
    """
    Creates MARKET trade_intent for (symbol, ts). Idempotent: if exists, returns None.
    """
    meta = {
        "ref_price": reference_price,
        "reason": "BTC_PUMP",
        "version": "2026-03-03",
        **(extra_meta or {}),
    }
    q = SQL_CREATE_MARKET_INTENT_IDEMPOTENT.format(intents_table=cfg.intents_table)
    row = await conn.fetchrow(
        q,
        symbol,
        ts,
        cfg.source,
        float(reference_price),
        float(cfg.quote_amount),
        cfg.intent_status,
        json.dumps(meta),
    )
    if not row:
        return None
    return int(row["id"])


def pick_targets(cfg: Config) -> List[str]:
    # Keep deterministic order from env. Limit size.
    if not cfg.symbols:
        return []
    return cfg.symbols[: max(0, cfg.max_targets_per_trigger)]


# ============================================================
# Runner
# ============================================================
class GracefulStop:
    def __init__(self) -> None:
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def wait(self) -> None:
        await self._stop.wait()

    def is_set(self) -> bool:
        return self._stop.is_set()


async def run(cfg: Config) -> None:
    stop = GracefulStop()

    def _handle_signal(sig, _frame=None):
        log.warning(f"Signal received: {sig}. Shutting down...")
        stop.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    conn: Optional[asyncpg.Connection] = None
    last_btc_ts = None
    cooldown_left = 0

    while not stop.is_set():
        try:
            if conn is None or conn.is_closed():
                conn = await asyncpg.connect(cfg.db_dsn)
                await ensure_single_instance(conn, cfg)
                log.info("DB connected")

            # --- get latest BTC candle (close)
            btc = await fetch_latest_candle(conn, cfg, cfg.btc_symbol)
            if not btc:
                log.warning(f"No candles found for {cfg.btc_symbol} in {cfg.candles_table}")
                await asyncio.sleep(cfg.poll_sec)
                continue

            btc_ts = btc["ts"]
            if last_btc_ts is None:
                last_btc_ts = btc_ts
                log.info(f"Bootstrap last_btc_ts={last_btc_ts} (waiting for next close)")
                await asyncio.sleep(cfg.poll_sec)
                continue

            if btc_ts == last_btc_ts:
                await asyncio.sleep(cfg.poll_sec)
                continue

            # New closed BTC candle arrived
            last_btc_ts = btc_ts

            o = float(btc["o"])
            c = float(btc["c"])
            vq = float(btc["v_quote"] or 0.0)
            pump_pct = compute_pump_pct(o, c)

            if cfg.log_debug:
                log.info(
                    f"BTC_CANDLE ts={btc_ts} o={o:.6f} c={c:.6f} pump={pump_pct*100:.3f}% vq={vq:.2f}"
                )

            if cooldown_left > 0:
                cooldown_left -= 1
                log.info(f"COOLDOWN active ({cooldown_left} candles left) after trigger")
                continue

            # Evaluate trigger
            if cfg.require_green and c <= o:
                log.info(f"NO_TRIGGER btc_not_green ts={btc_ts} o={o:.6f} c={c:.6f}")
                continue

            if vq < cfg.min_btc_vq:
                log.info(f"NO_TRIGGER btc_vq_low ts={btc_ts} vq={vq:.2f} < {cfg.min_btc_vq:.2f}")
                continue

            if pump_pct < cfg.pump_pct_th:
                log.info(
                    f"NO_TRIGGER pump_too_small ts={btc_ts} pump={pump_pct*100:.3f}% < {cfg.pump_pct_th*100:.3f}%"
                )
                continue

            # Triggered!
            targets = pick_targets(cfg)
            if not targets:
                log.warning("TRIGGERED but SYMBOLS list is empty; nothing to do.")
                cooldown_left = cfg.cooldown_candles
                continue

            log.warning(
                f"TRIGGER BTC_PUMP ts={btc_ts} pump={pump_pct*100:.3f}% vq={vq:.2f} -> intents={len(targets)}"
            )

            created = 0
            existed = 0
            missing_candle = 0
            errors = 0

            for sym in targets:
                # Align on the same candle ts (recommended when you have uq(symbol,ts))
                cc = await fetch_candle_at_ts(conn, cfg, sym, btc_ts)
                if not cc:
                    missing_candle += 1
                    if cfg.log_debug:
                        log.info(f"SKIP {sym} no candle at ts={btc_ts}")
                    continue

                ref_price = float(cc["c"])

                try:
                    intent_id = await create_market_intent(
                        conn,
                        cfg,
                        symbol=sym,
                        ts=btc_ts,
                        reference_price=ref_price,
                        extra_meta={
                            "btc_ts": str(btc_ts),
                            "btc_pump_pct": pump_pct,
                            "btc_vq": vq,
                            "target_candle_close": ref_price,
                        },
                    )
                    if intent_id is None:
                        existed += 1
                        log.info(f"INTENT_EXISTS {sym} ts={btc_ts} ref={ref_price:.6f}")
                    else:
                        created += 1
                        log.warning(f"INTENT_CREATED id={intent_id} {sym} ts={btc_ts} ref={ref_price:.6f}")
                except Exception as e:
                    errors += 1
                    log.error(f"INTENT_ERROR {sym} ts={btc_ts}: {e}")

            log.warning(
                f"TRIGGER_SUMMARY ts={btc_ts} created={created} existed={existed} "
                f"missing_candle={missing_candle} errors={errors}"
            )

            cooldown_left = cfg.cooldown_candles

        except (asyncpg.PostgresError, OSError, ConnectionError) as e:
            log.error(f"LOOP_ERROR: {e} | reconnecting in 3s")
            # try to release lock cleanly
            try:
                if conn and not conn.is_closed():
                    await release_single_instance(conn, cfg)
                    await conn.close()
            except Exception:
                pass
            conn = None
            await asyncio.sleep(3)

        except Exception as e:
            log.exception(f"FATAL_ERROR: {e} | restarting loop in 3s")
            try:
                if conn and not conn.is_closed():
                    await release_single_instance(conn, cfg)
                    await conn.close()
            except Exception:
                pass
            conn = None
            await asyncio.sleep(3)

    # Shutdown
    try:
        if conn and not conn.is_closed():
            await release_single_instance(conn, cfg)
            await conn.close()
    except Exception:
        pass
    log.info("Service stopped.")


def print_config(cfg: Config) -> None:
    safe = {
        "candles_table": cfg.candles_table,
        "intents_table": cfg.intents_table,
        "btc_symbol": cfg.btc_symbol,
        "symbols_count": len(cfg.symbols),
        "source": cfg.source,
        "tf_sec": cfg.tf_sec,
        "poll_sec": cfg.poll_sec,
        "pump_pct_th": cfg.pump_pct_th,
        "min_btc_vq": cfg.min_btc_vq,
        "require_green": cfg.require_green,
        "quote_amount": cfg.quote_amount,
        "intent_status": cfg.intent_status,
        "max_targets_per_trigger": cfg.max_targets_per_trigger,
        "cooldown_candles": cfg.cooldown_candles,
        "use_advisory_lock": cfg.use_advisory_lock,
        "advisory_lock_key": cfg.advisory_lock_key,
        "debug": cfg.log_debug,
    }
    log.info("CONFIG " + json.dumps(safe, ensure_ascii=False))


async def main():
    cfg = load_config()
    print_config(cfg)
    await run(cfg)


if __name__ == "__main__":
    asyncio.run(main())
