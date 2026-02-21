# support_bounce_service.py
#
# Simple support detection on 5m candles:
# - compute ONLY from the last SUP_WINDOW_MIN minutes (default 60min => 12 candles)
# - pivot lows are counted ONLY if the pivot candle is RED (close < open)
# - find 3 pivot lows inside a tolerance zone (SUP_TOL_PCT)
# - after each pivot low, price must rebound at least SUP_REBOUND_PCT within SUP_REBOUND_LOOKAHEAD candles
# - no DB, state only in memory
#
# ENV (recommended):
#   SYMBOLS=ARBUSDT,OPUSDT,...
#   LOG_LEVEL=INFO
#   SUP_POLL_SEC=60
#   SUP_LOOKBACK=400
#   SUP_WINDOW_MIN=60
#   SUP_PIVOT_LEFT=2
#   SUP_PIVOT_RIGHT=2
#   SUP_TOL_PCT=0.003
#   SUP_REBOUND_PCT=0.005
#   SUP_REBOUND_LOOKAHEAD=3
#   SUP_MIN_SEP_BARS=2
#   SUP_SIGNAL_COOLDOWN_SEC=1800

import os
import time
import asyncio
import logging
from dataclasses import dataclass
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple

import aiohttp


# ==========================================================
# LOGGING
# ==========================================================
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ==========================================================
# ENV HELPERS
# ==========================================================
def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v and v.strip() else default


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v and v.strip() else default


def parse_symbols() -> List[str]:
    raw = os.getenv("SYMBOLS", "").strip()
    if not raw:
        raise RuntimeError("SYMBOLS is empty")
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


# ==========================================================
# CONFIG
# ==========================================================
SYMBOLS = parse_symbols()
INTERVAL = "5m"

BINANCE_REST = "https://api.binance.com"

LOOKBACK = env_int("SUP_LOOKBACK", 400)

SUP_WINDOW_MIN = env_int("SUP_WINDOW_MIN", 60)
WINDOW_BARS = max(12, SUP_WINDOW_MIN // 5)  # 60min => 12 bars

PIVOT_L = env_int("SUP_PIVOT_LEFT", 2)
PIVOT_R = env_int("SUP_PIVOT_RIGHT", 2)

TOL_PCT = env_float("SUP_TOL_PCT", 0.003)          # 0.3%
REBOUND_PCT = env_float("SUP_REBOUND_PCT", 0.005)  # 0.5%
REBOUND_LOOKAHEAD = env_int("SUP_REBOUND_LOOKAHEAD", 3)  # 15 min

MIN_SEP_BARS = env_int("SUP_MIN_SEP_BARS", 2)
POLL_SEC = env_int("SUP_POLL_SEC", 60)

SIGNAL_COOLDOWN_MS = env_int("SUP_SIGNAL_COOLDOWN_SEC", 30 * 60) * 1000  # 30 min


# ==========================================================
# DATA
# ==========================================================
@dataclass
class Candle:
    open_time_ms: int
    o: float
    h: float
    l: float
    c: float
    v: float


buffers: Dict[str, Deque[Candle]] = {s: deque(maxlen=LOOKBACK) for s in SYMBOLS}
last_signal_ms: Dict[str, int] = {s: 0 for s in SYMBOLS}


# ==========================================================
# BINANCE REST
# ==========================================================
async def fetch_klines(session: aiohttp.ClientSession, symbol: str, limit: int) -> List[Candle]:
    url = f"{BINANCE_REST}/api/v3/klines"
    params = {"symbol": symbol, "interval": INTERVAL, "limit": str(limit)}

    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        resp.raise_for_status()
        arr = await resp.json()

    out: List[Candle] = []
    for k in arr:
        out.append(
            Candle(
                open_time_ms=int(k[0]),
                o=float(k[1]),
                h=float(k[2]),
                l=float(k[3]),
                c=float(k[4]),
                v=float(k[5]),
            )
        )
    return out


# ==========================================================
# SUPPORT LOGIC (ONLY on last WINDOW_BARS)
# ==========================================================
def is_red(c: Candle) -> bool:
    return c.c < c.o


def is_pivot_low(candles: List[Candle], i: int, left: int, right: int) -> bool:
    """
    Pivot low: candles[i].l is strictly lower than lows in [i-left, i+right] excluding i.
    """
    if i - left < 0 or i + right >= len(candles):
        return False
    low_i = candles[i].l
    for j in range(i - left, i + right + 1):
        if j == i:
            continue
        if candles[j].l <= low_i:
            return False
    return True


def rebound_ok(candles: List[Candle], pivot_i: int, rebound_pct: float, lookahead: int) -> bool:
    """
    After pivot low at pivot_i, require max(high) in next 'lookahead' candles to be >= pivot_low*(1+rebound_pct).
    """
    pivot_low = candles[pivot_i].l
    end = min(len(candles), pivot_i + 1 + lookahead)
    if pivot_i + 1 >= end:
        return False

    max_h = max(c.h for c in candles[pivot_i + 1 : end])
    return (max_h / pivot_low - 1.0) >= rebound_pct


def detect_3_bounce_support(
    candles: List[Candle],
    tol_pct: float,
    rebound_pct: float,
    rebound_lookahead: int,
    pivot_left: int,
    pivot_right: int,
    min_sep_bars: int,
) -> Optional[Tuple[float, List[int]]]:
    """
    Find 3 pivot lows within the same support zone (tolerance), each followed by rebound >= rebound_pct.
    IMPORTANT: pivot low candle must be RED (close < open).
    candles should already be sliced to last WINDOW_BARS.
    """
    n = len(candles)
    if n < (pivot_left + pivot_right + 3):
        return None

    pivots: List[int] = []
    for i in range(pivot_left, n - pivot_right):
        if not is_red(candles[i]):
            continue
        if is_pivot_low(candles, i, pivot_left, pivot_right) and rebound_ok(
            candles, i, rebound_pct, rebound_lookahead
        ):
            pivots.append(i)

    if len(pivots) < 3:
        return None

    for start_idx in range(len(pivots) - 1, -1, -1):
        base_i = pivots[start_idx]
        base_low = candles[base_i].l
        lo = base_low * (1.0 - tol_pct)
        hi = base_low * (1.0 + tol_pct)

        picked: List[int] = [base_i]
        last_i = base_i

        for k in range(start_idx - 1, -1, -1):
            pi = pivots[k]
            if last_i - pi < min_sep_bars:
                continue
            pl = candles[pi].l
            if lo <= pl <= hi:
                picked.append(pi)
                last_i = pi
                if len(picked) == 3:
                    picked_sorted = sorted(picked)
                    support_level = sum(candles[x].l for x in picked_sorted) / 3.0
                    return support_level, picked_sorted

    return None


# ==========================================================
# MAIN LOOP
# ==========================================================
def now_ms() -> int:
    return int(time.time() * 1000)


async def refresh_symbol(session: aiohttp.ClientSession, symbol: str) -> None:
    kl = await fetch_klines(session, symbol, limit=min(1000, LOOKBACK))
    buf = buffers[symbol]
    buf.clear()
    for c in kl[-LOOKBACK:]:
        buf.append(c)


async def scan_once(session: aiohttp.ClientSession) -> None:
    await asyncio.gather(*(refresh_symbol(session, s) for s in SYMBOLS))

    for s in SYMBOLS:
        all_candles = list(buffers[s])
        if len(all_candles) < WINDOW_BARS:
            continue

        candles = all_candles[-WINDOW_BARS:]  # ONLY last 60 min window

        hit = detect_3_bounce_support(
            candles=candles,
            tol_pct=TOL_PCT,
            rebound_pct=REBOUND_PCT,
            rebound_lookahead=REBOUND_LOOKAHEAD,
            pivot_left=PIVOT_L,
            pivot_right=PIVOT_R,
            min_sep_bars=MIN_SEP_BARS,
        )
        if not hit:
            continue

        support_level, pivs = hit
        last_close = candles[-1].c
        last_t = candles[-1].open_time_ms

        if now_ms() - last_signal_ms[s] < SIGNAL_COOLDOWN_MS:
            continue

        last_signal_ms[s] = now_ms()
        logging.warning(
            "✅ SUPPORT_3_BOUNCES %s | window=%dbars(=%dmin) support=%.8f tol=%.3f%% rebound=%.2f%% "
            "pivots=%s (red-only) last_close=%.8f last_t=%d",
            s,
            WINDOW_BARS,
            WINDOW_BARS * 5,
            support_level,
            TOL_PCT * 100.0,
            REBOUND_PCT * 100.0,
            pivs,
            last_close,
            last_t,
        )


async def main() -> None:
    logging.info(
        "Starting support_bounce_service | symbols=%d interval=%s window=%dbars(=%dmin) pivot=%d/%d tol=%.3f%% rebound=%.2f%% red-only",
        len(SYMBOLS),
        INTERVAL,
        WINDOW_BARS,
        WINDOW_BARS * 5,
        PIVOT_L,
        PIVOT_R,
        TOL_PCT * 100.0,
        REBOUND_PCT * 100.0,
    )

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await scan_once(session)
            except Exception as e:
                logging.exception("scan error: %s", e)
            await asyncio.sleep(POLL_SEC)


if __name__ == "__main__":
    asyncio.run(main())
