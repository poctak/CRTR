# support_bounce_service.py
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
# kolik 5m svíček držet v paměti
LOOKBACK = env_int("SUP_LOOKBACK", 400)

# pivot definice: low[i] je pivot low když je menší než low v okolí
PIVOT_L = env_int("SUP_PIVOT_LEFT", 3)
PIVOT_R = env_int("SUP_PIVOT_RIGHT", 3)

# tolerance zóny (0.002=0.2%, 0.004=0.4%)
TOL_PCT = env_float("SUP_TOL_PCT", 0.003)

# rebound po pivot minimu: alespoň +0.5%
REBOUND_PCT = env_float("SUP_REBOUND_PCT", 0.005)

# kolik svíček po pivotu hledat rebound (např. 12 = 1 hodina na 5m)
REBOUND_LOOKAHEAD = env_int("SUP_REBOUND_LOOKAHEAD", 12)

# minimální separace pivotů (aby se 1 chop nepočítal 3x)
MIN_SEP_BARS = env_int("SUP_MIN_SEP_BARS", 3)

# jak často refreshovat data (sekundy)
POLL_SEC = env_int("SUP_POLL_SEC", 60)

BINANCE_REST = "https://api.binance.com"

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

# per symbol candle buffer
buffers: Dict[str, Deque[Candle]] = {s: deque(maxlen=LOOKBACK) for s in SYMBOLS}

# cooldown, ať to nespamuje pořád dokola
last_signal_ms: Dict[str, int] = {s: 0 for s in SYMBOLS}
SIGNAL_COOLDOWN_MS = env_int("SUP_SIGNAL_COOLDOWN_SEC", 30 * 60) * 1000  # default 30 min

# ==========================================================
# BINANCE REST
# ==========================================================
async def fetch_klines(session: aiohttp.ClientSession, symbol: str, limit: int) -> List[Candle]:
    # GET /api/v3/klines?symbol=BTCUSDT&interval=5m&limit=500
    url = f"{BINANCE_REST}/api/v3/klines"
    params = {"symbol": symbol, "interval": INTERVAL, "limit": str(limit)}
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        resp.raise_for_status()
        arr = await resp.json()

    out: List[Candle] = []
    for k in arr:
        # kline format:
        # [0 open time, 1 open, 2 high, 3 low, 4 close, 5 volume, ...]
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
# SUPPORT LOGIC
# ==========================================================
def is_pivot_low(candles: List[Candle], i: int, left: int, right: int) -> bool:
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
    pivot_low = candles[pivot_i].l
    end = min(len(candles), pivot_i + 1 + lookahead)
    max_h = max(c.h for c in candles[pivot_i + 1 : end]) if pivot_i + 1 < end else 0.0
    if max_h <= 0:
        return False
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
    Vrátí (support_level, pivot_indices) pokud najde 3 pivot lows v zóně (tolerance) a po každém rebound >= rebound_pct.
    Nejjednodušší: vezme poslední 3 validní pivot lows a zkusí je seskupit.
    """
    n = len(candles)
    if n < (pivot_left + pivot_right + 10):
        return None

    pivots: List[int] = []
    for i in range(pivot_left, n - pivot_right):
        if is_pivot_low(candles, i, pivot_left, pivot_right):
            if rebound_ok(candles, i, rebound_pct, rebound_lookahead):
                pivots.append(i)

    if len(pivots) < 3:
        return None

    # vezmeme “poslední” pivoty a zkusíme najít 3, co spadají do stejné zóny
    # (nejjednodušší: jdeme odzadu a skládáme cluster kolem prvního)
    for start_idx in range(len(pivots) - 1, -1, -1):
        base_i = pivots[start_idx]
        base_low = candles[base_i].l
        lo = base_low * (1.0 - tol_pct)
        hi = base_low * (1.0 + tol_pct)

        picked: List[int] = [base_i]
        last_i = base_i

        # hledejme další 2 pivoty směrem dozadu (starší)
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
async def refresh_symbol(session: aiohttp.ClientSession, symbol: str) -> None:
    # natáhni posledních LOOKBACK svíček a naplň buffer
    kl = await fetch_klines(session, symbol, limit=min(1000, LOOKBACK))
    buf = buffers[symbol]
    buf.clear()
    for c in kl[-LOOKBACK:]:
        buf.append(c)

def now_ms() -> int:
    return int(time.time() * 1000)

async def scan_once(session: aiohttp.ClientSession) -> None:
    # pro jednoduchost refreshneme každé poll celé okno (na midcap 30 symbolů OK)
    tasks = [refresh_symbol(session, s) for s in SYMBOLS]
    await asyncio.gather(*tasks)

    for s in SYMBOLS:
        candles = list(buffers[s])
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
        t_ms = candles[-1].open_time_ms  # čas poslední svíčky
        if now_ms() - last_signal_ms[s] < SIGNAL_COOLDOWN_MS:
            continue

        last_signal_ms[s] = now_ms()
        last_close = candles[-1].c
        logging.warning(
            "✅ SUPPORT_3_BOUNCES %s | support=%.8f tol=%.3f%% rebound=%.2f%% pivots=%s last_close=%.8f last_t=%d",
            s,
            support_level,
            TOL_PCT * 100.0,
            REBOUND_PCT * 100.0,
            pivs,
            last_close,
            t_ms,
        )

async def main() -> None:
    logging.info("Starting support_bounce_service | symbols=%d interval=%s", len(SYMBOLS), INTERVAL)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await scan_once(session)
            except Exception as e:
                logging.exception("scan error: %s", e)
            await asyncio.sleep(POLL_SEC)

if __name__ == "__main__":
    asyncio.run(main())
