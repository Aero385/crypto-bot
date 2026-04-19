"""
Клиент Binance для фьючерсных данных.
Использует только ПУБЛИЧНЫЕ эндпоинты — API-ключ не нужен.
"""
import requests
import logging
import time
import threading
from typing import Optional, List, Dict
from datetime import datetime, timezone

log = logging.getLogger(__name__)

SPOT_URL = "https://api.binance.com"
FUTURES_URL = "https://fapi.binance.com"


class BinanceClient:
    """Работа с Binance Spot + Futures через публичные API."""

    def __init__(self):
        self.session = requests.Session()
        self._spot_symbols: Optional[set] = None
        self._futures_symbols: Optional[set] = None
        self._spot_symbols_ts: float = 0
        self._futures_symbols_ts: float = 0
        # Кэш klines: (symbol, interval, limit) → (timestamp, data)
        self._klines_cache: Dict[tuple, tuple] = {}
        self._klines_cache_ttl: int = 45  # секунд
        self._cache_lock = threading.Lock()
        # Счётчик API-вызовов для мониторинга
        self.api_calls: int = 0
        self.api_errors: int = 0
        self._api_calls_reset: float = time.time()

    def get_api_stats(self) -> Dict:
        """Статистика API-вызовов с момента последнего сброса."""
        elapsed = time.time() - self._api_calls_reset
        rpm = (self.api_calls / elapsed * 60) if elapsed > 0 else 0
        return {"calls": self.api_calls, "errors": self.api_errors,
                "elapsed_s": int(elapsed), "rpm": int(rpm)}

    def reset_api_stats(self):
        self.api_calls = 0
        self.api_errors = 0
        self._api_calls_reset = time.time()

    # -------- Базовые запросы --------
    def _get(self, url: str, params: dict = None, retries: int = 2):
        for attempt in range(retries):
            try:
                self.api_calls += 1
                r = self.session.get(url, params=params, timeout=10)
                if r.status_code == 200:
                    return r.json()
                if r.status_code == 429:
                    log.warning("Binance 429 rate limit, backing off %ds", 2 ** attempt)
                    self.api_errors += 1
                    time.sleep(2 ** attempt)
                    continue
                log.debug("Binance %s → %s", url, r.status_code)
                self.api_errors += 1
                return None
            except Exception as e:
                log.debug("Binance request error: %s", e)
                self.api_errors += 1
                time.sleep(0.5)
        return None

    # -------- Список торгуемых пар (обновляется раз в 30 мин) --------
    def spot_symbols(self) -> set:
        if self._spot_symbols is None or (time.time() - self._spot_symbols_ts > 1800):
            data = self._get(f"{SPOT_URL}/api/v3/exchangeInfo")
            if data:
                self._spot_symbols = {
                    s["symbol"] for s in data["symbols"]
                    if s["status"] == "TRADING" and s["quoteAsset"] == "USDT"
                }
                self._spot_symbols_ts = time.time()
            elif self._spot_symbols is None:
                self._spot_symbols = set()
        return self._spot_symbols

    def futures_symbols(self) -> set:
        if self._futures_symbols is None or (time.time() - self._futures_symbols_ts > 1800):
            data = self._get(f"{FUTURES_URL}/fapi/v1/exchangeInfo")
            if data:
                self._futures_symbols = {
                    s["symbol"] for s in data["symbols"]
                    if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT"
                    and s.get("contractType") == "PERPETUAL"
                }
                self._futures_symbols_ts = time.time()
            elif self._futures_symbols is None:
                self._futures_symbols = set()
        return self._futures_symbols

    def make_pair(self, coin: str) -> str:
        return f"{coin.upper()}USDT"

    # -------- Свечи (klines) с кэшем --------
    def klines(self, symbol: str, interval: str = "1m",
               limit: int = 100, futures: bool = False) -> Optional[List[Dict]]:
        """
        Свечи. interval: 1m, 5m, 15m, 1h, 4h, 1d.
        Кэш на 45с — избегаем дублирующих запросов в одном цикле.
        """
        cache_key = (symbol, interval, limit, futures)
        now = time.time()

        with self._cache_lock:
            cached = self._klines_cache.get(cache_key)
            if cached and (now - cached[0]) < self._klines_cache_ttl:
                return cached[1]

        url = f"{FUTURES_URL}/fapi/v1/klines" if futures else f"{SPOT_URL}/api/v3/klines"
        data = self._get(url, {"symbol": symbol, "interval": interval, "limit": limit})
        if not data:
            return None
        result = [
            {
                "open_time": k[0],
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "close_time": k[6],
                "quote_volume": float(k[7]),   # объём в USDT
                "trades": int(k[8]),
            }
            for k in data
        ]

        with self._cache_lock:
            self._klines_cache[cache_key] = (now, result)
            # Чистим устаревшие записи (раз в ~100 вызовов чтобы не копилось)
            if len(self._klines_cache) > 500:
                expired = [k for k, v in self._klines_cache.items()
                           if now - v[0] > self._klines_cache_ttl]
                for k in expired:
                    del self._klines_cache[k]

        return result

    # -------- Стакан --------
    def order_book(self, symbol: str, limit: int = 100) -> Optional[Dict]:
        return self._get(f"{SPOT_URL}/api/v3/depth",
                        {"symbol": symbol, "limit": limit})

    # -------- Open Interest --------
    def open_interest(self, symbol: str) -> Optional[float]:
        """Текущий OI в контрактах (для USDT-перпетуалов это сумма монет)."""
        data = self._get(f"{FUTURES_URL}/fapi/v1/openInterest", {"symbol": symbol})
        if not data:
            return None
        return float(data.get("openInterest", 0))

    def open_interest_history(self, symbol: str, period: str = "5m",
                               limit: int = 30) -> Optional[List[Dict]]:
        """
        История OI. period: 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d.
        Возвращает: [{timestamp, open_interest, open_interest_value_usd}, ...]
        """
        data = self._get(f"{FUTURES_URL}/futures/data/openInterestHist", {
            "symbol": symbol, "period": period, "limit": limit,
        })
        if not data:
            return None
        return [
            {
                "timestamp": int(d["timestamp"]),
                "open_interest": float(d["sumOpenInterest"]),
                "open_interest_value_usd": float(d["sumOpenInterestValue"]),
            }
            for d in data
        ]

    # -------- Funding Rate --------
    def funding_rate(self, symbol: str) -> Optional[float]:
        """Текущий funding rate (обычно обновляется каждые 8 часов)."""
        data = self._get(f"{FUTURES_URL}/fapi/v1/premiumIndex", {"symbol": symbol})
        if not data:
            return None
        return float(data.get("lastFundingRate", 0)) * 100  # в процентах

    def all_funding_rates(self) -> Optional[Dict[str, float]]:
        """Funding для всех пар одним запросом — эффективнее."""
        data = self._get(f"{FUTURES_URL}/fapi/v1/premiumIndex")
        if not data:
            return None
        return {
            d["symbol"]: float(d.get("lastFundingRate", 0)) * 100
            for d in data if d.get("symbol", "").endswith("USDT")
        }

    # -------- Ликвидации --------
    # Публичного REST endpoint для ликвидаций больше нет (Binance удалил в 2024).
    # Нужен WebSocket на !forceOrder@arr. Для простоты используем другой источник —
    # Coinglass не требует ключа для базовых данных.
    def recent_liquidations(self, symbol: str = None) -> Optional[List[Dict]]:
        """
        Заглушка. Ликвидации собираются через WebSocket-слушатель (см. liquidations.py).
        В этом методе возвращаем None — реальные данные в отдельном модуле.
        """
        return None

    # -------- 24h статистика --------
    def ticker_24h(self, symbol: str, futures: bool = False) -> Optional[Dict]:
        url = (f"{FUTURES_URL}/fapi/v1/ticker/24hr" if futures
               else f"{SPOT_URL}/api/v3/ticker/24hr")
        return self._get(url, {"symbol": symbol})
