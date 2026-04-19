"""
Мат. утилиты: ATR, baseline по часам недели, rolling-буферы.
"""
from collections import defaultdict, deque
from typing import List, Dict, Optional
from datetime import datetime, timezone
import statistics


def calculate_atr(klines: List[Dict], period: int = 14) -> Optional[float]:
    """
    Average True Range — мера волатильности.
    ATR показывает, на сколько в среднем монета двигается за свечу.

    True Range = max из:
      - high - low
      - |high - prev_close|
      - |low - prev_close|
    """
    if not klines or len(klines) < period + 1:
        return None

    trs = []
    for i in range(1, len(klines)):
        high = klines[i]["high"]
        low = klines[i]["low"]
        prev_close = klines[i - 1]["close"]

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        trs.append(tr)

    if len(trs) < period:
        return None

    # Простое среднее последних N TR
    return statistics.mean(trs[-period:])


def price_move_in_atr(current_price: float, prev_price: float,
                      atr: float) -> float:
    """На сколько ATR двинулась цена. Отрицательное = движение вниз."""
    if atr <= 0:
        return 0
    return (current_price - prev_price) / atr


class HourOfWeekBaseline:
    """
    Baseline объёма по часам недели.
    Ключ — (weekday, hour). Например (0, 14) = понедельник 14:00 UTC.
    Для каждого ключа храним список объёмов за последние N дней.

    Зачем: у крипты есть циркадные ритмы — ночью Азия, днём Европа+США.
    Сравнение текущего объёма с "таким же временем в прошлые недели" даёт
    намного более чистый сигнал чем просто среднее за 24ч.
    """

    def __init__(self, lookback_days: int = 14):
        # (weekday, hour) → deque[volume]
        self._buckets: Dict = defaultdict(lambda: deque(maxlen=lookback_days))

    @staticmethod
    def _key(ts: datetime) -> tuple:
        return (ts.weekday(), ts.hour)

    def add(self, ts: datetime, volume: float) -> None:
        self._buckets[self._key(ts)].append(volume)

    def baseline_for(self, ts: datetime) -> Optional[float]:
        """Медианный объём для этого часа недели."""
        bucket = self._buckets.get(self._key(ts))
        if not bucket or len(bucket) < 3:
            return None
        return statistics.median(bucket)

    def multiplier(self, ts: datetime, current_volume: float) -> Optional[float]:
        """Во сколько раз текущий объём выше baseline."""
        base = self.baseline_for(ts)
        if not base or base <= 0:
            return None
        return current_volume / base


class RollingBuffer:
    """
    Буфер для хранения временных рядов с автоматическим обрезанием по времени.
    Использует deque для O(1) append и эффективного popleft при прунинге.
    """

    def __init__(self, max_age_seconds: int):
        self.max_age = max_age_seconds
        self._items: deque = deque()  # [(timestamp, value), ...]

    def add(self, value, timestamp: float = None) -> None:
        import time
        ts = timestamp if timestamp is not None else time.time()
        self._items.append((ts, value))
        self._prune()

    def _prune(self) -> None:
        import time
        cutoff = time.time() - self.max_age
        # deque popleft — O(1) вместо O(n) list comprehension
        while self._items and self._items[0][0] < cutoff:
            self._items.popleft()

    def values(self) -> list:
        self._prune()
        return [v for _, v in self._items]

    def items(self) -> list:
        self._prune()
        return list(self._items)

    def __len__(self) -> int:
        self._prune()
        return len(self._items)

    def sum(self) -> float:
        return sum(self.values())


def percent_change(old: float, new: float) -> float:
    """% изменения. Если old==0 — возвращает 0."""
    if old == 0:
        return 0
    return ((new - old) / old) * 100
