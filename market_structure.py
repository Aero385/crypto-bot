"""
Market Structure — определение тренда и ключевых уровней.

Это фундамент для точек входа:
- В тренде вверх → ищем long на откатах
- В тренде вниз → ищем short на отскоках
- В рейндже → ищем отбой от границ

Методы:
1. EMA Cross (20/50/200) — классический тренд-фильтр
2. Higher Highs / Lower Lows — структура рынка
3. Support/Resistance — ближайшие уровни для стопов и целей
"""
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass


@dataclass
class MarketContext:
    """Полное описание рыночного контекста для монеты."""
    trend: str              # "uptrend" | "downtrend" | "ranging"
    trend_strength: float   # 0-1, где 1 = сильный тренд
    bias: str               # "long" | "short" | "neutral"

    ema_20: float
    ema_50: float
    ema_200: float
    price_vs_emas: str      # "above_all" | "below_all" | "mixed"

    nearest_support: float
    nearest_resistance: float
    range_width_pct: float  # ширина рейнджа в %

    swing_high: float       # последний локальный хай
    swing_low: float        # последний локальный лоу

    summary: str            # человекочитаемое описание


def ema(values: List[float], period: int) -> List[float]:
    """Exponential Moving Average."""
    if not values or len(values) < period:
        return []
    k = 2 / (period + 1)
    result = [sum(values[:period]) / period]  # SMA как начальное значение
    for v in values[period:]:
        result.append(v * k + result[-1] * (1 - k))
    return result


def find_swing_points(highs: List[float], lows: List[float],
                      lookback: int = 5) -> Tuple[List[float], List[float]]:
    """
    Найти swing highs и swing lows.
    Swing high = high выше N соседних хаёв с каждой стороны.
    """
    swing_highs = []
    swing_lows = []

    for i in range(lookback, len(highs) - lookback):
        # Swing high: центральная свеча выше всех соседних
        if all(highs[i] >= highs[j]
               for j in range(i - lookback, i + lookback + 1) if j != i):
            swing_highs.append(highs[i])

        # Swing low
        if all(lows[i] <= lows[j]
               for j in range(i - lookback, i + lookback + 1) if j != i):
            swing_lows.append(lows[i])

    return swing_highs, swing_lows


def find_support_resistance(swing_highs: List[float], swing_lows: List[float],
                            current_price: float) -> Tuple[float, float]:
    """Ближайшие уровни поддержки и сопротивления."""
    # Сопротивление = ближайший swing high ВЫШЕ текущей цены
    resistances = sorted([h for h in swing_highs if h > current_price])
    resistance = resistances[0] if resistances else current_price * 1.05

    # Поддержка = ближайший swing low НИЖЕ текущей цены
    supports = sorted([l for l in swing_lows if l < current_price], reverse=True)
    support = supports[0] if supports else current_price * 0.95

    return support, resistance


def analyze_structure(klines: List[Dict]) -> Optional[MarketContext]:
    """
    Полный анализ рыночной структуры.
    klines — часовые свечи, минимум 200 штук.
    """
    if not klines or len(klines) < 200:
        return None

    closes = [k["close"] for k in klines]
    highs = [k["high"] for k in klines]
    lows = [k["low"] for k in klines]
    current = closes[-1]

    # ---- EMA ----
    ema_20_vals = ema(closes, 20)
    ema_50_vals = ema(closes, 50)
    ema_200_vals = ema(closes, 200)

    if not ema_20_vals or not ema_50_vals or not ema_200_vals:
        return None

    e20 = ema_20_vals[-1]
    e50 = ema_50_vals[-1]
    e200 = ema_200_vals[-1]

    # Позиция цены относительно EMA
    above_20 = current > e20
    above_50 = current > e50
    above_200 = current > e200

    if above_20 and above_50 and above_200:
        price_vs_emas = "above_all"
    elif not above_20 and not above_50 and not above_200:
        price_vs_emas = "below_all"
    else:
        price_vs_emas = "mixed"

    # ---- Swing points ----
    swing_highs, swing_lows = find_swing_points(highs, lows, lookback=5)
    sh_recent, sl_recent = find_swing_points(
        highs[-50:], lows[-50:], lookback=3)

    # ---- Support / Resistance ----
    support, resistance = find_support_resistance(
        swing_highs[-10:], swing_lows[-10:], current)

    range_width = ((resistance - support) / current * 100) if current > 0 else 0

    # ---- Тренд через Higher Highs / Lower Lows ----
    # Берём последние 5 swing highs/lows и смотрим направление
    trend = "ranging"
    trend_strength = 0.0

    if len(sh_recent) >= 3 and len(sl_recent) >= 3:
        hh_count = sum(1 for i in range(1, len(sh_recent))
                       if sh_recent[i] > sh_recent[i-1])
        hl_count = sum(1 for i in range(1, len(sl_recent))
                       if sl_recent[i] > sl_recent[i-1])
        ll_count = sum(1 for i in range(1, len(sl_recent))
                       if sl_recent[i] < sl_recent[i-1])
        lh_count = sum(1 for i in range(1, len(sh_recent))
                       if sh_recent[i] < sh_recent[i-1])

        total = max(len(sh_recent) + len(sl_recent) - 2, 1)
        up_score = (hh_count + hl_count) / total
        down_score = (ll_count + lh_count) / total

        if up_score > 0.6:
            trend = "uptrend"
            trend_strength = min(up_score, 1.0)
        elif down_score > 0.6:
            trend = "downtrend"
            trend_strength = min(down_score, 1.0)
        else:
            trend = "ranging"
            trend_strength = 0.3

    # EMA подтверждение тренда
    if trend == "uptrend" and price_vs_emas == "above_all":
        trend_strength = min(trend_strength + 0.2, 1.0)
    elif trend == "downtrend" and price_vs_emas == "below_all":
        trend_strength = min(trend_strength + 0.2, 1.0)
    elif trend == "ranging" and price_vs_emas == "mixed":
        trend_strength = max(trend_strength - 0.1, 0.0)

    # ---- Bias ----
    if trend == "uptrend" and trend_strength >= 0.5:
        bias = "long"
    elif trend == "downtrend" and trend_strength >= 0.5:
        bias = "short"
    else:
        bias = "neutral"

    # ---- Summary ----
    ema_order = "выше EMA20/50/200" if price_vs_emas == "above_all" else \
                "ниже EMA20/50/200" if price_vs_emas == "below_all" else \
                "между EMA"

    trend_label = {"uptrend": "↑ аптренд", "downtrend": "↓ даунтренд",
                   "ranging": "↔ рейндж"}[trend]

    summary = f"{trend_label} ({trend_strength:.0%}), {ema_order}, рейндж {range_width:.1f}%"

    return MarketContext(
        trend=trend, trend_strength=trend_strength, bias=bias,
        ema_20=e20, ema_50=e50, ema_200=e200,
        price_vs_emas=price_vs_emas,
        nearest_support=support, nearest_resistance=resistance,
        range_width_pct=range_width,
        swing_high=sh_recent[-1] if sh_recent else max(highs[-24:]),
        swing_low=sl_recent[-1] if sl_recent else min(lows[-24:]),
        summary=summary,
    )
