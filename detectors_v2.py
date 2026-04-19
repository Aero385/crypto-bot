"""
Детекторы v2. Каждый детектор:
  - Получает текущее состояние рынка (цена, объём, OI и т.д.)
  - Если находит аномалию — создаёт Signal с весом и направлением
  - Отдаёт сигнал в ConfluenceEngine

Направление (direction) помогает потом сформировать общий вывод:
  bullish = ожидаемо движение вверх, bearish = вниз, None = нейтрально
"""
import logging
from typing import Optional, List, Dict
from datetime import datetime, timezone

from confluence import Signal
from indicators import (
    calculate_atr, HourOfWeekBaseline, RollingBuffer, percent_change
)

log = logging.getLogger(__name__)


def _fmt_usd(value: float) -> str:
    """Адаптивный формат USD: $1.2B / $15.3M / $340K / $500."""
    if value >= 1e9:
        return f"${value/1e9:.1f}B"
    if value >= 1e6:
        return f"${value/1e6:.1f}M"
    if value >= 1e3:
        return f"${value/1e3:.0f}K"
    return f"${value:.0f}"


# ============ 1. VOLUME SPIKE ============
class VolumeSpikeDetector:
    """Всплеск объёма относительно baseline по часам недели."""

    def __init__(self, config: dict):
        self.cfg = config["detectors"]["volume_spike"]
        self.baselines: Dict[str, HourOfWeekBaseline] = {}

    def update(self, coin: str, klines: list) -> Optional[Signal]:
        if not self.cfg["enabled"] or not klines or len(klines) < 2:
            return None

        baseline = self.baselines.setdefault(
            coin, HourOfWeekBaseline(self.cfg["baseline_days"])
        )

        # Добавляем часовые агрегаты в baseline
        # Берём закрытые часовые свечи (последняя может быть не закрыта)
        for k in klines[:-1]:
            ts = datetime.fromtimestamp(k["close_time"] / 1000, tz=timezone.utc)
            baseline.add(ts, k["quote_volume"])

        # Текущая свеча
        current = klines[-1]
        now = datetime.fromtimestamp(current["close_time"] / 1000, tz=timezone.utc)
        mult = baseline.multiplier(now, current["quote_volume"])

        if not mult:
            return None

        tiers = self.cfg["tiers"]
        weight = 0
        tier_name = None
        if mult >= 10:
            weight, tier_name = tiers["x10"], "x10"
        elif mult >= 7:
            weight, tier_name = tiers["x7"], "x7"
        elif mult >= 5:
            weight, tier_name = tiers["x5"], "x5"

        if weight == 0:
            return None

        cur_vol = current['quote_volume']
        base_vol = cur_vol / mult

        return Signal(
            coin=coin, detector="volume_spike", weight=weight,
            direction=None,  # объём сам по себе не даёт направления
            label=f"Объём {tier_name} ({mult:.1f}× от baseline)",
            details=f"{_fmt_usd(cur_vol)} vs ~{_fmt_usd(base_vol)} обычно",
        )


# ============ 2. PRICE MOVE В ATR ============
class PriceMoveATRDetector:
    """Движение цены в единицах ATR — автоматически адаптируется под волатильность."""

    def __init__(self, config: dict):
        self.cfg = config["detectors"]["price_move_atr"]

    def update(self, coin: str, klines_1h: list) -> Optional[Signal]:
        if not self.cfg["enabled"] or not klines_1h or len(klines_1h) < 20:
            return None

        atr = calculate_atr(klines_1h[:-1], period=14)
        if not atr or atr <= 0:
            return None

        # Текущее движение относительно цены час назад
        prev_close = klines_1h[-2]["close"]
        current = klines_1h[-1]["close"]
        move = current - prev_close
        atr_move = abs(move) / atr

        tiers = self.cfg["tiers"]
        weight = 0
        tier_name = None
        if atr_move >= 3.0:
            weight, tier_name = tiers["atr_3_0"], "3.0"
        elif atr_move >= 2.0:
            weight, tier_name = tiers["atr_2_0"], "2.0"
        elif atr_move >= 1.5:
            weight, tier_name = tiers["atr_1_5"], "1.5"

        if weight == 0:
            return None

        direction = "bullish" if move > 0 else "bearish"
        pct = (move / prev_close) * 100

        return Signal(
            coin=coin, detector="price_move_atr", weight=weight,
            direction=direction,
            label=f"Движение {atr_move:.1f} ATR ({pct:+.2f}%)",
            details=f"Аномально {'сильный рост' if move > 0 else 'сильное падение'} vs обычной волатильности",
        )


# ============ 3. BREAKOUT ============
class BreakoutDetector:
    """Пробой локального high/low за N часов."""

    def __init__(self, config: dict):
        self.cfg = config["detectors"]["breakout"]

    def update(self, coin: str, klines_1h: list) -> Optional[Signal]:
        if not self.cfg["enabled"] or not klines_1h:
            return None

        lookbacks = self.cfg["lookback_hours"]
        weights = self.cfg["weights"]
        current = klines_1h[-1]["close"]

        best_signal = None
        best_weight = 0

        for hours in lookbacks:
            if len(klines_1h) < hours + 1:
                continue

            window = klines_1h[-(hours + 1):-1]  # предыдущие N часов
            high = max(k["high"] for k in window)
            low = min(k["low"] for k in window)

            weight = weights.get(str(hours), 0)
            if weight <= best_weight:
                continue

            # Подтверждение объёмом (если включено)
            if self.cfg["require_volume_confirmation"]:
                avg_vol = sum(k["quote_volume"] for k in window) / len(window)
                if klines_1h[-1]["quote_volume"] < avg_vol * 1.3:
                    continue

            if current > high:
                best_signal = Signal(
                    coin=coin, detector="breakout", weight=weight,
                    direction="bullish",
                    label=f"Пробой {hours}ч high",
                    details=f"Цена ${current:.4f} > прежнего max ${high:.4f}",
                )
                best_weight = weight
            elif current < low:
                best_signal = Signal(
                    coin=coin, detector="breakout", weight=weight,
                    direction="bearish",
                    label=f"Пробой {hours}ч low",
                    details=f"Цена ${current:.4f} < прежнего min ${low:.4f}",
                )
                best_weight = weight

        return best_signal


# ============ 4. OPEN INTEREST ============
class OpenInterestDetector:
    """
    Резкое изменение OI.
    Рост OI + рост цены = новые деньги в лонги (сильный bullish)
    Рост OI + падение цены = новые шорты (bearish)
    """

    def __init__(self, config: dict):
        self.cfg = config["detectors"]["open_interest"]
        # coin → RollingBuffer[(timestamp, oi_usd)]
        self._history: Dict[str, RollingBuffer] = {}

    def update(self, coin: str, current_oi_usd: float,
               price_change_pct: float) -> Optional[Signal]:
        if not self.cfg["enabled"] or current_oi_usd <= 0:
            return None

        history = self._history.setdefault(
            coin, RollingBuffer(self.cfg["change_window_minutes"] * 60 * 2)
        )
        history.add(current_oi_usd)

        values = history.values()
        if len(values) < 3:
            return None

        old_oi = values[0]
        oi_change = percent_change(old_oi, current_oi_usd)

        tiers = self.cfg["tiers"]
        abs_change = abs(oi_change)

        weight = 0
        tier_name = None
        if abs_change >= 10:
            weight, tier_name = tiers["pct_10"], "10%"
        elif abs_change >= 5:
            weight, tier_name = tiers["pct_5"], "5%"
        elif abs_change >= 3:
            weight, tier_name = tiers["pct_3"], "3%"

        if weight == 0:
            return None

        # Определяем значение сигнала по комбинации OI + цены
        if oi_change > 0 and price_change_pct > 0:
            direction = "bullish"
            interp = "приток в лонги"
        elif oi_change > 0 and price_change_pct < 0:
            direction = "bearish"
            interp = "открытие шортов"
        elif oi_change < 0 and price_change_pct > 0:
            direction = "bullish"
            interp = "закрытие шортов (слабее)"
            weight *= 0.6
        else:  # OI ↓, price ↓
            direction = "bearish"
            interp = "закрытие лонгов"
            weight *= 0.8

        return Signal(
            coin=coin, detector="open_interest", weight=weight,
            direction=direction,
            label=f"OI {oi_change:+.1f}% ({tier_name})",
            details=interp,
        )


# ============ 5. FUNDING RATE ============
class FundingRateDetector:
    """Экстремальные значения funding rate = перекос позиций."""

    def __init__(self, config: dict):
        self.cfg = config["detectors"]["funding"]

    def update(self, coin: str, funding_pct: float) -> Optional[Signal]:
        if not self.cfg["enabled"] or funding_pct is None:
            return None

        thresholds = self.cfg["thresholds"]
        weights = self.cfg["weights"]
        abs_f = abs(funding_pct)

        if abs_f >= thresholds["extreme"]:
            weight = weights["extreme"]
            level = "экстремальный"
        elif abs_f >= thresholds["high"]:
            weight = weights["high"]
            level = "высокий"
        else:
            return None

        # Высокий позитивный funding = лонгисты платят → рынок перегружен лонгами
        # → вероятен каскад вниз (bearish в среднесроке)
        if funding_pct > 0:
            direction = "bearish"
            interp = "перекос в лонги → риск сквиза шортов"
            # Но: если рынок идёт вверх, это контр-тренд сигнал, так что снижаем вес
        else:
            direction = "bullish"
            interp = "перекос в шорты → риск шорт-сквиза"

        return Signal(
            coin=coin, detector="funding", weight=weight,
            direction=direction,
            label=f"Funding {funding_pct:+.3f}% ({level})",
            details=interp,
        )


# ============ 6. LIQUIDATIONS ============
class LiquidationsDetector:
    """Крупные ликвидации = импульсы в противоположную сторону."""

    def __init__(self, config: dict):
        self.cfg = config["detectors"]["liquidations"]

    def update(self, coin: str, liq_stats: dict) -> Optional[Signal]:
        if not self.cfg["enabled"] or not liq_stats:
            return None

        total = liq_stats.get("total_usd", 0)
        tiers = self.cfg["tiers"]

        weight = 0
        tier_name = None
        if total >= 10_000_000:
            weight, tier_name = tiers["usd_10m"], "$10M+"
        elif total >= 5_000_000:
            weight, tier_name = tiers["usd_5m"], "$5M+"
        elif total >= 1_000_000:
            weight, tier_name = tiers["usd_1m"], "$1M+"

        if weight == 0:
            return None

        # Ликвидации лонгов = массовая продажа = bearish (но короткий каскад,
        # после него часто отскок — тут зависит от стратегии)
        long_usd = liq_stats.get("long_usd", 0)
        short_usd = liq_stats.get("short_usd", 0)
        imbalance = liq_stats.get("imbalance", 0)

        if imbalance < -0.6:
            direction = "bearish"  # преобладали лонг-ликвидации
            interp = f"лонги ликвидируются (${long_usd/1e6:.1f}M)"
        elif imbalance > 0.6:
            direction = "bullish"  # шорт-сквиз
            interp = f"шорты ликвидируются (${short_usd/1e6:.1f}M)"
        else:
            direction = None
            interp = f"обе стороны"

        return Signal(
            coin=coin, detector="liquidations", weight=weight,
            direction=direction,
            label=f"Ликвидации {tier_name}",
            details=interp,
        )


# ============ 7. NETFLOW (on-chain) ============
class NetflowDetector:
    """Нетто-потоки на биржу / с биржи — сигнал аккумуляции или распределения."""

    def __init__(self, config: dict):
        self.cfg = config["detectors"]["netflow"]

    def update(self, coin: str, netflow_stats: dict) -> Optional[Signal]:
        if not self.cfg["enabled"] or not netflow_stats:
            return None

        direction_label = netflow_stats.get("direction")
        if direction_label == "neutral":
            return None

        net_usd = netflow_stats.get("net_usd", 0)
        weights = self.cfg["weights"]

        if direction_label == "bullish":
            weight = weights["outflow_bullish"]
            direction = "bullish"
            label = f"Отток с бирж ${abs(net_usd)/1e6:.1f}M"
            details = "крупные кошельки забирают холды"
        else:  # bearish
            weight = weights["inflow_bearish"]
            direction = "bearish"
            label = f"Приток на биржи ${abs(net_usd)/1e6:.1f}M"
            details = "возможно готовятся к продаже"

        return Signal(
            coin=coin, detector="netflow", weight=weight,
            direction=direction, label=label, details=details,
        )


# ============ 8. IMPULSE (короткие импульсы 5-15 мин) ============
class ImpulseDetector:
    """
    Детектор импульсов — резкие движения за 15-30 мин на 5-мин свечах.
    Ловит PUMP/DUMP которые часовые свечи пропускают.
    """

    def __init__(self, config: dict):
        self.cfg = config["detectors"].get("impulse", {
            "enabled": True,
            "tiers": {"pct_5": 2.0, "pct_8": 3.0, "pct_15": 4.0},
        })

    def update(self, coin: str, klines_5m: list) -> Optional[Signal]:
        if not self.cfg.get("enabled", True) or not klines_5m or len(klines_5m) < 6:
            return None

        current_close = klines_5m[-1]["close"]
        tiers = self.cfg.get("tiers", {"pct_5": 2.0, "pct_8": 3.0, "pct_15": 4.0})

        best_signal = None
        best_weight = 0

        # Проверяем окна: 15 мин (3 свечи) и 30 мин (6 свечей)
        for window_bars, window_label in [(3, "15мин"), (6, "30мин")]:
            if len(klines_5m) < window_bars + 1:
                continue

            prev_close = klines_5m[-(window_bars + 1)]["close"]
            if prev_close <= 0:
                continue

            pct = ((current_close - prev_close) / prev_close) * 100
            abs_pct = abs(pct)

            weight = 0
            if abs_pct >= 15:
                weight = tiers.get("pct_15", 4.0)
            elif abs_pct >= 8:
                weight = tiers.get("pct_8", 3.0)
            elif abs_pct >= 5:
                weight = tiers.get("pct_5", 2.0)

            if weight > best_weight:
                direction = "bullish" if pct > 0 else "bearish"
                action = "PUMP" if pct > 0 else "DUMP"
                best_signal = Signal(
                    coin=coin, detector="impulse", weight=weight,
                    direction=direction,
                    label=f"Импульс {action} {abs_pct:.1f}% ({window_label})",
                    details=f"${prev_close:.4f} → ${current_close:.4f} за {window_label}",
                )
                best_weight = weight

        return best_signal


# ============ 9. ORDERBOOK (унаследован, адаптирован) ============
class OrderbookDetector:
    """Крупные стены и дисбаланс в стакане."""

    def __init__(self, config: dict):
        self.cfg = config["detectors"]["orderbook"]

    def update(self, coin: str, orderbook: dict) -> Optional[Signal]:
        if not self.cfg["enabled"] or not orderbook:
            return None

        # Сумма объёмов
        bid_usd = sum(float(p) * float(q) for p, q in orderbook.get("bids", []))
        ask_usd = sum(float(p) * float(q) for p, q in orderbook.get("asks", []))

        signals_parts = []
        weight = 0
        direction = None

        # Ищем самую крупную стену
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])
        if bids:
            biggest_bid = max(bids, key=lambda x: float(x[0]) * float(x[1]))
            bw_usd = float(biggest_bid[0]) * float(biggest_bid[1])
            if bw_usd >= 3_000_000:
                weight += self.cfg["tiers"]["wall_3m"]; direction = "bullish"
                signals_parts.append(f"стена покупки ${bw_usd/1e6:.1f}M")
            elif bw_usd >= 1_000_000:
                weight += self.cfg["tiers"]["wall_1m"]; direction = "bullish"
                signals_parts.append(f"стена покупки ${bw_usd/1e6:.1f}M")
            elif bw_usd >= 500_000:
                weight += self.cfg["tiers"]["wall_500k"]; direction = "bullish"
                signals_parts.append(f"стена покупки ${bw_usd/1e3:.0f}K")

        if asks:
            biggest_ask = max(asks, key=lambda x: float(x[0]) * float(x[1]))
            aw_usd = float(biggest_ask[0]) * float(biggest_ask[1])
            if aw_usd >= 3_000_000:
                weight += self.cfg["tiers"]["wall_3m"]
                direction = "bearish" if direction != "bullish" else None
                signals_parts.append(f"стена продажи ${aw_usd/1e6:.1f}M")
            elif aw_usd >= 1_000_000:
                weight += self.cfg["tiers"]["wall_1m"]
                direction = "bearish" if direction != "bullish" else None
                signals_parts.append(f"стена продажи ${aw_usd/1e6:.1f}M")
            elif aw_usd >= 500_000:
                weight += self.cfg["tiers"]["wall_500k"]
                direction = "bearish" if direction != "bullish" else None
                signals_parts.append(f"стена продажи ${aw_usd/1e3:.0f}K")

        # Дисбаланс
        imbalance = self.cfg["imbalance"]
        if bid_usd > 0 and ask_usd > 0:
            ratio = max(bid_usd, ask_usd) / min(bid_usd, ask_usd)
            if ratio >= imbalance["threshold_ratio"]:
                weight += imbalance["weight"]
                if bid_usd > ask_usd:
                    if direction is None:
                        direction = "bullish"
                    signals_parts.append(f"давление покупки {ratio:.1f}×")
                else:
                    if direction is None:
                        direction = "bearish"
                    signals_parts.append(f"давление продажи {ratio:.1f}×")

        if weight == 0:
            return None

        return Signal(
            coin=coin, detector="orderbook", weight=weight,
            direction=direction,
            label="Стакан: " + ", ".join(signals_parts[:2]),
            details="; ".join(signals_parts),
        )
