"""
Entry Signals — конкретные торговые сетапы.

Каждый сетап возвращает EntrySignal с:
  - direction: long / short
  - entry: точка входа
  - stop_loss: где ставить стоп
  - targets: [цель 1, цель 2]
  - risk_reward: соотношение risk:reward
  - size_pct: рекомендуемый % от депозита
  - context: рыночная структура

Формула размера позиции:
  risk_amount = deposit × risk_per_trade_pct (обычно 1-2%)
  distance_to_stop = |entry - stop_loss|
  position_size = risk_amount / distance_to_stop

Это гарантирует что при стопе теряешь ровно risk_per_trade_pct от депозита.
"""
import logging
from html import escape as html_escape
from typing import Optional, List, Dict
from dataclasses import dataclass, field
from market_structure import MarketContext, analyze_structure
from indicators import calculate_atr

log = logging.getLogger(__name__)


@dataclass
class EntrySignal:
    """Полный торговый сигнал — entry, stop, target, размер."""
    coin: str
    direction: str            # "long" | "short"
    setup_name: str           # "Volume Breakout", "Liquidation Reversal" и т.д.

    entry_price: float
    stop_loss: float
    targets: List[float]      # [target1, target2]

    risk_pct: float           # % до стопа
    reward_pcts: List[float]  # [% до target1, % до target2]
    risk_reward: float        # risk:reward ratio (для target1)

    position_size_pct: float  # рекомендуемый % от депозита
    risk_per_trade_pct: float # сколько % депозита рискуем (обычно 1-2%)

    confluence_score: float   # score из confluence engine
    context: MarketContext    # рыночная структура

    reasons: List[str]        # почему вошли
    warnings: List[str]       # на что обратить внимание

    def format_telegram(self) -> str:
        """Форматирование для Telegram."""
        dir_emoji = "🟢 LONG" if self.direction == "long" else "🔴 SHORT"
        trend_emoji = {"uptrend": "↑", "downtrend": "↓", "ranging": "↔"}

        # Цели
        targets_str = ""
        for i, (t, rp) in enumerate(zip(self.targets, self.reward_pcts)):
            targets_str += f"  Цель {i+1}: ${t:,.4f} ({rp:+.2f}%)\n"

        # Причины
        reasons_str = "\n".join(f"  • {html_escape(r)}" for r in self.reasons)

        # Предупреждения
        warnings_str = ""
        if self.warnings:
            warnings_str = "\n⚠️ " + " | ".join(html_escape(w) for w in self.warnings)

        msg = (
            f"{'='*30}\n"
            f"{dir_emoji} <b>{self.coin}</b>\n"
            f"{'='*30}\n"
            f"\n"
            f"📍 <b>Сетап:</b> {self.setup_name}\n"
            f"\n"
            f"▶️ Вход: <b>${self.entry_price:,.4f}</b>\n"
            f"🛑 Стоп: ${self.stop_loss:,.4f} ({self.risk_pct:.2f}%)\n"
            f"{targets_str}"
            f"📊 R:R = 1:{self.risk_reward:.1f}\n"
            f"\n"
            f"💰 Размер: <b>{self.position_size_pct:.1f}%</b> от депо "
            f"(риск {self.risk_per_trade_pct:.1f}%)\n"
            f"\n"
            f"<b>Почему:</b>\n{reasons_str}\n"
            f"\n"
            f"📈 Контекст: {self.context.summary}\n"
            f"Score: {self.confluence_score:.1f}"
            f"{warnings_str}"
        )
        return msg


class EntrySignalGenerator:
    """
    Генератор торговых сигналов.
    Принимает данные от ConfluenceEngine и рыночную структуру,
    определяет подходящий сетап и рассчитывает entry/stop/target.
    """

    def __init__(self, config: dict):
        # Настройки риск-менеджмента
        self.risk_per_trade = config.get("risk_management", {}).get(
            "risk_per_trade_pct", 1.0) / 100  # 1% по умолчанию
        self.max_position_pct = config.get("risk_management", {}).get(
            "max_position_pct", 10.0) / 100   # макс 10% депо в одну сделку
        self.min_rr = config.get("risk_management", {}).get(
            "min_risk_reward", 1.5)            # минимум R:R 1:1.5

    def evaluate(self, coin: str, klines_1h: List[Dict],
                 confluence_score: float, signals: list,
                 funding_rate: float = None,
                 liq_stats: dict = None,
                 oi_change_pct: float = None) -> Optional[EntrySignal]:
        """
        Главный метод. Получает все данные и решает:
        1. Есть ли подходящий сетап?
        2. Если да — рассчитывает entry/stop/target
        3. Проверяет R:R (если ниже минимума — не шлём)
        """
        # Анализируем рыночную структуру
        ctx = analyze_structure(klines_1h)
        if not ctx:
            return None

        current_price = klines_1h[-1]["close"]
        atr = calculate_atr(klines_1h, 14)
        if not atr or atr <= 0:
            return None

        # Определяем какие детекторы сработали
        detector_names = {s.detector for s in signals}
        reasons = [s.label for s in signals]

        # ---- Пробуем каждый сетап ----

        # 1. Volume Breakout
        signal = self._try_volume_breakout(
            coin, current_price, atr, ctx, confluence_score,
            detector_names, reasons, signals)
        if signal:
            return signal

        # 2. Liquidation Cascade Reversal
        signal = self._try_liq_reversal(
            coin, current_price, atr, ctx, confluence_score,
            detector_names, reasons, signals, liq_stats)
        if signal:
            return signal

        # 3. OI Divergence
        signal = self._try_oi_divergence(
            coin, current_price, atr, ctx, confluence_score,
            detector_names, reasons, signals, oi_change_pct)
        if signal:
            return signal

        # 4. Funding Squeeze
        signal = self._try_funding_squeeze(
            coin, current_price, atr, ctx, confluence_score,
            detector_names, reasons, signals, funding_rate)
        if signal:
            return signal

        return None

    # ======== СЕТАП 1: Volume Breakout ========
    def _try_volume_breakout(self, coin, price, atr, ctx, score,
                              detectors, reasons, signals) -> Optional[EntrySignal]:
        """
        Условия:
          - volume_spike + breakout сработали одновременно
          - Тренд совпадает с направлением пробоя
          - R:R >= минимума
        """
        if "volume_spike" not in detectors or "breakout" not in detectors:
            return None

        # Определяем направление из breakout-сигнала
        breakout_sig = next((s for s in signals if s.detector == "breakout"), None)
        if not breakout_sig:
            return None

        direction = breakout_sig.direction
        if not direction or direction not in ("bullish", "bearish"):
            return None

        # Тренд должен подтверждать
        if direction == "bullish" and ctx.trend == "downtrend" and ctx.trend_strength > 0.7:
            return None  # контр-тренд слишком сильный
        if direction == "bearish" and ctx.trend == "uptrend" and ctx.trend_strength > 0.7:
            return None

        trade_dir = "long" if direction == "bullish" else "short"
        return self._build_signal(
            coin, trade_dir, "Volume Breakout", price, atr, ctx,
            score, reasons,
            stop_atr_mult=1.5,  # стоп = 1.5 ATR от входа
            target1_atr_mult=2.5,
            target2_atr_mult=4.0,
        )

    # ======== СЕТАП 2: Liquidation Cascade Reversal ========
    def _try_liq_reversal(self, coin, price, atr, ctx, score,
                           detectors, reasons, signals,
                           liq_stats) -> Optional[EntrySignal]:
        """
        Условия:
          - Крупные ликвидации ($5M+) + перекос в одну сторону
          - После каскада ликвидаций часто идёт разворот
        """
        if "liquidations" not in detectors or not liq_stats:
            return None

        total = liq_stats.get("total_usd", 0)
        if total < 5_000_000:
            return None

        imbalance = liq_stats.get("imbalance", 0)

        # Ликвидации лонгов → потенциальный отскок вверх (long)
        # Ликвидации шортов → потенциальный разворот вниз (short)
        if imbalance < -0.5:
            trade_dir = "long"  # лонги ликвидировались → ждём отскок
            reasons.append("Каскад лонг-ликвидаций → ожидаем отскок")
        elif imbalance > 0.5:
            trade_dir = "short"
            reasons.append("Каскад шорт-ликвидаций → ожидаем откат")
        else:
            return None  # обе стороны — нет чёткого сигнала

        warnings = ["Контр-тренд сетап — жёсткий стоп обязателен"]

        return self._build_signal(
            coin, trade_dir, "Liquidation Reversal", price, atr, ctx,
            score, reasons,
            stop_atr_mult=1.0,   # тайтовый стоп для контр-тренда
            target1_atr_mult=2.0,
            target2_atr_mult=3.0,
            warnings=warnings,
        )

    # ======== СЕТАП 3: OI Divergence ========
    def _try_oi_divergence(self, coin, price, atr, ctx, score,
                            detectors, reasons, signals,
                            oi_change_pct) -> Optional[EntrySignal]:
        """
        Условия:
          - OI + price движутся в противоположных направлениях (дивергенция)
          - Это сигнал слабости тренда → ожидаем разворот
        """
        if "open_interest" not in detectors or oi_change_pct is None:
            return None

        oi_sig = next((s for s in signals if s.detector == "open_interest"), None)
        if not oi_sig:
            return None

        # Ищем дивергенцию:
        # OI падает + цена растёт → закрывают шорты, движение слабое → short
        # OI падает + цена падает → закрывают лонги → long (ожидаем отскок)

        # OI divergence обычно контр-трендовый — только если тренд ослаб
        if "закрытие шортов" in (oi_sig.details or ""):
            trade_dir = "short"
            reasons.append("OI падает при росте цены → слабость движения")
        elif "закрытие лонгов" in (oi_sig.details or ""):
            trade_dir = "long"
            reasons.append("OI падает при падении цены → давление ослабло")
        else:
            return None

        warnings = ["Дивергенция OI — подтверди объёмом перед входом"]

        return self._build_signal(
            coin, trade_dir, "OI Divergence", price, atr, ctx,
            score, reasons,
            stop_atr_mult=1.5,
            target1_atr_mult=2.0,
            target2_atr_mult=3.5,
            warnings=warnings,
        )

    # ======== СЕТАП 4: Funding Squeeze ========
    def _try_funding_squeeze(self, coin, price, atr, ctx, score,
                              detectors, reasons, signals,
                              funding_rate) -> Optional[EntrySignal]:
        """
        Условия:
          - Экстремальный funding + ещё один подтверждающий сигнал
          - Высокий funding = все в лонгах → вероятен сквиз вниз (short)
          - Низкий funding = все в шортах → вероятен шорт-сквиз (long)
        """
        if "funding" not in detectors or funding_rate is None:
            return None

        # Нужно подтверждение ещё хотя бы одним детектором
        if len(detectors) < 2:
            return None

        if funding_rate > 0.05:
            trade_dir = "short"
            reasons.append(f"Funding {funding_rate:+.3f}% → перегрузка лонгов")
        elif funding_rate < -0.05:
            trade_dir = "long"
            reasons.append(f"Funding {funding_rate:+.3f}% → перегрузка шортов")
        else:
            return None

        warnings = ["Funding-сквиз — может реализоваться через 1-8 часов, не мгновенно"]

        return self._build_signal(
            coin, trade_dir, "Funding Squeeze", price, atr, ctx,
            score, reasons,
            stop_atr_mult=2.0,   # широкий стоп — сквизы бывают с шипами
            target1_atr_mult=3.0,
            target2_atr_mult=5.0,
            warnings=warnings,
        )

    # ======== Построение сигнала (общий код) ========
    def _build_signal(self, coin: str, direction: str, setup_name: str,
                      price: float, atr: float, ctx: MarketContext,
                      score: float, reasons: List[str],
                      stop_atr_mult: float = 1.5,
                      target1_atr_mult: float = 2.5,
                      target2_atr_mult: float = 4.0,
                      warnings: List[str] = None) -> Optional[EntrySignal]:
        """Общая логика расчёта entry/stop/target/position size."""
        if warnings is None:
            warnings = []

        # ---- Стоп ----
        if direction == "long":
            stop_raw = price - atr * stop_atr_mult
            # Не ставим стоп выше ближайшей поддержки — это глупо
            stop = min(stop_raw, ctx.nearest_support * 0.998)
            # Цели
            t1 = price + atr * target1_atr_mult
            t2 = price + atr * target2_atr_mult
            # Корректируем цель 1 по сопротивлению (если оно ближе)
            if ctx.nearest_resistance < t1:
                t1 = ctx.nearest_resistance * 0.998
        else:  # short
            stop_raw = price + atr * stop_atr_mult
            stop = max(stop_raw, ctx.nearest_resistance * 1.002)
            t1 = price - atr * target1_atr_mult
            t2 = price - atr * target2_atr_mult
            if ctx.nearest_support > t1:
                t1 = ctx.nearest_support * 1.002

        # ---- Risk/Reward ----
        risk_distance = abs(price - stop)
        reward_distance_1 = abs(t1 - price)
        reward_distance_2 = abs(t2 - price)

        if risk_distance == 0:
            return None

        rr1 = reward_distance_1 / risk_distance
        rr2 = reward_distance_2 / risk_distance

        # Проверка минимального R:R
        if rr1 < self.min_rr:
            log.debug("%s %s R:R %.1f < min %.1f, пропускаем",
                     coin, setup_name, rr1, self.min_rr)
            return None

        # ---- % изменений ----
        risk_pct = (risk_distance / price) * 100
        reward_pct_1 = (reward_distance_1 / price) * 100
        reward_pct_2 = (reward_distance_2 / price) * 100

        if direction == "short":
            risk_pct = -risk_pct  # для short стоп выше — убыток при росте

        # ---- Размер позиции ----
        # position_size_pct = risk_per_trade / |risk_pct|
        abs_risk_pct = abs(risk_pct) / 100
        if abs_risk_pct > 0:
            position_pct = self.risk_per_trade / abs_risk_pct
        else:
            position_pct = 0

        # Ограничение максимальным размером позиции
        position_pct = min(position_pct, self.max_position_pct)
        position_pct_display = position_pct * 100

        # ---- Предупреждения ----
        if ctx.trend == "ranging":
            warnings.append("Рейндж — возможны ложные пробои")
        if risk_pct > 5:
            warnings.append("Широкий стоп >5% — уменьши размер позиции")

        return EntrySignal(
            coin=coin,
            direction=direction,
            setup_name=setup_name,
            entry_price=price,
            stop_loss=round(stop, 6),
            targets=[round(t1, 6), round(t2, 6)],
            risk_pct=round(abs(risk_pct), 2),
            reward_pcts=[round(reward_pct_1, 2), round(reward_pct_2, 2)],
            risk_reward=round(rr1, 1),
            position_size_pct=round(position_pct_display, 1),
            risk_per_trade_pct=round(self.risk_per_trade * 100, 1),
            confluence_score=score,
            context=ctx,
            reasons=reasons,
            warnings=warnings,
        )
