"""
Confluence Engine — ядро v2.

Идея:
  1. Каждый детектор при срабатывании добавляет "сигнал" (Signal) в общий буфер
  2. Сигнал имеет weight (вес) и detector_name
  3. Раз в минуту engine проходит по буферу и для каждой монеты считает
     сумму весов за последние 15 минут + количество РАЗНЫХ детекторов
  4. Если score и detector_count превышают пороги — отправляется градуированный алерт
  5. Cooldown защищает от повторных алертов по той же монете
"""
from html import escape as html_escape
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from collections import defaultdict
from datetime import datetime, timezone
import time
import logging

log = logging.getLogger(__name__)


@dataclass
class Signal:
    """Один сигнал от одного детектора."""
    coin: str                      # тикер: BTC, ETH...
    detector: str                  # имя детектора: "volume_spike", "funding", ...
    weight: float                  # вклад в score
    direction: Optional[str]       # "bullish" | "bearish" | None
    label: str                     # короткое описание: "Vol x7"
    details: str = ""              # подробности для алерта
    timestamp: float = field(default_factory=time.time)


@dataclass
class Alert:
    """Готовый к отправке алерт."""
    coin: str
    tier: str                      # "watch" | "signal" | "strong"
    score: float
    direction: Optional[str]       # итоговое направление (bullish/bearish/mixed)
    signals: List[Signal]
    message: str                   # форматированный текст


class ConfluenceEngine:
    """Собирает сигналы детекторов и решает когда отправлять алерт."""

    def __init__(self, config: dict):
        self.cfg = config["confluence"]
        self.cooldown_cfg = config["cooldown"]
        self.window_sec = self.cfg["window_minutes"] * 60

        # coin → [Signal]
        self._signals: Dict[str, List[Signal]] = defaultdict(list)
        # coin + tier → last_alert_timestamp
        self._last_alert: Dict[str, float] = {}

    # -------- Приём сигналов от детекторов --------
    def add_signal(self, sig: Signal) -> None:
        self._signals[sig.coin].append(sig)
        log.debug("Signal: %s [%s] w=%s", sig.coin, sig.detector, sig.weight)

    # -------- Основной цикл --------
    def evaluate(self) -> List[Alert]:
        """
        Пройти по всем монетам и вернуть готовые алерты.
        Вызывать раз в минуту.
        """
        self._prune_old()
        alerts = []

        for coin, signals in list(self._signals.items()):
            if not signals:
                continue

            alert = self._evaluate_coin(coin, signals)
            if alert:
                alerts.append(alert)

        return alerts

    def _prune_old(self) -> None:
        """Удаляем сигналы старше window."""
        cutoff = time.time() - self.window_sec
        for coin in list(self._signals.keys()):
            self._signals[coin] = [s for s in self._signals[coin]
                                   if s.timestamp >= cutoff]
            if not self._signals[coin]:
                del self._signals[coin]

    def _evaluate_coin(self, coin: str, signals: List[Signal]) -> Optional[Alert]:
        """Посчитать score и решить — алертить ли."""
        # Дедупликация: от одного детектора берём только самый свежий сигнал
        # (чтобы volume spike не насчитывался 10 раз)
        latest_by_detector: Dict[str, Signal] = {}
        for s in signals:
            prev = latest_by_detector.get(s.detector)
            if not prev or s.timestamp > prev.timestamp:
                latest_by_detector[s.detector] = s

        unique_signals = list(latest_by_detector.values())
        total_score = sum(s.weight for s in unique_signals)
        detector_count = len(unique_signals)

        # Определяем tier
        tier = self._determine_tier(total_score, detector_count)
        if not tier:
            return None

        # Проверка cooldown
        if not self._cooldown_ok(coin, tier):
            return None

        # Направление — bullish/bearish/mixed
        direction = self._aggregate_direction(unique_signals)

        # Формируем сообщение
        message = self._format_alert(coin, tier, total_score, direction,
                                     unique_signals)

        # Помечаем время отправки
        self._last_alert[f"{coin}_{tier}"] = time.time()

        return Alert(
            coin=coin, tier=tier, score=total_score,
            direction=direction, signals=unique_signals, message=message,
        )

    def _determine_tier(self, score: float, count: int) -> Optional[str]:
        """Определить уровень алерта по score + min detector count."""
        tiers = self.cfg["tiers"]
        mins = self.cfg["min_detector_count"]

        # Идём от strong к watch
        if score >= tiers["strong"] and count >= mins["strong"]:
            return "strong"
        if score >= tiers["signal"] and count >= mins["signal"]:
            return "signal"
        if score >= tiers["watch"] and count >= mins["watch"]:
            return "watch"
        return None

    def _cooldown_ok(self, coin: str, tier: str) -> bool:
        """Не слать один и тот же coin+tier чаще чем раз в N минут."""
        key = f"{coin}_{tier}"
        last = self._last_alert.get(key, 0)
        cooldown_min = self.cooldown_cfg.get(f"{tier}_minutes", 30)
        return (time.time() - last) >= cooldown_min * 60

    def _aggregate_direction(self, signals: List[Signal]) -> str:
        """Определить итоговое направление по сигналам."""
        bullish = sum(s.weight for s in signals if s.direction == "bullish")
        bearish = sum(s.weight for s in signals if s.direction == "bearish")
        total = bullish + bearish

        if total == 0:
            return "unclear"
        if bullish > bearish * 2:
            return "bullish"
        if bearish > bullish * 2:
            return "bearish"
        return "mixed"

    def _format_alert(self, coin: str, tier: str, score: float,
                      direction: str, signals: List[Signal]) -> str:
        """HTML-форматирование для Telegram."""
        tier_emoji = {"watch": "👀", "signal": "⚡", "strong": "🚨"}
        tier_label = {"watch": "WATCH", "signal": "SIGNAL", "strong": "STRONG"}
        dir_emoji = {"bullish": "🟢", "bearish": "🔴",
                    "mixed": "🟡", "unclear": "⚪"}

        header = (
            f"{tier_emoji[tier]} <b>{tier_label[tier]}: {coin}</b> "
            f"{dir_emoji[direction]} {direction.upper()}\n"
            f"Score: <b>{score:.1f}</b> | Детекторов: {len(signals)}\n"
            f"—————\n"
        )

        # Список сработавших сигналов
        body_lines = []
        for s in sorted(signals, key=lambda x: -x.weight):
            line = f"• <b>{s.label}</b> (+{s.weight:.1f})"
            if s.details:
                line += f" — {html_escape(str(s.details))}"
            body_lines.append(line)

        footer = (
            f"\n—————\n"
            f"Окно: {self.cfg['window_minutes']}мин"
        )
        return header + "\n".join(body_lines) + footer
