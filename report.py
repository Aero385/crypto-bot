"""
Анализ эффективности алертов.

Запуск: python3 report.py

Что делает:
1. Проверяет outcomes для всех алертов старше 24 часов (смотрит что стало с ценой)
2. Считает точность по tier и по детекторам
3. Отправляет сводный отчёт в Telegram

Рекомендуется запускать раз в сутки через cron:
  0 12 * * * cd /path/to/bot && python3 report.py
"""
import logging
import yaml
from journal import AlertJournal
from binance_client import BinanceClient
from notifier import TelegramNotifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("report")


def fill_outcomes(journal: AlertJournal, binance: BinanceClient) -> int:
    """Для алертов старше 24ч подтянуть фактические цены через 1/4/24ч."""
    alerts = journal.unchecked_alerts_older_than(24)
    log.info("Проверяю исход %d алертов...", len(alerts))

    filled = 0
    for a in alerts:
        coin = a["coin"]
        initial_price = a["price"]
        alert_ts = a["ts"]

        pair = f"{coin}USDT"
        if pair not in binance.spot_symbols():
            continue

        # Запрашиваем свечи начиная с момента алерта + 24ч
        # Получаем 1-часовые свечи, ищем нужные точки по времени
        klines = binance.klines(pair, interval="1h", limit=30)
        if not klines:
            continue

        # Находим свечи ближе всего к alert_ts + 1h, 4h, 24h
        def price_at(offset_hours):
            target = (alert_ts + offset_hours * 3600) * 1000
            best = min(klines, key=lambda k: abs(k["close_time"] - target))
            return best["close"]

        try:
            p1 = price_at(1)
            p4 = price_at(4)
            p24 = price_at(24)
            journal.record_outcome(a["id"], p1, p4, p24,
                                   initial_price, a["direction"])
            filled += 1
        except Exception as e:
            log.debug("Ошибка outcome для %s: %s", coin, e)

    log.info("Заполнено исходов: %d", filled)
    return filled


def format_report(journal: AlertJournal) -> str:
    """Собрать текстовый отчёт за 7 дней."""
    tier_stats = journal.performance_by_tier(days=7)
    det_stats = journal.performance_by_detector(days=7)

    lines = ["<b>📊 Еженедельный отчёт</b>", "———"]

    # По tier
    lines.append("<b>Точность по уровням:</b>")
    for tier in ("watch", "signal", "strong"):
        stats = tier_stats.get(tier, {})
        correct = stats.get("correct", 0)
        wrong = stats.get("wrong", 0)
        neutral = stats.get("neutral", 0)
        total_graded = correct + wrong
        acc = correct / total_graded * 100 if total_graded else 0
        emoji = {"watch": "👀", "signal": "⚡", "strong": "🚨"}[tier]
        lines.append(
            f"{emoji} <b>{tier}</b>: {acc:.0f}% "
            f"({correct}✓ / {wrong}✗ / {neutral}·)"
        )

    # По детекторам — показываем топ-3 и антитоп-3 по точности
    lines.append("\n<b>Точность детекторов:</b>")
    graded = [(d, s) for d, s in det_stats.items()
              if s.get("accuracy") is not None and (s["correct"] + s["wrong"]) >= 3]
    graded.sort(key=lambda x: -x[1]["accuracy"])

    if graded:
        for d, s in graded[:5]:
            lines.append(
                f"• <b>{d}</b>: {s['accuracy']:.0f}% "
                f"({s['correct']}/{s['correct']+s['wrong']})"
            )
    else:
        lines.append("<i>Данных недостаточно — ждём больше алертов с исходами.</i>")

    lines.append("\n<i>Вердикт по движению через 4ч (скальпинг-горизонт)</i>")
    return "\n".join(lines)


def main():
    cfg = yaml.safe_load(open("config.yaml"))
    journal = AlertJournal()
    binance = BinanceClient()

    # Заполняем outcomes
    filled = fill_outcomes(journal, binance)

    # Формируем и отправляем отчёт
    report = format_report(journal)
    print(report.replace("<b>", "").replace("</b>", "")
                .replace("<i>", "").replace("</i>", ""))

    notifier = TelegramNotifier(cfg["telegram"]["bot_token"],
                                cfg["telegram"]["chat_id"])
    notifier.send(report)


if __name__ == "__main__":
    main()
