"""
Telegram-команды для управления ботом без перезапуска.

Поддерживаемые команды:
  /status           — текущее состояние бота
  /pause            — приостановить алерты
  /resume           — возобновить
  /stats            — статистика сигналов за последние 24ч
  /top              — топ монет по активности сигналов
  /threshold <tier> <value>  — изменить порог (watch/signal/strong)
  /cooldown <tier> <min>     — изменить cooldown
  /mute <COIN> <min>         — замьютить монету на N минут
  /unmute <COIN>             — размьютить
  /test             — отправить тестовый алерт
  /help             — показать команды

Архитектурно: работает через polling getUpdates в фоновом потоке.
Не мешает основному циклу сканирования.
"""
import requests
import logging
import threading
import time
from typing import Callable, Dict, Optional

log = logging.getLogger(__name__)


class CommandHandler:
    """Обработчик команд. Принимает callbacks для управления ботом."""

    def __init__(self, bot_token: str, allowed_chat_id: str):
        self.bot_token = bot_token
        self.allowed_chat_id = str(allowed_chat_id)
        self.api_url = f"https://api.telegram.org/bot{bot_token}"
        self._offset = 0
        self._running = False
        self._thread: Optional[threading.Thread] = None
        # Команда → обработчик (вызывается с аргументами: args, chat_id)
        self._handlers: Dict[str, Callable] = {}

    def register(self, command: str, handler: Callable) -> None:
        """Зарегистрировать обработчик команды. Команда без слеша."""
        self._handlers[command.lower()] = handler

    def _send(self, chat_id: str, text: str) -> None:
        try:
            requests.post(
                f"{self.api_url}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                timeout=10,
            )
        except Exception as e:
            log.error("Command reply failed: %s", e)

    def _poll_loop(self) -> None:
        log.info("Command polling started")
        while self._running:
            try:
                r = requests.get(
                    f"{self.api_url}/getUpdates",
                    params={"offset": self._offset, "timeout": 25},
                    timeout=30,
                )
                if r.status_code != 200:
                    time.sleep(5)
                    continue
                data = r.json()
                for upd in data.get("result", []):
                    self._offset = upd["update_id"] + 1
                    self._handle_update(upd)
            except Exception as e:
                log.debug("Poll error: %s", e)
                time.sleep(5)

    def _handle_update(self, update: dict) -> None:
        msg = update.get("message") or update.get("channel_post")
        if not msg:
            return

        text = (msg.get("text") or "").strip()
        if not text.startswith("/"):
            return

        chat_id = str(msg["chat"]["id"])
        # Разрешаем команды только из целевого чата
        if chat_id != self.allowed_chat_id:
            log.warning("Command from unauthorized chat: %s", chat_id)
            return

        # Парсим: /command arg1 arg2...
        parts = text[1:].split()
        if not parts:
            return
        cmd = parts[0].lower()
        # Убираем суффикс @botname если есть
        if "@" in cmd:
            cmd = cmd.split("@")[0]
        args = parts[1:]

        handler = self._handlers.get(cmd)
        if not handler:
            return

        try:
            reply = handler(args, chat_id)
            if reply:
                self._send(chat_id, reply)
        except Exception as e:
            log.exception("Handler for /%s failed: %s", cmd, e)
            self._send(chat_id, f"⚠️ Ошибка при выполнении /{cmd}: {e}")

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False


class BotController:
    """
    Логика обработчиков команд. Владеет ссылками на engine, config и т.п.
    """

    def __init__(self, bot):
        self.bot = bot
        self.paused = False
        self.min_score: float = 0  # фильтр: не слать ниже этого score (0 = выкл)
        # coin → unmute_timestamp
        self.muted: Dict[str, float] = {}
        # Журнал отправленных алертов для /stats
        # [(timestamp, coin, tier, score), ...]
        self.sent_log: list = []

    def log_alert(self, coin: str, tier: str, score: float) -> None:
        """Вызывать из основного цикла при отправке алерта."""
        self.sent_log.append((time.time(), coin, tier, score))
        # Оставляем только последние 7 дней
        cutoff = time.time() - 7 * 86400
        self.sent_log = [x for x in self.sent_log if x[0] >= cutoff]

    def is_muted(self, coin: str) -> bool:
        """Проверка в основном цикле перед отправкой."""
        exp = self.muted.get(coin)
        if not exp:
            return False
        if time.time() >= exp:
            del self.muted[coin]
            return False
        return True

    # ============ Обработчики команд ============

    def cmd_status(self, args, chat_id) -> str:
        cfg = self.bot.cfg
        uptime = int(time.time() - self.bot._start_time)
        h, m = divmod(uptime // 60, 60)

        universe_size = len(self.bot._universe)
        signal_count = sum(
            len(sigs) for sigs in self.bot.engine._signals.values()
        )
        muted_count = len(self.muted)

        status = "⏸ ПАУЗА" if self.paused else "▶️ АКТИВЕН"
        score_filter = f"≥ {self.min_score}" if self.min_score > 0 else "выкл"

        return (
            f"<b>Статус бота</b>\n"
            f"———\n"
            f"{status}\n"
            f"Uptime: {h}ч {m}м\n"
            f"Universe: {universe_size} монет\n"
            f"Активных сигналов в буфере: {signal_count}\n"
            f"Замьючено монет: {muted_count}\n"
            f"Фильтр score: {score_filter}\n"
            f"\n"
            f"<b>Текущие пороги confluence:</b>\n"
            f"• watch: {cfg['confluence']['tiers']['watch']}\n"
            f"• signal: {cfg['confluence']['tiers']['signal']}\n"
            f"• strong: {cfg['confluence']['tiers']['strong']}\n"
        )

    def cmd_pause(self, args, chat_id) -> str:
        self.paused = True
        return "⏸ Алерты приостановлены. /resume чтобы возобновить."

    def cmd_resume(self, args, chat_id) -> str:
        self.paused = False
        return "▶️ Алерты возобновлены."

    def cmd_stats(self, args, chat_id) -> str:
        # Статистика за 24 часа
        cutoff = time.time() - 86400
        recent = [x for x in self.sent_log if x[0] >= cutoff]

        if not recent:
            return "📊 За последние 24ч алертов не было."

        by_tier = {"watch": 0, "signal": 0, "strong": 0}
        by_coin = {}
        for _, coin, tier, _ in recent:
            by_tier[tier] = by_tier.get(tier, 0) + 1
            by_coin[coin] = by_coin.get(coin, 0) + 1

        top_coins = sorted(by_coin.items(), key=lambda x: -x[1])[:5]

        result = (
            f"<b>📊 Статистика за 24ч</b>\n"
            f"———\n"
            f"Всего алертов: <b>{len(recent)}</b>\n"
            f"• 👀 watch: {by_tier['watch']}\n"
            f"• ⚡ signal: {by_tier['signal']}\n"
            f"• 🚨 strong: {by_tier['strong']}\n"
            f"\n<b>Самые активные:</b>\n"
        )
        for coin, count in top_coins:
            result += f"• {coin}: {count}\n"
        return result

    def cmd_top(self, args, chat_id) -> str:
        """Топ монет по текущей активности в буфере сигналов."""
        activity = []
        for coin, sigs in self.bot.engine._signals.items():
            score = sum(s.weight for s in sigs)
            activity.append((coin, score, len(sigs)))

        activity.sort(key=lambda x: -x[1])
        top = activity[:10]

        if not top:
            return "📈 Сейчас активных сигналов нет."

        lines = ["<b>📈 Топ по активности (в буфере):</b>", "———"]
        for coin, score, count in top:
            lines.append(f"• <b>{coin}</b>: score {score:.1f} ({count} сигналов)")
        return "\n".join(lines)

    def cmd_threshold(self, args, chat_id) -> str:
        if len(args) != 2:
            return "Использование: /threshold <watch|signal|strong> <value>\nПример: /threshold signal 5.0"

        tier, value = args[0].lower(), args[1]
        if tier not in ("watch", "signal", "strong"):
            return "Tier должен быть watch, signal или strong"

        try:
            val = float(value)
        except ValueError:
            return f"Не смог распарсить '{value}' как число"

        old = self.bot.cfg["confluence"]["tiers"][tier]
        self.bot.cfg["confluence"]["tiers"][tier] = val
        # Engine читает из cfg напрямую, так что изменения применяются сразу
        return f"✓ Порог <b>{tier}</b>: {old} → <b>{val}</b>"

    def cmd_cooldown(self, args, chat_id) -> str:
        if len(args) != 2:
            return "Использование: /cooldown <watch|signal|strong> <minutes>"

        tier, minutes = args[0].lower(), args[1]
        if tier not in ("watch", "signal", "strong"):
            return "Tier должен быть watch, signal или strong"

        try:
            mins = int(minutes)
        except ValueError:
            return f"Не смог распарсить '{minutes}' как число"

        key = f"{tier}_minutes"
        old = self.bot.cfg["cooldown"].get(key, 30)
        self.bot.cfg["cooldown"][key] = mins
        return f"✓ Cooldown <b>{tier}</b>: {old} → <b>{mins}</b> минут"

    def cmd_mute(self, args, chat_id) -> str:
        if len(args) != 2:
            return "Использование: /mute <COIN> <minutes>\nПример: /mute DOGE 60"

        coin = args[0].upper()
        try:
            mins = int(args[1])
        except ValueError:
            return f"Не смог распарсить '{args[1]}' как число"

        self.muted[coin] = time.time() + mins * 60
        return f"🔇 {coin} замьючен на {mins} минут"

    def cmd_unmute(self, args, chat_id) -> str:
        if not args:
            return "Использование: /unmute <COIN>"
        coin = args[0].upper()
        if coin in self.muted:
            del self.muted[coin]
            return f"🔊 {coin} размьючен"
        return f"{coin} и так не был замьючен"

    def cmd_minscore(self, args, chat_id) -> str:
        if not args:
            if self.min_score > 0:
                return f"📊 Текущий фильтр: score ≥ <b>{self.min_score}</b>\nСбросить: /minscore 0"
            return "📊 Фильтр по score выключен.\nВключить: /minscore 5"

        try:
            val = float(args[0])
        except ValueError:
            return f"Не смог распарсить '{args[0]}' как число"

        old = self.min_score
        self.min_score = val
        if val <= 0:
            return "📊 Фильтр по score <b>выключен</b> — приходят все алерты"
        return f"📊 Фильтр: score ≥ <b>{val}</b> (было: {old})"

    # ============ Blacklist ============

    def cmd_blacklist(self, args, chat_id) -> str:
        bl = self.bot.cfg.get("filters", {}).get("blacklist", [])
        if not args:
            if not bl:
                return "🚫 Blacklist пуст."
            return f"🚫 <b>Blacklist ({len(bl)}):</b>\n{', '.join(bl)}"

        action = args[0].lower()
        if action == "add" and len(args) >= 2:
            coin = args[1].upper()
            if coin in bl:
                return f"{coin} уже в blacklist"
            bl.append(coin)
            return f"🚫 <b>{coin}</b> добавлен в blacklist"
        elif action in ("del", "remove") and len(args) >= 2:
            coin = args[1].upper()
            if coin not in bl:
                return f"{coin} не найден в blacklist"
            bl.remove(coin)
            return f"✓ <b>{coin}</b> убран из blacklist"
        else:
            return "Использование:\n/blacklist — показать список\n/blacklist add COIN\n/blacklist del COIN"

    # ============ Detectors ============

    def cmd_detector(self, args, chat_id) -> str:
        detectors = self.bot.cfg.get("detectors", {})
        det_names = [k for k in detectors if isinstance(detectors[k], dict)]

        if not args:
            lines = ["<b>🔧 Детекторы:</b>"]
            for name in det_names:
                on = detectors[name].get("enabled", True)
                emoji = "✅" if on else "❌"
                lines.append(f"  {emoji} {name}")
            lines.append("\n/detector &lt;name&gt; on|off")
            return "\n".join(lines)

        name = args[0].lower()
        if name not in det_names:
            return f"Детектор '{name}' не найден.\nДоступные: {', '.join(det_names)}"

        if len(args) < 2:
            on = detectors[name].get("enabled", True)
            return f"{'✅' if on else '❌'} <b>{name}</b>: {'включён' if on else 'выключен'}"

        action = args[1].lower()
        if action in ("on", "1", "true", "yes"):
            detectors[name]["enabled"] = True
            return f"✅ <b>{name}</b> включён"
        elif action in ("off", "0", "false", "no"):
            detectors[name]["enabled"] = False
            return f"❌ <b>{name}</b> выключен"
        else:
            return "Использование: /detector &lt;name&gt; on|off"

    # ============ Entry signals ============

    def cmd_entry(self, args, chat_id) -> str:
        rm = self.bot.cfg.setdefault("risk_management", {})

        if not args:
            on = rm.get("entry_signals_enabled", False)
            return (
                f"📍 <b>Точки входа: {'✅ вкл' if on else '❌ выкл'}</b>\n"
                f"• min score: {rm.get('min_score_for_entry', 5.0)}\n"
                f"• min детекторов: {rm.get('min_detectors_for_entry', 2)}\n"
                f"• min R:R: {rm.get('min_risk_reward', 1.5)}\n"
                f"• риск на сделку: {rm.get('risk_per_trade_pct', 1.0)}%\n"
                f"• макс позиция: {rm.get('max_position_pct', 10.0)}%\n"
                f"\nКоманды:\n"
                f"/entry on|off\n"
                f"/entry score &lt;N&gt;\n"
                f"/entry rr &lt;N&gt;\n"
                f"/entry risk &lt;N&gt;\n"
                f"/entry maxpos &lt;N&gt;"
            )

        action = args[0].lower()
        if action in ("on", "1", "true"):
            rm["entry_signals_enabled"] = True
            return "📍 Точки входа <b>включены</b>"
        elif action in ("off", "0", "false"):
            rm["entry_signals_enabled"] = False
            return "📍 Точки входа <b>выключены</b>"
        elif action == "score" and len(args) >= 2:
            try:
                val = float(args[1])
            except ValueError:
                return f"Не смог распарсить '{args[1]}'"
            old = rm.get("min_score_for_entry", 5.0)
            rm["min_score_for_entry"] = val
            return f"📍 Min score для entry: {old} → <b>{val}</b>"
        elif action == "rr" and len(args) >= 2:
            try:
                val = float(args[1])
            except ValueError:
                return f"Не смог распарсить '{args[1]}'"
            old = rm.get("min_risk_reward", 1.5)
            rm["min_risk_reward"] = val
            if hasattr(self.bot, 'entry_gen'):
                self.bot.entry_gen.min_rr = val
            return f"📍 Min R:R: {old} → <b>{val}</b>"
        elif action == "risk" and len(args) >= 2:
            try:
                val = float(args[1])
            except ValueError:
                return f"Не смог распарсить '{args[1]}'"
            old = rm.get("risk_per_trade_pct", 1.0)
            rm["risk_per_trade_pct"] = val
            if hasattr(self.bot, 'entry_gen'):
                self.bot.entry_gen.risk_per_trade = val / 100
            return f"📍 Риск на сделку: {old}% → <b>{val}%</b>"
        elif action == "maxpos" and len(args) >= 2:
            try:
                val = float(args[1])
            except ValueError:
                return f"Не смог распарсить '{args[1]}'"
            old = rm.get("max_position_pct", 10.0)
            rm["max_position_pct"] = val
            if hasattr(self.bot, 'entry_gen'):
                self.bot.entry_gen.max_position_pct = val / 100
            return f"📍 Макс позиция: {old}% → <b>{val}%</b>"
        else:
            return "Использование: /entry on|off|score|rr|risk|maxpos"

    # ============ Quiet hours ============

    def cmd_quiet(self, args, chat_id) -> str:
        qh = self.bot.cfg.setdefault("quiet_hours", {})

        if not args:
            on = qh.get("enabled", False)
            return (
                f"🌙 <b>Тихие часы: {'✅ вкл' if on else '❌ выкл'}</b>\n"
                f"• Период: {qh.get('start_hour_utc', 22)}:00 — {qh.get('end_hour_utc', 6)}:00 UTC\n"
                f"• Strong игнорирует: {'да' if qh.get('override_for_strong') else 'нет'}\n"
                f"\n/quiet on|off\n/quiet hours &lt;start&gt; &lt;end&gt;"
            )

        action = args[0].lower()
        if action in ("on", "1"):
            qh["enabled"] = True
            return f"🌙 Тихие часы <b>включены</b> ({qh.get('start_hour_utc',22)}-{qh.get('end_hour_utc',6)} UTC)"
        elif action in ("off", "0"):
            qh["enabled"] = False
            return "🌙 Тихие часы <b>выключены</b>"
        elif action == "hours" and len(args) >= 3:
            try:
                s, e = int(args[1]), int(args[2])
            except ValueError:
                return "Формат: /quiet hours 22 6"
            if not (0 <= s <= 23 and 0 <= e <= 23):
                return "Часы должны быть 0-23"
            qh["start_hour_utc"] = s
            qh["end_hour_utc"] = e
            return f"🌙 Тихие часы: <b>{s}:00 — {e}:00 UTC</b>"
        else:
            return "Использование: /quiet on|off|hours"

    # ============ Interval ============

    def cmd_interval(self, args, chat_id) -> str:
        if not args:
            cur = self.bot.cfg.get("scan_interval_seconds", 30)
            return f"⏱ Интервал сканирования: <b>{cur}с</b>\n/interval &lt;секунды&gt;"

        try:
            val = int(args[0])
        except ValueError:
            return f"Не смог распарсить '{args[0]}'"

        if val < 10:
            return "Минимум 10 секунд (защита от перегрузки API)"

        old = self.bot.cfg.get("scan_interval_seconds", 30)
        self.bot.cfg["scan_interval_seconds"] = val
        return f"⏱ Интервал: {old}с → <b>{val}с</b>"

    # ============ Config overview ============

    def cmd_config(self, args, chat_id) -> str:
        cfg = self.bot.cfg
        det = cfg.get("detectors", {})
        rm = cfg.get("risk_management", {})
        qh = cfg.get("quiet_hours", {})
        bl = cfg.get("filters", {}).get("blacklist", [])

        det_on = [k for k in det if isinstance(det[k], dict) and det[k].get("enabled")]
        det_off = [k for k in det if isinstance(det[k], dict) and not det[k].get("enabled")]

        score_filter = f"≥ {self.min_score}" if self.min_score > 0 else "выкл"
        entry_on = rm.get("entry_signals_enabled", False)
        quiet_on = qh.get("enabled", False)

        return (
            f"<b>⚙️ Полная конфигурация</b>\n"
            f"{'='*25}\n"
            f"\n<b>Confluence:</b>\n"
            f"  watch: {cfg['confluence']['tiers']['watch']} | "
            f"signal: {cfg['confluence']['tiers']['signal']} | "
            f"strong: {cfg['confluence']['tiers']['strong']}\n"
            f"\n<b>Cooldown:</b>\n"
            f"  watch: {cfg['cooldown']['watch_minutes']}мин | "
            f"signal: {cfg['cooldown']['signal_minutes']}мин | "
            f"strong: {cfg['cooldown']['strong_minutes']}мин\n"
            f"\n<b>Фильтры:</b>\n"
            f"  Score: {score_filter}\n"
            f"  Blacklist: {len(bl)} монет\n"
            f"\n<b>Детекторы ({len(det_on)} вкл / {len(det_off)} выкл):</b>\n"
            f"  ✅ {', '.join(det_on) if det_on else '—'}\n"
            f"  ❌ {', '.join(det_off) if det_off else '—'}\n"
            f"\n<b>Entry signals:</b> {'✅' if entry_on else '❌'}"
            + (f" (score≥{rm.get('min_score_for_entry',5)}, "
               f"R:R≥{rm.get('min_risk_reward',1.5)}, "
               f"risk {rm.get('risk_per_trade_pct',1)}%)" if entry_on else "") +
            f"\n\n<b>Quiet hours:</b> {'✅ ' + str(qh.get('start_hour_utc',22)) + '-' + str(qh.get('end_hour_utc',6)) + ' UTC' if quiet_on else '❌'}\n"
            f"<b>Интервал:</b> {cfg.get('scan_interval_seconds',30)}с\n"
        )

    # ============ Performance ============

    def cmd_perf(self, args, chat_id) -> str:
        perf = self.bot._perf
        api = self.bot.binance.get_api_stats()
        uptime = int(time.time() - self.bot._start_time)
        h, m = divmod(uptime // 60, 60)

        return (
            f"<b>⚡ Производительность</b>\n"
            f"{'=' * 25}\n"
            f"\n<b>Последний цикл #{perf['total_cycles']}:</b>\n"
            f"  Общее время: {perf['last_cycle_time']:.1f}с\n"
            f"  💎 Core: {perf['last_core_count']} монет за {perf['last_core_time']:.1f}с\n"
            f"  🔭 Radar: {perf['last_radar_count']} монет за {perf['last_radar_time']:.1f}с\n"
            f"\n<b>Binance API (с последнего цикла):</b>\n"
            f"  Вызовов: {api['calls']} ({api['rpm']} req/мин)\n"
            f"  Ошибок: {api['errors']}\n"
            f"\n<b>Кэш klines:</b> {len(self.bot.binance._klines_cache)} записей\n"
            f"<b>Буфер сигналов:</b> {sum(len(s) for s in self.bot.engine._signals.values())} шт\n"
            f"<b>Потоки:</b> 8 (ThreadPool)\n"
            f"<b>Uptime:</b> {h}ч {m}м\n"
        )

    # ============ Price ============

    def cmd_price(self, args, chat_id) -> str:
        if not args:
            return "Использование: /price BTC (или /price BTC ETH SOL)"

        results = []
        for coin_name in args[:5]:  # макс 5 монет
            sym = coin_name.upper()
            # Ищем в universe
            found = None
            for c in self.bot._universe:
                if (c.get("symbol") or "").upper() == sym:
                    found = c
                    break

            if found:
                price = found.get("current_price", 0)
                change_24h = found.get("price_change_percentage_24h", 0) or 0
                mcap = found.get("market_cap", 0) or 0
                vol = found.get("total_volume", 0) or 0
                arrow = "🟢" if change_24h >= 0 else "🔴"
                results.append(
                    f"{arrow} <b>{sym}</b>: ${price:,.4f}\n"
                    f"   24ч: {change_24h:+.2f}% | MCap: ${mcap/1e6:.0f}M | Vol: ${vol/1e6:.0f}M"
                )
            else:
                results.append(f"⚪ <b>{sym}</b>: не найден в universe")

        return "\n".join(results)

    # ============ Digest ============

    def cmd_digest(self, args, chat_id) -> str:
        """Сводка алертов за последние N часов (по умолчанию 24)."""
        hours = 24
        if args:
            try:
                hours = int(args[0])
            except ValueError:
                return f"Не смог распарсить '{args[0]}' как число часов"

        cutoff = time.time() - hours * 3600
        recent = [x for x in self.sent_log if x[0] >= cutoff]

        if not recent:
            return f"📋 За последние {hours}ч алертов не было."

        by_tier = {"watch": 0, "signal": 0, "strong": 0}
        by_coin = {}
        by_hour = {}
        for ts, coin, tier, score in recent:
            by_tier[tier] = by_tier.get(tier, 0) + 1
            by_coin[coin] = by_coin.get(coin, 0) + 1
            h = time.strftime("%H:00", time.gmtime(ts))
            by_hour[h] = by_hour.get(h, 0) + 1

        top_coins = sorted(by_coin.items(), key=lambda x: -x[1])[:10]
        peak_hour = max(by_hour.items(), key=lambda x: x[1]) if by_hour else ("—", 0)
        avg_score = sum(x[3] for x in recent) / len(recent)

        result = (
            f"<b>📋 Дайджест за {hours}ч</b>\n"
            f"{'=' * 25}\n"
            f"\n<b>Итого:</b> {len(recent)} алертов\n"
            f"  👀 watch: {by_tier['watch']}\n"
            f"  ⚡ signal: {by_tier['signal']}\n"
            f"  🚨 strong: {by_tier['strong']}\n"
            f"\n<b>Средний score:</b> {avg_score:.1f}\n"
            f"<b>Пик активности:</b> {peak_hour[0]} UTC ({peak_hour[1]} алертов)\n"
            f"\n<b>Топ-{len(top_coins)} монет:</b>\n"
        )
        for i, (coin, count) in enumerate(top_coins, 1):
            result += f"  {i}. {coin} — {count} алертов\n"

        return result

    # ============ Gainers / Losers ============

    def cmd_gainers(self, args, chat_id) -> str:
        """Топ монет по росту/падению за 24ч."""
        n = 10
        if args:
            try:
                n = min(int(args[0]), 20)
            except ValueError:
                pass

        coins = [c for c in self.bot._universe
                 if c.get("price_change_percentage_24h") is not None]

        if not coins:
            return "Нет данных. Подождите обновления universe."

        # Топ по росту
        gainers = sorted(coins, key=lambda x: x.get("price_change_percentage_24h", 0), reverse=True)[:n]
        # Топ по падению
        losers = sorted(coins, key=lambda x: x.get("price_change_percentage_24h", 0))[:n]

        lines = [f"<b>🏆 Топ-{n} по росту за 24ч:</b>"]
        for i, c in enumerate(gainers, 1):
            sym = (c.get("symbol") or "").upper()
            pct = c.get("price_change_percentage_24h", 0)
            price = c.get("current_price", 0)
            lines.append(f"  {i}. 🟢 <b>{sym}</b> +{pct:.2f}% (${price:,.4f})")

        lines.append(f"\n<b>📉 Топ-{n} по падению:</b>")
        for i, c in enumerate(losers, 1):
            sym = (c.get("symbol") or "").upper()
            pct = c.get("price_change_percentage_24h", 0)
            price = c.get("current_price", 0)
            lines.append(f"  {i}. 🔴 <b>{sym}</b> {pct:.2f}% (${price:,.4f})")

        return "\n".join(lines)

    # ============ Utility ============

    def cmd_test(self, args, chat_id) -> str:
        self.bot.notifier.send(
            "🧪 <b>Тестовый алерт</b>\n"
            "———\n"
            "Если ты это видишь — всё работает."
        )
        return None

    def cmd_help(self, args, chat_id) -> str:
        return (
            f"<b>📋 Команды бота</b>\n"
            f"{'=' * 25}\n"
            "\n<b>Мониторинг:</b>\n"
            "/status — состояние бота\n"
            "/stats — статистика за 24ч\n"
            "/top — топ активных монет\n"
            "/config — все текущие настройки\n"
            "/perf — производительность\n"
            "/price &lt;COIN&gt; — текущая цена\n"
            "/gainers [N] — топ роста/падения 24ч\n"
            "/digest [часы] — сводка алертов\n"
            "\n<b>Управление алертами:</b>\n"
            "/pause /resume — пауза\n"
            "/minscore &lt;N&gt; — фильтр по score\n"
            "/mute &lt;COIN&gt; &lt;мин&gt; — замьютить\n"
            "/unmute &lt;COIN&gt; — размьютить\n"
            "\n<b>Настройки:</b>\n"
            "/threshold &lt;tier&gt; &lt;val&gt; — порог\n"
            "/cooldown &lt;tier&gt; &lt;мин&gt; — cooldown\n"
            "/detector — вкл/выкл детекторы\n"
            "/entry — точки входа и риск\n"
            "/blacklist — управление blacklist\n"
            "/quiet — тихие часы\n"
            "/interval &lt;сек&gt; — интервал скана\n"
            "\n/test — тестовый алерт"
        )

    def register_all(self, handler: CommandHandler) -> None:
        """Зарегистрировать все команды в обработчике."""
        handler.register("status", self.cmd_status)
        handler.register("stats", self.cmd_stats)
        handler.register("top", self.cmd_top)
        handler.register("pause", self.cmd_pause)
        handler.register("resume", self.cmd_resume)
        handler.register("threshold", self.cmd_threshold)
        handler.register("cooldown", self.cmd_cooldown)
        handler.register("minscore", self.cmd_minscore)
        handler.register("blacklist", self.cmd_blacklist)
        handler.register("detector", self.cmd_detector)
        handler.register("entry", self.cmd_entry)
        handler.register("quiet", self.cmd_quiet)
        handler.register("interval", self.cmd_interval)
        handler.register("config", self.cmd_config)
        handler.register("mute", self.cmd_mute)
        handler.register("unmute", self.cmd_unmute)
        handler.register("perf", self.cmd_perf)
        handler.register("price", self.cmd_price)
        handler.register("digest", self.cmd_digest)
        handler.register("gainers", self.cmd_gainers)
        handler.register("test", self.cmd_test)
        handler.register("help", self.cmd_help)
        handler.register("start", self.cmd_help)
