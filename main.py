"""
Crypto Alert Bot v2.
Поддерживает config.yaml (локально) и переменные окружения (Railway/Docker).
Запуск: python3 main.py
"""
import os, time, yaml, logging, signal as sys_signal, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from notifier import TelegramNotifier
from coingecko import CoinGeckoClient
from binance_client import BinanceClient
from liquidations import LiquidationTracker
from netflow import EtherscanNetflow, ERC20_CONTRACTS
from confluence import ConfluenceEngine
from detectors_v2 import (
    VolumeSpikeDetector, PriceMoveATRDetector, BreakoutDetector,
    OpenInterestDetector, FundingRateDetector, LiquidationsDetector,
    NetflowDetector, OrderbookDetector, ImpulseDetector,
)
from commands import CommandHandler, BotController
from journal import AlertJournal
from entry_signals import EntrySignalGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("bot")

def load_config(path="config.yaml"):
    env_token = os.getenv("TELEGRAM_BOT_TOKEN")
    env_chat = os.getenv("TELEGRAM_CHAT_ID")
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except FileNotFoundError:
        cfg = {}
    if env_token and env_chat:
        log.info("Конфиг: переменные окружения (Railway/Docker)")
        cfg.setdefault("telegram", {})
        cfg["telegram"]["bot_token"] = env_token
        cfg["telegram"]["chat_id"] = env_chat
        eth = os.getenv("ETHERSCAN_API_KEY")
        if eth:
            cfg.setdefault("detectors", {}).setdefault("netflow", {})["etherscan_api_key"] = eth
    else:
        log.info("Конфиг: %s", path)
    _defaults(cfg)
    return cfg

def _defaults(c):
    c.setdefault("scan_interval_seconds", 30)
    c.setdefault("universe_refresh_minutes", 60)
    # Обратная совместимость: если нет streams, создаём из filters
    if "streams" not in c:
        f = c.get("filters", {})
        c["streams"] = {
            "core": {
                "enabled": True, "top_n_coins": f.get("top_n_coins", 200),
                "min_market_cap_usd": f.get("min_market_cap_usd", 100000000),
                "min_volume_24h_usd": f.get("min_volume_24h_usd", 15000000),
                "min_age_days": f.get("min_age_days", 30),
                "scan_interval_seconds": 30, "label": "💎",
                "confluence_tiers": {"watch":2.0,"signal":4.0,"strong":6.5},
                "min_detector_count": {"watch":1,"signal":2,"strong":3},
            },
            "radar": {
                "enabled": True, "top_n_coins": 1500,
                "min_market_cap_usd": 5000000, "max_market_cap_usd": 200000000,
                "min_volume_24h_usd": 1000000, "min_age_days": 7,
                "detectors_enabled": ["volume_spike","price_move_atr","breakout"],
                "scan_interval_seconds": 120, "label": "🔭",
                "volume_spike_min_multiplier": 10, "price_move_min_atr": 3.0,
                "confluence_tiers": {"watch":3.0,"signal":5.0,"strong":8.0},
                "min_detector_count": {"watch":1,"signal":2,"strong":2},
                "entry_signals_enabled": False,
            },
        }
    c.setdefault("filters", {"blacklist":["USDT","USDC","DAI","TUSD","BUSD","FDUSD","USDD","PYUSD","USDE","USD1"],"whitelist":[]})
    d = c.setdefault("detectors", {})
    d.setdefault("volume_spike",{"enabled":True,"baseline_mode":"hour_of_week","baseline_days":14,"tiers":{"x5":1.0,"x7":2.0,"x10":3.5}})
    d.setdefault("price_move_atr",{"enabled":True,"atr_period_minutes":60,"atr_lookback_hours":24,"tiers":{"atr_1_5":1.0,"atr_2_0":2.0,"atr_3_0":3.5}})
    d.setdefault("breakout",{"enabled":True,"lookback_hours":[4,24,168],"require_volume_confirmation":True,"weights":{"4":1.0,"24":2.0,"168":3.5}})
    d.setdefault("orderbook",{"enabled":True,"depth_levels":100,"tiers":{"wall_500k":1.0,"wall_1m":2.0,"wall_3m":3.5},"imbalance":{"threshold_ratio":3.0,"weight":1.5}})
    d.setdefault("open_interest",{"enabled":True,"change_window_minutes":15,"tiers":{"pct_3":1.0,"pct_5":2.0,"pct_10":3.5}})
    d.setdefault("funding",{"enabled":True,"thresholds":{"extreme":0.1,"high":0.05},"weights":{"extreme":3.0,"high":1.5}})
    d.setdefault("liquidations",{"enabled":True,"aggregate_window_minutes":5,"tiers":{"usd_1m":1.0,"usd_5m":2.5,"usd_10m":3.5}})
    d.setdefault("netflow",{"enabled":True,"etherscan_api_key":"","min_transfer_usd":500000,"window_minutes":30,"weights":{"inflow_bearish":1.5,"outflow_bullish":2.0}})
    d.setdefault("impulse",{"enabled":True,"tiers":{"pct_5":2.0,"pct_8":3.0,"pct_15":4.0}})
    c.setdefault("confluence",{"window_minutes":15,"tiers":{"watch":2.0,"signal":4.0,"strong":6.5},"min_detector_count":{"watch":1,"signal":2,"strong":3}})
    c.setdefault("cooldown",{"watch_minutes":15,"signal_minutes":30,"strong_minutes":60})
    c.setdefault("quiet_hours",{"enabled":False,"start_hour_utc":22,"end_hour_utc":6,"override_for_strong":True})

def is_quiet_time(cfg, override=False):
    q = cfg.get("quiet_hours") or {}
    if not q.get("enabled") or override: return False
    hour = datetime.now(timezone.utc).hour
    s, e = q["start_hour_utc"], q["end_hour_utc"]
    return (s <= hour < e) if s < e else (hour >= s or hour < e)

class AlertBotV2:
    def __init__(self, config_path="config.yaml"):
        self.cfg = load_config(config_path)
        self._cfg_lock = threading.Lock()  # защита конфига от гонок
        tg = self.cfg["telegram"]
        self.notifier = TelegramNotifier(tg["bot_token"], tg["chat_id"])
        self.cg = CoinGeckoClient()
        self.binance = BinanceClient()
        self.liquidations = LiquidationTracker(window_minutes=self.cfg["detectors"]["liquidations"].get("aggregate_window_minutes",5))
        self.netflow_client = EtherscanNetflow(
            api_key=self.cfg["detectors"]["netflow"].get("etherscan_api_key",""),
            window_minutes=self.cfg["detectors"]["netflow"].get("window_minutes",30),
            min_transfer_usd=self.cfg["detectors"]["netflow"].get("min_transfer_usd",500000))
        self.engine = ConfluenceEngine(self.cfg)
        self.det_volume = VolumeSpikeDetector(self.cfg)
        self.det_price = PriceMoveATRDetector(self.cfg)
        self.det_breakout = BreakoutDetector(self.cfg)
        self.det_oi = OpenInterestDetector(self.cfg)
        self.det_funding = FundingRateDetector(self.cfg)
        self.det_liq = LiquidationsDetector(self.cfg)
        self.det_netflow = NetflowDetector(self.cfg)
        self.det_book = OrderbookDetector(self.cfg)
        self.det_impulse = ImpulseDetector(self.cfg)
        # Entry signals
        self.entry_gen = EntrySignalGenerator(self.cfg)
        # Два потока
        self._core_universe, self._radar_universe = [], []
        self._universe = []  # core + radar combined (для обратной совместимости)
        self._last_universe_update = 0
        self._last_radar_scan = 0
        self._last_netflow_check, self._cached_funding, self._last_funding_update = 0, {}, 0
        self._running, self._start_time = True, time.time()
        self.journal = AlertJournal()
        self.controller = BotController(self)
        self.commands = CommandHandler(tg["bot_token"], tg["chat_id"])
        self.controller.register_all(self.commands)
        # Метрики производительности
        self._perf = {"last_core_time": 0, "last_radar_time": 0,
                      "last_core_count": 0, "last_radar_count": 0,
                      "total_cycles": 0, "last_cycle_time": 0}
        # ThreadPool для параллельной обработки
        self._pool = ThreadPoolExecutor(max_workers=8)
        # Lock для engine (add_signal вызывается из разных потоков)
        self._engine_lock = threading.Lock()

    def _refresh_universe(self):
        interval = self.cfg["universe_refresh_minutes"] * 60
        if time.time() - self._last_universe_update < interval and self._core_universe: return

        streams = self.cfg.get("streams", {})
        bl = set(s.upper() for s in self.cfg.get("filters",{}).get("blacklist",[]))
        wl = set(s.upper() for s in self.cfg.get("filters",{}).get("whitelist",[]))

        # Загружаем максимальное кол-во монет (для обоих потоков)
        core_cfg = streams.get("core", {})
        radar_cfg = streams.get("radar", {})
        max_n = max(core_cfg.get("top_n_coins", 200), radar_cfg.get("top_n_coins", 1500))

        log.info("Загружаю топ-%d монет с CoinGecko...", max_n)
        all_coins = self.cg.get_top_markets(top_n=max_n)
        if not all_coins: log.warning("Нет данных CoinGecko"); return

        # Общая фильтрация (blacklist/whitelist)
        all_coins = [c for c in all_coins
            if (c.get("symbol") or "").upper() not in bl
            and (not wl or (c.get("symbol") or "").upper() in wl)]

        # Core stream
        if core_cfg.get("enabled", True):
            self._core_universe = [c for c in all_coins
                if (c.get("market_cap") or 0) >= core_cfg.get("min_market_cap_usd", 100000000)
                and (c.get("total_volume") or 0) >= core_cfg.get("min_volume_24h_usd", 15000000)]
        else:
            self._core_universe = []

        # Radar stream — монеты которых НЕТ в core
        core_ids = {c["id"] for c in self._core_universe}
        if radar_cfg.get("enabled", True):
            max_mcap = radar_cfg.get("max_market_cap_usd", 200000000)
            self._radar_universe = [c for c in all_coins
                if c["id"] not in core_ids
                and (c.get("market_cap") or 0) >= radar_cfg.get("min_market_cap_usd", 5000000)
                and (c.get("market_cap") or 0) <= max_mcap
                and (c.get("total_volume") or 0) >= radar_cfg.get("min_volume_24h_usd", 1000000)]
        else:
            self._radar_universe = []

        # Combined для обратной совместимости (commands, netflow)
        self._universe = self._core_universe + self._radar_universe
        self._last_universe_update = time.time()
        log.info("Universe: 💎 core=%d | 🔭 radar=%d | total=%d",
                len(self._core_universe), len(self._radar_universe), len(self._universe))

    def _refresh_funding(self):
        if time.time() - self._last_funding_update < 300: return
        data = self.binance.all_funding_rates()
        if data: self._cached_funding = data; self._last_funding_update = time.time()

    def _add_signal_safe(self, sig):
        """Thread-safe добавление сигнала в engine."""
        if sig:
            with self._engine_lock:
                self.engine.add_signal(sig)

    def _process_coin(self, coin_data):
        sym = coin_data["symbol"].upper()
        pair = self.binance.make_pair(sym)
        if pair not in self.binance.spot_symbols(): return
        kl = self.binance.klines(pair, "1h", 200)
        if not kl or len(kl) < 24: return
        for det in [self.det_volume, self.det_price, self.det_breakout]:
            if s := det.update(sym, kl): self._add_signal_safe(s)
        if pair in self.binance.futures_symbols():
            # Импульс-детектор: 5-мин свечи для быстрых PUMP/DUMP
            kl5 = self.binance.klines(pair, "5m", 12)  # последние 60 мин
            if kl5 and len(kl5) >= 6:
                if s := self.det_impulse.update(sym, kl5): self._add_signal_safe(s)
            oi = self.binance.open_interest_history(pair, "5m", 6)
            if oi and len(oi) >= 2:
                pc = ((kl[-1]["close"]-kl[-2]["close"])/kl[-2]["close"]*100) if kl[-2]["close"] else 0
                if s := self.det_oi.update(sym, oi[-1]["open_interest_value_usd"], pc): self._add_signal_safe(s)
            fr = self._cached_funding.get(pair)
            if fr is not None:
                if s := self.det_funding.update(sym, fr): self._add_signal_safe(s)
            ls = self.liquidations.get_stats(pair)
            if ls and ls["total_usd"] > 0:
                if s := self.det_liq.update(sym, ls): self._add_signal_safe(s)
        nf = self.netflow_client.get_stats(sym)
        if nf and nf["direction"] != "neutral":
            if s := self.det_netflow.update(sym, nf): self._add_signal_safe(s)

    def _process_batch(self, coins, processor, label, batch_size=10):
        """Обработка монет пачками через ThreadPool."""
        t0 = time.time()
        processed = 0
        # Разбиваем на пачки для контроля rate limit
        for i in range(0, len(coins), batch_size):
            if not self._running: break
            batch = coins[i:i+batch_size]
            futures = {self._pool.submit(processor, c): c for c in batch}
            for fut in as_completed(futures):
                try:
                    fut.result()
                    processed += 1
                except Exception as e:
                    log.debug("%s batch error: %s", label, e)
            # Пауза между пачками для rate limit
            if i + batch_size < len(coins):
                time.sleep(0.3)
        elapsed = time.time() - t0
        return processed, elapsed

    def _check_orderbook(self, top, n=30):
        for c in top[:n]:
            pair = self.binance.make_pair(c["symbol"].upper())
            if pair not in self.binance.spot_symbols(): continue
            book = self.binance.order_book(pair, 100)
            if book and (s := self.det_book.update(c["symbol"].upper(), book)): self._add_signal_safe(s)

    def _check_netflow(self):
        if time.time() - self._last_netflow_check < 300: return
        if not self.cfg["detectors"]["netflow"]["enabled"]: return
        usyms = {c["symbol"].upper(): c for c in self._universe}
        for sym, (contract, dec) in ERC20_CONTRACTS.items():
            if sym in usyms and (p := usyms[sym].get("current_price", 0)) > 0:
                self.netflow_client.check_token(sym, contract, dec, p)
        self._last_netflow_check = time.time()

    def _build_market_context(self, coin: str) -> str:
        """Собрать рыночный контекст для алерта: цена, 24h, funding."""
        lines = []

        # Цена и 24h изменение из CoinGecko
        cg_data = None
        for c in self._universe:
            if (c.get("symbol") or "").upper() == coin:
                cg_data = c
                break

        if cg_data:
            price = cg_data.get("current_price", 0)
            change_24h = cg_data.get("price_change_percentage_24h", 0) or 0
            high_24h = cg_data.get("high_24h", 0) or 0
            low_24h = cg_data.get("low_24h", 0) or 0
            mcap = cg_data.get("market_cap", 0) or 0
            arrow = "🟢" if change_24h >= 0 else "🔴"

            lines.append(f"{arrow} Цена: <b>${price:,.4f}</b> ({change_24h:+.2f}% 24ч)")
            if high_24h and low_24h:
                lines.append(f"📊 24ч: ${low_24h:,.4f} — ${high_24h:,.4f}")
            if mcap >= 1e9:
                lines.append(f"MCap: ${mcap/1e9:.1f}B")
            elif mcap >= 1e6:
                lines.append(f"MCap: ${mcap/1e6:.0f}M")

        # Funding rate
        pair = self.binance.make_pair(coin)
        fr = self._cached_funding.get(pair)
        if fr is not None:
            fr_emoji = "🟢" if abs(fr) < 0.05 else ("🔴" if abs(fr) >= 0.1 else "🟡")
            lines.append(f"Фандинг: {fr:+.4f}% {fr_emoji}")

        if not lines:
            return ""
        return "\n" + "\n".join(lines)

    def _dispatch_alerts(self):
        with self._engine_lock:
            alerts = self.engine.evaluate()
        if not alerts: return
        if self.controller.paused: return
        for a in alerts:
            if self.controller.is_muted(a.coin): continue
            if a.direction == "unclear":
                log.debug("Unclear %s, пропускаю", a.coin)
                continue
            if self.controller.min_score > 0 and a.score < self.controller.min_score:
                log.debug("Score %.1f < фильтр %.1f, пропускаю %s", a.score, self.controller.min_score, a.coin)
                continue
            override = a.tier == "strong" and self.cfg.get("quiet_hours",{}).get("override_for_strong")
            if is_quiet_time(self.cfg, override=override): continue
            # Добавляем рыночный контекст к алерту
            msg = a.message + self._build_market_context(a.coin)
            if self.notifier.send(msg):
                log.info("✓ %s [%s] score=%.1f", a.coin, a.tier, a.score)
                self.controller.log_alert(a.coin, a.tier, a.score)
                price = next((c.get("current_price",0) for c in self._universe if c["symbol"].upper()==a.coin), 0)
                self.journal.record(a.coin, a.tier, a.score, a.direction, price, a.signals)

                # --- Entry signal ---
                rm = self.cfg.get("risk_management", {})
                min_score = rm.get("min_score_for_entry", 5.0)
                min_det = rm.get("min_detectors_for_entry", 2)
                if not rm.get("entry_signals_enabled"):
                    pass
                elif a.score < min_score or len(a.signals) < min_det:
                    log.info("📍 %s: entry пропуск (score=%.1f/%.1f, det=%d/%d)",
                            a.coin, a.score, min_score, len(a.signals), min_det)
                else:
                    pair = self.binance.make_pair(a.coin)
                    # klines берутся из кэша — повторный запрос не уходит в API
                    klines = self.binance.klines(pair, "1h", 200)
                    if klines and len(klines) >= 200:
                        funding = self._cached_funding.get(pair)
                        liq_stats = self.liquidations.get_stats(pair)
                        entry = self.entry_gen.evaluate(
                            a.coin, klines, a.score, a.signals,
                            funding_rate=funding, liq_stats=liq_stats,
                        )
                        if entry:
                            self.notifier.send(entry.format_telegram())
                            log.info("📍 Entry: %s %s %s R:R=1:%.1f",
                                    a.coin, entry.direction, entry.setup_name, entry.risk_reward)
                        else:
                            log.info("📍 %s: entry проверен — сетап не найден", a.coin)
                    else:
                        log.info("📍 %s: мало свечей для entry (%d/200)",
                                a.coin, len(klines) if klines else 0)

    def _process_coin_radar(self, coin_data):
        """Облегчённая обработка для radar stream — только vol/price/breakout."""
        sym = coin_data["symbol"].upper()
        pair = self.binance.make_pair(sym)
        if pair not in self.binance.spot_symbols(): return
        kl = self.binance.klines(pair, "1h", 200)
        if not kl or len(kl) < 24: return
        # Только три базовых детектора
        if s := self.det_volume.update(sym, kl): self._add_signal_safe(s)
        if s := self.det_price.update(sym, kl): self._add_signal_safe(s)
        if s := self.det_breakout.update(sym, kl): self._add_signal_safe(s)

    def run(self):
        log.info("="*50); log.info("Crypto Alert Bot v2 — confluence scalping"); log.info("="*50)
        if not self.notifier.test_connection(): log.error("Telegram не работает"); return
        self.liquidations.start(); self.commands.start()
        log.info("Прогрев 10с..."); time.sleep(10)
        def shutdown(sig, frame): log.info("Stopping..."); self._running = False
        sys_signal.signal(sys_signal.SIGINT, shutdown)
        sys_signal.signal(sys_signal.SIGTERM, shutdown)

        streams = self.cfg.get("streams", {})
        core_cfg = streams.get("core", {})
        radar_cfg = streams.get("radar", {})
        core_interval = core_cfg.get("scan_interval_seconds", 30)
        radar_interval = radar_cfg.get("scan_interval_seconds", 120)

        rm = self.cfg.get("risk_management", {})
        entry_status = ("✅ вкл (score≥{}, R:R≥{})".format(
            rm.get("min_score_for_entry", 5.0),
            rm.get("min_risk_reward", 1.5),
        ) if rm.get("entry_signals_enabled") else "❌ выкл")

        self.notifier.send(
            "🤖 <b>Crypto Alert Bot v2 запущен</b>\n"
            f"Режим: <b>dual-stream confluence</b>\n"
            f"💎 Core: топ-{core_cfg.get('top_n_coins',200)} (mcap $100M+)\n"
            f"🔭 Radar: топ-{radar_cfg.get('top_n_coins',1500)} (mcap $5M-200M)\n"
            f"Окно: {self.cfg['confluence']['window_minutes']}мин\n"
            f"📍 Точки входа: {entry_status}\n"
            "⚡ Параллельная обработка: 8 потоков\n"
            "Алерты через ~30 мин | /help для команд")

        cycle = 0
        while self._running:
            cycle += 1
            cycle_start = time.time()
            try:
                self._refresh_universe(); self._refresh_funding()
                self.binance.reset_api_stats()

                # === CORE STREAM (каждый цикл) ===
                if self._core_universe:
                    core_top = sorted(self._core_universe, key=lambda x: x.get("total_volume",0), reverse=True)
                    if cycle % 4 == 0: self._check_orderbook(core_top, 30)
                    self._check_netflow()
                    log.info("💎 Core цикл %d: %d монет", cycle, len(core_top))
                    count, elapsed = self._process_batch(core_top, self._process_coin, "Core", batch_size=10)
                    self._perf["last_core_time"] = elapsed
                    self._perf["last_core_count"] = count
                    log.info("💎 Core: %d монет за %.1fс", count, elapsed)

                # === RADAR STREAM (реже — каждые radar_interval сек) ===
                radar_due = (time.time() - self._last_radar_scan) >= radar_interval
                if self._radar_universe and radar_due:
                    radar_top = sorted(self._radar_universe, key=lambda x: x.get("total_volume",0), reverse=True)
                    log.info("🔭 Radar скан: %d монет", len(radar_top))
                    count, elapsed = self._process_batch(radar_top, self._process_coin_radar, "Radar", batch_size=15)
                    self._perf["last_radar_time"] = elapsed
                    self._perf["last_radar_count"] = count
                    self._last_radar_scan = time.time()
                    log.info("🔭 Radar: %d монет за %.1fс", count, elapsed)

                self._dispatch_alerts()

                cycle_time = time.time() - cycle_start
                self._perf["total_cycles"] = cycle
                self._perf["last_cycle_time"] = cycle_time
                api = self.binance.get_api_stats()
                log.info("⏱ Цикл %d: %.1fс | API: %d вызовов (%d err)",
                        cycle, cycle_time, api["calls"], api["errors"])

            except Exception as e: log.exception("Ошибка: %s", e)

            # Адаптивный sleep: если цикл занял больше интервала, не спим
            sleep_time = max(1, core_interval - (time.time() - cycle_start))
            time.sleep(sleep_time)

        self._pool.shutdown(wait=False)
        self.liquidations.stop(); self.commands.stop(); log.info("Остановлен.")

if __name__ == "__main__":
    AlertBotV2().run()
