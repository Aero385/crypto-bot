"""
WebSocket-слушатель ликвидаций Binance Futures.
Публичный стрим !forceOrder@arr — все ликвидации в реальном времени.
Работает в фоновом потоке, накапливая агрегаты по каждой монете.
"""
import json
import logging
import threading
import time
from collections import defaultdict
from typing import Dict

import websocket  # pip install websocket-client

from indicators import RollingBuffer

log = logging.getLogger(__name__)

WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"


class LiquidationTracker:
    """
    Слушает ликвидации Binance Futures в фоне.
    Для каждой монеты хранит rolling-буфер последних N минут.
    """

    def __init__(self, window_minutes: int = 5):
        self.window_sec = window_minutes * 60
        # symbol → RollingBuffer[(usd_value, side)]
        # side: 'long' (ликвидирован лонг) или 'short'
        self._buffers: Dict[str, RollingBuffer] = defaultdict(
            lambda: RollingBuffer(self.window_sec)
        )
        self._lock = threading.Lock()
        self._stop = False
        self._thread = None

    def _on_message(self, ws, message: str):
        try:
            data = json.loads(message)
            order = data.get("o", {})
            symbol = order.get("s")
            if not symbol or not symbol.endswith("USDT"):
                return

            price = float(order.get("ap", 0))       # avg price
            qty = float(order.get("q", 0))          # filled qty
            side = order.get("S", "")               # SELL → лонг ликвидирован

            usd = price * qty
            if usd < 100:  # микропозиции — игнор
                return

            # SELL в forceOrder = ликвидация ЛОНГА (закрывается продажей)
            liq_side = "long" if side == "SELL" else "short"

            with self._lock:
                self._buffers[symbol].add((usd, liq_side))

        except Exception as e:
            log.debug("Liquidation parse error: %s", e)

    def _on_error(self, ws, error):
        log.warning("Liquidation WS error: %s", error)

    def _on_close(self, ws, code, msg):
        log.info("Liquidation WS closed: %s %s", code, msg)

    def _run_forever(self):
        """Запускает WS с авто-переподключением."""
        while not self._stop:
            try:
                ws = websocket.WebSocketApp(
                    WS_URL,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                log.error("Liquidation WS crashed: %s", e)
            if not self._stop:
                time.sleep(5)  # пауза перед переподключением

    def start(self):
        """Запустить слушатель в фоновом потоке."""
        if self._thread and self._thread.is_alive():
            return
        self._stop = False
        self._thread = threading.Thread(target=self._run_forever, daemon=True)
        self._thread.start()
        log.info("Liquidation tracker started")

    def stop(self):
        self._stop = True

    # -------- Запросы агрегатов --------
    def get_stats(self, symbol: str) -> Dict:
        """
        Вернуть агрегаты за окно:
          {total_usd, long_usd, short_usd, count, imbalance}
        """
        with self._lock:
            items = self._buffers.get(symbol)
            if not items:
                return {"total_usd": 0, "long_usd": 0, "short_usd": 0,
                        "count": 0, "imbalance": 0}
            values = items.values()

        long_usd = sum(v for v, s in values if s == "long")
        short_usd = sum(v for v, s in values if s == "short")
        total = long_usd + short_usd

        # imbalance: +1 = все ликвидации шортов, -1 = все ликвидации лонгов
        if total > 0:
            imbalance = (short_usd - long_usd) / total
        else:
            imbalance = 0

        return {
            "total_usd": total,
            "long_usd": long_usd,
            "short_usd": short_usd,
            "count": len(values),
            "imbalance": imbalance,
        }
