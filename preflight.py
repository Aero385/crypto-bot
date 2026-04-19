"""
Preflight-чекер. Запускай ПЕРЕД основным ботом чтобы убедиться что всё ок.

Проверяет:
  - config.yaml загружается
  - Telegram токен валиден, бот доступен
  - chat_id корректный (можем отправить сообщение)
  - Binance API доступен
  - CoinGecko API доступен
  - Etherscan ключ работает (опционально)
  - WebSocket для ликвидаций открывается

Запуск: python3 preflight.py
"""
import sys
import yaml
import logging
import time

logging.basicConfig(level=logging.WARNING)  # тихо, кроме ошибок

# ANSI цвета
G = "\033[92m"; R = "\033[91m"; Y = "\033[93m"; B = "\033[94m"; N = "\033[0m"


def check(label, fn):
    print(f"  {B}▸{N} {label}... ", end="", flush=True)
    try:
        result = fn()
        if result is True:
            print(f"{G}OK{N}")
            return True
        elif result is None:
            print(f"{Y}SKIP{N}")
            return True
        else:
            print(f"{R}FAIL{N}")
            if isinstance(result, str):
                print(f"    {R}→{N} {result}")
            return False
    except Exception as e:
        print(f"{R}ERROR{N}")
        print(f"    {R}→{N} {e}")
        return False


def main():
    print(f"\n{B}═══ Crypto Alert Bot — Preflight Check ═══{N}\n")

    # --- 1. Config ---
    print(f"{B}1. Конфигурация{N}")
    try:
        cfg = yaml.safe_load(open("config.yaml"))
    except FileNotFoundError:
        print(f"  {R}✗ config.yaml не найден{N}")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"  {R}✗ Синтаксическая ошибка в YAML: {e}{N}")
        sys.exit(1)
    print(f"  {G}✓{N} config.yaml загружен")

    # Проверка обязательных полей
    def cfg_filled(path):
        ptr = cfg
        for key in path.split("."):
            ptr = ptr.get(key, {}) if isinstance(ptr, dict) else None
            if ptr is None:
                return False
        if isinstance(ptr, str):
            return not (ptr.startswith("ВСТАВЬ") or ptr == "")
        return True

    check("telegram.bot_token заполнен",
          lambda: cfg_filled("telegram.bot_token") or
                  "Токен не заполнен в config.yaml")
    check("telegram.chat_id заполнен",
          lambda: cfg_filled("telegram.chat_id") or
                  "chat_id не заполнен")

    # --- 2. Telegram ---
    print(f"\n{B}2. Telegram{N}")
    from notifier import TelegramNotifier
    notifier = TelegramNotifier(cfg["telegram"]["bot_token"],
                                cfg["telegram"]["chat_id"])

    tg_ok = check("Токен бота валиден", notifier.test_connection)
    if tg_ok:
        check("Можем отправить сообщение в канал",
              lambda: notifier.send("🧪 Preflight check OK") or
                      "Не удалось — проверь что бот админ канала")

    # --- 3. Binance ---
    print(f"\n{B}3. Binance{N}")
    from binance_client import BinanceClient
    binance = BinanceClient()

    def check_spot():
        syms = binance.spot_symbols()
        if not syms:
            return "Нет ответа — возможно, твой регион заблокирован (VPN/прокси)"
        return True

    def check_futures():
        fut = binance.futures_symbols()
        return True if fut else "Нет доступа к фьючерсам"

    def check_klines():
        k = binance.klines("BTCUSDT", "1h", 5)
        return True if k and len(k) == 5 else "Не получили свечи BTC"

    def check_funding():
        f = binance.all_funding_rates()
        return True if f and "BTCUSDT" in f else "Funding не получен"

    check("Spot API", check_spot)
    check("Futures API", check_futures)
    check("Свечи (klines)", check_klines)
    check("Funding rate", check_funding)

    # --- 4. CoinGecko ---
    print(f"\n{B}4. CoinGecko{N}")
    from coingecko import CoinGeckoClient
    cg = CoinGeckoClient()

    def check_cg():
        top = cg.get_top_markets(top_n=10)
        if not top or len(top) < 5:
            return "CoinGecko не отвечает (возможно, 429 rate limit)"
        return True

    check("Получение топа монет", check_cg)

    # --- 5. Etherscan (опционально) ---
    print(f"\n{B}5. Etherscan (on-chain netflow){N}")
    eth_key = cfg["detectors"]["netflow"].get("etherscan_api_key", "")
    if eth_key.startswith("ВСТАВЬ") or not eth_key:
        print(f"  {Y}⊘ Пропущено: Etherscan ключ не задан. Netflow отключится.{N}")
        print(f"    Получить бесплатно: https://etherscan.io/myapikey")
    else:
        import requests
        def check_eth():
            r = requests.get("https://api.etherscan.io/api", params={
                "module": "stats", "action": "ethsupply", "apikey": eth_key,
            }, timeout=10)
            if r.status_code != 200:
                return f"HTTP {r.status_code}"
            data = r.json()
            if data.get("status") != "1":
                return data.get("message", "unknown error")
            return True
        check("Etherscan API ключ", check_eth)

    # --- 6. WebSocket ликвидаций ---
    print(f"\n{B}6. WebSocket ликвидаций (Binance Futures){N}")
    def check_ws():
        import websocket
        try:
            ws = websocket.create_connection(
                "wss://fstream.binance.com/ws/!forceOrder@arr", timeout=5)
            ws.close()
            return True
        except Exception as e:
            return f"{e}"
    check("Подключение к fstream.binance.com", check_ws)

    # --- 7. Зависимости ---
    print(f"\n{B}7. Python-зависимости{N}")
    for mod in ["requests", "yaml", "websocket", "sqlite3"]:
        check(f"import {mod}", lambda m=mod: __import__(m) and True)

    # --- Итог ---
    print(f"\n{B}═══ Готово ═══{N}")
    print(f"Если все ключевые пункты {G}OK{N} — можно запускать {B}python3 main.py{N}\n")


if __name__ == "__main__":
    main()
