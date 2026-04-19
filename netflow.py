"""
On-chain netflow модуль.

Честное описание возможностей:
- Работает только для ETH и ERC-20 токенов (через Etherscan API)
- Использует хардкоженный список адресов горячих кошельков бирж (известные адреса)
- Бесплатный Etherscan API: 5 запросов/сек, 100k запросов/день
- НЕ заменяет Glassnode/CryptoQuant — даёт ~20% от той же информации

Для полного покрытия (Bitcoin, Solana, Tron, все биржи) нужны платные источники.
"""
import requests
import logging
import time
from typing import Optional, Dict, List
from indicators import RollingBuffer

log = logging.getLogger(__name__)

# Известные адреса горячих кошельков бирж.
# Источник: публичные labels Etherscan + самые крупные адреса из Nansen.
# Это НЕ полный список — биржи используют сотни адресов. Но топ-кошельки
# покрывают 70-80% потоков.
EXCHANGE_WALLETS = {
    # Binance
    "0x28c6c06298d514db089934071355e5743bf21d60": "Binance 14",
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549": "Binance 15",
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": "Binance 16",
    "0x56eddb7aa87536c09ccc2793473599fd21a8b17f": "Binance 17",
    "0x9696f59e4d72e237be84ffd425dcad154bf96976": "Binance 18",
    "0x4d9ff50ef4da947364bb9650892b2554e7be5e2b": "Binance 19",
    "0x3c783c21a0383057d128bae431894a5c19f9cf06": "Binance 20",
    # Coinbase
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "Coinbase 1",
    "0x503828976d22510aad0201ac7ec88293211d23da": "Coinbase 2",
    "0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740": "Coinbase 3",
    "0x3cd751e6b0078be393132286c442345e5dc49699": "Coinbase 4",
    # Kraken
    "0x2910543af39aba0cd09dbb2d50200b3e800a63d2": "Kraken 1",
    "0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13": "Kraken 2",
    # OKX
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "OKX 1",
    "0x236f9f97e0e62388479bf9e5ba4889e46b0273c3": "OKX 2",
    # Bybit
    "0xf89d7b9c864f589bbf53a82105107622b35eaa40": "Bybit 1",
}


class EtherscanNetflow:
    """Отслеживает крупные переводы ERC-20 токенов на/с известных кошельков бирж."""

    def __init__(self, api_key: str, window_minutes: int = 30,
                 min_transfer_usd: float = 500000):
        self.api_key = api_key
        self.window_sec = window_minutes * 60
        self.min_usd = min_transfer_usd
        # symbol → RollingBuffer
        # Каждая запись: (usd_value, direction)
        # direction: +1 = отток с биржи (bullish), -1 = приток на биржу (bearish)
        self._flows: Dict[str, RollingBuffer] = {}
        self._last_checked: Dict[str, int] = {}  # contract → last_block
        self.session = requests.Session()

    def _has_valid_key(self) -> bool:
        return self.api_key and not self.api_key.startswith("ВСТАВЬ")

    def check_token(self, symbol: str, contract_address: str,
                    decimals: int, price_usd: float) -> None:
        """
        Проверить свежие переводы токена на/с адресов бирж.
        Вызывать периодически (раз в несколько минут) для каждого интересного токена.
        """
        if not self._has_valid_key() or price_usd <= 0:
            return

        try:
            # Получаем последние 100 транзакций контракта
            r = self.session.get("https://api.etherscan.io/api", params={
                "module": "account",
                "action": "tokentx",
                "contractaddress": contract_address,
                "page": 1,
                "offset": 100,
                "sort": "desc",
                "apikey": self.api_key,
            }, timeout=15)

            if r.status_code != 200:
                return
            data = r.json()
            if data.get("status") != "1":
                return

            buffer = self._flows.setdefault(symbol, RollingBuffer(self.window_sec))
            last_block = self._last_checked.get(contract_address, 0)
            max_block = last_block

            for tx in data.get("result", []):
                block = int(tx["blockNumber"])
                if block <= last_block:
                    continue
                max_block = max(max_block, block)

                from_addr = tx["from"].lower()
                to_addr = tx["to"].lower()
                amount = float(tx["value"]) / (10 ** decimals)
                usd = amount * price_usd

                if usd < self.min_usd:
                    continue

                ts = int(tx["timeStamp"])

                # Направление
                if to_addr in EXCHANGE_WALLETS:
                    # Перевод НА биржу → возможная продажа (bearish)
                    buffer.add((usd, -1), timestamp=ts)
                    log.info("📥 %s: $%s → биржа (%s)", symbol, f"{usd:,.0f}",
                             EXCHANGE_WALLETS[to_addr])
                elif from_addr in EXCHANGE_WALLETS:
                    # Вывод С биржи → накопление (bullish)
                    buffer.add((usd, +1), timestamp=ts)
                    log.info("📤 %s: $%s с биржи (%s)", symbol, f"{usd:,.0f}",
                             EXCHANGE_WALLETS[from_addr])

            self._last_checked[contract_address] = max_block

        except Exception as e:
            log.debug("Netflow check error for %s: %s", symbol, e)

    def get_stats(self, symbol: str) -> Dict:
        """
        Агрегаты за окно:
          {inflow_usd, outflow_usd, net_usd, direction}
          direction: 'bullish' (отток преобладает) / 'bearish' / 'neutral'
        """
        buffer = self._flows.get(symbol)
        if not buffer:
            return {"inflow_usd": 0, "outflow_usd": 0, "net_usd": 0,
                    "direction": "neutral"}

        inflow = 0.0   # на биржу
        outflow = 0.0  # с биржи
        for usd, direction in buffer.values():
            if direction == -1:
                inflow += usd
            else:
                outflow += usd

        net = outflow - inflow  # положительное = отток (bullish)

        if abs(net) < self.min_usd:
            dir_label = "neutral"
        elif net > 0:
            dir_label = "bullish"
        else:
            dir_label = "bearish"

        return {
            "inflow_usd": inflow,
            "outflow_usd": outflow,
            "net_usd": net,
            "direction": dir_label,
        }


# Справочник контрактов топовых ERC-20 токенов.
# Чтобы сканировать больше — дополни этот словарь.
# Можно вытащить автоматически из CoinGecko (у них есть поле `platforms`).
ERC20_CONTRACTS = {
    "USDT": ("0xdac17f958d2ee523a2206206994597c13d831ec7", 6),
    "USDC": ("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", 6),
    "LINK": ("0x514910771af9ca656af840dff83e8264ecf986ca", 18),
    "UNI":  ("0x1f9840a85d5af5bf1d1762f925bdaddc4201f984", 18),
    "SHIB": ("0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce", 18),
    "MATIC":("0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0", 18),
    "APE":  ("0x4d224452801aced8b2f0aebe155379bb5d594381", 18),
    "PEPE": ("0x6982508145454ce325ddbe47a25d4ec3d2311933", 18),
    "FLOKI":("0xcf0c122c6b73ff809c693db761e7baebe62b6a2e", 9),
    # дополни при необходимости — контракт ищешь на Etherscan или CoinGecko
}
