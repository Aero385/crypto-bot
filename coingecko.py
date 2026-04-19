"""Загрузка данных с CoinGecko API (бесплатный tier, без ключа)."""
import requests
import logging
import time
from typing import List, Dict
from datetime import datetime, timedelta, timezone

log = logging.getLogger(__name__)

BASE_URL = "https://api.coingecko.com/api/v3"


class CoinGeckoClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _get(self, path: str, params: dict = None, retries: int = 3):
        """GET-запрос с ретраями и бэкоффом (CG часто 429-ит на free tier)."""
        for attempt in range(retries):
            try:
                r = self.session.get(f"{BASE_URL}{path}", params=params, timeout=20)
                if r.status_code == 429:
                    wait = 30 * (attempt + 1)
                    log.warning("CoinGecko rate limit, waiting %ss", wait)
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return r.json()
            except Exception as e:
                log.error("CG request failed (attempt %d): %s", attempt + 1, e)
                time.sleep(5)
        return None

    def get_top_markets(self, top_n: int = 500) -> List[Dict]:
        """
        Топ монет по маркеткапу со всей рыночной инфой.
        Возвращает: id, symbol, current_price, market_cap, total_volume,
                    price_change_percentage_1h/24h, ath_date и т.д.
        """
        all_coins = []
        per_page = 250  # максимум CG
        pages = (top_n + per_page - 1) // per_page

        for page in range(1, pages + 1):
            data = self._get("/coins/markets", {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": per_page,
                "page": page,
                "price_change_percentage": "1h,24h,7d",
            })
            if not data:
                break
            all_coins.extend(data)
            time.sleep(2)  # вежливая пауза между страницами

        return all_coins[:top_n]

    def get_recently_added(self, days: int = 7) -> List[Dict]:
        """
        Монеты, добавленные на CoinGecko за последние N дней.
        Используется для детекта новых листингов.
        """
        data = self._get("/coins/list/new")
        if not data:
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        fresh = []
        for coin in data:
            activated = coin.get("activated_at")
            if not activated:
                continue
            # activated_at — unix timestamp
            coin_date = datetime.fromtimestamp(activated, tz=timezone.utc)
            if coin_date >= cutoff:
                fresh.append(coin)
        return fresh
