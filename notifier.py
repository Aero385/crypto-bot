"""Отправка алертов в Telegram через Bot API."""
import requests
import logging

log = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{bot_token}"

    def send(self, text: str) -> bool:
        """Отправить сообщение. Возвращает True при успехе."""
        try:
            r = requests.post(
                f"{self.api_url}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=10,
            )
            if r.status_code == 200:
                return True
            log.error("Telegram error %s: %s", r.status_code, r.text)
            return False
        except Exception as e:
            log.error("Telegram send failed: %s", e)
            return False

    def test_connection(self) -> bool:
        """Проверить что бот настроен правильно."""
        try:
            r = requests.get(f"{self.api_url}/getMe", timeout=10)
            if r.status_code == 200:
                name = r.json().get("result", {}).get("username", "?")
                log.info("Telegram bot OK: @%s", name)
                return True
            log.error("Bot token invalid: %s", r.text)
            return False
        except Exception as e:
            log.error("Cannot reach Telegram: %s", e)
            return False
