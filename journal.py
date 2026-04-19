"""
Журнал алертов в SQLite.

Зачем: через неделю-две у тебя накопится много алертов. Чтобы не гадать
"какие пороги лучше", нужно смотреть, какие алерты были правильными
(цена пошла в предсказанном направлении), а какие ложными.

Таблица alerts хранит всё. Таблица outcomes заполняется постфактум —
раз в час проверяем цену через N часов после алерта и записываем результат.
"""
import sqlite3
import logging
import time
import json
from pathlib import Path
from typing import List, Dict, Optional
from contextlib import contextmanager

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,              -- unix timestamp
    coin TEXT NOT NULL,
    tier TEXT NOT NULL,               -- watch / signal / strong
    score REAL NOT NULL,
    direction TEXT,                   -- bullish / bearish / mixed / unclear
    price REAL,
    signals_json TEXT,                -- список сработавших детекторов
    checked INTEGER DEFAULT 0         -- проверили ли outcome
);

CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(ts);
CREATE INDEX IF NOT EXISTS idx_alerts_coin ON alerts(coin);
CREATE INDEX IF NOT EXISTS idx_alerts_checked ON alerts(checked);

CREATE TABLE IF NOT EXISTS outcomes (
    alert_id INTEGER PRIMARY KEY,
    price_1h REAL,
    price_4h REAL,
    price_24h REAL,
    pct_1h REAL,
    pct_4h REAL,
    pct_24h REAL,
    verdict TEXT,                     -- 'correct' / 'wrong' / 'neutral'
    FOREIGN KEY (alert_id) REFERENCES alerts(id)
);
"""


class AlertJournal:
    def __init__(self, db_path: str = "alerts.db"):
        self.db_path = db_path
        self._init_db()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._conn() as c:
            c.executescript(SCHEMA)
        log.info("Alert journal: %s", Path(self.db_path).resolve())

    # -------- Запись --------
    def record(self, coin: str, tier: str, score: float,
               direction: str, price: float, signals: list) -> int:
        """Записать отправленный алерт. Возвращает ID."""
        signals_data = [
            {"detector": s.detector, "weight": s.weight, "label": s.label}
            for s in signals
        ]
        with self._conn() as c:
            cur = c.execute("""
                INSERT INTO alerts (ts, coin, tier, score, direction, price, signals_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (int(time.time()), coin, tier, score, direction, price,
                  json.dumps(signals_data, ensure_ascii=False)))
            return cur.lastrowid

    def record_outcome(self, alert_id: int, price_1h: float,
                       price_4h: float, price_24h: float,
                       initial_price: float, direction: str) -> None:
        """Записать фактический исход через 1/4/24 часа."""
        pct_1h = ((price_1h - initial_price) / initial_price * 100
                  if initial_price > 0 else 0)
        pct_4h = ((price_4h - initial_price) / initial_price * 100
                  if initial_price > 0 else 0)
        pct_24h = ((price_24h - initial_price) / initial_price * 100
                   if initial_price > 0 else 0)

        # Вердикт по движению через 4ч (основной горизонт для скальпинга)
        verdict = self._verdict(direction, pct_4h)

        with self._conn() as c:
            c.execute("""
                INSERT OR REPLACE INTO outcomes
                (alert_id, price_1h, price_4h, price_24h, pct_1h, pct_4h, pct_24h, verdict)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (alert_id, price_1h, price_4h, price_24h,
                  pct_1h, pct_4h, pct_24h, verdict))
            c.execute("UPDATE alerts SET checked = 1 WHERE id = ?", (alert_id,))

    def _verdict(self, direction: str, pct_move: float) -> str:
        """Оценка: сбылся ли сигнал."""
        # Порог: движение должно быть не меньше 0.5% чтобы считаться значимым
        if abs(pct_move) < 0.5:
            return "neutral"

        if direction == "bullish":
            return "correct" if pct_move > 0.5 else "wrong"
        if direction == "bearish":
            return "correct" if pct_move < -0.5 else "wrong"
        return "neutral"  # для mixed/unclear не оцениваем

    # -------- Чтение / запросы --------
    def unchecked_alerts_older_than(self, hours: float) -> List[Dict]:
        """Алерты, для которых пора проверить исход (прошло >hours с отправки)."""
        cutoff = int(time.time() - hours * 3600)
        with self._conn() as c:
            rows = c.execute("""
                SELECT * FROM alerts
                WHERE checked = 0 AND ts <= ? AND direction IN ('bullish','bearish')
                ORDER BY ts ASC
                LIMIT 100
            """, (cutoff,)).fetchall()
            return [dict(r) for r in rows]

    def performance_by_tier(self, days: int = 7) -> Dict:
        """Сбывшиеся / ложные алерты по уровням за N дней."""
        cutoff = int(time.time() - days * 86400)
        with self._conn() as c:
            rows = c.execute("""
                SELECT a.tier, o.verdict, COUNT(*) as n
                FROM alerts a
                JOIN outcomes o ON a.id = o.alert_id
                WHERE a.ts >= ?
                GROUP BY a.tier, o.verdict
            """, (cutoff,)).fetchall()

        result = {"watch": {}, "signal": {}, "strong": {}}
        for r in rows:
            result[r["tier"]][r["verdict"]] = r["n"]
        return result

    def performance_by_detector(self, days: int = 7) -> Dict:
        """
        Какие детекторы дают правильные сигналы чаще других.
        Помогает понять что работает, а что шумит.
        """
        cutoff = int(time.time() - days * 86400)
        # Нужно распарсить signals_json для каждого алерта и сопоставить с verdict
        with self._conn() as c:
            rows = c.execute("""
                SELECT a.signals_json, o.verdict
                FROM alerts a
                JOIN outcomes o ON a.id = o.alert_id
                WHERE a.ts >= ?
            """, (cutoff,)).fetchall()

        # detector → {correct: n, wrong: n, neutral: n}
        stats = {}
        for r in rows:
            try:
                sigs = json.loads(r["signals_json"])
            except Exception:
                continue
            for s in sigs:
                d = s.get("detector", "unknown")
                if d not in stats:
                    stats[d] = {"correct": 0, "wrong": 0, "neutral": 0}
                stats[d][r["verdict"]] = stats[d].get(r["verdict"], 0) + 1

        # Добавляем процент точности
        for d, v in stats.items():
            total_graded = v["correct"] + v["wrong"]
            v["accuracy"] = (v["correct"] / total_graded * 100
                             if total_graded > 0 else None)
            v["total"] = v["correct"] + v["wrong"] + v["neutral"]
        return stats
