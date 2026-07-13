"""
Снапшоты состояния БД (issue B16/B17, Этап 3.5).

WAL растёт бесконечно, а узел, отставший дальше, чем хранится журнал, не может
догнать состояние по одним записям. Решение — периодический снапшот: полный
слепок таблиц данных на момент некоторого ``seq``. Имея снапшот, можно:

* обрезать WAL до ``seq`` снапшота (компакция) — старые записи уже учтены в нём;
* восстановить новый/сильно отставший узел: загрузить снапшот, затем догнать
  хвост WAL после ``seq`` (классический log-shipping: снапшот + журнал).

Снапшот хранится как JSON-дамп таблиц в таблице ``snapshots`` (одна строка id=1).
Формат прозрачен и легко передаётся между узлами по HTTP. Для учебного проекта
с небольшими данными этого достаточно; в проде взяли бы бинарный бэкап СУБД.
"""

import json
import logging
import time
from typing import Dict, List, Optional, Tuple

# Таблицы данных в порядке вставки (родители раньше детей — из-за внешних
# ключей). Восстановление удаляет в обратном порядке, вставляет в прямом.
_TABLES: List[Tuple[str, List[str]]] = [
    ("users", ["nick", "password", "created_at"]),
    ("rooms", ["name", "owner", "created_at"]),
    ("room_members", ["room", "nick", "joined_at"]),
    ("messages", ["msg_id", "room", "nick", "text", "ts", "client_msg_id"]),
]


class SnapshotManager:
    """Создание, чтение и восстановление снапшотов состояния БД."""

    def __init__(self, db_connection):
        self.db = db_connection

    async def create(self, seq: int) -> Dict:
        """
        Снимает слепок таблиц данных на момент ``seq`` и сохраняет его
        (перезаписывая предыдущий). Возвращает снапшот в виде dict.
        """
        tables: Dict[str, List[list]] = {}
        for name, columns in _TABLES:
            async with self.db.execute(
                f"SELECT {', '.join(columns)} FROM {name}"
            ) as cursor:
                rows = await cursor.fetchall()
                tables[name] = [list(row) for row in rows]

        snapshot = {"seq": seq, "tables": tables}
        await self.db.execute(
            "INSERT OR REPLACE INTO snapshots (id, seq, data, created_at) "
            "VALUES (1, ?, ?, ?)",
            (seq, json.dumps(tables), int(time.time())),
        )
        await self.db.commit()
        total = sum(len(r) for r in tables.values())
        logging.info(f"[Snapshot] Создан снапшот на seq={seq} ({total} строк)")
        return snapshot

    async def load_latest(self) -> Optional[Dict]:
        """Возвращает последний снапшот ``{seq, tables}`` или None."""
        async with self.db.execute(
            "SELECT seq, data FROM snapshots WHERE id = 1"
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return {"seq": int(row[0]), "tables": json.loads(row[1])}

    async def restore(self, snapshot: Dict):
        """
        Полностью заменяет таблицы данных содержимым снапшота. Выполняется одной
        транзакцией: сначала очищаем (дети → родители), затем вставляем
        (родители → дети), чтобы не нарушить внешние ключи.
        """
        tables = snapshot.get("tables", {})

        # Очистка в обратном порядке (сначала дети).
        for name, _ in reversed(_TABLES):
            await self.db.execute(f"DELETE FROM {name}")

        # Вставка в прямом порядке (сначала родители).
        for name, columns in _TABLES:
            rows = tables.get(name, [])
            if not rows:
                continue
            placeholders = ", ".join("?" for _ in columns)
            await self.db.executemany(
                f"INSERT INTO {name} ({', '.join(columns)}) VALUES ({placeholders})",
                [tuple(r) for r in rows],
            )

        await self.db.commit()
        total = sum(len(r) for r in tables.values())
        logging.info(
            f"[Snapshot] Восстановлено из снапшота seq={snapshot.get('seq')} "
            f"({total} строк)"
        )
