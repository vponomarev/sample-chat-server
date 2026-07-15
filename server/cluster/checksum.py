"""
Сверка целостности реплик по контрольным суммам (issue B18, Этап 4.4).

Репликация может «молча разойтись»: из-за бага применения WAL, ручного
вмешательства в БД реплики или потерянной записи данные на реплике перестают
совпадать с master, хотя оба узла живы и seq выглядит правдоподобно. Обычные
метрики (lag) этого не ловят — lag=0 при разошедшемся содержимом.

Идея сверки: каждый узел детерминированно хеширует свои таблицы данных
(sha256 по строкам в порядке первичного ключа) и отдаёт суммы по HTTP. Master
опрашивает реплики и сравнивает. Важное различение:

  * реплика ОТСТАЁТ (её seq < seq master) — суммы законно различаются, это не
    порча, узел ещё догоняет журнал;
  * реплика РАЗОШЛАСЬ (seq >= seq master, но содержимое отличается) — это уже
    настоящее расхождение, требующее внимания (пересинхронизация из снапшота).

Детерминизм важнее скорости: строки сортируются по ключу, а не по rowid/порядку
вставки, чтобы одинаковые данные давали одинаковый хеш независимо от истории.
Для учебного проекта с малыми данными полный пересчёт дёшев; в проде считали бы
инкрементально (например, дерево Меркла по диапазонам ключей).
"""

import hashlib
import json
import logging
from typing import Dict, List, Tuple

# Таблицы данных и колонки — те же, что в снапшотах (Этап 3.5), плюс порядок
# сортировки по первичному ключу для детерминированного хеша.
_TABLES: List[Tuple[str, List[str], str]] = [
    ("users", ["nick", "password", "created_at"], "nick"),
    ("rooms", ["name", "owner", "created_at"], "name"),
    ("room_members", ["room", "nick", "joined_at"], "room, nick"),
    ("messages", ["msg_id", "room", "nick", "text", "ts", "client_msg_id"], "msg_id"),
]


class ChecksumManager:
    """Считает детерминированные контрольные суммы таблиц данных."""

    def __init__(self, db_connection):
        self.db = db_connection

    async def compute(self) -> Dict:
        """
        Возвращает ``{"tables": {имя: sha256hex}, "overall": sha256hex,
        "counts": {имя: n}}`` по текущему состоянию БД.
        """
        table_hashes: Dict[str, str] = {}
        counts: Dict[str, int] = {}

        for name, columns, order_by in _TABLES:
            hasher = hashlib.sha256()
            count = 0
            async with self.db.execute(
                f"SELECT {', '.join(columns)} FROM {name} ORDER BY {order_by}"
            ) as cursor:
                async for row in cursor:
                    # Каноничная сериализация строки: компактный JSON без пробелов,
                    # с сохранением Unicode. Каждая строка завершается \n, чтобы
                    # границы строк не «слипались» в хеше.
                    line = json.dumps(
                        list(row), ensure_ascii=False, separators=(",", ":")
                    )
                    hasher.update(line.encode("utf-8"))
                    hasher.update(b"\n")
                    count += 1
            table_hashes[name] = hasher.hexdigest()
            counts[name] = count

        # Общий хеш — по строкам "имя:хеш" в фиксированном порядке таблиц.
        overall = hashlib.sha256()
        for name, _, _ in _TABLES:
            overall.update(f"{name}:{table_hashes[name]}\n".encode("utf-8"))

        return {
            "tables": table_hashes,
            "overall": overall.hexdigest(),
            "counts": counts,
        }

    @staticmethod
    def diverging_tables(a: Dict, b: Dict) -> List[str]:
        """Имена таблиц, чьи суммы в двух отчётах различаются."""
        ta = a.get("tables", {})
        tb = b.get("tables", {})
        names = set(ta) | set(tb)
        return sorted(n for n in names if ta.get(n) != tb.get(n))
