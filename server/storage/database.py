"""База данных SQLite."""

import aiosqlite
from contextlib import asynccontextmanager
from pathlib import Path


class Database:
    """Асинхронная обёртка над SQLite."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def connect(self):
        """Подключение к БД и создание таблиц."""
        # Создаём директорию для БД если не существует
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row

        # Включаем WAL режим для лучшей производительности
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA foreign_keys=ON")

        await self._create_tables()
        await self._db.commit()

    async def _create_tables(self):
        """Создание таблиц схемы."""
        # Пользователи
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                nick        TEXT PRIMARY KEY,
                password    TEXT,
                created_at  INTEGER NOT NULL
            )
        """)

        # Комнаты
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS rooms (
                name        TEXT PRIMARY KEY,
                owner       TEXT NOT NULL,
                created_at  INTEGER NOT NULL
            )
        """)

        # Участники комнат
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS room_members (
                room        TEXT NOT NULL,
                nick        TEXT NOT NULL,
                joined_at   INTEGER NOT NULL,
                PRIMARY KEY (room, nick),
                FOREIGN KEY (room) REFERENCES rooms(name),
                FOREIGN KEY (nick) REFERENCES users(nick)
            )
        """)

        # Сообщения
        # client_msg_id — идентификатор от клиента для идемпотентности (дедуп ретраев)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                msg_id        TEXT PRIMARY KEY,
                room          TEXT NOT NULL,
                nick          TEXT NOT NULL,
                text          TEXT NOT NULL,
                ts            INTEGER NOT NULL,
                client_msg_id TEXT,
                FOREIGN KEY (room) REFERENCES rooms(name),
                FOREIGN KEY (nick) REFERENCES users(nick)
            )
        """)

        # Миграция для БД, созданных до появления колонки client_msg_id
        try:
            await self._db.execute(
                "ALTER TABLE messages ADD COLUMN client_msg_id TEXT"
            )
        except aiosqlite.OperationalError:
            pass  # колонка уже существует

        # Идемпотентность: один (автор, client_msg_id) = одно сообщение
        await self._db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_client_msg_id "
            "ON messages(nick, client_msg_id) WHERE client_msg_id IS NOT NULL"
        )

        # WAL журнал (для репликации в Фазе 3)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS wal (
                seq         INTEGER PRIMARY KEY AUTOINCREMENT,
                ts          INTEGER NOT NULL,
                operation   TEXT NOT NULL,
                table_name  TEXT NOT NULL,
                data        TEXT NOT NULL
            )
        """)

        # Метаданные кластера
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS cluster_meta (
                key         TEXT PRIMARY KEY,
                value       TEXT NOT NULL
            )
        """)

        # Снапшот состояния (Этап 3.5): одна строка (id=1) с JSON-дампом таблиц
        # данных на момент seq. Нужен для компакции WAL и восстановления узлов,
        # отставших дальше, чем хранится журнал.
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id          INTEGER PRIMARY KEY CHECK (id = 1),
                seq         INTEGER NOT NULL,
                data        TEXT NOT NULL,
                created_at  INTEGER NOT NULL
            )
        """)

        # Создаём комнату #general по умолчанию
        await self._db.execute("""
            INSERT OR IGNORE INTO rooms (name, owner, created_at)
            VALUES ('#general', 'system', 0)
        """)

    async def close(self):
        """Закрытие соединения."""
        if self._db:
            await self._db.close()

    @property
    def connection(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._db

    async def execute(self, query: str, params: tuple = ()):
        """Выполнение запроса без возврата результатов."""
        async with self._db.execute(query, params) as cursor:
            return cursor

    async def fetchone(self, query: str, params: tuple = ()):
        """Выполнение запроса и возврат одной строки."""
        async with self._db.execute(query, params) as cursor:
            return await cursor.fetchone()

    async def fetchall(self, query: str, params: tuple = ()):
        """Выполнение запроса и возврат всех строк."""
        async with self._db.execute(query, params) as cursor:
            return await cursor.fetchall()

    async def commit(self):
        """Коммит транзакции."""
        await self._db.commit()

    @asynccontextmanager
    async def transaction(self):
        """
        Транзакция: все запросы внутри блока применяются атомарно.

        При успехе — commit, при любой ошибке — rollback (никакого
        «полу-применённого» состояния). Пример использования:

            async with db.transaction():
                await db.execute("DELETE FROM room_members WHERE room = ?", (room,))
                await db.execute("DELETE FROM rooms WHERE name = ?", (room,))
        """
        try:
            yield self._db
            await self._db.commit()
        except Exception:
            await self._db.rollback()
            raise
