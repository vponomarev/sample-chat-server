"""
Тесты базы данных (storage/database.py).
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from storage.database import Database


class TestDatabase:
    """Тесты Database."""

    @pytest.mark.asyncio
    async def test_connect_creates_tables(self, temp_db_path):
        """Проверка создания таблиц при подключении."""
        db = Database(temp_db_path)
        await db.connect()
        
        # Проверка существования таблиц
        tables = await db.fetchall(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        table_names = [t["name"] for t in tables]
        
        assert "users" in table_names
        assert "rooms" in table_names
        assert "messages" in table_names
        assert "room_members" in table_names
        assert "wal" in table_names
        assert "cluster_meta" in table_names
        
        await db.close()

    @pytest.mark.asyncio
    async def test_default_general_room(self, temp_db_path):
        """Проверка создания комнаты #general по умолчанию."""
        db = Database(temp_db_path)
        await db.connect()
        
        room = await db.fetchone(
            "SELECT name FROM rooms WHERE name = '#general'"
        )
        
        assert room is not None
        assert room["name"] == "#general"
        
        await db.close()

    @pytest.mark.asyncio
    async def test_insert_user(self, database):
        """Тест вставки пользователя."""
        import time
        
        await database.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("testuser", "hash123", int(time.time()))
        )
        await database.commit()
        
        user = await database.fetchone(
            "SELECT nick, password FROM users WHERE nick = 'testuser'"
        )
        
        assert user is not None
        assert user["nick"] == "testuser"
        assert user["password"] == "hash123"

    @pytest.mark.asyncio
    async def test_insert_room(self, database):
        """Тест вставки комнаты."""
        import time
        
        await database.execute(
            "INSERT INTO rooms (name, owner, created_at) VALUES (?, ?, ?)",
            ("#test-room", "owner", int(time.time()))
        )
        await database.commit()
        
        room = await database.fetchone(
            "SELECT name, owner FROM rooms WHERE name = '#test-room'"
        )
        
        assert room is not None
        assert room["name"] == "#test-room"
        assert room["owner"] == "owner"

    @pytest.mark.asyncio
    async def test_insert_message(self, database):
        """Тест вставки сообщения."""
        import time
        
        # Сначала создадим комнату и пользователя (FK constraints)
        await database.execute(
            "INSERT OR IGNORE INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("testuser", None, int(time.time()))
        )
        await database.commit()
        
        msg_id = "test-msg-123"
        ts = int(time.time())
        
        await database.execute(
            "INSERT INTO messages (msg_id, room, nick, text, ts) VALUES (?, ?, ?, ?, ?)",
            (msg_id, "#general", "testuser", "Hello!", ts)
        )
        await database.commit()
        
        msg = await database.fetchone(
            "SELECT msg_id, room, nick, text FROM messages WHERE msg_id = ?",
            (msg_id,)
        )
        
        assert msg is not None
        assert msg["msg_id"] == msg_id
        assert msg["room"] == "#general"
        assert msg["nick"] == "testuser"
        assert msg["text"] == "Hello!"

    @pytest.mark.asyncio
    async def test_insert_room_member(self, database):
        """Тест вставки участника комнаты."""
        import time
        
        # Сначала создадим пользователя и комнату
        await database.execute(
            "INSERT OR IGNORE INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("member", None, int(time.time()))
        )
        
        await database.execute(
            "INSERT INTO room_members (room, nick, joined_at) VALUES (?, ?, ?)",
            ("#general", "member", int(time.time()))
        )
        await database.commit()
        
        member = await database.fetchone(
            "SELECT room, nick FROM room_members WHERE room = '#general' AND nick = 'member'"
        )
        
        assert member is not None
        assert member["room"] == "#general"
        assert member["nick"] == "member"

    @pytest.mark.asyncio
    async def test_wal_entry(self, database):
        """Тест WAL записи."""
        import time
        import json
        
        await database.execute(
            "INSERT INTO wal (ts, operation, table_name, data) VALUES (?, ?, ?, ?)",
            (int(time.time()), "INSERT", "messages", json.dumps({"msg_id": "123"}))
        )
        await database.commit()
        
        entry = await database.fetchone(
            "SELECT seq, operation, table_name, data FROM wal ORDER BY seq DESC LIMIT 1"
        )
        
        assert entry is not None
        assert entry["operation"] == "INSERT"
        assert entry["table_name"] == "messages"
        
        import json
        data = json.loads(entry["data"])
        assert data["msg_id"] == "123"

    @pytest.mark.asyncio
    async def test_fetchall(self, database):
        """Тест получения всех записей."""
        import time
        
        # Вставляем несколько комнат
        for i in range(3):
            await database.execute(
                "INSERT OR IGNORE INTO rooms (name, owner, created_at) VALUES (?, ?, ?)",
                (f"#room{i}", "owner", int(time.time()))
            )
        await database.commit()
        
        rooms = await database.fetchall("SELECT name FROM rooms ORDER BY name")
        
        assert len(rooms) >= 3  # Как минимум #general + 3 новых

    @pytest.mark.asyncio
    async def test_connection_property(self, database):
        """Тест свойства connection."""
        conn = database.connection
        assert conn is not None

    @pytest.mark.asyncio
    async def test_double_close(self, temp_db_path):
        """Тест двойного закрытия (не должно вызывать ошибок)."""
        db = Database(temp_db_path)
        await db.connect()
        await db.close()
        await db.close()  # Не должно вызывать ошибку
