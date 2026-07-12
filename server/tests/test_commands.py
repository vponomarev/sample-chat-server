"""
Тесты IRC команд (irc/commands.py).
"""

import pytest
import sys
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent))

from irc.commands import CommandHandler
from network.ws_manager import ConnectionRegistry


class TestCommandHandler:
    """Тесты CommandHandler."""

    @pytest.fixture
    def mock_ws(self):
        """Мок WebSocket соединения."""
        ws = MagicMock()
        ws.send_str = AsyncMock()
        return ws

    @pytest.fixture
    def handler(self, database, ws_manager):
        """Создание обработчика команд."""
        return CommandHandler(database, ws_manager)

    @pytest.mark.asyncio
    async def test_register_success(self, handler, mock_ws):
        """Тест успешной регистрации."""
        await handler.handle_register(mock_ws, {
            "nick": "newuser",
            "password": "pass123"
        })

        # Проверка ответа
        call_args = mock_ws.send_str.call_args
        response = json.loads(call_args[0][0])

        assert response["event"] == "OK"
        assert response["cmd"] == "REGISTER"
        assert response["nick"] == "newuser"

    @pytest.mark.asyncio
    async def test_register_missing_nick(self, handler, mock_ws):
        """Тест регистрации без ника."""
        await handler.handle_register(mock_ws, {
            "password": "pass123"
        })

        call_args = mock_ws.send_str.call_args
        response = json.loads(call_args[0][0])

        assert response["event"] == "ERROR"
        assert "Nick is required" in response["message"]

    @pytest.mark.asyncio
    async def test_register_duplicate_nick(self, handler, mock_ws, database):
        """Тест регистрации с существующим ником."""
        import time
        
        # Создаём пользователя
        await database.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("existing", "hash", int(time.time()))
        )
        await database.commit()

        await handler.handle_register(mock_ws, {
            "nick": "existing",
            "password": "pass123"
        })

        call_args = mock_ws.send_str.call_args
        response = json.loads(call_args[0][0])

        assert response["event"] == "ERROR"
        assert "already exists" in response["message"]

    @pytest.mark.asyncio
    async def test_login_success(self, handler, mock_ws, database):
        """Тест успешного логина."""
        import time
        import bcrypt
        
        # Создаём пользователя с паролем
        password_hash = bcrypt.hashpw(b"pass123", bcrypt.gensalt()).decode()
        await database.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("testuser", password_hash, int(time.time()))
        )
        await database.commit()

        await handler.handle_login(mock_ws, {
            "nick": "testuser",
            "password": "pass123"
        })

        call_args = mock_ws.send_str.call_args
        response = json.loads(call_args[0][0])

        assert response["event"] == "OK"
        assert response["cmd"] == "LOGIN"

    @pytest.mark.asyncio
    async def test_login_user_not_found(self, handler, mock_ws):
        """Тест логина с несуществующим пользователем."""
        await handler.handle_login(mock_ws, {
            "nick": "nonexistent",
            "password": "pass123"
        })

        call_args = mock_ws.send_str.call_args
        response = json.loads(call_args[0][0])

        assert response["event"] == "ERROR"
        assert "User not found" in response["message"]

    @pytest.mark.asyncio
    async def test_login_wrong_password(self, handler, mock_ws, database):
        """Тест логина с неправильным паролем."""
        import time
        import bcrypt
        
        password_hash = bcrypt.hashpw(b"correct", bcrypt.gensalt()).decode()
        await database.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("user", password_hash, int(time.time()))
        )
        await database.commit()

        await handler.handle_login(mock_ws, {
            "nick": "user",
            "password": "wrong"
        })

        call_args = mock_ws.send_str.call_args
        response = json.loads(call_args[0][0])

        assert response["event"] == "ERROR"
        assert "Invalid password" in response["message"]

    @pytest.mark.asyncio
    async def test_login_without_password(self, handler, mock_ws, database):
        """Тест логина без пароля (если пароль не установлен)."""
        import time
        
        await database.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("nopass", None, int(time.time()))
        )
        await database.commit()

        await handler.handle_login(mock_ws, {
            "nick": "nopass",
            "password": ""
        })

        call_args = mock_ws.send_str.call_args
        response = json.loads(call_args[0][0])

        assert response["event"] == "OK"

    @pytest.mark.asyncio
    async def test_list_rooms(self, handler, mock_ws):
        """Тест списка комнат."""
        await handler.handle_list_rooms(mock_ws, {})

        call_args = mock_ws.send_str.call_args
        response = json.loads(call_args[0][0])

        assert response["event"] == "ROOM_LIST"
        assert "#general" in response["rooms"]

    @pytest.mark.asyncio
    async def test_create_room_success(self, handler, mock_ws, database):
        """Тест создания комнаты."""
        import time
        
        # Сначала логинимся
        await database.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("creator", None, int(time.time()))
        )
        await database.commit()

        # Устанавливаем сессию
        handler.ws_manager.sessions[mock_ws] = {"nick": "creator", "authenticated": True}
        handler.ws_manager.connections.add(mock_ws)

        await handler.handle_create_room(mock_ws, {
            "room": "#new-room"
        })

        # Может прийти несколько ответов (OK + ROOM_LIST)
        calls = mock_ws.send_str.call_args_list
        found_ok = False
        for call in calls:
            response = json.loads(call[0][0])
            if response.get("event") == "OK" and response.get("cmd") == "CREATE_ROOM":
                found_ok = True
                break

        assert found_ok

    @pytest.mark.asyncio
    async def test_create_room_not_authenticated(self, handler, mock_ws):
        """Тест создания комнаты без аутентификации."""
        handler.ws_manager.sessions[mock_ws] = {"nick": None, "authenticated": False}

        await handler.handle_create_room(mock_ws, {
            "room": "#room"
        })

        call_args = mock_ws.send_str.call_args
        response = json.loads(call_args[0][0])

        assert response["event"] == "ERROR"
        assert "Authentication required" in response["message"]

    @pytest.mark.asyncio
    async def test_create_room_duplicate(self, handler, mock_ws, database):
        """Тест создания существующей комнаты."""
        import time
        
        await database.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("user", None, int(time.time()))
        )
        await database.execute(
            "INSERT INTO rooms (name, owner, created_at) VALUES (?, ?, ?)",
            ("#existing", "user", int(time.time()))
        )
        await database.commit()

        handler.ws_manager.sessions[mock_ws] = {"nick": "user", "authenticated": True}
        handler.ws_manager.connections.add(mock_ws)

        await handler.handle_create_room(mock_ws, {
            "room": "#existing"
        })

        call_args = mock_ws.send_str.call_args
        response = json.loads(call_args[0][0])

        assert response["event"] == "ERROR"
        assert "already exists" in response["message"]

    @pytest.mark.asyncio
    async def test_join_room(self, handler, mock_ws, database):
        """Тест присоединения к комнате."""
        import time
        
        await database.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("joiner", None, int(time.time()))
        )
        await database.commit()

        handler.ws_manager.sessions[mock_ws] = {"nick": "joiner", "authenticated": True}
        handler.ws_manager.connections.add(mock_ws)

        await handler.handle_join(mock_ws, {
            "room": "#general"
        })

        # Проверяем что был отправлен USER_LIST
        calls = mock_ws.send_str.call_args_list
        found_user_list = False
        for call in calls:
            response = json.loads(call[0][0])
            if response["event"] == "USER_LIST":
                found_user_list = True
                break

        assert found_user_list

    @pytest.mark.asyncio
    async def test_msg_success(self, handler, mock_ws, database):
        """Тест отправки сообщения."""
        import time
        
        await database.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("sender", None, int(time.time()))
        )
        await database.commit()

        handler.ws_manager.sessions[mock_ws] = {"nick": "sender", "authenticated": True}
        handler.ws_manager.connections.add(mock_ws)
        handler.ws_manager.room_connections["#general"] = {mock_ws}

        await handler.handle_msg(mock_ws, {
            "room": "#general",
            "text": "Hello!",
            "client_msg_id": "test-123"
        })

        # Проверяем что сообщение было отправлено
        calls = mock_ws.send_str.call_args_list
        found_message = False
        found_ack = False
        
        for call in calls:
            response = json.loads(call[0][0])
            if response.get("event") == "MESSAGE":
                found_message = True
                assert response["text"] == "Hello!"
            if response.get("event") == "ACK":
                found_ack = True
                assert response["client_msg_id"] == "test-123"

        assert found_message
        assert found_ack

    @pytest.mark.asyncio
    async def test_msg_not_authenticated(self, handler, mock_ws):
        """Тест отправки сообщения без аутентификации."""
        handler.ws_manager.sessions[mock_ws] = {"nick": None, "authenticated": False}

        await handler.handle_msg(mock_ws, {
            "room": "#general",
            "text": "Hello!"
        })

        call_args = mock_ws.send_str.call_args
        response = json.loads(call_args[0][0])

        assert response["event"] == "ERROR"
        assert "Authentication required" in response["message"]

    @pytest.mark.asyncio
    async def test_delete_room_general(self, handler, mock_ws, database):
        """Тест удаления комнаты #general (запрещено)."""
        import time
        
        await database.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("admin", None, int(time.time()))
        )
        await database.commit()

        handler.ws_manager.sessions[mock_ws] = {"nick": "admin", "authenticated": True}
        handler.ws_manager.connections.add(mock_ws)

        await handler.handle_delete_room(mock_ws, {
            "room": "#general"
        })

        call_args = mock_ws.send_str.call_args
        response = json.loads(call_args[0][0])

        assert response["event"] == "ERROR"
        assert "Cannot delete #general" in response["message"]

    @pytest.mark.asyncio
    async def test_who_command(self, handler, mock_ws, database):
        """Тест команды WHO."""
        import time
        
        await database.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("user", None, int(time.time()))
        )
        await database.execute(
            "INSERT INTO room_members (room, nick, joined_at) VALUES (?, ?, ?)",
            ("#general", "user", int(time.time()))
        )
        await database.commit()

        handler.ws_manager.sessions[mock_ws] = {"nick": "user", "authenticated": True}
        handler.ws_manager.connections.add(mock_ws)

        await handler.handle_who(mock_ws, {
            "room": "#general"
        })

        call_args = mock_ws.send_str.call_args
        response = json.loads(call_args[0][0])

        assert response["event"] == "USER_LIST"
        assert "room" in response

    @pytest.mark.asyncio
    async def test_msg_idempotent_duplicate(self, handler, mock_ws, database):
        """Повтор с тем же client_msg_id не создаёт дубль в БД."""
        import time

        await database.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("sender", None, int(time.time()))
        )
        await database.commit()

        handler.ws_manager.sessions[mock_ws] = {"nick": "sender", "authenticated": True}
        handler.ws_manager.connections.add(mock_ws)
        handler.ws_manager.room_connections["#general"] = {mock_ws}

        payload = {"room": "#general", "text": "Hello!", "client_msg_id": "dup-1"}

        # Первая отправка
        await handler.handle_msg(mock_ws, payload)
        # Повтор (как будто клиент не получил ACK и переслал)
        await handler.handle_msg(mock_ws, payload)

        # В БД должна быть ровно одна запись
        row = await database.fetchone(
            "SELECT COUNT(*) AS c FROM messages WHERE client_msg_id = ?", ("dup-1",)
        )
        assert row["c"] == 1

        # Второй ACK помечен как duplicate и msg_id совпадает с первым
        acks = [
            json.loads(c[0][0]) for c in mock_ws.send_str.call_args_list
            if json.loads(c[0][0]).get("event") == "ACK"
        ]
        assert len(acks) == 2
        assert acks[1].get("duplicate") is True
        assert acks[0]["msg_id"] == acks[1]["msg_id"]

    @pytest.mark.asyncio
    async def test_msg_too_long(self, handler, mock_ws, database):
        """Слишком длинное сообщение отклоняется без вставки."""
        import time
        from irc.commands import MAX_MESSAGE_LEN

        await database.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("sender", None, int(time.time()))
        )
        await database.commit()

        handler.ws_manager.sessions[mock_ws] = {"nick": "sender", "authenticated": True}
        handler.ws_manager.connections.add(mock_ws)

        await handler.handle_msg(mock_ws, {
            "room": "#general",
            "text": "x" * (MAX_MESSAGE_LEN + 1),
        })

        call_args = mock_ws.send_str.call_args
        response = json.loads(call_args[0][0])
        assert response["event"] == "ERROR"
        assert "too long" in response["message"]

        row = await database.fetchone("SELECT COUNT(*) AS c FROM messages")
        assert row["c"] == 0

    @pytest.mark.asyncio
    async def test_register_nick_too_long(self, handler, mock_ws):
        """Слишком длинный ник отклоняется при регистрации."""
        from irc.commands import MAX_NICK_LEN

        await handler.handle_register(mock_ws, {
            "nick": "n" * (MAX_NICK_LEN + 1),
            "password": "pass123",
        })

        call_args = mock_ws.send_str.call_args
        response = json.loads(call_args[0][0])
        assert response["event"] == "ERROR"
        assert "too long" in response["message"]

    @pytest.mark.asyncio
    async def test_register_requires_password(self, handler, mock_ws):
        """Регистрация без пароля отклоняется (issue #15)."""
        await handler.handle_register(mock_ws, {"nick": "nopass", "password": ""})

        response = json.loads(mock_ws.send_str.call_args[0][0])
        assert response["event"] == "ERROR"
        assert "Password is required" in response["message"]

    @pytest.mark.asyncio
    async def test_msg_rate_limit(self, handler, mock_ws, database):
        """После лимита сообщений следующее отклоняется (issue #17)."""
        import time
        from irc.commands import RATE_LIMIT_MSGS

        await database.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("flooder", None, int(time.time()))
        )
        await database.commit()
        handler.ws_manager.sessions[mock_ws] = {"nick": "flooder", "authenticated": True}
        handler.ws_manager.connections.add(mock_ws)
        handler.ws_manager.room_connections["#general"] = {mock_ws}

        # Ровно лимит — проходят
        for i in range(RATE_LIMIT_MSGS):
            await handler.handle_msg(mock_ws, {
                "room": "#general", "text": f"m{i}", "client_msg_id": f"c{i}"
            })
        # Следующее — отклонено
        await handler.handle_msg(mock_ws, {
            "room": "#general", "text": "over", "client_msg_id": "cX"
        })

        last = json.loads(mock_ws.send_str.call_args[0][0])
        assert last["event"] == "ERROR"
        assert "Rate limit" in last["message"]

        # В БД — ровно лимит сообщений, «лишнее» не сохранено
        row = await database.fetchone("SELECT COUNT(*) AS c FROM messages")
        assert row["c"] == RATE_LIMIT_MSGS

    @pytest.mark.asyncio
    async def test_reconnect_requires_relogin(self, handler, mock_ws, database):
        """
        Новый сокет (как после reconnect) не аутентифицирован: MSG отклоняется,
        а после повторного LOGIN — проходит. Это и есть поведение, на которое
        опирается авто-релогин клиента (issue #6).
        """
        import time
        import bcrypt

        password_hash = bcrypt.hashpw(b"secret", bcrypt.gensalt()).decode()
        await database.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("bob", password_hash, int(time.time()))
        )
        await database.commit()

        # Новый сокет после переподключения — сессия чистая, не аутентифицирован
        new_ws = MagicMock()
        new_ws.send_str = AsyncMock()
        await handler.ws_manager.add_connection(new_ws)

        # MSG до логина — ошибка
        await handler.handle_msg(new_ws, {"room": "#general", "text": "hi"})
        resp = json.loads(new_ws.send_str.call_args[0][0])
        assert resp["event"] == "ERROR"
        assert "Authentication required" in resp["message"]

        # Повторный LOGIN на новом сокете
        await handler.handle_login(new_ws, {"nick": "bob", "password": "secret"})
        assert handler.ws_manager.is_authenticated(new_ws) is True

        # Теперь MSG проходит
        handler.ws_manager.room_connections["#general"] = {new_ws}
        await handler.handle_msg(new_ws, {
            "room": "#general", "text": "back online", "client_msg_id": "r-1"
        })
        events = [json.loads(c[0][0]).get("event") for c in new_ws.send_str.call_args_list]
        assert "MESSAGE" in events
        assert "ACK" in events
        # Ошибок аутентификации после релогина быть не должно
        assert not any(
            json.loads(c[0][0]).get("message") == "Authentication required"
            for c in new_ws.send_str.call_args_list[-2:]
        )
