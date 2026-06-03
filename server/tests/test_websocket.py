"""
Тесты WebSocket handlers (network/ws_handler.py).
"""

import pytest
import sys
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from aiohttp.test_utils import TestServer, TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))

from network.ws_manager import WebSocketHandler


@pytest.fixture
async def ws_client():
    """Фикстура WebSocket клиента."""
    from main import create_app
    app = create_app()
    server = TestServer(app)
    await server.start_server()
    client = TestClient(server)
    await client.start_server()
    yield client
    await client.close()
    await server.close()


class TestWebSocketHandler:
    """Тесты WebSocket обработчиков."""

    @pytest.mark.asyncio
    async def test_websocket_connection(self, ws_client):
        """Тест подключения к WebSocket."""
        ws = await ws_client.ws_connect("/ws")
        
        # Проверка что подключение установлено
        assert not ws.closed
        
        await ws.close()

    @pytest.mark.asyncio
    async def test_websocket_invalid_json(self, ws_client):
        """Тест отправки невалидного JSON."""
        ws = await ws_client.ws_connect("/ws")
        
        # Отправляем невалидный JSON
        await ws.send_str("not valid json")
        
        msg = await ws.receive_json()
        
        assert msg["event"] == "ERROR"
        assert "Invalid JSON" in msg["message"]
        
        await ws.close()

    @pytest.mark.asyncio
    async def test_websocket_unknown_command(self, ws_client):
        """Тест отправки неизвестной команды."""
        ws = await ws_client.ws_connect("/ws")
        
        await ws.send_json({"cmd": "UNKNOWN_CMD"})
        
        msg = await ws.receive_json()
        
        assert msg["event"] == "ERROR"
        assert "Unknown command" in msg["message"]
        
        await ws.close()

    @pytest.mark.asyncio
    async def test_websocket_register_flow(self, ws_client):
        """Тест потока регистрации через WebSocket."""
        ws = await ws_client.ws_connect("/ws")
        
        # Регистрация с уникальным ником
        import time
        unique_nick = f"wstestuser{int(time.time())}"
        
        await ws.send_json({
            "cmd": "REGISTER",
            "nick": unique_nick,
            "password": "pass123"
        })
        
        msg = await ws.receive_json()
        assert msg["event"] == "OK"
        assert msg["nick"] == unique_nick
        
        await ws.close()


class TestWebSocketManager:
    """Тесты WebSocket менеджера."""

    @pytest.fixture
    def ws_manager(self):
        return WebSocketHandler()

    @pytest.fixture
    def mock_ws(self):
        ws = MagicMock()
        ws.send_str = AsyncMock()
        return ws

    @pytest.mark.asyncio
    async def test_add_connection(self, ws_manager, mock_ws):
        """Тест добавления подключения."""
        await ws_manager.add_connection(mock_ws)
        
        assert mock_ws in ws_manager.connections
        assert mock_ws in ws_manager.sessions
        assert ws_manager.sessions[mock_ws]["authenticated"] is False

    @pytest.mark.asyncio
    async def test_remove_connection(self, ws_manager, mock_ws):
        """Тест удаления подключения."""
        await ws_manager.add_connection(mock_ws)
        ws_manager.remove_connection(mock_ws)
        
        assert mock_ws not in ws_manager.connections
        assert mock_ws not in ws_manager.sessions

    @pytest.mark.asyncio
    async def test_set_session(self, ws_manager, mock_ws):
        """Тест установки сессии."""
        await ws_manager.add_connection(mock_ws)
        ws_manager.set_session(mock_ws, "testnick", authenticated=True)
        
        session = ws_manager.get_session(mock_ws)
        assert session["nick"] == "testnick"
        assert session["authenticated"] is True

    @pytest.mark.asyncio
    async def test_is_authenticated(self, ws_manager, mock_ws):
        """Тест проверки аутентификации."""
        await ws_manager.add_connection(mock_ws)
        
        assert ws_manager.is_authenticated(mock_ws) is False
        
        ws_manager.set_session(mock_ws, "testnick", authenticated=True)
        assert ws_manager.is_authenticated(mock_ws) is True

    @pytest.mark.asyncio
    async def test_join_room(self, ws_manager, mock_ws):
        """Тест присоединения к комнате."""
        await ws_manager.add_connection(mock_ws)
        await ws_manager.join_room(mock_ws, "#general")
        
        assert mock_ws in ws_manager.room_connections.get("#general", set())

    @pytest.mark.asyncio
    async def test_leave_room(self, ws_manager, mock_ws):
        """Тест покидания комнаты."""
        await ws_manager.add_connection(mock_ws)
        await ws_manager.join_room(mock_ws, "#general")
        ws_manager.leave_room(mock_ws, "#general")
        
        assert mock_ws not in ws_manager.room_connections.get("#general", set())

    @pytest.mark.asyncio
    async def test_broadcast_to_room(self, ws_manager):
        """Тест рассылки по комнате."""
        # Создаём несколько мок подключений
        ws1 = MagicMock()
        ws1.send_str = AsyncMock()
        
        ws2 = MagicMock()
        ws2.send_str = AsyncMock()
        
        await ws_manager.add_connection(ws1)
        await ws_manager.add_connection(ws2)
        
        await ws_manager.join_room(ws1, "#test")
        await ws_manager.join_room(ws2, "#test")
        
        # Рассылка
        await ws_manager.broadcast_to_room("#test", {"event": "TEST"})
        
        # Оба должны получить сообщение
        assert ws1.send_str.called
        assert ws2.send_str.called

    @pytest.mark.asyncio
    async def test_broadcast_exclude(self, ws_manager):
        """Тест рассылки с исключением."""
        ws1 = MagicMock()
        ws1.send_str = AsyncMock()
        
        ws2 = MagicMock()
        ws2.send_str = AsyncMock()
        
        await ws_manager.add_connection(ws1)
        await ws_manager.add_connection(ws2)
        
        await ws_manager.join_room(ws1, "#test")
        await ws_manager.join_room(ws2, "#test")
        
        # Рассылка с исключением ws1
        await ws_manager.broadcast_to_room("#test", {"event": "TEST"}, exclude=ws1)
        
        # ws1 не должен получить, ws2 должен
        assert not ws1.send_str.called
        assert ws2.send_str.called

    @pytest.mark.asyncio
    async def test_send_to(self, ws_manager, mock_ws):
        """Тест отправки конкретному подключению."""
        await ws_manager.add_connection(mock_ws)
        
        await ws_manager.send_to(mock_ws, {"event": "TEST"})
        
        assert mock_ws.send_str.called

    def test_get_connected_count(self, ws_manager, mock_ws):
        """Тест подсчёта подключений."""
        assert ws_manager.get_connected_count() == 0
        
        ws_manager.connections.add(mock_ws)
        assert ws_manager.get_connected_count() == 1

    def test_get_room_members_count(self, ws_manager, mock_ws):
        """Тест подсчёта участников комнаты."""
        ws_manager.room_connections["#test"] = {mock_ws}
        assert ws_manager.get_room_members_count("#test") == 1
