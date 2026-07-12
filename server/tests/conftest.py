"""
Тестовые фикстуры и утилиты.
"""

import os
import sys
import tempfile
import pytest
from pathlib import Path

# Добавляем server в path для относительных импортов
sys.path.insert(0, str(Path(__file__).parent.parent))

from aiohttp import web
from aiohttp.test_utils import TestServer, TestClient

from storage.database import Database
from network.ws_manager import ConnectionRegistry
from irc.commands import CommandHandler


@pytest.fixture
def temp_db_path():
    """Создание временной БД."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        yield str(db_path)


@pytest.fixture
async def database(temp_db_path):
    """Фикстура базы данных."""
    db = Database(temp_db_path)
    await db.connect()
    yield db
    await db.close()


@pytest.fixture
def ws_manager():
    """Фикстура WebSocket менеджера."""
    return ConnectionRegistry()


@pytest.fixture
def command_handler(database, ws_manager):
    """Фикстура обработчика команд."""
    return CommandHandler(database, ws_manager)


@pytest.fixture
def app_config():
    """Конфигурация для тестов."""
    return {
        "host": "127.0.0.1",
        "port": 0,  # Автовыбор порта
        "db_path": ":memory:",
        "server_id": "test-server",
    }


@pytest.fixture
async def aiohttp_client(aiohttp_client):
    """Фикстура HTTP клиента."""
    from main import create_app
    
    async def factory():
        app = create_app()
        return await aiohttp_client(app)
    
    return factory


@pytest.fixture
async def ws_client(aiohttp_client):
    """Фикстура WebSocket клиента."""
    from main import create_app
    
    app = create_app()
    client = await aiohttp_client(app)
    return client


@pytest.fixture
def mock_cluster():
    """Мок кластера для тестов."""
    class MockCluster:
        def __init__(self):
            self.is_master = True
            self.replication = None
            self.election = None
            
        def get_cluster_servers(self):
            return [
                {"host": "localhost", "port": 8080, "role": "master"}
            ]
    
    return MockCluster()
