"""
Тесты HTTP endpoints (network/routes.py).
"""

import pytest
import sys
from pathlib import Path
from aiohttp.test_utils import TestServer, TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
async def http_client():
    """Фикстура HTTP клиента."""
    from main import create_app
    app = create_app()
    server = TestServer(app)
    await server.start_server()
    client = TestClient(server)
    await client.start_server()
    yield client
    await client.close()
    await server.close()


class TestHealthEndpoint:
    """Тесты health check endpoint."""

    @pytest.mark.asyncio
    async def test_health_check(self, http_client):
        """Тест health check."""
        resp = await http_client.get("/health")
        
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "ok"


class TestMetricsEndpoint:
    """Тесты metrics endpoint."""

    @pytest.mark.asyncio
    async def test_metrics_endpoint(self, http_client):
        """Тест Prometheus метрик."""
        resp = await http_client.get("/metrics")
        
        assert resp.status == 200
        text = await resp.text()
        
        # Проверка наличия метрик
        assert "python_gc" in text or "irc_" in text

    @pytest.mark.asyncio
    async def test_metrics_content_type(self, http_client):
        """Тест content-type метрик."""
        resp = await http_client.get("/metrics")
        
        assert "text/plain" in resp.headers.get("Content-Type", "")


class TestServersEndpoint:
    """Тесты servers endpoint."""

    @pytest.mark.asyncio
    async def test_get_servers_single(self, http_client):
        """Тест получения списка серверов (одиночный режим)."""
        resp = await http_client.get("/api/servers")
        
        assert resp.status == 200
        data = await resp.json()
        
        assert "servers" in data
        assert len(data["servers"]) >= 1
        assert data["servers"][0]["role"] == "master"

    @pytest.mark.asyncio
    async def test_get_servers_format(self, http_client):
        """Тест формата ответа servers."""
        resp = await http_client.get("/api/servers")
        data = await resp.json()
        
        server = data["servers"][0]
        assert "host" in server
        assert "port" in server
        assert "role" in server


class TestStaticFiles:
    """Тесты статических файлов."""

    @pytest.mark.asyncio
    async def test_index_html(self, http_client):
        """Тест главной страницы."""
        resp = await http_client.get("/")
        
        assert resp.status == 200
        text = await resp.text()
        assert "<!DOCTYPE html>" in text or "<html" in text

    @pytest.mark.asyncio
    async def test_css_file(self, http_client):
        """Тест CSS файла."""
        resp = await http_client.get("/static/css/style.css")
        
        assert resp.status == 200
        text = await resp.text()
        assert "body" in text or ":root" in text

    @pytest.mark.asyncio
    async def test_js_file(self, http_client):
        """Тест JS файла."""
        resp = await http_client.get("/static/js/app.js")
        
        assert resp.status == 200
        text = await resp.text()
        assert "class" in text or "function" in text

    @pytest.mark.asyncio
    async def test_manifest(self, http_client):
        """Тест PWA manifest."""
        resp = await http_client.get("/static/manifest.json")
        
        assert resp.status == 200
        data = await resp.json()
        
        assert "name" in data or "short_name" in data


class Test404:
    """Тесты 404 ошибок."""

    @pytest.mark.asyncio
    async def test_nonexistent_route(self, http_client):
        """Тест несуществующего маршрута."""
        resp = await http_client.get("/nonexistent-route-12345")
        
        assert resp.status == 404
