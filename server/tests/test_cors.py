"""
Тесты CORS-middleware с настраиваемым allowlist (network/routes.py, issue #16).
"""

import sys
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer, TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))

from network.routes import make_cors_middleware


@pytest.fixture
async def make_client():
    """Фабрика тест-клиентов с CORS-middleware для заданного allowlist."""
    created = []

    async def factory(allowed_origins):
        app = web.Application(middlewares=[make_cors_middleware(allowed_origins)])

        async def ping(request):
            return web.json_response({"ok": True})

        app.router.add_get("/ping", ping)
        server = TestServer(app)
        await server.start_server()
        client = TestClient(server)
        await client.start_server()
        created.append((client, server))
        return client

    yield factory

    for client, server in created:
        await client.close()
        await server.close()


class TestCorsAllowAll:
    @pytest.mark.asyncio
    async def test_star_echoes_star_for_any_origin(self, make_client):
        client = await make_client(["*"])
        resp = await client.get("/ping", headers={"Origin": "http://anything.example"})
        assert resp.headers["Access-Control-Allow-Origin"] == "*"

    @pytest.mark.asyncio
    async def test_star_preflight(self, make_client):
        client = await make_client(["*"])
        resp = await client.options("/ping")
        assert resp.status == 200
        assert resp.headers["Access-Control-Allow-Origin"] == "*"
        assert "GET" in resp.headers["Access-Control-Allow-Methods"]


class TestCorsAllowlist:
    @pytest.mark.asyncio
    async def test_allowed_origin_reflected(self, make_client):
        client = await make_client(["http://good.example"])
        resp = await client.get("/ping", headers={"Origin": "http://good.example"})
        assert resp.headers["Access-Control-Allow-Origin"] == "http://good.example"
        assert resp.headers.get("Vary") == "Origin"

    @pytest.mark.asyncio
    async def test_disallowed_origin_gets_no_cors(self, make_client):
        client = await make_client(["http://good.example"])
        resp = await client.get("/ping", headers={"Origin": "http://evil.example"})
        assert "Access-Control-Allow-Origin" not in resp.headers

    @pytest.mark.asyncio
    async def test_preflight_allowed_origin(self, make_client):
        client = await make_client(["http://good.example"])
        resp = await client.options("/ping", headers={"Origin": "http://good.example"})
        assert resp.status == 200
        assert resp.headers["Access-Control-Allow-Origin"] == "http://good.example"

    @pytest.mark.asyncio
    async def test_preflight_disallowed_origin_no_acao(self, make_client):
        client = await make_client(["http://good.example"])
        resp = await client.options("/ping", headers={"Origin": "http://evil.example"})
        assert resp.status == 200  # preflight отвечает, но без разрешающего заголовка
        assert "Access-Control-Allow-Origin" not in resp.headers
