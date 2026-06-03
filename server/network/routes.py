"""HTTP маршруты."""

import json
from aiohttp import web
from pathlib import Path

from network.ws_handler import websocket_handler


@web.middleware
async def cors_middleware(request: web.Request, handler):
    """CORS middleware для поддержки кросс-доменных запросов."""
    # Обработка preflight запросов
    if request.method == "OPTIONS":
        return web.Response(
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
                "Access-Control-Max-Age": "3600",
            }
        )
    
    # Выполнение обработчика
    response = await handler(request)
    
    # Добавление CORS заголовков
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    
    return response


async def serve_index(request: web.Request) -> web.Response:
    """Отдача index.html."""
    static_path = Path(__file__).parent.parent.parent / "client" / "index.html"
    return web.FileResponse(static_path)


def setup_routes(app: web.Application):
    """Настройка HTTP маршрутов."""
    # Добавляем CORS middleware
    app.middlewares.append(cors_middleware)
    
    # WebSocket endpoint
    app.router.add_get("/ws", websocket_handler)

    # Health check
    app.router.add_get("/health", health_check)

    # Prometheus метрики
    app.router.add_get("/metrics", metrics_endpoint)

    # Список серверов (для кластера)
    app.router.add_get("/api/servers", get_servers)

    # Статика (клиент)
    static_path = Path(__file__).parent.parent.parent / "client"
    if static_path.exists():
        # Добавляем индекс по умолчанию
        app.router.add_get("/", serve_index)
        app.router.add_static("/static/", static_path, show_index=False)


async def health_check(request: web.Request) -> web.Response:
    """Health check endpoint."""
    return web.json_response({"status": "ok"})


async def metrics_endpoint(request: web.Request) -> web.Response:
    """Prometheus metrics endpoint."""
    from observability.metrics import registry
    from prometheus_client import generate_latest
    
    metrics_data = generate_latest(registry).decode('utf-8')
    return web.Response(
        text=metrics_data,
        content_type='text/plain',
    )


async def get_servers(request: web.Request) -> web.Response:
    """Список серверов кластера."""
    cluster = request.app.get("cluster")
    
    if cluster:
        # Кластер включён - получаем список от кластера
        servers = cluster.get_cluster_servers()
    else:
        # Одиночный сервер
        servers = [
            {"host": request.host.split(":")[0], "port": 8080, "role": "master"}
        ]
    
    return web.json_response({"servers": servers})
