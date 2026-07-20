"""HTTP маршруты."""

import json
from aiohttp import web
from pathlib import Path

from network.ws_handler import websocket_handler


_CORS_METHODS = "GET, POST, PUT, DELETE, OPTIONS"
_CORS_HEADERS = "Content-Type, Authorization"


def make_cors_middleware(allowed_origins):
    """
    Создаёт CORS-middleware с настраиваемым allowlist (issue #16).

    ``allowed_origins`` — список Origin или ``["*"]`` (все). При ``*`` возвращаем
    ``Access-Control-Allow-Origin: *``. Иначе отражаем Origin запроса, только если
    он в списке (и добавляем ``Vary: Origin``, чтобы кэши не смешивали ответы);
    незнакомый Origin не получает CORS-заголовков — браузер сам заблокирует.
    """
    allow_all = "*" in allowed_origins
    allowed = set(allowed_origins)

    def _acao_for(request: web.Request):
        if allow_all:
            return "*"
        origin = request.headers.get("Origin")
        return origin if origin in allowed else None

    @web.middleware
    async def cors_middleware(request: web.Request, handler):
        acao = _acao_for(request)

        # Preflight
        if request.method == "OPTIONS":
            headers = {
                "Access-Control-Allow-Methods": _CORS_METHODS,
                "Access-Control-Allow-Headers": _CORS_HEADERS,
                "Access-Control-Max-Age": "3600",
            }
            if acao:
                headers["Access-Control-Allow-Origin"] = acao
                if acao != "*":
                    headers["Vary"] = "Origin"
            return web.Response(headers=headers)

        response = await handler(request)

        if acao:
            response.headers["Access-Control-Allow-Origin"] = acao
            response.headers["Access-Control-Allow-Methods"] = _CORS_METHODS
            response.headers["Access-Control-Allow-Headers"] = _CORS_HEADERS
            if acao != "*":
                response.headers["Vary"] = "Origin"

        return response

    return cors_middleware


CLIENT_DIR = Path(__file__).parent.parent.parent / "client"


async def serve_index(request: web.Request) -> web.Response:
    """Отдача index.html."""
    return web.FileResponse(CLIENT_DIR / "index.html")


async def serve_client_root_file(request: web.Request) -> web.Response:
    """
    Отдача файлов, которые должны лежать в корне сайта: sw.js и manifest.json.
    Service Worker обязан отдаваться из корня, иначе его scope='/' не сработает.
    """
    name = Path(request.path).name  # "sw.js" | "manifest.json"
    path = CLIENT_DIR / name
    if not path.is_file():
        return web.Response(status=404)
    return web.FileResponse(path)


async def serve_favicon(request: web.Request) -> web.Response:
    """Иконка вкладки — переиспользуем PWA-иконку."""
    return web.FileResponse(CLIENT_DIR / "icons" / "icon-192.png")


def setup_routes(app: web.Application):
    """Настройка HTTP маршрутов."""
    # CORS middleware с allowlist из конфигурации (issue #16)
    from config import CORS_ALLOWED_ORIGINS
    app.middlewares.append(make_cors_middleware(CORS_ALLOWED_ORIGINS))
    
    # WebSocket endpoint
    app.router.add_get("/ws", websocket_handler)

    # Health checks: liveness (процесс жив) и readiness (готов к трафику).
    # /health оставлен как liveness ради обратной совместимости (docker-compose,
    # скрипты). /health/live — явный синоним, /health/ready — готовность.
    app.router.add_get("/health", health_check)
    app.router.add_get("/health/live", liveness_check)
    app.router.add_get("/health/ready", readiness_check)

    # Prometheus метрики
    app.router.add_get("/metrics", metrics_endpoint)

    # Список серверов (для кластера)
    app.router.add_get("/api/servers", get_servers)

    # Статика клиента — по путям, которые ждёт index.html (js/, css/, icons/,
    # manifest.json, sw.js). Ссылки в index.html относительные, поэтому
    # разрешаются в /js/…, /css/…, /manifest.json от корня.
    if CLIENT_DIR.exists():
        app.router.add_get("/", serve_index)
        app.router.add_get("/sw.js", serve_client_root_file)
        app.router.add_get("/manifest.json", serve_client_root_file)
        app.router.add_get("/favicon.ico", serve_favicon)
        app.router.add_static("/js/", CLIENT_DIR / "js", show_index=False)
        app.router.add_static("/css/", CLIENT_DIR / "css", show_index=False)
        app.router.add_static("/icons/", CLIENT_DIR / "icons", show_index=False)


async def health_check(request: web.Request) -> web.Response:
    """Liveness (обратная совместимость): процесс жив и отвечает."""
    return web.json_response({"status": "ok"})


async def liveness_check(request: web.Request) -> web.Response:
    """
    Liveness: жив ли процесс. Если обработчик выполнился — событийный цикл
    отвечает; тело фиксированное. Оркестратор по 200 понимает «не перезапускать».
    """
    return web.json_response({"status": "alive"})


async def readiness_check(request: web.Request) -> web.Response:
    """
    Readiness: готов ли узел принимать трафик. 200 — готов, 503 — нет
    (например, реплика догоняет WAL или идут выборы мастера). Балансировщик
    по 503 временно выводит узел из ротации, не убивая его.
    """
    cluster = request.app.get("cluster")
    if not cluster:
        # Одиночный сервер: готов, как только отвечает.
        return web.json_response({"ready": True, "role": "standalone"})

    info = cluster.get_readiness()
    return web.json_response(info, status=200 if info["ready"] else 503)


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
