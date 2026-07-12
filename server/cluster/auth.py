"""
Аутентификация межсерверного трафика кластера (issue #10, Этап 3.1).

Кластерные endpoint'ы, меняющие состояние (выборы и репликация), не должны быть
открыты кому угодно: иначе посторонний может, например, объявить себя мастером
через ``/cluster/election/coordinator`` или подсунуть поддельные WAL-записи.

Защита простая и уместная для примера — **общий секрет** (shared token): все узлы
кластера знают один секрет и передают его в заголовке каждого запроса. Это не
защищает от подслушивания трафика (для этого нужен TLS, Этап 4.3), но закрывает
самый грубый анти-паттерн — публично доступные управляющие ручки.

Что защищаем и что нет:

* ``/cluster/election/*`` и ``/cluster/replication/*`` — **защищены** (меняют состояние);
* ``/cluster/health`` и ``/cluster/state`` — **открыты**: это observability-ручки
  (аналогично ``/health`` и ``/metrics``), их удобно опрашивать мониторингом.

Если секрет не задан, middleware не навешивается вовсе (обратная совместимость),
а сервер при старте кластера предупреждает, что трафик не аутентифицирован.
"""

import hmac
import logging

from aiohttp import web

# Заголовок, в котором узлы передают общий секрет.
CLUSTER_TOKEN_HEADER = "X-Cluster-Token"

# Префиксы путей, требующих аутентификации (меняют состояние кластера).
_PROTECTED_PREFIXES = ("/cluster/election/", "/cluster/replication/")


def auth_headers(secret: str) -> dict:
    """Заголовки для исходящих кластерных запросов (пустой dict, если секрета нет)."""
    return {CLUSTER_TOKEN_HEADER: secret} if secret else {}


def _is_protected(path: str) -> bool:
    return path.startswith(_PROTECTED_PREFIXES)


def make_cluster_auth_middleware(secret: str):
    """
    Создаёт aiohttp-middleware, проверяющую общий секрет на защищённых
    кластерных путях. Сравнение — constant-time (``hmac.compare_digest``),
    чтобы не давать подсказок по времени ответа.
    """

    @web.middleware
    async def cluster_auth_middleware(request: web.Request, handler):
        if _is_protected(request.path):
            provided = request.headers.get(CLUSTER_TOKEN_HEADER, "")
            if not hmac.compare_digest(provided, secret):
                logging.warning(
                    "[Cluster] Отклонён неаутентифицированный запрос %s %s",
                    request.method, request.path,
                )
                return web.json_response(
                    {"error": "Unauthorized cluster request"}, status=401
                )
        return await handler(request)

    return cluster_auth_middleware
