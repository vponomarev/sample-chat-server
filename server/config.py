"""Конфигурация сервера.

Значения берутся из ``config/config.yaml`` (базовые настройки), а переменные
окружения их переопределяют. Такой порядок удобен: файл — читаемые дефолты для
разработки, env — переопределение в Docker/кластере.
"""

import logging
import os
from pathlib import Path

import yaml

# Путь к корню проекта
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_FILE = PROJECT_ROOT / "config" / "config.yaml"


def _load_yaml() -> dict:
    """Читает config/config.yaml. При отсутствии/ошибке — пустой конфиг."""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}
    except Exception as e:  # некорректный YAML не должен ронять сервер
        logging.warning("Не удалось прочитать %s: %s", CONFIG_FILE, e)
        return {}


_YAML = _load_yaml()
_SRV = _YAML.get("server", {})
_DB = _YAML.get("database", {})
_LOG = _YAML.get("logging", {})
_CLUSTER = _YAML.get("cluster", {})


def _resolve_db_path(raw: str) -> str:
    """Относительный путь из YAML разрешаем от корня проекта."""
    p = Path(raw)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    return str(p)


# Сервер (env > yaml > умолчание)
HOST = os.getenv("CHAT_HOST", _SRV.get("host", "0.0.0.0"))
PORT = int(os.getenv("CHAT_PORT", _SRV.get("port", 8080)))

# База данных
_DEFAULT_DB = _resolve_db_path(_DB["path"]) if _DB.get("path") else str(PROJECT_ROOT / "data" / "chat.db")
DB_PATH = os.getenv("CHAT_DB_PATH", _DEFAULT_DB)

# CORS: список разрешённых Origin через запятую или "*" (все). По умолчанию "*" —
# удобно для локального стенда, но это осознанно широкая настройка (issue #16):
# в проде указывают конкретные домены (env CORS_ALLOWED_ORIGINS или yaml).
_CORS_RAW = os.getenv("CORS_ALLOWED_ORIGINS", _SRV.get("cors_allowed_origins", "*"))
CORS_ALLOWED_ORIGINS = [o.strip() for o in str(_CORS_RAW).split(",") if o.strip()] or ["*"]

# Логирование
LOG_LEVEL = os.getenv("LOG_LEVEL", _LOG.get("level", "INFO"))
LOG_FORMAT = os.getenv("LOG_FORMAT", _LOG.get("format", "json"))

# Rate limit сообщений: не более N за окно W секунд на подключение (issue #17).
# Конфигурируемо, чтобы, например, нагрузочный тест (Этап 5.3) мог поднять лимит
# и мерить пропускную способность без искусственных отказов anti-flood.
RATE_LIMIT_MSGS = int(os.getenv("RATE_LIMIT_MSGS", _SRV.get("rate_limit_msgs", 10)))
RATE_LIMIT_WINDOW_SEC = float(
    os.getenv("RATE_LIMIT_WINDOW_SEC", _SRV.get("rate_limit_window_sec", 10))
)

# Имя сервера (для кластера)
SERVER_ID = os.getenv("SERVER_ID", "server1")
SERVER_NAME = os.getenv("SERVER_NAME", SERVER_ID)

# Кластер (Фаза 3)
CLUSTER_ENABLED = os.getenv("CLUSTER_ENABLED", "false").lower() == "true"
# Формат: "server2@host:port,server3@host:port" — ID соседа указывается явно.
PEERS = os.getenv("PEERS", "")

# Общий секрет для аутентификации межсерверного трафика (issue #10).
# Все узлы кластера должны знать один и тот же секрет. Пусто → управляющие
# кластерные endpoint'ы открыты (сервер предупредит об этом при старте).
# В проде задаётся через env CLUSTER_SECRET, а не хранится в репозитории.
CLUSTER_SECRET = os.getenv("CLUSTER_SECRET", _CLUSTER.get("secret", ""))

# Режим репликации (Этап 4.2):
#   "async" (умолч.) — master отвечает клиенту сразу, реплики догоняют фоном
#                      (низкая задержка, но свежая запись может пропасть при
#                      падении master до репликации);
#   "sync"           — master подтверждает запись только после того, как её
#                      получило большинство узлов (durable на кворуме, но выше
#                      задержка и зависимость от доступности реплик).
REPLICATION_MODE = os.getenv(
    "REPLICATION_MODE", _CLUSTER.get("replication_mode", "async")
).lower()


def parse_peers(peers_str: str) -> list:
    """
    Парсинг списка пиров из строки.

    Основной формат — с явным ID: ``id@host:port`` (например
    ``server2@localhost:8082``). ID соседа берётся из конфигурации, а не
    угадывается по позиции — это важно для Bully-выборов, где лидер
    определяется по числовому ID.
    """
    if not peers_str:
        return []

    peers = []
    for i, peer in enumerate(peers_str.split(",")):
        peer = peer.strip()
        if not peer:
            continue

        if "@" in peer:
            server_id, addr = peer.split("@", 1)
            server_id = server_id.strip()
        else:
            # Обратная совместимость со старым форматом host:port без ID.
            # ID приходится угадывать — это ненадёжно, поэтому предупреждаем.
            addr = peer
            server_id = f"server{i + 2}"
            logging.warning(
                "PEERS: у пира '%s' не указан ID (формат id@host:port); "
                "использую предполагаемый '%s'", peer, server_id
            )

        if ":" not in addr:
            logging.warning("PEERS: пропускаю некорректный адрес пира: '%s'", peer)
            continue

        host, port = addr.rsplit(":", 1)
        peers.append({
            "host": host.strip(),
            "port": int(port),
            "server_id": server_id,
        })
    return peers


PEERS_LIST = parse_peers(PEERS)

# Таймауты
WS_HEARTBEAT_INTERVAL = 30  # секунды
WS_HEARTBEAT_TIMEOUT = 60   # секунды

# Пути
STATIC_PATH = PROJECT_ROOT / "client"


def get_config() -> dict:
    """Возвращает конфигурацию как словарь."""
    return {
        "host": HOST,
        "port": PORT,
        "db_path": DB_PATH,
        "server_id": SERVER_ID,
        "server_name": SERVER_NAME,
        "static_path": str(STATIC_PATH),
        "ws_heartbeat_interval": WS_HEARTBEAT_INTERVAL,
        "ws_heartbeat_timeout": WS_HEARTBEAT_TIMEOUT,
        "cluster_enabled": CLUSTER_ENABLED,
        "cluster_secret": CLUSTER_SECRET,
        "replication_mode": REPLICATION_MODE,
        "peers": PEERS_LIST,
        "log_level": LOG_LEVEL,
        "log_format": LOG_FORMAT,
    }
