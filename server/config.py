"""Конфигурация сервера."""

import os
from pathlib import Path

# Путь к корню проекта
PROJECT_ROOT = Path(__file__).parent.parent

# Сервер
HOST = os.getenv("CHAT_HOST", "0.0.0.0")
PORT = int(os.getenv("CHAT_PORT", 8080))

# База данных
DB_PATH = os.getenv("CHAT_DB_PATH", str(PROJECT_ROOT / "data" / "chat.db"))

# Имя сервера (для кластера)
SERVER_ID = os.getenv("SERVER_ID", "server1")
SERVER_NAME = os.getenv("SERVER_NAME", SERVER_ID)

# Кластер (Фаза 3)
CLUSTER_ENABLED = os.getenv("CLUSTER_ENABLED", "false").lower() == "true"
PEERS = os.getenv("PEERS", "")  # Формат: "server2:8080,server3:8080"

def parse_peers(peers_str: str) -> list:
    """Парсинг строки пиров."""
    if not peers_str:
        return []
    
    peers = []
    for peer in peers_str.split(","):
        peer = peer.strip()
        if ":" in peer:
            host, port = peer.rsplit(":", 1)
            peers.append({
                "host": host,
                "port": int(port),
                "server_id": f"server{len(peers) + 2}"  # server2, server3, ...
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
        "peers": PEERS_LIST,
    }
