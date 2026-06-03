"""Prometheus метрики."""

from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry

# Создаём отдельный реестр для наших метрик
registry = CollectorRegistry()

# === Подключения ===
irc_connected_clients = Gauge(
    "irc_connected_clients",
    "Количество подключённых клиентов",
    labelnames=["server"],
    registry=registry,
)

irc_websocket_connections_total = Counter(
    "irc_websocket_connections_total",
    "Общее количество WebSocket подключений",
    labelnames=["server"],
    registry=registry,
)

# === Сообщения ===
irc_messages_total = Counter(
    "irc_messages_total",
    "Общее количество сообщений",
    labelnames=["server", "room"],
    registry=registry,
)

# === Комнаты ===
irc_rooms_active = Gauge(
    "irc_rooms_active",
    "Количество активных комнат",
    labelnames=["server"],
    registry=registry,
)

irc_rooms_total = Gauge(
    "irc_rooms_total",
    "Общее количество комнат",
    labelnames=["server"],
    registry=registry,
)

irc_room_members = Gauge(
    "irc_room_members",
    "Количество участников в комнате",
    labelnames=["server", "room"],
    registry=registry,
)

# === Производительность команд ===
irc_command_duration_seconds = Histogram(
    "irc_command_duration_seconds",
    "Время выполнения команд",
    labelnames=["cmd"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
    registry=registry,
)

# === Кластер ===
cluster_is_master = Gauge(
    "cluster_is_master",
    "Является ли сервер master (1 или 0)",
    labelnames=["server"],
    registry=registry,
)

cluster_is_slave = Gauge(
    "cluster_is_slave",
    "Является ли сервер slave (1 или 0)",
    labelnames=["server"],
    registry=registry,
)

cluster_replication_lag = Gauge(
    "cluster_replication_lag",
    "Отставание репликации (количество записей WAL)",
    labelnames=["server"],
    registry=registry,
)

cluster_uptime_seconds = Gauge(
    "cluster_uptime_seconds",
    "Время работы сервера в секундах",
    labelnames=["server"],
    registry=registry,
)

cluster_peers_alive = Gauge(
    "cluster_peers_alive",
    "Количество живых пиров",
    labelnames=["server"],
    registry=registry,
)


def setup_metrics():
    """Инициализация метрик."""
    pass


def update_connected_clients(count: int, server: str = "server1"):
    """Обновление количества подключённых клиентов."""
    irc_connected_clients.labels(server=server).set(count)


def increment_websocket_connections(server: str = "server1"):
    """Инкремент счётчика подключений."""
    irc_websocket_connections_total.labels(server=server).inc()


def increment_messages(room: str, server: str = "server1"):
    """Инкремент счётчика сообщений."""
    irc_messages_total.labels(server=server, room=room).inc()


def update_room_members(room: str, count: int, server: str = "server1"):
    """Обновление количества участников в комнате."""
    irc_room_members.labels(server=server, room=room).set(count)


def update_rooms_active(count: int, server: str = "server1"):
    """Обновление количества активных комнат."""
    irc_rooms_active.labels(server=server).set(count)


def update_rooms_total(count: int, server: str = "server1"):
    """Обновление общего количества комнат."""
    irc_rooms_total.labels(server=server).set(count)


def update_cluster_is_master(is_master: bool, server: str = "server1"):
    """Обновление статуса master."""
    value = 1 if is_master else 0
    cluster_is_master.labels(server=server).set(value)
    cluster_is_slave.labels(server=server).set(1 - value)


def update_replication_lag(lag: int, server: str = "server1"):
    """Обновление отставания репликации."""
    cluster_replication_lag.labels(server=server).set(lag)


def update_uptime(uptime: float, server: str = "server1"):
    """Обновление времени работы."""
    cluster_uptime_seconds.labels(server=server).set(uptime)


def update_peers_alive(count: int, server: str = "server1"):
    """Обновление количества живых пиров."""
    cluster_peers_alive.labels(server=server).set(count)
