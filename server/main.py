"""Точка входа сервера."""

import asyncio
import logging
from aiohttp import web

from config import get_config
from storage.database import Database
from network.ws_manager import ConnectionRegistry
from network.routes import setup_routes
from observability.logger import setup_logging
from observability.metrics import setup_metrics


async def on_startup(app: web.Application):
    """Инициализация при старте."""
    config = get_config()

    # База данных
    db = Database(config["db_path"])
    await db.connect()
    app["db"] = db

    # WebSocket менеджер для рассылки сообщений
    app["ws_manager"] = ConnectionRegistry()

    # Кластер (Фаза 3)
    if config.get("cluster_enabled") and config.get("peers"):
        from cluster.manager import ClusterManager
        
        cluster = ClusterManager(
            app=app,
            server_id=config["server_id"],
            host=config["host"],
            port=config["port"],
            peers=config["peers"],
            db_connection=db.connection,
            secret=config.get("cluster_secret", "")
        )
        app["cluster"] = cluster
        await cluster.start()
        
        logging.info(f"Кластер запущен: {config['server_id']}")
    else:
        app["cluster"] = None
        logging.info("Кластер отключён (одиночный сервер)")

    logging.info(f"Сервер запущен на {config['host']}:{config['port']}")


async def on_shutdown(app: web.Application):
    """Очистка при остановке."""
    # Остановка кластера
    cluster = app.get("cluster")
    if cluster:
        await cluster.stop()
    
    await app["db"].close()
    logging.info("Сервер остановлен")


def create_app() -> web.Application:
    """Создание и настройка приложения."""
    config = get_config()
    setup_logging(config["log_level"], config["log_format"])
    setup_metrics(config["server_id"])

    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    # Настройка маршрутов
    setup_routes(app)

    return app


def main():
    """Запуск сервера."""
    config = get_config()
    app = create_app()

    web.run_app(
        app,
        host=config["host"],
        port=config["port"],
        print=lambda x: logging.info(x),
    )


if __name__ == "__main__":
    main()
