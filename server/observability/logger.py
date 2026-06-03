"""Настройка логирования."""

import logging
import sys
import json


class JSONFormatter(logging.Formatter):
    """JSON форматтер для структурированного логирования."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "ts": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Добавляем дополнительные поля если есть
        if hasattr(record, "component"):
            log_data["component"] = record.component
        if hasattr(record, "event"):
            log_data["event"] = record.event
        if hasattr(record, "nick"):
            log_data["nick"] = record.nick
        if hasattr(record, "room"):
            log_data["room"] = record.room

        return json.dumps(log_data)


def setup_logging():
    """Настройка логирования."""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    # Используем JSON форматтер
    console_handler.setFormatter(JSONFormatter())
    root_logger.addHandler(console_handler)

    # Логгер для приложения
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
