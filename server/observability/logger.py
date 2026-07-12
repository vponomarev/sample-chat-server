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


def setup_logging(level: str = "INFO", fmt: str = "json"):
    """
    Настройка логирования.

    :param level: уровень (INFO/DEBUG/WARNING/…), из config.yaml или env LOG_LEVEL.
    :param fmt: "json" — структурные логи; иначе — простой текстовый формат.
    """
    log_level = getattr(logging, str(level).upper(), logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()  # идемпотентность при повторном вызове

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    if str(fmt).lower() == "json":
        console_handler.setFormatter(JSONFormatter())
    else:
        console_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
    root_logger.addHandler(console_handler)

    # Логгер для приложения
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
