"""
Correlation ID и трассировка пути сообщения (issue B20, Этап 5.4).

Когда что-то ломается в распределённой системе, главный вопрос — «что именно
произошло с этим конкретным запросом на всём его пути?». Чтобы ответить, каждому
входящему запросу присваивается **correlation id**, который затем попадает во все
логи, порождённые при его обработке, и в ответы клиенту. Потом по одному id можно
`grep`-нуть весь путь: приём команды → запись в БД → репликация → ответ/ошибка.

Реализация опирается на ``contextvars``: id живёт в контексте текущей asyncio-задачи.
Так его не нужно протаскивать параметром через все функции — любой ``logging`` во
время обработки команды сам подхватит id через ``CorrelationIdFilter``. Поскольку
обработка одной команды (включая рассылку на реплики) идёт в одной задаче, id
автоматически покрывает и логи репликации.
"""

import contextvars
import logging
import uuid

# id текущего запроса; пустая строка — вне контекста запроса (фон, старт и т.п.).
_correlation_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default=""
)


def new_correlation_id() -> str:
    """Короткий уникальный id (12 hex-символов достаточно для трассировки)."""
    return uuid.uuid4().hex[:12]


def get_correlation_id() -> str:
    """Текущий correlation id (пустая строка, если вне контекста запроса)."""
    return _correlation_id.get()


def set_correlation_id(cid: str) -> contextvars.Token:
    """Устанавливает id для текущего контекста. Вернёт токен для сброса."""
    return _correlation_id.set(cid)


def reset_correlation_id(token: contextvars.Token) -> None:
    """Возвращает предыдущее значение id (парно к set_correlation_id)."""
    _correlation_id.reset(token)


class CorrelationIdFilter(logging.Filter):
    """
    Проставляет ``record.correlation_id`` каждой записи лога из текущего
    контекста. Навешивается на handler, поэтому покрывает все логгеры.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = get_correlation_id()
        return True
