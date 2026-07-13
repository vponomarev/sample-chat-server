"""
Circuit breaker для вызовов к пирам (issue B11, Этап 3.2).

Когда пир недоступен, бессмысленно долбить его каждым запросом: каждый вызов
ждёт таймаут, копятся зависшие соединения, а мастер тормозит на обслуживании
живых клиентов. Паттерн «предохранитель» (circuit breaker) размыкает цепь после
серии ошибок и какое-то время вообще не пытается звонить, давая пиру время
восстановиться, а вызывающему — быстро получать отказ.

Три состояния:

* ``CLOSED`` — норма, вызовы проходят. Считаем подряд идущие ошибки.
* ``OPEN`` — после ``failure_threshold`` ошибок цепь разомкнута: вызовы
  мгновенно отклоняются (``allow()`` → False) в течение ``reset_timeout``.
* ``HALF_OPEN`` — по истечении паузы пропускаем пробный вызов. Успех → ``CLOSED``,
  ошибка → снова ``OPEN``.

Класс синхронный и не привязан к транспорту: вызывающий сам решает, звонить ли
(``allow()``), и сообщает исход (``record_success()`` / ``record_failure()``).
Часы инъектируются (``time_func``) — это делает поведение детерминированным в
тестах и совместимым со средой, где ``time.monotonic`` использовать нельзя.
"""

import logging
import time


class CircuitState:
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Предохранитель для одного адресата (например, одного пира)."""

    def __init__(
        self,
        name: str = "peer",
        failure_threshold: int = 3,
        reset_timeout: float = 10.0,
        time_func=time.monotonic,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self._now = time_func

        self._state = CircuitState.CLOSED
        self._failures = 0
        self._opened_at = 0.0

    @property
    def state(self) -> str:
        return self._state

    def allow(self) -> bool:
        """
        Можно ли делать вызов прямо сейчас. Побочно переводит OPEN → HALF_OPEN,
        когда истекла пауза (чтобы пропустить пробный вызов).
        """
        if self._state == CircuitState.OPEN:
            if self._now() - self._opened_at >= self.reset_timeout:
                self._transition(CircuitState.HALF_OPEN)
                return True
            return False
        # CLOSED и HALF_OPEN пропускают вызов.
        return True

    def record_success(self):
        """Успешный вызов: сбрасываем счётчик и замыкаем цепь."""
        self._failures = 0
        if self._state != CircuitState.CLOSED:
            self._transition(CircuitState.CLOSED)

    def record_failure(self):
        """
        Ошибка вызова. В HALF_OPEN любая ошибка сразу размыкает цепь; в CLOSED
        размыкаем по достижении порога подряд идущих ошибок.
        """
        self._failures += 1
        if self._state == CircuitState.HALF_OPEN:
            self._open()
        elif self._failures >= self.failure_threshold:
            self._open()

    def _open(self):
        self._opened_at = self._now()
        self._transition(CircuitState.OPEN)

    def _transition(self, new_state: str):
        if new_state != self._state:
            logging.info(
                "[CircuitBreaker:%s] %s → %s", self.name, self._state, new_state
            )
            self._state = new_state
