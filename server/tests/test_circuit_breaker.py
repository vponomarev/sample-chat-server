"""
Тесты circuit breaker (cluster/circuit_breaker.py, Этап 3.2).

Часы инъектируются через ``time_func``, поэтому переходы по времени проверяются
детерминированно, без реального сна.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from cluster.circuit_breaker import CircuitBreaker, CircuitState


class _Clock:
    """Управляемые часы для тестов."""

    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


class TestCircuitBreaker:
    def _breaker(self, **kw):
        clock = _Clock()
        br = CircuitBreaker(failure_threshold=3, reset_timeout=10.0,
                            time_func=clock, **kw)
        return br, clock

    def test_starts_closed_and_allows(self):
        br, _ = self._breaker()
        assert br.state == CircuitState.CLOSED
        assert br.allow() is True

    def test_opens_after_threshold_failures(self):
        br, _ = self._breaker()
        for _ in range(3):
            br.record_failure()
        assert br.state == CircuitState.OPEN
        assert br.allow() is False

    def test_success_resets_failure_count(self):
        br, _ = self._breaker()
        br.record_failure()
        br.record_failure()
        br.record_success()  # счётчик обнулён
        br.record_failure()
        br.record_failure()
        # Всего 2 подряд после сброса — цепь ещё замкнута
        assert br.state == CircuitState.CLOSED

    def test_half_open_after_timeout(self):
        br, clock = self._breaker()
        for _ in range(3):
            br.record_failure()
        assert br.allow() is False  # ещё рано
        clock.advance(10.0)
        # Пауза истекла — пропускаем пробный вызов, состояние HALF_OPEN
        assert br.allow() is True
        assert br.state == CircuitState.HALF_OPEN

    def test_half_open_success_closes(self):
        br, clock = self._breaker()
        for _ in range(3):
            br.record_failure()
        clock.advance(10.0)
        br.allow()  # → HALF_OPEN
        br.record_success()
        assert br.state == CircuitState.CLOSED
        assert br.allow() is True

    def test_half_open_failure_reopens(self):
        br, clock = self._breaker()
        for _ in range(3):
            br.record_failure()
        clock.advance(10.0)
        br.allow()  # → HALF_OPEN
        br.record_failure()  # пробный вызов снова упал
        assert br.state == CircuitState.OPEN
        assert br.allow() is False
