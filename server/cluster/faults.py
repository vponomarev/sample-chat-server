"""
Инъекция сетевых сбоев для хаос-тестирования (issue B22, Этап 5.2).

Чтобы проверить отказоустойчивость, мало ронять процессы — надо ещё уметь
портить сеть между узлами: добавлять задержки и «терять» пакеты. OS-уровневые
инструменты (`tc netem` на Linux, `pfctl`/`dnctl` на macOS) требуют root и
непереносимы, поэтому для учебного стенда делаем инъекцию на уровне приложения:
через aiohttp ``TraceConfig`` на межсерверных HTTP-сессиях кластера.

Управление — переменными окружения (по умолчанию всё выключено, накладных
расходов ноль — TraceConfig вообще не навешивается):

  * ``CHAOS_LATENCY_MS`` — добавочная задержка к каждому исходящему кластерному
    запросу, мс;
  * ``CHAOS_DROP_PCT`` — доля запросов (0..100), которые «теряются»: запрос
    подвешивается дольше таймаута сессии, и клиент получает таймаут — ровно так
    приложение видит потерю пакетов.

Инъекция затрагивает только межсерверный трафик (heartbeat/выборы/репликация),
не клиентские соединения — цель в том, чтобы наблюдать поведение кластера
(выборы, догон, кворум) под деградацией сети.
"""

import asyncio
import logging
import os
import random
from typing import List, Optional

import aiohttp

# На сколько «подвесить» потерянный запрос: заведомо больше любого таймаута
# кластерной сессии (heartbeat 5s, выборы 5s, репликация 10s), чтобы сработал
# таймаут клиента. Реальная задержка не наступит — запрос отменят по таймауту.
_DROP_HANG_SEC = 3600.0


class ChaosConfig:
    """Параметры инъекции сетевых сбоев, считанные из окружения."""

    def __init__(self, latency_ms: int = 0, drop_pct: float = 0.0):
        self.latency_ms = max(0, int(latency_ms))
        self.drop_pct = min(100.0, max(0.0, float(drop_pct)))

    @classmethod
    def from_env(cls) -> "ChaosConfig":
        return cls(
            latency_ms=_int_env("CHAOS_LATENCY_MS", 0),
            drop_pct=_float_env("CHAOS_DROP_PCT", 0.0),
        )

    @property
    def enabled(self) -> bool:
        return self.latency_ms > 0 or self.drop_pct > 0

    def delay_seconds(self, roll: float) -> float:
        """
        По случайной величине ``roll`` из [0, 1) возвращает, сколько секунд
        «спать» перед запросом: большое значение = потеря (сработает таймаут),
        latency = задержка, 0 = без вмешательства. Чистая функция — тестируемо.
        """
        if self.drop_pct > 0 and roll * 100.0 < self.drop_pct:
            return _DROP_HANG_SEC
        return self.latency_ms / 1000.0


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def chaos_trace_configs() -> List[aiohttp.TraceConfig]:
    """
    Список TraceConfig для кластерных ClientSession. Пустой, если хаос выключен —
    тогда сессии работают без накладных расходов. Иначе один TraceConfig,
    добавляющий задержку/потерю перед каждым запросом.
    """
    config = ChaosConfig.from_env()
    if not config.enabled:
        return []

    logging.warning(
        "[Chaos] Инъекция сетевых сбоев включена: latency=%dms, drop=%.0f%%",
        config.latency_ms, config.drop_pct,
    )

    async def _on_request_start(session, context, params):
        delay = config.delay_seconds(random.random())
        if delay > 0:
            await asyncio.sleep(delay)

    trace = aiohttp.TraceConfig()
    trace.on_request_start.append(_on_request_start)
    return [trace]
