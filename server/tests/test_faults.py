"""
Тесты инъекции сетевых сбоев (cluster/faults.py, Этап 5.2).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from cluster.faults import ChaosConfig, chaos_trace_configs, _DROP_HANG_SEC


class TestChaosConfig:
    def test_disabled_by_default(self):
        c = ChaosConfig()
        assert c.enabled is False
        # Выключено → сон не добавляется ни при каком roll.
        assert c.delay_seconds(0.0) == 0.0
        assert c.delay_seconds(0.999) == 0.0

    def test_latency_only(self):
        c = ChaosConfig(latency_ms=200)
        assert c.enabled is True
        assert c.delay_seconds(0.5) == 0.2  # 200мс задержки, без потерь

    def test_drop_always(self):
        c = ChaosConfig(drop_pct=100)
        assert c.enabled is True
        # Любой roll < 1.0 → 100% попадает в «потерю» (подвешивание).
        assert c.delay_seconds(0.0) == _DROP_HANG_SEC
        assert c.delay_seconds(0.99) == _DROP_HANG_SEC

    def test_drop_probability_boundary(self):
        c = ChaosConfig(latency_ms=50, drop_pct=10)
        # roll*100 < 10 → потеря; иначе — обычная задержка.
        assert c.delay_seconds(0.05) == _DROP_HANG_SEC   # 5 < 10 → drop
        assert c.delay_seconds(0.10) == 0.05             # 10 < 10 ложно → latency
        assert c.delay_seconds(0.5) == 0.05

    def test_clamping(self):
        c = ChaosConfig(latency_ms=-5, drop_pct=250)
        assert c.latency_ms == 0
        assert c.drop_pct == 100.0

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("CHAOS_LATENCY_MS", "150")
        monkeypatch.setenv("CHAOS_DROP_PCT", "25")
        c = ChaosConfig.from_env()
        assert c.latency_ms == 150
        assert c.drop_pct == 25.0

    def test_from_env_invalid_falls_back(self, monkeypatch):
        monkeypatch.setenv("CHAOS_LATENCY_MS", "not-a-number")
        c = ChaosConfig.from_env()
        assert c.latency_ms == 0


class TestChaosTraceConfigs:
    def test_empty_when_disabled(self, monkeypatch):
        monkeypatch.delenv("CHAOS_LATENCY_MS", raising=False)
        monkeypatch.delenv("CHAOS_DROP_PCT", raising=False)
        assert chaos_trace_configs() == []

    def test_one_trace_when_enabled(self, monkeypatch):
        import aiohttp
        monkeypatch.setenv("CHAOS_LATENCY_MS", "100")
        configs = chaos_trace_configs()
        assert len(configs) == 1
        assert isinstance(configs[0], aiohttp.TraceConfig)
