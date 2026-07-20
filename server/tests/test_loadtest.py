"""
Тесты матчасти нагрузочного теста (scripts/loadtest.py, Этап 5.3).

Проверяем чистые функции статистики (перцентили/сводка) — сам прогон нагрузки
это скрипт, его гоняют вручную.
"""

import sys
from pathlib import Path

# scripts/ не пакет — добавляем в путь, чтобы импортировать loadtest как модуль.
# Импорт безопасен: весь запуск скрипта под `if __name__ == "__main__"`.
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

import loadtest


class TestPercentile:
    def test_empty(self):
        assert loadtest.percentile([], 50) == 0.0

    def test_bounds(self):
        s = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        assert loadtest.percentile(s, 0) == 10     # нижняя граница
        assert loadtest.percentile(s, 100) == 100  # верхняя граница

    def test_nearest_rank(self):
        s = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        assert loadtest.percentile(s, 50) == 50
        assert loadtest.percentile(s, 90) == 90
        assert loadtest.percentile(s, 95) == 100  # ceil(9.5)=10 → последний
        assert loadtest.percentile(s, 99) == 100

    def test_single_sample(self):
        assert loadtest.percentile([42], 50) == 42
        assert loadtest.percentile([42], 99) == 42


class TestStats:
    def test_summary_basic(self):
        st = loadtest.Stats()
        for v in (10, 20, 30, 40):
            st.add(v)
        st.add_error()

        s = st.summary()
        assert s["count"] == 4
        assert s["errors"] == 1
        assert s["mean_ms"] == 25.0
        assert s["p50_ms"] == 20      # ceil(0.5*4)=2 → index1
        assert s["p95_ms"] == 40      # ceil(0.95*4)=4 → index3
        assert s["max_ms"] == 40

    def test_summary_empty(self):
        s = loadtest.Stats().summary()
        assert s["count"] == 0
        assert s["errors"] == 0
        assert s["mean_ms"] == 0.0
        assert s["p95_ms"] == 0.0
