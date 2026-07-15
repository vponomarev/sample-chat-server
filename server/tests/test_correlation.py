"""
Тесты correlation id и трассировки (observability/correlation.py, Этап 5.4).
"""

import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from observability.correlation import (
    CorrelationIdFilter,
    get_correlation_id,
    new_correlation_id,
    reset_correlation_id,
    set_correlation_id,
)
from observability.logger import JSONFormatter


def _record(msg="hello"):
    return logging.LogRecord("test", logging.INFO, "path", 1, msg, None, None)


class TestCorrelationId:
    def test_new_id_is_short_hex_and_unique(self):
        a = new_correlation_id()
        b = new_correlation_id()
        assert len(a) == 12
        assert all(c in "0123456789abcdef" for c in a)
        assert a != b

    def test_set_get_reset_roundtrip(self):
        assert get_correlation_id() == ""  # вне контекста — пусто
        token = set_correlation_id("abc123")
        try:
            assert get_correlation_id() == "abc123"
        finally:
            reset_correlation_id(token)
        assert get_correlation_id() == ""

    def test_filter_stamps_current_id(self):
        rec = _record()
        token = set_correlation_id("cid-777")
        try:
            assert CorrelationIdFilter().filter(rec) is True
            assert rec.correlation_id == "cid-777"
        finally:
            reset_correlation_id(token)

    def test_json_formatter_includes_id_when_set(self):
        rec = _record()
        token = set_correlation_id("cid-abc")
        try:
            CorrelationIdFilter().filter(rec)
            out = json.loads(JSONFormatter().format(rec))
        finally:
            reset_correlation_id(token)
        assert out["correlation_id"] == "cid-abc"

    def test_json_formatter_omits_id_when_empty(self):
        rec = _record()
        CorrelationIdFilter().filter(rec)  # контекст пуст → ""
        out = json.loads(JSONFormatter().format(rec))
        assert "correlation_id" not in out
