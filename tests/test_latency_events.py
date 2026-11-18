from common import latency

from unittest.mock import Mock
import json
import pytest


def test_worker_timings_are_correlated_milliseconds_without_order_contents(monkeypatch):
    events = []
    monkeypatch.setattr(latency._log, "info", lambda fmt, msg: events.append(json.loads(msg)))
    command = {"command_id": "synthetic-id", "private": "never-log-me"}
    latency.received(command, Mock(timestamp=lambda: (1, 1700000000000)), 1700000000123)
    with latency.span("command_execution", "cancel", command):
        with pytest.raises(RuntimeError):
            with latency.span("broker_request", "cancel"):
                raise RuntimeError("never-log-me")
    receive, request, dispatch = events
    assert receive["pickup_ms"] == 123
    assert receive["command_tag"] == request["command_tag"] == dispatch["command_tag"]
    assert request["outcome"] == "RuntimeError" and request["elapsed_ms"] >= 0
    assert isinstance(dispatch["start_ms"], int) and isinstance(dispatch["end_ms"], int)
    assert "never-log-me" not in json.dumps(events)
