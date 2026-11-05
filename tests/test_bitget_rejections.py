"""Definite rejections must not poison the journal or terminate intake."""
from datetime import datetime, timezone
import json
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
import requests

from bitget.adapter import Adapter, PREFIX
from common.consumer import consume
from common.errors import OutcomeUnknown, RequestRejected
from common.runner import Runner
from common import timed_cancel
from test_bitget import command, instrument


def response(status=400, code="40017", msg="Overnight market orders are not supported", **extra):
    result = Mock(status_code=status)
    result.json.return_value = {"code": code, "msg": msg, "data": None, **extra}
    return result


@pytest.fixture
def runner(tmp_path):
    instance = Runner("bitget", {
        "journal_account": "rejections-test", "trading_enabled": True,
        "routing": {"account_id": "TEST"}, "api_key": "key-private-value",
        "api_secret": "secret-private-value", "passphrase": "pass-private-value",
        "cancel_confirmation_delays_seconds": [0], "cancel_after_seconds": 0,
    }, execute=True, journal_path=tmp_path / "bitget.sqlite3")
    instance.adapter._stock_instrument = Mock(return_value=instrument()["list"][0])
    yield instance
    instance.close()


def payload(identity="rejected-1", **updates):
    return command(id=identity, account_id="TEST", **updates)


@pytest.mark.parametrize("status,code", [(400, "40017"), (200, "40017"), (200, "25202"), (429, "429")])
def test_clear_rejection_is_cached_and_next_order_can_submit(runner, status, code):
    with patch("requests.request", side_effect=[response(status, code), response(200, "00000", "success", data={"orderId": "next-id"})]) as request:
        first = runner.process(payload())
        assert first["status"] == "rejected" and first["submission_outcome"] == "not_submitted"
        assert first["http_status"] == status and first["broker_error_code"] == code
        assert "Overnight market orders" in first["error"]
        assert runner.trading_enabled() and runner.journal.unresolved(runner.scope) == []
        duplicate = runner.process(payload())
        assert duplicate["status"] == "rejected" and duplicate["duplicate"] is True
        assert request.call_count == 1
        assert runner.process(payload("next"))["status"] == "submitted"
        assert request.call_count == 2


@pytest.mark.parametrize("reply", [
    response(400, "40010", "Request timed out"),
    response(200, "40725", "service return an error"),
    response(400, "45001", "Unknown error"),
    response(500, "40017", "Parameter error"),
    response(200, "79999", "Undocumented response"),
    response(400, "00000", "success", data={"orderId": "possibly-accepted"}),
    response(400, "40017", "Parameter error", data={"items": [{"orderId": "possibly-accepted"}]}),
    response(400, "79999", "Internal server error"),
    response(302, "40017", "Parameter error"),
    response(400, "25212", "Duplicate clientOid"),
])
def test_uncertain_response_still_blocks_new_orders_without_replay(runner, reply):
    with patch("requests.request", return_value=reply) as request:
        result = runner.process(payload())
        assert result["status"] == "unknown"
        assert not runner.trading_enabled() and len(runner.journal.unresolved(runner.scope)) == 1
        assert runner.process(payload())["duplicate"] is True
        assert request.call_count == 1


@pytest.mark.parametrize("status", [200, 400, 502])
def test_unreadable_response_cannot_be_mistaken_for_rejection(runner, status):
    reply = Mock(status_code=status)
    reply.json.side_effect = ValueError("not json")
    with patch("requests.request", return_value=reply):
        assert runner.process(payload())["status"] == "unknown"


def test_network_timeout_after_post_still_requires_reconciliation(runner):
    with patch("requests.request", side_effect=requests.Timeout) as request:
        assert runner.process(payload())["status"] == "unknown"
        assert request.call_count == 1 and not runner.trading_enabled()


def test_read_failure_before_post_records_zero_write_rejection(runner):
    runner.adapter._stock_instrument = Mock(side_effect=OutcomeUnknown("Quote request timed out"))
    with patch("requests.request") as request:
        result = runner.process(payload())
    request.assert_not_called()
    assert result["status"] == "rejected" and result["requests_sent"] == 0
    assert result["submission_outcome"] == "not_submitted" and runner.trading_enabled()


def test_error_message_preserves_reason_but_redacts_credentials(runner):
    message = "Invalid request " + " ".join(runner.config[k] for k in ["api_key", "api_secret", "passphrase"])
    with patch("requests.request", return_value=response(msg=message)):
        result = runner.process(payload())
    assert "Invalid request" in result["error"] and "[redacted]" in result["error"]
    assert all(runner.config[k] not in result["error"] for k in ["api_key", "api_secret", "passphrase"])


def test_worker_continues_to_next_order_after_rejection(runner, monkeypatch, capsys):
    import kafka
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    records = [SimpleNamespace(topic="trading-commands", partition=0, offset=i, timestamp=now,
                               value=json.dumps(payload(identity)).encode()) for i, identity in enumerate(["reject", "next"])]
    consumer = Mock()
    consumer.poll.side_effect = [{"partition": [record]} for record in records]
    monkeypatch.setattr(kafka, "KafkaConsumer", Mock(return_value=consumer))
    with patch("requests.request", side_effect=[response(), response(200, "00000", "success", data={"orderId": "next-id"})]):
        consume(runner, {"kafka": {"publish_snapshots": False}}, max_messages=2)
    results = {r["id"]: r for r in (json.loads(line) for line in capsys.readouterr().out.splitlines()) if "id" in r}
    assert results["reject"]["status"] == "rejected" and results["next"]["status"] == "submitted"
    assert consumer.poll.call_count == consumer.commit.call_count == 2
    assert runner.journal.unresolved(runner.scope) == []


def test_health_includes_the_rejection_reason(runner, monkeypatch):
    import kafka
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    record = SimpleNamespace(topic="trading-commands", partition=0, offset=0, timestamp=now,
                             value=json.dumps(payload()).encode())
    consumer = Mock()
    consumer.poll.return_value = {"partition": [record]}
    monkeypatch.setattr(kafka, "KafkaConsumer", Mock(return_value=consumer))
    with patch("requests.request", return_value=response()):
        consume(runner, {"kafka": {"publish_snapshots": False}}, max_messages=1)
    health = json.loads(runner.journal_path.with_suffix(".status.json").read_text())
    assert health["last_result"]["status"] == "rejected"
    assert health["last_result"]["broker_error_code"] == "40017"
    assert "Overnight market orders" in health["last_result"]["error"]
    assert health["status"] == "stopped" and runner.trading_enabled()


def test_live_incident_market_order_is_rejected_locally_and_limit_can_follow(runner):
    runner.adapter.config["default_outside_rth"] = "Auto"
    with patch("bitget.adapter.datetime") as clock, patch("requests.request") as request:
        clock.now.return_value = datetime.fromisoformat("2026-11-05T01:28:36+00:00")
        from common.errors import UnsupportedOrder
        with pytest.raises(UnsupportedOrder, match="explicit limit_price"):
            runner.process(payload(order_type="market", limit_price=None, symbol="AEHL", quantity="10"))
        request.assert_not_called()
        assert runner.journal.unresolved(runner.scope) == [] and runner.trading_enabled()
        request.return_value = response(200, "00000", "success", data={"orderId": "next-limit"})
        assert runner.process(payload("next-limit", symbol="AEHL"))["status"] == "submitted"
        assert request.call_count == 1


def test_timed_submission_rejection_is_terminal_without_cancellation(runner):
    runner.config["fok_policy"] = runner.adapter.config["fok_policy"] = "timed_cancel"
    with patch("requests.request", return_value=response()) as request:
        result = runner.process(payload(time_in_force="fok"))
    assert result["status"] == "rejected" and result["submission_outcome"] == "not_submitted"
    assert request.call_count == 1
    assert runner.unresolved_timed() == [] and runner.trading_enabled()
    state, raw = runner.journal.db.execute("SELECT state,record FROM timed_cancellations").fetchone()
    assert state == "complete" and json.loads(raw)["result"]["submission_outcome"] == "not_submitted"


def test_durable_timed_rejection_recovers_outer_pending_command(runner):
    runner.adapter.config["fok_policy"] = "timed_cancel"
    normalized = {k: v for k, v in command(time_in_force="fok").items() if k != "account_id"}
    normalized["asset_class"] = "us_equity"
    runner.journal.reserve(runner.scope, normalized)
    with patch("requests.request", return_value=response()):
        with pytest.raises(RequestRejected):
            timed_cancel.execute(runner.adapter, "bitget", normalized, runner.journal, runner.scope)
    runner.recover_pending()
    assert runner.journal.unresolved(runner.scope) == [] and runner.unresolved_timed() == []


def test_old_unexplained_unknown_is_not_automatically_cleared(runner):
    original = command()
    runner.journal.reserve(runner.scope, original)
    runner.journal.finish(runner.scope, original["id"], {"status": "unknown", "error": "Bitget returned HTTP 400"})
    runner.recover_pending()
    assert len(runner.journal.unresolved(runner.scope)) == 1
