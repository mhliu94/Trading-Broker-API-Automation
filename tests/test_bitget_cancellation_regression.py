"""November 2 cancel-all regression: real adapter/runner, mocked external I/O."""
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from bitget.adapter import Adapter, PREFIX
from common.consumer import consume
from common.errors import AuthenticationError, BrokerError, OutcomeUnknown
from common.runner import Runner


def order(identity="old", **changes):
    return {"orderId": identity, "symbol": "AAPL.US", "status": "New",
            "submittedAt": "2025-01-01T14:00:00Z", **changes}


@pytest.mark.parametrize("count", [0, 1, 53, 999])
def test_history_accepts_short_live_pages_without_has_more(count):
    adapter = Adapter({})
    rows = [order(str(i)) for i in range(count)]
    adapter._request = Mock(return_value={"orders": rows})
    assert adapter._history(0, 100) == rows
    adapter._request.assert_called_once_with(
        "GET", PREFIX + "/trade/history-orders", {"market": "US", "startAt": 0, "endAt": 100})


def test_full_page_without_flag_splits_and_retains_both_boundaries():
    adapter = Adapter({})
    adapter._request = Mock(side_effect=[
        {"orders": [order(str(i)) for i in range(1000)]},
        {"orders": [order("left")]},
        {"orders": [order("right")], "hasMore": False},
    ])
    assert [row["orderId"] for row in adapter._history(0, 3)] == ["left", "right"]
    assert [(call.args[2]["startAt"], call.args[2]["endAt"])
            for call in adapter._request.call_args_list] == [(0, 3), (0, 1), (2, 3)]


def test_full_single_second_without_flag_cannot_claim_completeness():
    adapter = Adapter({})
    adapter._request = Mock(return_value={"orders": [order(str(i)) for i in range(1000)]})
    with pytest.raises(BrokerError, match="completeness within one second"):
        adapter._history(42, 42)
    assert adapter._request.call_count == 1


def test_explicit_false_proves_full_single_second_is_complete():
    adapter = Adapter({})
    rows = [order(str(i)) for i in range(1000)]
    adapter._request = Mock(return_value={"orders": rows, "hasMore": False})
    assert adapter._history(42, 42) == rows


@pytest.mark.parametrize("flag", [None, 0, 1, "false", "true", []])
def test_nonempty_history_rejects_explicit_malformed_pagination(flag):
    adapter = Adapter({})
    adapter._request = Mock(return_value={"orders": [order()], "hasMore": flag})
    with pytest.raises(BrokerError, match="invalid boolean hasMore"):
        adapter._history(0, 3)


@pytest.fixture
def runner(tmp_path):
    result = Runner("bitget", {
        "journal_account": "regression-only", "trading_enabled": True,
        "routing": {"account_id": "TEST_BITGET"},
    }, execute=True, journal_path=tmp_path / "bitget.sqlite3")
    yield result
    result.close()


def cancel_command(identity="cancel"):
    return {"id": identity, "type": "CANCEL_OPEN_ORDERS", "account_id": "TEST_BITGET"}


def test_cancel_all_missing_flag_finds_old_gtc_and_cancels_only_active_stock(runner):
    old = order()
    runner.adapter._request = Mock(side_effect=[
        {"orders": []},
        {"orders": [old, order("done", status="Filled"), order("foreign", symbol="700.HK")]},
        old,
        {"list": [{"symbol": "AAPL.US", "currency": "USD", "board": "USMain"}]},
        {"orderId": "old", "symbol": "AAPL.US"},
    ])
    result = runner.process(cancel_command())
    assert result["status"] == "cancel_requested"
    assert result["data"]["cancellations_requested"] == 1
    assert runner.journal.unresolved(runner.scope) == []
    calls = runner.adapter._request.call_args_list
    assert [call.args[0] for call in calls] == ["GET", "GET", "GET", "GET", "POST"]
    assert calls[1].args[2]["startAt"] == 0
    assert calls[-1].kwargs["payload"] == {"orderId": "old", "symbol": "AAPL.US"}
    assert runner.process(cancel_command())["duplicate"] is True
    assert runner.adapter._request.call_count == 5


def test_smart_algo_symbol_cancel_reaches_only_matching_broker_order_and_deduplicates(runner):
    target = order("pom", symbol="POM.US")
    other = order("other", symbol="AAPL.US")
    runner.adapter._request = Mock(side_effect=[
        {"orders": [target, other]}, {"orders": [target, other]}, target,
        {"list": [{"symbol": "POM.US", "currency": "USD", "board": "USMain"}]},
        {"orderId": "pom", "symbol": "POM.US"},
    ])
    command = {**cancel_command("smart-stop"), "symbol": "POM",
               "parent_id": "parent", "trading_mode": "H"}
    result = runner.process(command, require_account=True)
    assert result["status"] == "cancel_requested"
    assert result["data"]["cancellations_requested"] == 1
    assert "POM.US" in result["data"]["scope"]
    writes = [call for call in runner.adapter._request.call_args_list if call.args[0] == "POST"]
    assert len(writes) == 1
    assert writes[0].kwargs["payload"] == {"orderId": "pom", "symbol": "POM.US"}
    assert runner.process(command, require_account=True)["duplicate"] is True
    assert runner.adapter._request.call_count == 5


@pytest.mark.parametrize("failure", [
    BrokerError("Malformed history"), OutcomeUnknown("Read timed out"), ValueError("private-detail"),
])
def test_discovery_failure_is_rejected_without_poisoning_journal(runner, failure):
    runner.adapter._request = Mock(side_effect=[{"orders": []}, failure])
    result = runner.process(cancel_command())
    assert result["status"] == "rejected"
    assert result["submission_outcome"] == "not_submitted" and result["requests_sent"] == 0
    assert result["error_type"] == "CancellationDiscoveryError"
    assert "private-detail" not in result["error"]
    assert runner.journal.unresolved(runner.scope) == [] and runner.trading_enabled()
    assert all(call.args[0] == "GET" for call in runner.adapter._request.call_args_list)
    assert runner.process(cancel_command())["duplicate"] is True
    assert runner.adapter._request.call_count == 2
    runner.adapter._request = Mock(side_effect=[{"orders": []}, {"orders": []}])
    assert runner.process(cancel_command("fresh-cancel"))["status"] == "ok"


def run_consumer(monkeypatch, runner, max_messages=2):
    import kafka
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    payloads = [cancel_command(), {"id": "next", "type": "GET_CAPABILITIES", "account_id": "TEST_BITGET"}]
    records = [SimpleNamespace(topic="trading-commands", partition=0, offset=i,
                               timestamp=now, value=json.dumps(payload).encode())
               for i, payload in enumerate(payloads)]
    consumer = Mock()
    consumer.poll.side_effect = [{"partition": [record]} for record in records]
    monkeypatch.setattr(kafka, "KafkaConsumer", Mock(return_value=consumer))
    producer = Mock()
    monkeypatch.setattr(kafka, "KafkaProducer", producer)
    consume(runner, {"kafka": {"publish_snapshots": False}}, max_messages=max_messages)
    producer.assert_not_called()
    consumer.close.assert_called_once()
    return consumer


@pytest.mark.parametrize("history", [
    {"orders": [order(str(i), status="Filled") for i in range(53)]},
    {"orders": [order()], "hasMore": "false"},
    OutcomeUnknown("Read timed out"),
])
def test_worker_processes_next_command_after_empty_scope_or_discovery_failure(
        runner, monkeypatch, capsys, history):
    runner.adapter._request = Mock(side_effect=[{"orders": []}, history])
    consumer = run_consumer(monkeypatch, runner)
    assert consumer.poll.call_count == consumer.commit.call_count == 2
    assert [next(iter(call.args[0].values())).offset for call in consumer.commit.call_args_list] == [1, 2]
    messages = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    results = {item["id"]: item for item in messages if "id" in item}
    assert results["cancel"]["status"] == ("ok" if isinstance(history, dict) and "hasMore" not in history else "rejected")
    assert results["next"]["status"] == "ok"
    assert runner.journal.unresolved(runner.scope) == []
    health = json.loads(runner.journal_path.with_suffix(".status.json").read_text())
    assert health["processed"] == 2 and health["status"] == "stopped"
    assert all(call.args[0] == "GET" for call in runner.adapter._request.call_args_list)


@pytest.mark.parametrize("successful_writes", [0, 1])
def test_worker_continues_and_preserves_unknown_actual_cancellation(
        runner, monkeypatch, capsys, successful_writes):
    entries = [order("01"), order("02"), order("03")]
    responses = [{"orders": entries}, {"orders": []}]
    for index in range(successful_writes + 1):
        responses.extend([
            entries[index],
            {"list": [{"symbol": "AAPL.US", "currency": "USD", "board": "USMain"}]},
            {"orderId": entries[index]["orderId"], "symbol": "AAPL.US"}
            if index < successful_writes else OutcomeUnknown("Cancellation write timed out"),
        ])
    responses.extend(entries[:successful_writes + 1])
    runner.adapter._request = Mock(side_effect=responses)
    consumer = run_consumer(monkeypatch, runner)
    assert consumer.poll.call_count == consumer.commit.call_count == 2
    unresolved = runner.journal.unresolved(runner.scope)
    assert len(unresolved) == 1 and unresolved[0]["state"] == "unknown"
    assert runner.process(cancel_command())["duplicate"] is True
    assert sum(call.args[0] == "POST" for call in runner.adapter._request.call_args_list) == successful_writes + 1
    health = json.loads(runner.journal_path.with_suffix(".status.json").read_text())
    assert health["status"] == "reconciliation_required"
    messages = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    result = next(item for item in messages if item.get("id") == "cancel")
    assert result["data"]["not_requested_order_ids"] == [entry["orderId"] for entry in entries[successful_writes + 1:]]
    assert result["data"]["cancellations_requested"] == successful_writes


def test_discovery_rejection_survives_restart_without_blocking_or_replaying(runner):
    runner.adapter._request = Mock(side_effect=BrokerError("Read failed"))
    assert runner.process(cancel_command())["status"] == "rejected"
    restarted = Runner("bitget", runner.config, execute=True, journal_path=runner.journal_path)
    try:
        restarted.adapter._request = Mock(side_effect=[{"orders": []}, {"orders": []}])
        cached = restarted.process(cancel_command())
        assert cached["status"] == "rejected" and cached["duplicate"] is True
        restarted.adapter._request.assert_not_called()
        assert restarted.journal.unresolved(restarted.scope) == []
        assert restarted.process(cancel_command("new-cancel"))["status"] == "ok"
    finally:
        restarted.close()


def test_existing_unknown_cancellation_is_not_cleared_by_upgrade(runner, monkeypatch):
    command = {"id": "old-unknown", "action": "cancel_open_orders", "broker": "bitget"}
    runner.journal.reserve(runner.scope, command)
    runner.journal.finish(runner.scope, command["id"], {"id": command["id"], "status": "unknown"})
    runner.adapter._request = Mock()
    consumer = run_consumer(monkeypatch, runner)
    assert consumer.poll.call_count == consumer.commit.call_count == 2
    assert all(call.args[0] == "GET" for call in runner.adapter._request.call_args_list)
    assert runner.journal.unresolved(runner.scope)[0]["state"] == "unknown"
    health = json.loads(runner.journal_path.with_suffix(".status.json").read_text())
    assert health["status"] == "reconciliation_required"


def test_discovery_authentication_failure_still_stops_intake(runner, monkeypatch):
    runner.adapter._request = Mock(side_effect=AuthenticationError("Access denied", no_submission=True))
    consumer = run_consumer(monkeypatch, runner)
    assert consumer.poll.call_count == 1
    consumer.commit.assert_not_called()
    health = json.loads(runner.journal_path.with_suffix(".status.json").read_text())
    assert health["status"] == "authentication_required"
