"""Delayed cancellation status must not terminate the live listener or replay writes."""
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from common.commands import normalize
from common.consumer import consume
from common.errors import AuthenticationError, BrokerError, ConfigurationError, OutcomeUnknown
from common.runner import Runner


@pytest.fixture
def runner(tmp_path):
    worker = Runner("bitget", {"journal_account": "reconciliation-test", "trading_enabled": True,
        "routing": {"account_id": "TEST_BITGET"}}, execute=True,
        journal_path=tmp_path / "bitget.sqlite3")
    yield worker
    worker.close()


def seed(worker, targets=("001",), *, unrequested=()):
    payload = {"id": "cancel", "type": "CANCEL_OPEN_ORDERS", "account_id": "TEST_BITGET"}
    command = normalize(payload, "bitget", worker.config)
    worker.journal.reserve(worker.scope, command)
    worker.journal.finish(worker.scope, "cancel", {"id": "cancel", "action": "cancel_open_orders",
        "status": "unknown", "data": {"requests": [
            {"order_id": target, "symbol": "AAPL.US", "status": "outcome_unknown"} for target in targets],
            "not_requested_order_ids": list(unrequested)}})
    return payload


def detail(order_id="001", status="Canceled", **fields):
    return {"orderId": order_id, "symbol": "AAPL.US", "status": status,
            "submittedAt": "2026-11-05T18:00:00Z", "executedQuantity": 0, **fields}


@pytest.mark.parametrize("state,outcome", [("Canceled", "canceled"), ("Filled", "filled"),
    ("PartialWithdrawal", "canceled"), ("Rejected", "rejected"), ("Expired", "found")])
def test_terminal_evidence_reconciles_durably_without_any_write(runner, state, outcome):
    payload = seed(runner)
    runner.adapter.stock_order_detail = Mock(return_value=detail(status=state))
    runner.adapter.cancel_stock_order = Mock()
    runner.adapter.place_order = Mock()
    report = runner.reconcile_pending()
    assert report["status"] == "ok" and report["writes_sent"] == 0
    assert report["resolved"][0]["orders"][0]["status"] == state
    assert runner.journal.unresolved(runner.scope) == [] and runner.trading_enabled()
    cached = runner.process(payload)
    assert cached["duplicate"] and cached["outcome"] == outcome and not cached["orders_replayed"]
    assert json.loads(cached["evidence"])["orders"][0]["order_id"] == "001"
    runner.adapter.cancel_stock_order.assert_not_called()
    runner.adapter.place_order.assert_not_called()


@pytest.mark.parametrize("observed", [detail(status="PendingCancel"), detail(order_id="other"),
    detail(symbol="MSFT.US"), BrokerError("Read timeout"), ValueError("private transport detail")])
def test_nonterminal_mismatched_or_failed_read_keeps_uncertainty(runner, observed):
    seed(runner)
    runner.adapter.stock_order_detail = Mock(side_effect=observed) if isinstance(observed, Exception) else Mock(return_value=observed)
    report = runner.reconcile_pending()
    assert report["status"] == "pending" and not report["resolved"]
    assert len(runner.journal.unresolved(runner.scope)) == 1 and not runner.trading_enabled()
    assert "private transport detail" not in json.dumps(report)


def test_cancel_all_requires_all_attempted_targets_terminal_and_preserves_unrequested(runner):
    seed(runner, ("001", "002"), unrequested=("003",))
    runner.adapter.stock_order_detail = Mock(side_effect=[detail(), detail("002", "New")])
    assert runner.reconcile_pending()["status"] == "pending"
    runner.adapter.stock_order_detail.side_effect = [detail(), detail("002", "Filled")]
    assert runner.reconcile_pending()["status"] == "ok"
    cached = runner.process({"id": "cancel", "type": "CANCEL_OPEN_ORDERS", "account_id": "TEST_BITGET"})
    assert cached["previous_result"]["data"]["not_requested_order_ids"] == ["003"]
    assert len(json.loads(cached["evidence"])["orders"]) == 2


def run_worker(monkeypatch, worker, *, times=(10,), payload=None, snapshots=False):
    import kafka
    clock = [0.0]
    moments = iter(times)
    consumer = Mock()
    consumer.observed_statuses = []
    consumer.topics.return_value = {"account-details", "order-status"}
    payload = payload or {"id": "query", "type": "GET_CAPABILITIES", "account_id": "TEST_BITGET"}
    def poll(**kwargs):
        consumer.observed_statuses.append(json.loads(worker.journal_path.with_suffix(".status.json").read_text())["status"])
        try:
            clock[0] = next(moments)
            return {}
        except StopIteration:
            record = SimpleNamespace(topic="trading-commands", partition=0, offset=20,
                timestamp=int(datetime.now(timezone.utc).timestamp() * 1000), value=json.dumps(payload).encode())
            return {"partition": [record]}
    consumer.poll.side_effect = poll
    monkeypatch.setattr(kafka, "KafkaConsumer", Mock(return_value=consumer))
    monkeypatch.setattr(kafka, "KafkaProducer", Mock())
    monkeypatch.setattr("common.consumer.time.monotonic", lambda: clock[0])
    reporter = Mock()
    reporter.refresh.return_value = {"errors": []}
    monkeypatch.setattr("common.reporting.RuntimeSnapshots", Mock(return_value=reporter))
    publisher = Mock()
    monkeypatch.setattr("common.publication.SnapshotPublisher", Mock(return_value=publisher))
    consume(worker, {"kafka": {"publish_snapshots": snapshots, "snapshot_interval_seconds": 5}}, max_messages=1)
    consumer.close.assert_called_once()
    return consumer, reporter


def test_three_exponential_attempts_then_keep_polling_and_snapshots_alive(runner, monkeypatch, capsys):
    seed(runner)
    read_times = []
    def read(_):
        import common.consumer
        read_times.append(common.consumer.time.monotonic())
        return detail(status="PendingCancel")
    runner.adapter.stock_order_detail = Mock(side_effect=read)
    runner.adapter.cancel_stock_order = Mock()
    consumer, reporter = run_worker(monkeypatch, runner, times=(9, 10, 29, 30, 69, 70, 100), snapshots=True)
    assert read_times == [10, 30, 70]
    assert consumer.poll.call_count == 8 and consumer.commit.call_count == 1
    assert set(consumer.observed_statuses) == {"running"}
    assert any(call.args[0] == "periodic" for call in reporter.refresh.call_args_list)
    runner.adapter.cancel_stock_order.assert_not_called()
    health = json.loads(runner.journal_path.with_suffix(".status.json").read_text())
    assert health["reconciliation"]["status"] == "pending" and health["processed"] == 1
    assert health["reconciliation"]["retry_exhausted"] is True
    assert health["reconciliation"]["next_check_seconds"] is None
    messages = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    retries = [item for item in messages if item.get("event") == "order_reconciliation"]
    assert [item["attempt"] for item in retries] == [1, 2, 3]
    assert [item["next_check_seconds"] for item in retries] == [20, 40, None]
    assert next(item for item in messages if item.get("id") == "query")["status"] == "ok"


def test_third_status_check_unblocks_new_submissions_without_replaying_cancel(runner, monkeypatch):
    seed(runner)
    runner.adapter.stock_order_detail = Mock(side_effect=[detail(status="New"), detail(status="PendingCancel"), detail()])
    runner.adapter.place_order = Mock(return_value={"orderId": "new-order"})
    runner.adapter.cancel_stock_order = Mock()
    payload = {"id": "new", "type": "LIMIT_ORDER", "symbol": "AAPL", "side": "buy",
               "quantity": 1, "limit_price": 1, "account_id": "TEST_BITGET"}
    consumer, _ = run_worker(monkeypatch, runner, times=(10, 30, 70), payload=payload)
    assert runner.adapter.stock_order_detail.call_count == 3
    runner.adapter.place_order.assert_called_once()
    runner.adapter.cancel_stock_order.assert_not_called()
    assert consumer.commit.call_count == 1 and not runner.journal.unresolved(runner.scope)


def test_restart_automatically_reconciles_persisted_cancel(runner, monkeypatch):
    seed(runner)
    restarted = Runner("bitget", runner.config, execute=True, journal_path=runner.journal_path)
    try:
        restarted.adapter.stock_order_detail = Mock(return_value=detail())
        restarted.adapter.cancel_stock_order = Mock()
        run_worker(monkeypatch, restarted)
        assert restarted.journal.unresolved(restarted.scope) == []
        restarted.adapter.cancel_stock_order.assert_not_called()
    finally:
        restarted.close()


def test_unknown_without_exact_order_reference_stays_visible_and_worker_continues(runner, monkeypatch):
    seed(runner, targets=())
    runner.adapter.stock_order_detail = Mock()
    consumer, _ = run_worker(monkeypatch, runner)
    assert consumer.commit.call_count == 1
    runner.adapter.stock_order_detail.assert_not_called()
    assert runner.journal.unresolved(runner.scope)
    with pytest.raises(ConfigurationError, match="unresolved"):
        runner.process({"id": "new", "type": "LIMIT_ORDER", "symbol": "AAPL", "side": "buy",
                        "quantity": 1, "limit_price": 1})


def test_reconciliation_authentication_failure_still_stops_intake(runner, monkeypatch):
    seed(runner)
    runner.adapter.stock_order_detail = Mock(side_effect=AuthenticationError("Access denied"))
    consumer, _ = run_worker(monkeypatch, runner)
    assert consumer.poll.call_count == 1
    consumer.commit.assert_not_called()
    health = json.loads(runner.journal_path.with_suffix(".status.json").read_text())
    assert health["status"] == "authentication_required"


def test_timed_cancellation_uses_exact_ticket_validation_and_clears_both_journals(runner):
    runner.config["ioc_policy"] = runner.adapter.config["ioc_policy"] = "timed_cancel"
    import common.timed_cancel as timed
    command = normalize({"id": "ioc", "type": "LIMIT_ORDER_IOC", "symbol": "AAPL", "side": "buy",
                         "quantity": 1, "limit_price": 1}, "bitget", runner.config)
    runner.journal.reserve(runner.scope, command)
    timed._ensure(runner.journal)
    _, plan = timed.prepare(runner.adapter, "bitget", command)
    record = {"command_id": "ioc", "broker": "bitget", "command": {**command, "time_in_force": "day"},
        "request": plan["request"], "client_order_id": plan["client_order_id"], "order_id": "001",
        "state": "cancel_unknown", "requested_time_in_force": "ioc", "dispatch_at": 0, "deadline_at": 0.5}
    runner.journal.db.execute("INSERT INTO timed_cancellations VALUES (?,?,?,?,?,?)",
        (runner.scope, "ioc", "bitget", "unused", "cancel_unknown", json.dumps(record)))
    runner.journal.finish(runner.scope, "ioc", {"id": "ioc", "status": "unknown"})
    runner.adapter.stock_order_detail = Mock(return_value=detail(orderType="LO", side="Buy", quantity=1,
        price=1, remark=plan["client_order_id"]))
    runner.adapter.cancel_stock_order = Mock()
    assert runner.reconcile_pending()["status"] == "ok"
    assert not runner.journal.unresolved(runner.scope) and not runner.unresolved_timed()
    runner.adapter.cancel_stock_order.assert_not_called()
