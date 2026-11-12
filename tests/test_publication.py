from datetime import datetime, timezone
from decimal import Decimal
import json
import sqlite3
import threading
from types import SimpleNamespace

import pytest

from common.publication import PublicationError, SnapshotPublisher, validate_order_snapshot


NOW = datetime(2026, 10, 25, 18, tzinfo=timezone.utc)
ROUTING = {"account_id": "paper-test", "account_num_id": 7, "timezone": "America/New_York"}


def order(**changes):
    return {"schema_version": 1, "account_id": "paper-test", "account_num_id": 7,
            "order_id": "900719925474099312345", "created_at": "2026-10-25T14:00:00Z",
            "updated_at": "2026-10-25T14:01:00Z", "symbol": "AAPL", "side": "BUY",
            "order_type": "LIMIT", "status": "NEW", "quantity": 10,
            "filled_quantity": 0, "average_fill_price": None, "limit_price": 100,
            **changes}


def account(**changes):
    return {"account_id": "paper-test", "account_num_id": 7, "cash": 25.5,
            "cash_by_currency": {"USD": 25.5}, "positions": [{"symbol": "AAPL", "qty": -3, "avg_price": None}],
            "ts": "2026-10-25T18:00:00Z", "trading_enabled": False,
            "cash_basis": "cash", **changes}


class Producer:
    def __init__(self, outcomes=()):
        self.sent = []
        self.outcomes = list(outcomes)
        self.before_ack = None

    def send(self, topic, *, key, value):
        self.sent.append((topic, key, value))
        outcome = self.outcomes.pop(0) if self.outcomes else None
        def get(timeout):
            assert timeout > 0
            if self.before_ack:
                self.before_ack()
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome if outcome is not None else SimpleNamespace(partition=2, offset=len(self.sent))
        return SimpleNamespace(get=get)

    def snapshots(self):
        return [json.loads(item[2]) for item in self.sent]


def publisher(tmp_path, producer=None, **kwargs):
    return SnapshotPublisher(tmp_path / "account.publication.sqlite", ROUTING, producer or Producer(), **kwargs)


def test_account_payload_empty_key_numeric_cash_and_basis_are_preserved(tmp_path):
    producer = Producer()
    pub = publisher(tmp_path, producer)
    result = pub.publish_account(account(buying_power=11.25))
    # Extra fields must themselves already be plain JSON; canonical financial
    # fields get numeric normalization. Buying power is intentionally separate.
    assert result == {"topic": "account-details", "partition": 2, "offset": 1}
    assert producer.sent[0][1] == ROUTING["account_id"].encode()
    value = producer.snapshots()[0]
    assert value["cash"] == 25.5 and value["cash_basis"] == "cash"
    assert value["positions"][0] == {"symbol": "AAPL", "qty": -3, "avg_price": None}
    pub.close()


@pytest.mark.parametrize("changes", [{"cash": None}, {"cash": "25.5"}, {"cash": True},
                                     {"cash": float("nan")}, {"cash": float("inf")},
                                     {"cash_by_currency": {"USD": None}},
                                     {"account_num_id": True}])
def test_unknown_or_invalid_cash_is_not_sent_as_false_zero(tmp_path, changes):
    producer = Producer()
    pub = publisher(tmp_path, producer)
    with pytest.raises(PublicationError):
        pub.publish_account(account(**changes))
    assert producer.sent == []
    pub.close()


def test_negative_diluted_position_cost_is_preserved_but_execution_price_cannot_be_negative(tmp_path):
    producer = Producer()
    pub = publisher(tmp_path, producer)
    pub.publish_account(account(positions=[{"symbol": "AAPL", "qty": 3, "avg_price": Decimal("-12.5")}]))
    assert producer.snapshots()[0]["positions"][0]["avg_price"] == -12.5
    invalid_fill = order(status="FILLED", filled_quantity=10, average_fill_price=-12.5)
    assert pub.publish_orders([invalid_fill], now=NOW)["failed"] == 1
    assert len(producer.sent) == 1
    pub.close()


def test_order_full_snapshot_key_sequence_and_pending_commit_before_send(tmp_path):
    producer = Producer()
    pub = publisher(tmp_path, producer)
    def inspect_pending():
        with sqlite3.connect(pub.path) as db:
            row = db.execute("SELECT published,pending FROM snapshot_publications").fetchone()
        assert row[0] is None
        assert json.loads(row[1])["sequence"] == 0
    producer.before_ack = inspect_pending
    assert pub.publish_orders([order()], now=NOW)["delivered"] == 1
    assert producer.sent[0][1] == b'["paper-test","900719925474099312345"]'
    assert producer.snapshots()[0] == dict(order(), sequence=0)
    row = pub.db.execute("SELECT published,pending,acknowledgement FROM snapshot_publications").fetchone()
    assert row[1] is None and json.loads(row[2])["offset"] == 1
    pub.close()


def test_changed_fill_and_terms_increment_but_observation_time_does_not(tmp_path):
    producer = Producer()
    pub = publisher(tmp_path, producer)
    pub.publish_orders([order()], now=NOW)
    assert pub.publish_orders([order(updated_at="2026-10-25T17:00:00Z")], now=NOW)["delivered"] == 0
    partial = order(status="PARTIALLY_FILLED", filled_quantity=4, average_fill_price=99.5,
                    updated_at="2026-10-25T14:02:00Z")
    assert pub.publish_orders([partial], now=NOW)["delivered"] == 1
    assert pub.publish_orders([dict(partial, limit_price=101)], now=NOW)["delivered"] == 1
    assert [item["sequence"] for item in producer.snapshots()] == [0, 1, 2]
    pub.close()


def test_restart_republishes_completed_snapshot_once_exactly(tmp_path):
    producer = Producer()
    completed = order(status="FILLED", filled_quantity=10, average_fill_price=100)
    pub = publisher(tmp_path, producer)
    pub.publish_orders([completed], now=NOW)
    original = producer.sent[0]
    pub.close()
    restarted = publisher(tmp_path, producer)
    assert restarted.publish_orders([dict(completed, updated_at="2026-10-25T18:00:00Z")], now=NOW)["delivered"] == 1
    assert producer.sent[-1] == original
    assert restarted.publish_orders([completed], now=NOW)["delivered"] == 0
    restarted.close()


def test_failed_delivery_retries_exact_pending_before_newer_state(tmp_path):
    producer = Producer([TimeoutError()])
    pub = publisher(tmp_path, producer)
    assert pub.publish_orders([order()], now=NOW)["failed"] == 1
    original = producer.sent[0]
    pub.close()
    restarted = publisher(tmp_path, producer)
    changed = order(status="PARTIALLY_FILLED", filled_quantity=2, average_fill_price=100)
    result = restarted.publish_orders([changed], now=NOW)
    assert result["delivered"] == 2 and result["failed"] == 0
    assert producer.sent[1] == original
    assert producer.snapshots()[2]["sequence"] == 1
    restarted.close()


def test_todays_pending_is_retried_even_when_source_omits_order(tmp_path):
    producer = Producer([TimeoutError()])
    pub = publisher(tmp_path, producer)
    pub.publish_orders([order()], now=NOW)
    assert pub.publish_orders([], now=NOW)["delivered"] == 1
    assert producer.sent[0] == producer.sent[1]
    pub.close()


def test_pending_failure_does_not_get_overwritten_or_block_other_order_and_account(tmp_path):
    producer = Producer([TimeoutError(), TimeoutError()])
    pub = publisher(tmp_path, producer)
    pub.publish_orders([order()], now=NOW)
    changed = order(status="PARTIALLY_FILLED", filled_quantity=2, average_fill_price=100)
    result = pub.publish_orders([changed, order(order_id="second")], now=NOW)
    assert result["failed"] == 1 and result["delivered"] == 1
    assert producer.sent[0] == producer.sent[1]
    assert json.loads(pub.db.execute("SELECT pending FROM snapshot_publications WHERE order_id=?", (order()["order_id"],)).fetchone()[0])["filled_quantity"] == 0
    assert pub.publish_account(account())["offset"] == 4
    pub.close()


def test_crash_after_kafka_ack_before_database_ack_retries_same_sequence(tmp_path):
    producer = Producer()
    pub = publisher(tmp_path, producer)
    pub.db.execute("CREATE TRIGGER fail_ack BEFORE UPDATE OF published ON snapshot_publications BEGIN SELECT RAISE(ABORT, 'simulated crash'); END")
    pub.db.commit()
    assert pub.publish_orders([order()], now=NOW)["failed"] == 1
    original = producer.sent[0]
    assert pub.db.execute("SELECT pending FROM snapshot_publications").fetchone()[0] is not None
    pub.db.execute("DROP TRIGGER fail_ack")
    pub.db.commit()
    pub.close()
    restarted = publisher(tmp_path, producer)
    assert restarted.publish_orders([], now=NOW)["delivered"] == 1
    assert producer.sent[1] == original
    restarted.close()


def test_prior_day_and_future_snapshots_never_publish_and_prior_pending_retained(tmp_path):
    producer = Producer([TimeoutError()])
    pub = publisher(tmp_path, producer)
    pub.publish_orders([order()], now=NOW)
    tomorrow = datetime(2026, 10, 26, 18, tzinfo=timezone.utc)
    before = len(producer.sent)
    assert pub.publish_orders([], now=tomorrow)["skipped"] == 1
    result = pub.publish_orders([order(order_id="past", created_at="2026-10-24T12:00:00Z"),
                                 order(order_id="future", created_at="2026-10-27T12:00:00Z", updated_at="2026-10-27T12:00:00Z")], now=NOW)
    # The current-day failed row is eligible again in this explicit test clock.
    assert result["skipped"] == 2 and len(producer.sent) == before + 1
    assert pub.db.execute("SELECT count(*) FROM snapshot_publications").fetchone()[0] == 1
    pub.close()


@pytest.mark.parametrize("current,created,included", [
    ("2026-03-08T16:00:00+00:00", "2026-03-08T04:59:59Z", False),
    ("2026-03-08T16:00:00+00:00", "2026-03-08T05:00:00Z", True),
    ("2026-03-08T16:00:00+00:00", "2026-03-09T04:00:00Z", False),
    ("2026-11-01T16:00:00+00:00", "2026-11-01T04:00:00Z", True),
    ("2026-11-01T16:00:00+00:00", "2026-11-02T04:59:59Z", True),
    ("2026-11-01T16:00:00+00:00", "2026-11-02T05:00:00Z", False),
])
def test_creation_day_uses_configured_timezone_and_dst(tmp_path, current, created, included):
    producer = Producer()
    pub = publisher(tmp_path, producer)
    result = pub.publish_orders([order(created_at=created, updated_at=created)], now=datetime.fromisoformat(current))
    assert result["failed"] == 0 and result["delivered"] == int(included)
    pub.close()


def test_creation_timestamp_immutable_and_update_never_decreases(tmp_path):
    producer = Producer()
    pub = publisher(tmp_path, producer)
    pub.publish_orders([order()], now=NOW)
    assert pub.publish_orders([order(created_at="2026-10-25T14:00:01Z")], now=NOW)["failed"] == 1
    changed = order(status="CANCELED", updated_at="2026-10-25T14:00:00Z")
    assert pub.publish_orders([changed], now=NOW)["delivered"] == 1
    assert producer.snapshots()[-1]["updated_at"] == order()["updated_at"]
    pub.close()


def test_corrupt_saved_state_is_never_reset_to_sequence_zero(tmp_path):
    producer = Producer()
    pub = publisher(tmp_path, producer)
    pub.publish_orders([order()], now=NOW)
    pub.db.execute("UPDATE snapshot_publications SET published='broken'")
    pub.db.commit()
    result = pub.publish_orders([order()], now=NOW)
    assert result["failed"] == 1 and "Corrupt" in result["errors"][0]["error"]
    assert len(producer.sent) == 1
    pub.close()


@pytest.mark.parametrize("changes", [
    {"filled_quantity": None}, {"filled_quantity": -1}, {"filled_quantity": 11},
    {"status": "FILLED", "filled_quantity": 0}, {"status": "FILLED", "filled_quantity": 9},
    {"status": "PARTIALLY_FILLED", "filled_quantity": 0},
    {"status": "PARTIALLY_FILLED", "filled_quantity": 10},
    {"average_fill_price": 12}, {"quantity": None}, {"quantity": "10"},
    {"quantity": float("inf")}, {"quantity": True}, {"filled_quantity": float("nan")},
    {"created_at": "2026-10-25T14:00:00"}, {"order_id": 123},
    {"side": "BUY_TO_COVER"}, {"status": "SUCCESS"}, {"limit_price": None},
    {"schema_version": True}, {"unknown_field": "ignored?"},
])
def test_invalid_or_unknown_fill_data_is_rejected_without_delivery(tmp_path, changes):
    producer = Producer()
    pub = publisher(tmp_path, producer)
    assert pub.publish_orders([order(**changes)], now=NOW)["failed"] == 1
    assert producer.sent == []
    pub.close()


def test_notional_order_and_partial_cancellation_retain_fills_without_invention():
    result = validate_order_snapshot(order(quantity=None, notional=100, status="FILLED", filled_quantity=Decimal("0.5")))
    assert result["average_fill_price"] is None and result["filled_quantity"] == .5
    result = validate_order_snapshot(order(status="CANCELED", filled_quantity=2, average_fill_price=99))
    assert result["filled_quantity"] == 2 and result["status"] == "CANCELED"


def test_conflicting_duplicate_snapshots_rejected_before_new_send(tmp_path):
    producer = Producer()
    pub = publisher(tmp_path, producer)
    result = pub.publish_orders([order(), order(limit_price=101)], now=NOW)
    assert result["failed"] == 1 and producer.sent == []
    pub.close()


def test_account_routing_and_delivery_ack_required(tmp_path):
    producer = Producer([SimpleNamespace(partition=-1, offset=-1)])
    pub = publisher(tmp_path, producer)
    with pytest.raises(PublicationError, match="routing"):
        pub.publish_account(account(account_id="other"))
    with pytest.raises(PublicationError, match="acknowledgement"):
        pub.publish_account(account())
    result = pub.publish_orders([order(account_id="other")], now=NOW)
    assert result["failed"] == 1 and len(producer.sent) == 1
    pub.close()


def test_independent_accounts_share_database_without_sequence_collision(tmp_path):
    producer = Producer()
    first = publisher(tmp_path, producer)
    first.publish_orders([order()], now=NOW)
    second = SnapshotPublisher(first.path, {"account_id": "second", "timezone": "UTC"}, producer)
    other = order(account_id="second")
    del other["account_num_id"]
    assert second.publish_orders([other], now=NOW)["delivered"] == 1
    assert [snapshot["sequence"] for snapshot in producer.snapshots()] == [0, 0]
    first.close()
    second.close()


def test_two_publishers_serialize_pending_commits_and_sequence_changes(tmp_path):
    producer = Producer()
    first = publisher(tmp_path, producer)
    second = publisher(tmp_path, producer)
    awaiting_ack, release_ack = threading.Event(), threading.Event()
    results = []
    def wait_first_ack():
        if len(producer.sent) == 1:
            awaiting_ack.set()
            assert release_ack.wait(3)
    producer.before_ack = wait_first_ack
    first_thread = threading.Thread(target=lambda: results.append(first.publish_orders([order()], now=NOW)))
    second_thread = threading.Thread(target=lambda: results.append(second.publish_orders([order(limit_price=101)], now=NOW)))
    first_thread.start()
    assert awaiting_ack.wait(3)
    second_thread.start()
    assert len(producer.sent) == 1
    release_ack.set()
    first_thread.join(3)
    second_thread.join(3)
    assert not first_thread.is_alive() and not second_thread.is_alive()
    assert all(result["delivered"] == 1 and result["failed"] == 0 for result in results)
    assert [snapshot["sequence"] for snapshot in producer.snapshots()] == [0, 1]
    first.close()
    second.close()


def test_max_sequence_never_rolls_over(tmp_path):
    producer = Producer()
    pub = publisher(tmp_path, producer)
    pub.publish_orders([order()], now=NOW)
    previous = dict(order(), sequence=2**53 - 1)
    pub.db.execute("UPDATE snapshot_publications SET published=?", (json.dumps(previous),))
    pub.db.commit()
    assert pub.publish_orders([order(limit_price=101)], now=NOW)["failed"] == 1
    assert len(producer.sent) == 1
    pub.close()
