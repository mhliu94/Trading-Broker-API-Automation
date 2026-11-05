"""Bitget IOC emulation through the public runner; all broker calls are mocked."""
import json
from unittest.mock import Mock, patch

import pytest

from bitget.adapter import PREFIX
from common import timed_cancel as timed
from common.errors import CommandError, ConfigurationError, OutcomeUnknown, RequestRejected, UnsupportedOrder
from common.runner import Runner
from test_bitget import command, instrument, order


def config(**updates):
    return {"trading_enabled": True, "journal_account": "ioc-test",
            "ioc_policy": "timed_cancel", "ioc_cancel_after_seconds": 0.5,
            "cancel_confirmation_delays_seconds": [0], **updates}


def payload(**updates):
    return command(time_in_force="ioc", **updates)


@pytest.fixture
def runner(tmp_path):
    instance = Runner("bitget", config(), True, tmp_path / "commands.sqlite3")
    instance.adapter._stock_instrument = Mock(return_value=instrument()["list"][0])
    yield instance
    instance.close()


def detail(status="New", **updates):
    return order("accepted-id", quantity="0.125", status=status, **updates)


@pytest.mark.parametrize("kind", ["limit", "market"])
def test_preview_preserves_order_and_exposes_emulated_ioc(kind):
    instance = Runner("bitget", config(), False)
    result = instance.process(payload(order_type=kind, limit_price=None if kind == "market" else "190.50"))
    assert result["request"]["orderType"] == ("MO" if kind == "market" else "LO")
    assert result["request"]["timeInForce"] == "Day"
    assert result["request"]["submittedQuantity"] == "0.125"
    assert result["execution_plan"]["native_ioc"] is False
    assert result["execution_plan"]["requested_time_in_force"] == "ioc"
    assert result["execution_plan"]["cancel_after_seconds"] == 0.5
    assert result["requests_sent"] == 0


def test_ioc_requires_opt_in_and_fok_remains_independent():
    with pytest.raises(UnsupportedOrder):
        Runner("bitget", {}, False).process(payload())
    with pytest.raises(UnsupportedOrder):
        Runner("bitget", config(), False).process(command(time_in_force="fok"))
    with pytest.raises(ConfigurationError, match="ioc_policy"):
        Runner("bitget", config(ioc_policy="bad"), False).process(payload())
    other = Runner("alpaca", config(), False).process(payload(broker="alpaca", quantity="1"))
    assert "execution_plan" not in other
    assert other["request"]["time_in_force"] == "ioc"


@pytest.mark.parametrize("kind", ["IOC", "LIMIT_ORDER_IOC", "immediate_or_cancel"])
def test_ioc_command_aliases_preserve_limit_intent(kind):
    request = command()
    request.pop("order_type")
    request.pop("time_in_force")
    result = Runner("bitget", config(), False).process({**request, "type": kind})
    assert result["request"]["orderType"] == "LO"
    assert result["request"]["submittedPrice"] == "190.5"
    assert result["execution_plan"]["requested_time_in_force"] == "ioc"
    with pytest.raises(CommandError, match="conflicts"):
        Runner("bitget", config(), False).process({**request, "type": kind, "time_in_force": "day"})
    with pytest.raises(CommandError, match="conflicts"):
        Runner("bitget", config(), False).process({**request, "type": kind, "cancel_unfilled": True})


@pytest.mark.parametrize("value", [-1, 301, True, None, "nan", "bad"])
def test_invalid_ioc_delay_fails_before_submission(value):
    with pytest.raises(ConfigurationError, match="ioc_cancel_after_seconds"):
        Runner("bitget", config(ioc_cancel_after_seconds=value), False).process(payload())


def test_overnight_market_ioc_is_rejected_before_submission():
    with pytest.raises(UnsupportedOrder, match="Overnight"):
        Runner("bitget", config(), False).process(payload(
            order_type="market", limit_price=None, options={"outside_rth": "Overnight"}))


@pytest.mark.parametrize("kind", ["limit", "market"])
def test_waits_half_second_after_acceptance_then_cancels_partial_remainder(runner, kind):
    clock, events = [100.0], []
    row = detail(orderType="MO" if kind == "market" else "LO",
                 price=None if kind == "market" else "190.5", executedQuantity="0.05")
    canceled = False

    def request(method, path, params=None, payload=None):
        nonlocal canceled
        events.append((method, path, clock[0], payload))
        if path.endswith("place-order"):
            clock[0] += 1.2  # A slow submission still gets the full 0.5-second wait.
            return {"orderId": "accepted-id"}
        if path.endswith("cancel-order"):
            canceled = True
            return {"orderId": "accepted-id"}
        return {**row, "status": "PartialWithdrawal" if canceled else "PartialFilled"}

    runner.adapter._request = Mock(side_effect=request)
    with patch.object(timed.time, "monotonic", side_effect=lambda: clock[0]), \
         patch.object(timed.time, "time", side_effect=lambda: 1700000000 + clock[0]), \
         patch.object(timed.time, "sleep", side_effect=lambda seconds: clock.__setitem__(0, clock[0] + seconds)) as sleep:
        result = runner.process(payload(order_type=kind, limit_price=None if kind == "market" else "190.50"))
        duplicate = runner.process(payload(order_type=kind, limit_price=None if kind == "market" else "190.50"))
    writes = [event for event in events if event[0] == "POST"]
    assert len(writes) == 2
    assert writes[1][1] == PREFIX + "/trade/cancel-order"
    assert writes[1][2] == pytest.approx(101.7)
    assert writes[1][3] == {"orderId": "accepted-id", "symbol": "AAPL.US"}
    sleep.assert_called_once_with(pytest.approx(0.5))
    assert writes[0][3]["timeInForce"] == "Day"
    assert result["data"]["order"]["executedQuantity"] == "0.05"
    assert result["data"]["cancellation_confirmed"] is True
    assert result["data"]["native_ioc"] is False
    assert duplicate["duplicate"] is True and runner.trading_enabled()
    record = json.loads(runner.journal.db.execute("SELECT record FROM timed_cancellations").fetchone()[0])
    assert record["deadline_at"] - record["accepted_at"] == 0.5


def test_already_filled_order_needs_no_cancel(runner):
    runner.adapter._request = Mock(side_effect=[{"orderId": "accepted-id"}, detail("Filled")])
    with patch.object(timed.time, "sleep"):
        result = runner.process(payload())
    assert result["data"]["terminal_state"] == "Filled"
    assert runner.adapter._request.call_count == 2
    assert runner.unresolved_timed() == []


@pytest.mark.parametrize("changes", [{"orderId": "another-id"}, {"symbol": "MSFT.US"},
                                    {"side": "Sell"}, {"quantity": "2"}, {"price": "1"}, {"orderType": "MO"}])
def test_wrong_ticket_is_never_canceled(runner, changes):
    runner.adapter._request = Mock(side_effect=[{"orderId": "accepted-id"}, {**detail(), **changes}])
    with patch.object(timed.time, "sleep"):
        result = runner.process(payload())
    assert result["status"] == "unknown" and not runner.trading_enabled()
    assert runner.adapter._request.call_count == 2
    assert runner.unresolved_timed()[0]["state"] == "cancel_pending"


def test_unknown_submission_is_not_canceled_or_replayed(runner):
    runner.adapter._request = Mock(side_effect=OutcomeUnknown("submission timeout"))
    result = runner.process(payload())
    assert result["status"] == "unknown"
    assert runner.process(payload())["duplicate"] is True
    assert runner.recover_pending()["status"] == "reconciliation_required"
    runner.adapter._request.assert_called_once()


def test_definite_submission_rejection_does_not_leave_pending_cancel(runner):
    runner.adapter._request = Mock(side_effect=RequestRejected("denied", code="40017", http_status=400))
    assert runner.process(payload())["status"] == "rejected"
    assert runner.trading_enabled() and runner.unresolved_timed() == []
    runner.adapter._request.assert_called_once()


def test_cancel_timeout_recovery_reads_without_repeating_writes(runner):
    runner.adapter._request = Mock(side_effect=[{"orderId": "accepted-id"}, detail(), detail(),
        OutcomeUnknown("cancel timeout"), detail()])
    with patch.object(timed.time, "sleep"):
        result = runner.process(payload())
    assert result["status"] == "unknown" and not runner.trading_enabled()
    runner.adapter._request = Mock(return_value=detail("Canceled"))
    assert runner.recover_pending()["status"] == "ok"
    runner.adapter._request.assert_called_once_with("GET", PREFIX + "/trade/order-detail", {"orderId": "accepted-id"})
    assert runner.process(payload())["status"] == "reconciled"


def test_restart_recovers_accepted_order_and_preserves_deadline(tmp_path):
    path = tmp_path / "restart.sqlite3"
    instance = Runner("bitget", config(), True, path)
    instance.adapter.place_order = Mock(return_value={"orderId": "accepted-id"})
    with patch.object(timed, "_finish_pending", side_effect=KeyboardInterrupt):
        with pytest.raises(KeyboardInterrupt):
            instance.process(payload())
    saved = json.loads(instance.journal.db.execute("SELECT record FROM timed_cancellations").fetchone()[0])
    assert saved["deadline_at"] - saved["accepted_at"] == 0.5
    instance.close()

    restarted = Runner("bitget", config(), True, path)
    restarted.adapter._stock_instrument = Mock(return_value=instrument()["list"][0])
    restarted.adapter._request = Mock(side_effect=[detail(), detail(), {"orderId": "accepted-id"}, detail("Canceled")])
    with patch.object(timed.time, "sleep"):
        assert restarted.recover_pending()["status"] == "ok"
    calls = restarted.adapter._request.call_args_list
    assert len([call for call in calls if call.args[0] == "POST"]) == 1
    assert calls[2].args == ("POST", PREFIX + "/trade/cancel-order")
    assert restarted.process(payload())["status"] == "reconciled"
    restarted.close()
