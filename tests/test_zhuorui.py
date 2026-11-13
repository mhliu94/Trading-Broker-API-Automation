from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from common.errors import ConfigurationError, UnsupportedOrder, OutcomeUnknown
from zhuorui.adapter import Adapter, PINNED_COMMIT
from zhuorui.bridge_worker import NativeMarketClient, read_orders, read_tradability, verify_checkout


def command(**changes):
    result = dict(id="zr-1", broker="zhuorui", action="place_order", symbol="AAPL", side="buy",
                  quantity="1", order_type="limit", time_in_force="day", limit_price="100.25")
    result.update(changes)
    return result


def test_native_fok_never_claimed_and_timed_cancel_requires_opt_in():
    with pytest.raises(UnsupportedOrder, match="no native FOK"):
        Adapter({}).build_order(command(time_in_force="fok"))
    result = Adapter({"allow_timed_cancel_fok": True}).build_order(command(time_in_force="fok"))
    assert result["native_fok"] is False
    assert result["partial_fills_possible"] is True
    assert result["execution_semantics"] == "limit_then_timed_cancel"
    assert result["order_type"] == "fok"


@pytest.mark.parametrize("changes", [{"quantity": "0.5"}, {"limit_price": "100.251"}, {"time_in_force": "gtc"},
                                      {"time_in_force": "ioc"}, {"asset_class": "crypto"}])
def test_incompatible_orders_are_rejected_without_rounding(changes):
    with pytest.raises(UnsupportedOrder):
        Adapter({}).build_order(command(**changes))


def test_regular_day_limit_is_offline_and_preserves_price():
    result = Adapter({}).build_order(command(auction_intent="luld_reopening", options={"allow_pre_post": False}))
    assert result["limit_price"] == "100.25"
    assert result["allow_pre_post"] is False
    assert Adapter({}).capabilities()["luld_auction_participation"] is None


@pytest.mark.parametrize("side", ["buy", "sell"])
@pytest.mark.parametrize("price", ["0.0910", "0.0911", "0.0001", "0.9999", "1.00"])
def test_equity_price_bands_preserve_legal_subdollar_limits(side, price):
    from decimal import Decimal
    result = Adapter({}).build_order(command(side=side, limit_price=price))
    assert Decimal(result["limit_price"]) == Decimal(price)


@pytest.mark.parametrize("price", ["0.09115", "0.00001", "1.0001"])
def test_off_tick_limits_are_rejected_without_changing_the_price(price):
    with pytest.raises(UnsupportedOrder, match="tick-aligned"):
        Adapter({}).build_order(command(limit_price=price))


def test_market_is_not_silently_changed_to_extended_hours_limit():
    client = Mock()
    client.prepare_market_order.return_value = ("limit", 1, "101", True, {})
    guarded = NativeMarketClient(client, False, ValueError)
    with pytest.raises(ValueError, match="Native Market"):
        guarded.prepare_market_order("AAPL", "buy", 1)
    opted_in = NativeMarketClient(client, True, ValueError)
    assert opted_in.prepare_market_order("AAPL", "buy", 1)[0] == "limit"


def test_combines_today_and_history_and_filters_exact_window():
    start = datetime(2026, 10, 25, 4, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    stamp = int(start.timestamp() * 1000)
    client = Mock()
    client.query.return_value = {"code": "000000", "data": [{"code": "AAPL", "ts": "US", "orderTxnReference": "a", "entrustTime": stamp}]}
    client.query_orders_for_date.return_value = {"code": "000000", "data": [
        {"code": "AAPL", "ts": "US", "orderTxnReference": "a", "entrustTime": stamp, "entrustStatus": "FILLED"},
        {"code": "AAPL", "ts": "US", "orderTxnReference": "b", "entrustTime": stamp + 1},
        {"code": "AAPL", "ts": "US", "orderTxnReference": "c", "entrustTime": int(end.timestamp() * 1000)}]}
    rows = read_orders(client, start, end)
    assert [row["orderTxnReference"] for row in rows] == ["a", "b"]
    assert rows[0]["entrustStatus"] == "FILLED"
    assert client.query_orders_for_date.call_args.args[0].isoformat() == "2026-10-25"


def test_tradability_never_equates_quote_visibility_with_permission():
    client = Mock()
    client.now.return_value = 1000
    client._request.side_effect = [{"data": [{"code": "AAPL", "ts": "US", "suspension": 1, "time": 1000000, "delay": False}]}, {}]
    runtime = SimpleNamespace(QUOTE_PATH="/quote", MARKET_STATUS_PATH="/status", ApiError=ValueError,
                              parse_session=Mock(return_value=SimpleNamespace(name="regular")))
    result = read_tradability(client, "AAPL", SimpleNamespace(quote_max_age_seconds=30), runtime)
    assert result["found"] is True
    assert result["halted"] is False
    assert result["tradable"] is None
    assert result["account_tradable"] is None
    assert result["luld_auction_participation"] is None
    assert client._request.call_count == 2


def test_delayed_active_quote_does_not_prove_no_halt():
    client = Mock()
    client.now.return_value = 1000
    client._request.side_effect = [{"data": [{"code": "AAPL", "ts": "US", "suspension": 1, "time": 1000000, "delay": True}]}, {}]
    runtime = SimpleNamespace(QUOTE_PATH="/quote", MARKET_STATUS_PATH="/status", ApiError=ValueError,
                              parse_session=Mock(side_effect=ValueError))
    result = read_tradability(client, "AAPL", SimpleNamespace(quote_max_age_seconds=30), runtime)
    assert result["halted"] is None
    assert result["market_session"] is None


def test_checkout_revision_is_pinned(tmp_path):
    with patch("subprocess.run", return_value=SimpleNamespace(stdout="different", returncode=0)):
        with pytest.raises(ValueError, match="pinned"):
            verify_checkout(tmp_path, PINNED_COMMIT)


def test_adapter_uses_isolated_subprocess_and_passes_no_keys(tmp_path):
    adapter = Adapter({"upstream_root": str(tmp_path), "upstream_config": str(tmp_path / "private.json"), "python_executable": "python-for-zhuorui"})
    response = SimpleNamespace(returncode=0, stdout=json.dumps({"ok": True, "result": []}))
    with patch("subprocess.run", return_value=response) as run:
        assert adapter.positions() == []
    assert run.call_args.args[0][:2] == ["python-for-zhuorui", "-I"]
    request = json.loads(run.call_args.kwargs["input"])
    assert request["expected_commit"] == PINNED_COMMIT
    assert request["operation"] == "positions"


def test_unknown_cancellation_is_reported_as_unknown_not_success():
    adapter = Adapter({"allow_timed_cancel_fok": True})
    adapter._bridge = Mock(return_value={"status": "cancel_unknown", "message": "Reconcile order"})
    with pytest.raises(OutcomeUnknown):
        adapter.place_order(command(time_in_force="fok"))


def test_missing_borrow_endpoint_does_not_become_zero_inventory():
    result = Adapter({}).borrow_availability("AAPL")
    assert result["supported"] is False
    assert result["available_quantity"] is None
    assert result["shortable"] is None
    assert result["locate_required"] is None


def test_routing_binding_is_forwarded_to_worker(tmp_path):
    adapter = Adapter({"upstream_root": str(tmp_path), "upstream_config": str(tmp_path / "private.json"),
                       "routing": {"account_id": "zhuorui-main", "account_num_id": 42, "server_id": "win-host"}})
    response = SimpleNamespace(returncode=0, stdout=json.dumps({"ok": True, "result": []}))
    with patch("subprocess.run", return_value=response) as run:
        adapter.positions()
    binding = json.loads(run.call_args.kwargs["input"])["expected_binding"]
    assert binding == {"account_id": "zhuorui-main", "account_num_id": "42", "server_id": "win-host"}


def test_routing_conflict_fails_before_launch(tmp_path):
    adapter = Adapter({"upstream_root": str(tmp_path), "upstream_config": str(tmp_path / "private.json"),
                       "account_id": "wrong-account", "routing": {"account_id": "right-account"}})
    with patch("subprocess.run") as run:
        with pytest.raises(ConfigurationError, match="conflicts"):
            adapter.positions()
    run.assert_not_called()


def test_routing_requires_account_selector_and_accepts_equivalent_legacy_number():
    with pytest.raises(ConfigurationError, match="requires account_id"):
        Adapter({"routing": {"server_id": "host-only"}})._expected_binding()
    assert Adapter({"account_num_id": "042", "routing": {"account_num_id": 42}})._expected_binding() == {"account_num_id": "42"}


def test_cancellation_recovery_defaults_to_preview_and_uses_registry(tmp_path, capsys):
    from zhuorui import recover_cancellations
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"brokers": {"zhuorui": {"upstream_root": "${UNSET_ZHUORUI_TEST_ROOT}"}}}))
    with patch.object(recover_cancellations, "adapter_for") as registry:
        assert recover_cancellations.main(["--config", str(path)]) == 0
    registry.assert_not_called()
    assert json.loads(capsys.readouterr().out)["status"] == "dry_run"
    path.write_text(json.dumps({"brokers": {"zhuorui": {}}}))
    fake_adapter = Mock()
    fake_adapter.recover_cancellations.return_value = {"orders_replayed": False, "reports": []}
    with patch.object(recover_cancellations, "adapter_for", return_value=fake_adapter) as registry:
        assert recover_cancellations.main(["--config", str(path), "--execute"]) == 0
    registry.assert_called_once_with("zhuorui", {})
    fake_adapter.recover_cancellations.assert_called_once_with()


@pytest.mark.parametrize("ack", [None, [], {}, {"status": "unrecognized"}, {"status": "submitted"},
                                  {"status": "submitted", "order_reference": True}, {"status": "submitted", "order_reference": "bad\x7f"},
                                  {"status": "submitted", "order_reference": "one", "command_id": "wrong"},
                                  {"status": "duplicate", "previous_state": "unknown"},
                                  {"status": "duplicate", "previous_state": "dispatching"},
                                  {"status": "duplicate", "previous_state": "submitted"}])
def test_zhuorui_missing_ack_and_uncertain_duplicate_never_claim_submission(ack):
    adapter = Adapter({})
    adapter._bridge = Mock(return_value=ack)
    with pytest.raises(OutcomeUnknown):
        adapter.place_order(command())
    adapter._bridge.assert_called_once()


@pytest.mark.parametrize("status", ["submitted", "cancel_requested"])
def test_zhuorui_known_success_requires_broker_order_reference(status):
    adapter = Adapter({})
    ack = {"status": status, "order_reference": "broker-reference", "command_id": "zr-1"}
    adapter._bridge = Mock(return_value=ack)
    assert adapter.place_order(command()) == ack
