"""Cancellation contract tests use mocked broker transport; no live writes."""
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock, call, patch
from urllib.parse import parse_qs, urlsplit

import pytest

from common.cancellation import cancel_order, cancel_open_orders
from common.errors import BrokerError, CommandError, OutcomeUnknown
from alpaca.adapter import Adapter as Alpaca
from bitget.adapter import Adapter as Bitget
from robinhood.adapter import Adapter as Robinhood
from tradestation.adapter import Adapter as TradeStation
from longbridge.adapter import Adapter as Longbridge
from common.registry import adapter_for
from zhuorui.adapter import Adapter as Zhuorui


def Webull(config):
    return adapter_for("webull", config)


def test_alpaca_single_does_not_cancel_completed_and_quotes_identifier():
    adapter = Alpaca({})
    adapter._http = Mock()
    adapter.http.request.return_value = {"id": "a/b", "status": "filled", "symbol": "AAPL", "asset_class": "us_equity"}
    assert cancel_order(adapter, "alpaca", {"order_id": "a/b"})["status"] == "no_action"
    adapter.http.request.assert_called_once_with("GET", "/v2/orders/a%2Fb")


def test_alpaca_single_ack_is_not_cancellation_confirmation():
    adapter = Alpaca({})
    adapter._http = Mock()
    adapter.http.request.side_effect = [{"id": "a", "status": "new", "symbol": "AAPL", "asset_class": "us_equity"}, None]
    result = cancel_order(adapter, "alpaca", {"order_id": "a"})
    assert result["status"] == "cancel_requested"
    assert result["cancellation_confirmed"] is False
    assert adapter.http.request.call_count == 2


def test_alpaca_scoped_cancel_preserves_each_outcome_and_excludes_crypto_options():
    adapter = Alpaca({})
    adapter._http = Mock()
    adapter.http.request.side_effect = [[
        {"id": "old-gtc", "status": "new", "symbol": "AAPL", "asset_class": "us_equity", "created_at": "2020-01-01T14:00:00Z"},
        {"id": "crypto", "status": "new", "symbol": "BTC/USD", "asset_class": "crypto"},
        {"id": "option", "status": "new", "symbol": "AAPL261016C00190000", "asset_class": "us_option"},
        {"id": "other", "status": "new", "symbol": "MSFT", "asset_class": "us_equity"}], None, BrokerError("rejected")]
    result = cancel_open_orders(adapter, "alpaca")
    assert result["status"] == "cancel_rejected" and result["cancellations_requested"] == 1
    assert result["cancellation_confirmed"] is False
    assert adapter.http.request.call_args_list == [
        call("GET", "/v2/orders", params={"status": "open", "limit": 500, "direction": "desc", "nested": "false"}),
        call("DELETE", "/v2/orders/old-gtc"), call("DELETE", "/v2/orders/other")]


@pytest.mark.parametrize("response", [None, {}, [None], [{"id": "a"}], [{"id": "a", "status": True}]])
def test_alpaca_malformed_discovery_blocks_before_any_delete(response):
    adapter = Alpaca({})
    adapter._http = Mock()
    adapter.http.request.return_value = response
    with pytest.raises(BrokerError):
        cancel_open_orders(adapter, "alpaca")
    assert adapter.http.request.call_count == 1
    assert adapter.http.request.call_args.args[0] == "GET"


def test_alpaca_paginates_all_open_dates_before_per_order_writes():
    adapter = Alpaca({})
    adapter._http = Mock()
    first = [{"id": str(index), "status": "new", "symbol": "BTC/USD", "asset_class": "crypto"} for index in range(500)]
    first[0] = {"id": "old", "status": "new", "symbol": "AAPL", "asset_class": "us_equity", "created_at": "2020-01-01T14:00:00Z"}
    adapter.http.request.side_effect = [first, [{"id": "last", "status": "new", "symbol": "MSFT", "asset_class": "us_equity"}], None, None]
    result = cancel_open_orders(adapter, "alpaca")
    assert result["cancellations_requested"] == 2
    calls = adapter.http.request.call_args_list
    assert calls[1].kwargs["params"]["before_order_id"] == "499"
    assert all(item.args[0] == "GET" for item in calls[:2])
    assert [item.args[1] for item in calls[2:]] == ["/v2/orders/old", "/v2/orders/last"]
    assert not any(item.args == ("DELETE", "/v2/orders") for item in calls)


def test_robinhood_specific_order_resolves_stock_detail_then_uses_cancel_hook():
    adapter = Robinhood({"account_number": "mine"})
    adapter.stock_order_detail = Mock(return_value={"order_id": "a", "symbol": "AAPL", "state": "confirmed"})
    adapter.cancel_stock_order = Mock(return_value={"accepted": True, "order_id": "a"})
    assert cancel_order(adapter, "robinhood", {"order_id": "a"})["status"] == "cancel_requested"
    adapter.stock_order_detail.assert_called_once_with("a")
    adapter.cancel_stock_order.assert_called_once_with("a", symbol="AAPL")


def test_robinhood_wrong_account_failure_from_detail_blocks_cancel_hook():
    adapter = Robinhood({"account_number": "mine"})
    adapter.stock_order_detail = Mock(side_effect=BrokerError("Order belongs to another account"))
    adapter.cancel_stock_order = Mock()
    with pytest.raises(BrokerError, match="account"):
        cancel_order(adapter, "robinhood", {"order_id": "a"})
    adapter.cancel_stock_order.assert_not_called()


def test_robinhood_bulk_uses_all_date_stock_discovery_hook():
    adapter = Robinhood({"account_number": "mine"})
    adapter.open_stock_orders = Mock(return_value=[
        {"order_id": "old", "symbol": "AAPL", "state": "confirmed", "created_at": "2020-01-01T14:00:00Z"},
        {"order_id": "queued", "symbol": "MSFT", "state": "queued"}])
    adapter.cancel_stock_order = Mock(side_effect=[{"accepted": True, "order_id": "old"}, {"accepted": True, "order_id": "queued"}])
    result = cancel_open_orders(adapter, "robinhood")
    assert [row["order_id"] for row in result["requests"]] == ["old", "queued"]
    adapter.open_stock_orders.assert_called_once_with()
    assert adapter.cancel_stock_order.call_args_list == [call("old", symbol="AAPL"), call("queued", symbol="MSFT")]


def test_tradestation_queries_open_account_orders_and_excludes_terminal_pending():
    adapter = TradeStation({"account_id": "mine"})
    adapter._orders = Mock(return_value=[
        {"AssetType": "STOCK", "Symbol": "AAPL", "OrderID": "old", "AccountID": "mine", "Status": "ACK", "OpenedDateTime": "2020-01-01"},
        {"AssetType": "STOCK", "Symbol": "AAPL", "OrderID": "done", "AccountID": "mine", "Status": "FLL"},
        {"AssetType": "STOCK", "Symbol": "AAPL", "OrderID": "pending", "AccountID": "mine", "Status": "UCN"}])
    adapter._request = Mock(return_value={"AssetType": "STOCK", "Symbol": "AAPL", "OrderID": "old", "Message": "Cancel request sent"})
    result = cancel_open_orders(adapter, "tradestation")
    assert result["cancellations_requested"] == 1
    adapter._orders.assert_called_once_with("/brokerage/accounts/mine/orders", {"status": "Open"})
    adapter._request.assert_called_once_with("DELETE", "/orderexecution/orders/old")


def test_tradestation_foreign_account_blocks_entire_snapshot_before_writes():
    adapter = TradeStation({"account_id": "mine"})
    adapter._orders = Mock(return_value=[{"AssetType": "STOCK", "Symbol": "AAPL", "OrderID": "a", "AccountID": "theirs", "Status": "ACK"}])
    adapter._request = Mock()
    with pytest.raises(BrokerError, match="account"):
        cancel_open_orders(adapter, "tradestation")
    adapter._request.assert_not_called()


def test_longbridge_old_gtc_discovered_with_explicit_full_history_and_today_override():
    adapter = Longbridge({})
    adapter._request = Mock(side_effect=[
        {"orders": [{"symbol": "AAPL.US", "order_id": "old", "status": "NewStatus", "submitted_at": "1"},
                    {"symbol": "AAPL.US", "order_id": "changed", "status": "NewStatus", "submitted_at": "2"}]},
        {"orders": [{"symbol": "AAPL.US", "order_id": "changed", "status": "FilledStatus"}]}, {}])
    with patch("common.cancellation.time.time", return_value=100):
        result = cancel_open_orders(adapter, "longbridge")
    assert result["cancellations_requested"] == 1
    params = parse_qs(urlsplit(adapter._request.call_args_list[0].args[1]).query)
    assert params["start_at"] == ["0"] and params["end_at"] == ["100"]
    assert "NewStatus" in params["status"]
    assert adapter._request.call_args_list[-1] == call("delete", "/v1/trade/order?order_id=old")


def test_longbridge_has_more_splits_history_before_any_cancellation():
    adapter = Longbridge({})
    adapter._request = Mock(side_effect=[{"orders": [], "has_more": True},
        {"orders": [{"symbol": "AAPL.US", "order_id": "a", "status": "NewStatus"}]},
        {"orders": [{"symbol": "AAPL.US", "order_id": "b", "status": "NewStatus"}]}, {"orders": []}, {}, {}])
    with patch("common.cancellation.time.time", return_value=10):
        result = cancel_open_orders(adapter, "longbridge")
    assert result["cancellations_requested"] == 2
    assert all(item.args[0] == "get" for item in adapter._request.call_args_list[:4])


def test_longbridge_incomplete_history_sends_no_cancels():
    adapter = Longbridge({"max_pages": 1})
    adapter._request = Mock(return_value={"orders": [], "has_more": True})
    with pytest.raises(BrokerError, match="limit"):
        cancel_open_orders(adapter, "longbridge")
    assert adapter._request.call_count == 1


def test_longbridge_specific_gets_detail_then_delete_query_parameter():
    adapter = Longbridge({})
    adapter._request = Mock(side_effect=[{"symbol": "AAPL.US", "order_id": "a&b", "status": "NewStatus"}, {}])
    assert cancel_order(adapter, "longbridge", {"order_id": "a&b"})["status"] == "cancel_requested"
    assert adapter._request.call_args_list == [call("get", "/v1/trade/order?order_id=a%26b"),
                                              call("delete", "/v1/trade/order?order_id=a%26b")]


def test_webull_uses_child_client_id_with_account_and_skips_completed_legs():
    adapter = Webull({"account_id": "mine"})
    adapter._trade = Mock()
    adapter._pages = Mock(return_value=[{"client_order_id": "group", "orders": [
        {"symbol": "AAPL", "instrument_type": "EQUITY", "market": "US", "order_id": "old", "client_order_id": "child", "status": "SUBMITTED", "place_time_at": "2020-01-01"},
        {"symbol": "AAPL", "instrument_type": "EQUITY", "market": "US", "order_id": "done", "client_order_id": "filled", "status": "FILLED"}]}])
    adapter._call = Mock(return_value={"client_order_id": "child"})
    result = cancel_order(adapter, "webull", {"order_id": "old"})
    assert result["cancellations_requested"] == 1
    adapter._call.assert_called_once_with(adapter._trade.order_v3.cancel_order, mutation=True,
                                         account_id="mine", client_order_id="child")
    adapter._pages.assert_called_once_with(adapter._trade.order_v3.list_order_open, account_id="mine")


def test_webull_missing_client_id_blocks_all_before_cancellation():
    adapter = Webull({"account_id": "mine"})
    adapter._trade = Mock()
    adapter._pages = Mock(return_value=[{"orders": [{"symbol": "AAPL", "instrument_type": "EQUITY", "market": "US", "order_id": "a", "status": "SUBMITTED"}]}])
    adapter._call = Mock()
    with pytest.raises(CommandError):
        cancel_open_orders(adapter, "webull")
    adapter._call.assert_not_called()


def test_bitget_specific_resolves_symbol_from_verified_stock_detail():
    adapter = Bitget({})
    adapter.stock_order_detail = Mock(return_value={"orderId": "a", "symbol": "AAPL.US", "status": "New"})
    adapter.cancel_stock_order = Mock(return_value={"orderId": "a", "symbol": "AAPL.US"})
    assert cancel_order(adapter, "bitget", {"order_id": "a"})["status"] == "cancel_requested"
    adapter.stock_order_detail.assert_called_once_with("a")
    adapter.cancel_stock_order.assert_called_once_with("a", symbol="AAPL.US")


def test_bitget_specific_symbol_mismatch_rejected_before_cancel():
    adapter = Bitget({})
    adapter.stock_order_detail = Mock(return_value={"orderId": "a", "symbol": "AAPL.US", "status": "New"})
    adapter.cancel_stock_order = Mock()
    with pytest.raises(BrokerError, match="symbol"):
        cancel_order(adapter, "bitget", {"order_id": "a", "symbol": "MSFT"})
    adapter.cancel_stock_order.assert_not_called()


def test_bitget_cancel_all_uses_exact_stock_ids_and_skips_terminal_pending_states():
    adapter = Bitget({})
    adapter.open_stock_orders = Mock(return_value=[
        {"orderId": "a", "symbol": "AAPL.US", "status": "New", "submittedAt": "2020-01-01T14:00:00Z"},
        {"orderId": "b", "symbol": "MSFT.US", "status": "PartialFilled"},
        {"orderId": "done", "symbol": "AAPL.US", "status": "Filled"},
        {"orderId": "pending", "symbol": "MSFT.US", "status": "PendingCancel"}])
    adapter.cancel_stock_order = Mock(side_effect=[{"orderId": "a"}, {"orderId": "b"}])
    result = cancel_open_orders(adapter, "bitget")
    assert result["cancellations_requested"] == 2 and result["cancellation_confirmed"] is False
    assert adapter.cancel_stock_order.call_args_list == [call("a", symbol="AAPL.US"), call("b", symbol="MSFT.US")]
    adapter.open_stock_orders.assert_called_once_with()


@pytest.mark.parametrize("symbol", ["POM", "POM.US"])
def test_bitget_smart_cancel_only_cancels_matching_symbol(symbol):
    adapter = Bitget({})
    adapter.open_stock_orders = Mock(return_value=[
        {"orderId": "pom", "symbol": "POM.US", "status": "New"},
        {"orderId": "other", "symbol": "AAPL.US", "status": "New"},
        {"orderId": "filled", "symbol": "POM.US", "status": "Filled"},
        {"orderId": "pending", "symbol": "POM.US", "status": "PendingCancel"}])
    adapter.cancel_stock_order = Mock(return_value={"orderId": "pom"})
    result = cancel_open_orders(adapter, "bitget", {"symbol": symbol})
    assert result["cancellations_requested"] == 1
    assert result["cancellation_confirmed"] is False
    assert "POM.US" in result["scope"]
    adapter.cancel_stock_order.assert_called_once_with("pom", symbol="POM.US")


def test_bitget_smart_cancel_without_matching_symbol_sends_nothing():
    adapter = Bitget({})
    adapter.open_stock_orders = Mock(return_value=[
        {"orderId": "other", "symbol": "AAPL.US", "status": "New"}])
    adapter.cancel_stock_order = Mock()
    assert cancel_open_orders(adapter, "bitget", {"symbol": "POM"})["status"] == "no_action"
    adapter.cancel_stock_order.assert_not_called()


def test_bitget_smart_cancel_validates_all_symbols_before_any_write():
    adapter = Bitget({})
    adapter.open_stock_orders = Mock(return_value=[
        {"orderId": "pom", "symbol": "POM.US", "status": "New"},
        {"orderId": "invalid", "status": "New"}])
    adapter.cancel_stock_order = Mock()
    with pytest.raises((BrokerError, CommandError)):
        cancel_open_orders(adapter, "bitget", {"symbol": "POM"})
    adapter.cancel_stock_order.assert_not_called()


def test_bitget_unknown_cancel_acknowledgement_stops_without_retry():
    adapter = Bitget({})
    adapter.open_stock_orders = Mock(return_value=[{"orderId": identity, "symbol": "AAPL.US", "status": "New"} for identity in ("a", "b")])
    adapter.cancel_stock_order = Mock(side_effect=OutcomeUnknown("unconfirmed acknowledgement"))
    result = cancel_open_orders(adapter, "bitget")
    assert result["status"] == "outcome_unknown" and result["not_requested_order_ids"] == ["b"]
    adapter.cancel_stock_order.assert_called_once_with("a", symbol="AAPL.US")


def test_uncertain_first_write_stops_bulk_and_retains_unattempted_ids():
    adapter = TradeStation({"account_id": "mine"})
    adapter._orders = Mock(return_value=[{"AssetType": "STOCK", "Symbol": "AAPL", "OrderID": order, "AccountID": "mine", "Status": "ACK"} for order in ("a", "b")])
    adapter._request = Mock(side_effect=OutcomeUnknown("timeout"))
    result = cancel_open_orders(adapter, "tradestation")
    assert result["status"] == "outcome_unknown"
    assert result["not_requested_order_ids"] == ["b"]
    assert adapter._request.call_count == 1


@pytest.mark.parametrize("operation", ["cancel_order", "cancel_open_orders"])
def test_zhuorui_retains_command_id_and_upstream_scope(operation):
    adapter = Zhuorui({})
    adapter._bridge = Mock(return_value={"status": "cancel_requested", "cancellations_requested": 1})
    command = {"id": "durable-1", "order_id": "broker-1"}
    function = cancel_order if operation == "cancel_order" else cancel_open_orders
    result = function(adapter, "zhuorui", command)
    assert result["cancellation_confirmed"] is False
    assert "US" in result["scope"]
    adapter._bridge.assert_called_once_with(operation, {"command_id": "durable-1", "order_id": "broker-1"})


def test_zhuorui_worker_executes_original_cancel_command_under_journal_lock(tmp_path):
    from zhuorui.bridge_worker import execute_order
    journal = Mock()
    journal.pending_cancellations.return_value = []
    captured = []
    def executor_factory(*args, **kwargs):
        executor = Mock()
        def execute(command):
            captured.append(command)
            args[5](command, "cancel_requested", "Acknowledged", cancellations_requested=1)
        executor.execute.side_effect = execute
        return executor
    runtime = SimpleNamespace(load_listener_settings=Mock(return_value=SimpleNamespace(journal_file=tmp_path / "journal")),
        InstanceLock=Mock(return_value=nullcontext()), CommandJournal=Mock(return_value=journal),
        binding=Mock(return_value={}), CommandExecutor=executor_factory, CancelCommand=SimpleNamespace, ApiError=ValueError)
    result = execute_order({"operation": "cancel_open_orders", "config": "private-path",
                            "payload": {"command_id": "cmd", "order_id": None}}, {}, {}, Mock(), runtime)
    assert captured[0].cancel_all is True and captured[0].command_id == "cmd"
    assert result["status"] == "cancel_requested" and result["cancellation_confirmed"] is False
    journal.close.assert_called_once_with()


@pytest.mark.parametrize("bad", [None, "", " a", "a\n", True, 123])
def test_invalid_order_id_rejected_before_any_adapter_access(bad):
    with pytest.raises(CommandError):
        cancel_order(None, "alpaca", {"order_id": bad})


def test_webull_paginates_entire_open_snapshot_before_write():
    adapter = Webull({"account_id": "mine", "query_interval": 0})
    adapter._trade = Mock()
    adapter._call = Mock(side_effect=[
        {"data": [{"orders": [{"symbol": "AAPL", "instrument_type": "EQUITY", "market": "US", "order_id": "a", "client_order_id": "ca", "status": "PENDING"}]}], "pagination_key": "page2"},
        {"data": [{"orders": [{"symbol": "AAPL", "instrument_type": "EQUITY", "market": "US", "order_id": "b", "client_order_id": "cb", "status": "PARTIAL_FILLED"}]}]},
        {"client_order_id": "ca"}, {"client_order_id": "cb"}])
    assert cancel_open_orders(adapter, "webull")["cancellations_requested"] == 2
    calls = adapter._call.call_args_list
    assert calls[0].kwargs == {"account_id": "mine", "pagination_key": None}
    assert calls[1].kwargs == {"account_id": "mine", "pagination_key": "page2"}
    assert all(item.kwargs.get("mutation") is True for item in calls[2:])


def test_webull_actual_sdk_cancel_builds_v3_account_client_request():
    from webull.trade.trade.v3.order_operation_v3 import OrderOperationV3
    api = Mock()
    OrderOperationV3(api).cancel_order(account_id="mine", client_order_id="client")
    request = api.get_response.call_args.args[0]
    assert request.get_action_name() == "/trading/orders/cancel"
    assert request.get_body_params() == {"account_id": "mine", "client_order_id": "client"}
    assert request.get_version() == "v3"


@pytest.mark.parametrize("raw", [{}, [], {"client_order_id": "different"}])
def test_webull_unmatched_acknowledgement_is_unknown(raw):
    adapter = Webull({"account_id": "mine"})
    adapter._trade = Mock()
    adapter._pages = Mock(return_value=[{"orders": [{"symbol": "AAPL", "instrument_type": "EQUITY", "market": "US", "order_id": "a", "client_order_id": "ca", "status": "SUBMITTED"}]}])
    adapter._call = Mock(return_value=raw)
    assert cancel_open_orders(adapter, "webull")["status"] == "outcome_unknown"
    assert adapter._call.call_count == 1


def test_bitget_all_date_discovery_completes_before_any_cancellation():
    adapter = Bitget({})
    events = []
    def discover():
        events.append("discovery_complete")
        return [{"orderId": str(index), "symbol": "AAPL.US", "status": "New"} for index in range(101)]
    def cancel(identity, symbol):
        assert events[0] == "discovery_complete"
        events.append(identity)
        return {"orderId": identity, "symbol": symbol}
    adapter.open_stock_orders = Mock(side_effect=discover)
    adapter.cancel_stock_order = Mock(side_effect=cancel)
    assert cancel_open_orders(adapter, "bitget")["cancellations_requested"] == 101
    assert adapter.cancel_stock_order.call_count == 101
    assert events[1:] == [str(index) for index in range(101)]
    adapter.open_stock_orders.assert_called_once_with()


def test_unknown_open_order_state_refuses_whole_cancel_all_before_write():
    adapter = TradeStation({"account_id": "mine"})
    adapter._orders = Mock(return_value=[{"AssetType": "STOCK", "Symbol": "AAPL", "OrderID": "a", "AccountID": "mine", "Status": "ACK"},
                                       {"AssetType": "STOCK", "Symbol": "AAPL", "OrderID": "b", "AccountID": "mine", "Status": "OTHER"}])
    adapter._request = Mock()
    with pytest.raises(BrokerError, match="Unrecognized"):
        cancel_open_orders(adapter, "tradestation")
    adapter._request.assert_not_called()


def test_zhuorui_duplicate_unknown_is_not_success():
    adapter = Zhuorui({})
    adapter._bridge = Mock(return_value={"status": "duplicate", "previous_state": "unknown"})
    with pytest.raises(OutcomeUnknown):
        cancel_open_orders(adapter, "zhuorui", {"id": "durable"})
