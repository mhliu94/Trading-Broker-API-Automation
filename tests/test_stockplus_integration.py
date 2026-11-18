"""Actual Stock+ adapter across common cancel/snapshot interfaces; no live calls."""
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, parse_qs
from unittest.mock import Mock, patch
import base64
import hashlib
import hmac
import json

import pytest

from bitget.adapter import Adapter, PREFIX
from common.cancellation import cancel_order, cancel_open_orders
from common.errors import AuthenticationError, BrokerError, OutcomeUnknown
from common.snapshots import collect_account, collect_orders, map_order
from common.publication import validate_order_snapshot

START = datetime(2026, 10, 25, tzinfo=timezone.utc)
END = datetime(2026, 10, 26, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 25, 16, tzinfo=timezone.utc)


def order(identity="001", **changes):
    return {"orderId": identity, "symbol": "AAPL.US", "currency": "USD", "side": "Buy", "orderType": "LO", "status": "New",
            "submittedAt": "2026-10-25T14:00:00Z", "updatedAt": "2026-10-25T14:05:00Z", "quantity": "3",
            "executedQuantity": "0", "executedPrice": None, "price": "190.5", "timeInForce": "Day", **changes}


def static(symbol="AAPL.US"):
    return {"list": [{"symbol": symbol, "currency": "USD", "board": "USMain", "exchange": "NASD"}]}


def test_common_cancel_all_includes_old_stock_excludes_options_foreign_and_terminal():
    adapter = Adapter({})
    old = order("old", submittedAt="2020-01-02T15:00:00Z", timeInForce="GoodTilCanceled")
    entries = [old, order("option", symbol="AAPL261016C00190000.US"), order("foreign", symbol="700.HK"),
               order("terminal", status="Filled"), order("pending", status="PendingCancel")]
    adapter._request = Mock(side_effect=[{"orders": entries}, {"orders": entries, "hasMore": False}, old, static(), {"orderId": "old", "symbol": "AAPL.US"}])
    result = cancel_open_orders(adapter, "bitget")
    assert result["status"] == "cancel_requested" and result["cancellations_requested"] == 1
    assert result["cancellation_confirmed"] is False
    assert result["requests"][0]["order_id"] == "old"
    calls = adapter._request.call_args_list
    assert calls[1].args[2]["startAt"] == 0
    assert calls[-1].args == ("POST", PREFIX + "/trade/cancel-order")
    assert calls[-1].kwargs == {"payload": {"orderId": "old", "symbol": "AAPL.US"}}
    assert all(PREFIX in call.args[1] for call in calls)


def test_common_specific_cancel_without_symbol_resolves_exact_stock_detail():
    adapter = Adapter({})
    adapter._request = Mock(side_effect=[order(), order(), static(), {"orderId": "001", "symbol": "AAPL.US"}])
    result = cancel_order(adapter, "bitget", {"id": "cancel-one", "order_id": "001"})
    assert result["cancellations_requested"] == 1 and result["cancellation_confirmed"] is False
    assert adapter._request.call_args.kwargs["payload"] == {"symbol": "AAPL.US", "orderId": "001"}


@pytest.mark.parametrize("replacement", [order(symbol="MSFT.US"), order(symbol="AAPL261016C00190000.US")])
def test_common_cancel_detects_changed_or_derivative_detail_before_write(replacement):
    adapter = Adapter({})
    adapter._request = Mock(side_effect=[order(), replacement])
    result = cancel_order(adapter, "bitget", {"order_id": "001", "symbol": "AAPL"})
    assert result["status"] == "cancel_rejected" and result["cancellations_requested"] == 0
    assert all(call.args[0] == "GET" for call in adapter._request.call_args_list)


def test_common_cancel_recheck_terminal_race_does_not_claim_request():
    adapter = Adapter({})
    adapter._request = Mock(side_effect=[order(), order(status="Filled", executedQuantity="3", executedPrice="190"), static()])
    result = cancel_order(adapter, "bitget", {"order_id": "001"})
    assert result["status"] == "no_action" and result["cancellations_requested"] == 0
    assert all(call.args[0] == "GET" for call in adapter._request.call_args_list)


def test_common_cancel_all_ambiguous_first_write_stops_and_reports_remaining_ids():
    adapter = Adapter({})
    entries = [order("one"), order("two")]
    adapter._request = Mock(side_effect=[{"orders": entries}, {"orders": [], "hasMore": False}, entries[0], static(), OutcomeUnknown("timeout")])
    result = cancel_open_orders(adapter, "bitget")
    assert result["status"] == "outcome_unknown" and result["not_requested_order_ids"] == ["two"]
    assert sum(call.args[0] == "POST" for call in adapter._request.call_args_list) == 1


def test_common_cancel_all_refuses_incomplete_history_before_write():
    adapter = Adapter({})
    adapter._request = Mock(side_effect=[{"orders": [order()]}, {"orders": [], "hasMore": "false"}])
    with pytest.raises(BrokerError):
        cancel_open_orders(adapter, "bitget")
    assert all(call.args[0] == "GET" for call in adapter._request.call_args_list)


def test_real_signed_transport_for_placement_and_common_cancellation():
    adapter = Adapter({"api_key": "key", "api_secret": "secret", "passphrase": "passphrase"})
    transmitted = []
    command = {"id": "cmd1", "symbol": "AAPL", "side": "buy", "order_type": "limit", "quantity": "3", "limit_price": "190.5", "time_in_force": "day"}
    def send(method, url, *, headers, data, **kwargs):
        parsed = urlsplit(url)
        assert parsed.path.startswith(PREFIX + "/")
        prehash = headers["ACCESS-TIMESTAMP"] + method + parsed.path + ("?" + parsed.query if parsed.query else "") + (data or "")
        signature = base64.b64encode(hmac.new(b"secret", prehash.encode(), hashlib.sha256).digest()).decode()
        assert headers["ACCESS-SIGN"] == signature and kwargs["allow_redirects"] is False
        transmitted.append((method, parsed.path, json.loads(data) if data else None))
        if parsed.path.endswith("/market/static"):
            assert parse_qs(parsed.query)["symbol"] == ["AAPL.US"]
            result = static()
        elif parsed.path.endswith("/trade/order-detail"):
            assert parse_qs(parsed.query)["orderId"] == ["001"]
            result = order(remark=adapter.correlation("cmd1"))
        else:
            assert method == "POST" and headers["Content-Type"] == "application/json"
            result = {"orderId": "001", "symbol": "AAPL.US"}
        response = Mock(status_code=200)
        response.json.return_value = {"code": "00000", "data": result}
        return response
    with patch("requests.request", side_effect=send), patch("time.sleep"):
        assert adapter.place_order(command)["orderId"] == "001"
        assert cancel_order(adapter, "bitget", {"order_id": "001"})["status"] == "cancel_requested"
    writes = [record for record in transmitted if record[0] == "POST"]
    assert [record[1] for record in writes] == [PREFIX + "/trade/place-order", PREFIX + "/trade/cancel-order"]
    assert writes[0][2]["remark"] == adapter.correlation("cmd1")
    assert "clientOid" not in writes[0][2] and writes[0][2]["submittedQuantity"] == "3"


def test_common_account_snapshot_uses_actual_stockplus_cash_and_stock_positions():
    adapter = Adapter({})
    adapter._request = Mock(side_effect=[{"list": [{"currency": "USD", "totalCash": "125.5", "cashInfos": [{"currency": "USD", "availableCash": "100"}]}]},
        {"channels": [{"accountChannel": "channel", "positions": [{"symbol": "AAPL.US", "currency": "USD", "quantity": "-3", "availableQuantity": "-3", "costPrice": "190"}]}]}])
    result = collect_account(adapter, "bitget")
    assert result["cash"] == 100 and result["positions"][0]["symbol"] == "AAPL"
    assert result["positions"][0]["qty"] == -3
    assert adapter._request.call_args_list[0].args[1] == PREFIX + "/asset/account"


def test_common_order_collection_maps_manual_and_terminal_orders_to_ktrader_v1():
    adapter = Adapter({})
    raw = order("manual", executedQuantity="1", executedPrice="190.25", status="PartialWithdrawal", remark="cmd:correlation")
    adapter._request = Mock(side_effect=[{"orders": [raw, order("option", symbol="AAPL261016C00190000.US")]}, {"orders": [], "hasMore": False}])
    snapshots = collect_orders(adapter, "bitget", START, END, now=NOW)
    assert len(snapshots) == 1
    full = {**snapshots[0], "schema_version": 1, "account_id": "stock-account", "sequence": 0}
    validate_order_snapshot(full, sequenced=True)
    assert full["symbol"] == "AAPL" and full["status"] == "CANCELED"
    assert full["filled_quantity"] == 1 and full["average_fill_price"] == 190.25
    assert full["message"] == "Correlation: cmd:correlation" and "client_order_id" not in full
    # Use the independently supplied reference contract when available locally.
    schema_path = Path.home() / "GigaMoney-Automation-Linux/tests/fixtures/order-status.v1.schema.json"
    if schema_path.exists():
        import jsonschema
        jsonschema.Draft202012Validator(json.loads(schema_path.read_text()), format_checker=jsonschema.FormatChecker()).validate(full)
    assert map_order("bitget", raw, NOW) == snapshots[0]


def test_common_snapshot_errors_preserve_healthy_orders_without_fabricating_fill_price():
    adapter = Adapter({})
    entries = [order("bad", executedQuantity="1", executedPrice=None), order("good")]
    adapter._request = Mock(side_effect=[{"orders": entries}, {"orders": [], "hasMore": False}])
    errors = []
    result = collect_orders(adapter, "bitget", START, END, now=NOW, errors=errors)
    assert [row["order_id"] for row in result] == ["good"]
    assert errors[0]["order_id"] == "bad" and errors[0]["error_type"] == "BrokerError"


def test_common_snapshot_authentication_failure_is_not_hidden_as_empty_success():
    adapter = Adapter({})
    adapter._request = Mock(side_effect=AuthenticationError("access denied", no_submission=True))
    errors = []
    with pytest.raises(AuthenticationError):
        collect_orders(adapter, "bitget", START, END, errors=errors)
    assert errors == []
