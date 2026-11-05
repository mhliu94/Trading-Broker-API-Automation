from datetime import datetime, timezone
from unittest.mock import Mock, patch
import pytest

from bitget.adapter import Adapter, PREFIX
from common.errors import BrokerError, OutcomeUnknown, UnsupportedOrder
from common.publication import validate_order_snapshot

NOW = datetime(2026, 10, 25, 15, tzinfo=timezone.utc)


def row(**changes):
    value = {"orderId": "001", "symbol": "AAPL.US", "side": "Buy", "orderType": "LO", "status": "New",
             "submittedAt": "2026-10-25T14:00:00Z", "updatedAt": "2026-10-25T14:05:00Z", "quantity": "3",
             "executedQuantity": "1", "executedPrice": "190.25", "price": "190.5", "currency": "USD", "timeInForce": "Day"}
    value.update(changes)
    return value


def static():
    return {"list": [{"symbol": "AAPL.US", "currency": "USD", "board": "USMain"}]}


def test_all_us_stock_positions_from_every_account_channel():
    adapter = Adapter({})
    apple = {"symbol": "AAPL.US", "market": "US", "quantity": "-2", "availableQuantity": "-2", "costPrice": "190.5", "currency": "USD"}
    adapter._request = Mock(return_value={"channels": [{"accountChannel": "channel1", "positions": [apple,
        {**apple, "symbol": "700.HK", "market": "HK"}, {**apple, "symbol": "AAPL261016C00190000.US"}]},
        {"accountChannel": "channel2", "positions": [{**apple, "symbol": "MSFT.US"}]}]})
    result = adapter.positions()
    assert [position["symbol"] for position in result] == ["AAPL.US", "MSFT.US"]
    assert result[0]["quantity"] == "-2" and result[1]["accountChannel"] == "channel2"
    assert adapter._request.call_args.args == ("GET", PREFIX + "/asset/stock-position")


def test_positions_fail_on_missing_channel_or_list():
    adapter = Adapter({})
    for response in ({}, {"channels": [{}]}, {"channels": [{"accountChannel": "one", "positions": None}]}):
        adapter._request = Mock(return_value=response)
        with pytest.raises(BrokerError):
            adapter.positions()


def test_open_orders_include_old_gtc_from_full_available_history():
    adapter = Adapter({})
    old = row(orderId="old", submittedAt="2025-01-01T14:00:00Z", updatedAt="2025-01-01T14:00:00Z", timeInForce="GoodTilCanceled")
    adapter._request = Mock(side_effect=[{"orders": [row()]}, {"hasMore": False, "orders": [old, row(status="Filled")]}])
    with patch("time.time", return_value=1000):
        result = adapter.open_stock_orders()
    assert [order["orderId"] for order in result] == ["old"]
    assert adapter._request.call_args.args[2] == {"market": "US", "startAt": 0, "endAt": 1000}


def test_unknown_order_status_does_not_silently_disappear_from_cancel_scope():
    adapter = Adapter({})
    adapter._request = Mock(side_effect=[{"orders": [row(status="NewUnknownState")]}, {"hasMore": False, "orders": []}])
    with pytest.raises(BrokerError, match="unknown status"):
        adapter.open_stock_orders()


def test_detail_exact_id_and_stock_scope_required():
    adapter = Adapter({})
    for response in (row(orderId="wrong"), row(symbol="AAPL261016C00190000.US"), row(symbol="700.HK")):
        adapter._request = Mock(return_value=response)
        with pytest.raises(BrokerError):
            adapter.stock_order_detail("001")
    adapter._request = Mock(return_value=row())
    assert adapter.stock_order_detail("001")["symbol"] == "AAPL.US"
    assert adapter._request.call_args.args == ("GET", PREFIX + "/trade/order-detail", {"orderId": "001"})


def test_cancel_verifies_detail_and_instrument_before_mutating():
    adapter = Adapter({})
    ack = {"orderId": "001", "symbol": "AAPL.US"}
    adapter._request = Mock(side_effect=[row(), static(), ack])
    assert adapter.cancel_stock_order("001", "AAPL") == ack
    assert [call.args[0] for call in adapter._request.call_args_list] == ["GET", "GET", "POST"]
    assert adapter._request.call_args.kwargs == {"payload": {"orderId": "001", "symbol": "AAPL.US"}}


def test_cancel_refuses_mismatched_symbol_without_write():
    adapter = Adapter({})
    adapter._request = Mock(return_value=row())
    with pytest.raises(BrokerError, match="symbol differs"):
        adapter.cancel_stock_order("001", "MSFT")
    assert adapter._request.call_count == 1


def test_cancel_terminal_order_does_not_mutate():
    adapter = Adapter({})
    adapter._request = Mock(side_effect=[row(status="Filled"), static()])
    result = adapter.cancel_stock_order("001")
    assert result["already_terminal"] is True and result["cancel_requested"] is False
    assert all(call.args[0] == "GET" for call in adapter._request.call_args_list)


def test_cancel_ack_mismatch_remains_unknown():
    adapter = Adapter({})
    adapter._request = Mock(side_effect=[row(), static(), {"orderId": "other"}, row()])
    with pytest.raises(OutcomeUnknown):
        adapter.cancel_stock_order("001")


def test_account_snapshot_uses_actual_usd_cash_and_signed_stock_shares():
    adapter = Adapter({})
    adapter._request = Mock(return_value={"list": [{"currency": "USD", "totalCash": "25.1", "buyPower": "75.3",
        "cashInfos": [{"currency": "USD", "availableCash": "20.2"}]}]})
    adapter.positions = Mock(return_value=[{"symbol": "AAPL.US", "quantity": "-3", "availableQuantity": "-2",
                                            "costPrice": "190.5", "accountChannel": "channel1", "currency": "USD"}])
    result = adapter.snapshot_account()
    assert result["cash"] == 25.1 and result["available_cash_by_currency"] == {"USD": 20.2}
    assert result["positions"] == [{"symbol": "AAPL", "qty": -3, "avg_price": 190.5, "available_qty": -2,
                                     "currency": "USD", "account_channel": "channel1"}]


def test_missing_usd_balance_is_not_invented_zero():
    adapter = Adapter({})
    adapter._request = Mock(return_value={"list": [{"currency": "CNH", "totalCash": "100"}]})
    with pytest.raises(BrokerError):
        adapter.snapshot_account()


def test_order_snapshot_uses_cumulative_fills_plain_symbol_and_correlation():
    raw = row(status="PartialWithdrawal", remark="cmd:abc")
    result = Adapter({}).snapshot_order(raw, NOW)
    result = validate_order_snapshot({**result, "schema_version": 1, "account_id": "account"})
    assert result["order_id"] == "001" and result["symbol"] == "AAPL"
    assert result["quantity"] == 3 and result["filled_quantity"] == 1
    assert result["average_fill_price"] == 190.25 and result["status"] == "CANCELED"
    assert result["message"] == "Correlation: cmd:abc" and "client_order_id" not in result


def test_market_buy_snapshot_remains_share_quantity_not_notional():
    result = Adapter({}).snapshot_order(row(orderType="MO", status="Filled"), NOW)
    assert result["quantity"] == 3 and "notional" not in result


def test_snapshot_rejects_option_symbol_missing_fill_price_or_status():
    for raw in (row(symbol="AAPL261016C00190000.US"), row(executedPrice=None), row(status="mystery")):
        with pytest.raises(BrokerError):
            Adapter({}).snapshot_order(raw, NOW)


def test_capabilities_never_advertise_crypto_or_native_fok():
    result = Adapter({}).capabilities()
    assert result["asset_classes"] == ["us_equity"] and result["native_fok"] is False
    assert result["luld_applicable"] is True and result["server_idempotency"] is False


def test_snapshot_rejects_inconsistent_currency_instead_of_inventing_usd():
    with pytest.raises(BrokerError, match="currency"):
        Adapter({}).snapshot_order(row(currency="HKD"), NOW)
