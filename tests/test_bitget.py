from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch
import base64
import hashlib
import hmac
import json

import pytest

from bitget.adapter import Adapter, PREFIX
from common.commands import normalize
from common.errors import BrokerError, ConfigurationError, OutcomeUnknown, UnsupportedOrder


def command(**changes):
    result = dict(id="bg-stock-1", broker="bitget", action="place_order", symbol="AAPL",
                  side="buy", quantity="0.125", order_type="limit", time_in_force="day", limit_price="190.50")
    result.update(changes)
    return result


def instrument(symbol="AAPL.US"):
    return {"list": [{"symbol": symbol, "board": "USMain", "currency": "USD", "exchange": "NASD"}]}


def order(identity="001", **changes):
    result = {"orderId": identity, "symbol": "AAPL.US", "status": "New", "quantity": "2",
              "executedQuantity": "0", "executedPrice": None, "submittedAt": "2026-10-25T14:00:00Z",
              "updatedAt": "2026-10-25T14:00:00Z", "side": "Buy", "orderType": "LO", "price": "190.5", "currency": "USD"}
    result.update(changes)
    return result


def test_limit_stock_payload_preserves_shares_and_correlation():
    result = Adapter({}).build_order(command())
    assert result == {"symbol": "AAPL.US", "side": "Buy", "orderType": "LO", "submittedQuantity": "0.125",
                      "timeInForce": "Day", "outsideRth": "AnyTime", "submittedPrice": "190.5",
                      "remark": "cmd:" + hashlib.sha256(b"bg-stock-1").hexdigest()[:60]}
    assert len(result["remark"]) == 64
    assert "clientOid" not in result
    assert result == Adapter({}).build_order(command(symbol="AAPL.US"))


@pytest.mark.parametrize("side", ["buy", "sell"])
def test_market_orders_use_share_quantity(side):
    result = Adapter({}).build_order(command(side=side, order_type="market", limit_price=None, quantity="3"))
    assert result["submittedQuantity"] == "3" and result["orderType"] == "MO"
    assert result["outsideRth"] == "AnyTime"
    assert "submittedPrice" not in result


@pytest.mark.parametrize("kind,tif", [("market", "fok"), ("market", "ioc"), ("limit", "fok"), ("limit", "ioc")])
def test_unsupported_tif_is_not_silently_downgraded(kind, tif):
    with pytest.raises(UnsupportedOrder, match="FOK"):
        Adapter({}).build_order(command(order_type=kind, time_in_force=tif))


@pytest.mark.parametrize("symbol", ["BTCUSDT", "BTC-USD", "700.HK", "AAPL261016C00190000.US", "AAPL261016C190000.US", "AAPL/US"])
def test_non_us_stock_symbols_rejected(symbol):
    with pytest.raises(UnsupportedOrder):
        Adapter({}).build_order(command(symbol=symbol))


@pytest.mark.parametrize("asset", ["crypto", "option", "us_option", "future", "hk_equity"])
def test_non_stock_asset_class_rejected(asset):
    with pytest.raises(UnsupportedOrder):
        Adapter({}).build_order(command(asset_class=asset))


@pytest.mark.parametrize("config", [{"account_type": "classic_spot"}, {"borrow_scope": "uta"}, {"asset_class": "crypto"}])
def test_old_crypto_configuration_cannot_select_an_old_endpoint(config):
    with pytest.raises(ConfigurationError):
        Adapter(config)


def test_extended_limit_and_gtd_validation():
    adapter = Adapter({})
    result = adapter.build_order(command(time_in_force="gtd", options={"expire_date": "2026-11-01", "extended_hours": True}))
    assert result["timeInForce"] == "GoodTilDate" and result["expireDate"] == "2026-11-01"
    assert result["outsideRth"] == "AnyTime"
    with pytest.raises(UnsupportedOrder):
        adapter.build_order(command(time_in_force="gtd"))
    with pytest.raises(UnsupportedOrder):
        adapter.build_order(command(options={"expire_date": "2026-11-01"}))
    with pytest.raises(UnsupportedOrder):
        adapter.build_order(command(options={"outside_rth": "RTHOnly", "extended_hours": True}))


@pytest.mark.parametrize("kind", ["market", "limit"])
@pytest.mark.parametrize("options,expected", [
    ({}, "AnyTime"),
    ({"extended_hours": True}, "AnyTime"),
    ({"outside_rth": "ANY_TIME"}, "AnyTime"),
    ({"extended_hours": False}, "RTHOnly"),
    ({"outside_rth": "RTHOnly"}, "RTHOnly"),
    ({"outside_rth": "OVERNIGHT"}, "Overnight"),
])
def test_signed_post_preserves_order_type_and_session_after_normalization(kind, options, expected):
    adapter = Adapter({"api_key": "test-key", "api_secret": "test-secret", "passphrase": "test-pass"})
    static = Mock(status_code=200)
    static.json.return_value = {"code": "00000", "data": instrument()}
    response = Mock(status_code=200)
    response.json.return_value = {"code": "00000", "data": {"orderId": "123", "symbol": "AAPL.US"}}
    with patch("requests.request", side_effect=[static, response]) as request, patch("time.time_ns", return_value=1234000000):
        payload = command(order_type=kind, options=options)
        if kind == "market":
            del payload["limit_price"]
        adapter.place_order(normalize(payload, "bitget", {}))
    assert request.call_count == 2
    args, kwargs = request.call_args
    assert args == ("POST", "https://api.bitget.com" + PREFIX + "/trade/place-order")
    signed = ("1234POST" + PREFIX + "/trade/place-order" + kwargs["data"]).encode()
    expected_signature = base64.b64encode(hmac.new(b"test-secret", signed, hashlib.sha256).digest()).decode()
    assert kwargs["headers"]["ACCESS-SIGN"] == expected_signature
    body = json.loads(kwargs["data"])
    assert body["submittedQuantity"] == "0.125"
    assert body["outsideRth"] == expected
    assert body["orderType"] == ("MO" if kind == "market" else "LO")
    if kind == "market":
        assert "submittedPrice" not in body
    else:
        assert body["submittedPrice"] == "190.5"
    assert kwargs["allow_redirects"] is False


def test_luld_order_keeps_regular_session_default():
    payload = normalize(command(auction_intent="luld_reopening"), "bitget", {})
    assert Adapter({}).build_order(payload)["outsideRth"] == "RTHOnly"
    with pytest.raises(UnsupportedOrder, match="LULD intent requires regular trading hours"):
        Adapter({}).build_order({**payload, "options": {"outside_rth": "AnyTime"}})


def test_extended_market_broker_rejection_is_reported_without_retry_or_conversion():
    adapter = Adapter({"api_key": "key", "api_secret": "secret", "passphrase": "pass"})
    adapter._stock_instrument = Mock(return_value=instrument()["list"][0])
    response = Mock(status_code=200)
    response.json.return_value = {"code": "TEST_REJECTED", "msg": "Order type unavailable in this session"}
    with patch("requests.request", return_value=response) as request:
        with pytest.raises(BrokerError, match="Order type unavailable in this session"):
            adapter.place_order(command(order_type="market", limit_price=None))
    assert request.call_count == 1
    body = json.loads(request.call_args.kwargs["data"])
    assert body["orderType"] == "MO" and body["outsideRth"] == "AnyTime"


def test_get_signature_includes_exact_encoded_query():
    adapter = Adapter({"api_key": "key", "api_secret": "secret", "passphrase": "pass"})
    response = Mock(status_code=200)
    response.json.return_value = {"code": "00000", "data": {"list": []}}
    with patch("requests.request", return_value=response) as request, patch("time.time_ns", return_value=1234000000):
        adapter._request("GET", PREFIX + "/market/static", {"symbol": "AAPL.US,BRK.B.US"})
    args, kwargs = request.call_args
    path = PREFIX + "/market/static?symbol=AAPL.US%2CBRK.B.US"
    assert args[1] == "https://api.bitget.com" + path
    assert kwargs["headers"]["ACCESS-SIGN"] == adapter.signature("secret", "1234", "GET", path)
    assert kwargs["data"] is None


def test_ambiguous_post_is_never_retried():
    import requests
    adapter = Adapter({"api_key": "test", "api_secret": "test", "passphrase": "test"})
    adapter._stock_instrument = Mock(return_value=instrument()["list"][0])
    with patch("requests.request", side_effect=requests.Timeout) as request:
        with pytest.raises(OutcomeUnknown):
            adapter.place_order(command())
    assert request.call_count == 1


@pytest.mark.parametrize("result", [{}, {"orderId": "123", "symbol": "MSFT.US"}])
def test_ambiguous_placement_acknowledgement_requires_reconciliation(result):
    adapter = Adapter({})
    adapter._request = Mock(side_effect=[instrument(), result])
    with pytest.raises(OutcomeUnknown):
        adapter.place_order(command())


def test_instrument_preflight_rejects_missing_or_nonstock_before_write():
    for data in ({"list": []}, {"list": [{"symbol": "AAPL.US", "currency": "USD", "board": "USOption"}]}):
        adapter = Adapter({})
        adapter._request = Mock(return_value=data)
        with pytest.raises(UnsupportedOrder):
            adapter.place_order(command())
        assert adapter._request.call_count == 1
        assert adapter._request.call_args.args[0] == "GET"


def test_today_history_merges_terminal_updates_and_filters_exact_day():
    adapter = Adapter({})
    start = datetime(2026, 10, 25, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    adapter._request = Mock(side_effect=[{"orders": [order(), order("older", submittedAt="2026-10-24T12:00:00Z")]},
                                        {"hasMore": False, "orders": [order(status="Filled", updatedAt="2026-10-25T14:05:00Z"),
                                                                     order("next", submittedAt="2026-10-26T00:00:00Z")]}])
    result = adapter.orders_today(start, end)
    assert len(result) == 1 and result[0]["status"] == "Filled"
    assert adapter._request.call_args.args[2] == {"market": "US", "startAt": int(start.timestamp()), "endAt": int(end.timestamp()) - 1}


def test_history_has_more_splits_without_losing_second_boundaries():
    adapter = Adapter({})
    adapter._request = Mock(side_effect=[{"orders": [], "hasMore": True}, {"orders": [order("a")], "hasMore": False},
                                        {"orders": [order("b")], "hasMore": False}])
    assert [row["orderId"] for row in adapter._history(0, 3)] == ["a", "b"]
    assert [call.args[2] for call in adapter._request.call_args_list] == [
        {"market": "US", "startAt": 0, "endAt": 3}, {"market": "US", "startAt": 0, "endAt": 1},
        {"market": "US", "startAt": 2, "endAt": 3}]


def test_empty_history_without_has_more_preserves_other_split_windows():
    adapter = Adapter({})
    adapter._request = Mock(side_effect=[{"orders": [], "hasMore": True}, {"orders": []},
                                        {"orders": [order("retained")], "hasMore": False}])
    assert [row["orderId"] for row in adapter._history(0, 3)] == ["retained"]
    assert [(call.args[2]["startAt"], call.args[2]["endAt"])
            for call in adapter._request.call_args_list] == [(0, 3), (0, 1), (2, 3)]


def test_empty_live_history_without_has_more_allows_account_order_refresh():
    adapter = Adapter({})
    adapter._request = Mock(side_effect=[{"orders": []}, {"orders": []}])
    start = datetime(2026, 10, 25, tzinfo=timezone.utc)
    assert adapter.orders_today(start, start + timedelta(days=1)) == []


@pytest.mark.parametrize("data", [{"orders": [], "hasMore": True},
                                 {"orders": [], "hasMore": "false"}, {"orders": [], "hasMore": None},
                                 {"orders": [], "hasMore": 0}, {"orders": None}])
def test_history_cannot_claim_complete_on_overflow_or_invalid_metadata(data):
    adapter = Adapter({})
    adapter._request = Mock(return_value=data)
    with pytest.raises(BrokerError):
        adapter._history(42, 42)


@pytest.mark.parametrize("status,expected", [("Normal", True), ("Halted", None), ("unknown", None)])
def test_tradability_preserves_unknown_account_permission_and_unrecognized_status(status, expected):
    adapter = Adapter({})
    adapter._request = Mock(side_effect=[instrument(), {"list": [{"symbol": "AAPL.US", "tradeStatus": status, "timestamp": "2026-10-25T14:00:00Z"}]}])
    result = adapter.tradability("AAPL")
    assert result["tradable"] is expected and result["account_tradable"] is None
    assert result["luld_applicable"] is True


def test_borrow_query_is_unknown_and_never_calls_crypto_loans():
    adapter = Adapter({})
    adapter._request = Mock()
    result = adapter.borrow_availability("AAPL")
    assert result["symbol"] == "AAPL.US" and result["supported"] is False
    assert result["available_quantity"] is result["shortable"] is result["fee_rate"] is None
    adapter._request.assert_not_called()


def test_plain_ticker_normalization_matches_shared_us_stock_scope():
    assert Adapter({}).build_order(command(symbol=" aapl.us "))["symbol"] == "AAPL.US"
    with pytest.raises(UnsupportedOrder):
        Adapter({}).build_order(command(market="HK"))


def test_trade_requests_are_locally_paced_to_stockplus_quota():
    adapter = Adapter({"api_key": "k", "api_secret": "s", "passphrase": "p"})
    adapter._trade_times.extend([0.0] * 30)
    response = Mock(status_code=200)
    response.json.return_value = {"code": "00000", "data": {"orders": []}}
    with patch("time.monotonic", side_effect=[1.0, 30.0]), patch("time.sleep") as sleep, patch("requests.request", return_value=response):
        adapter._request("GET", PREFIX + "/trade/today-orders", {"market": "US"})
    sleep.assert_called_once_with(29.0)
    assert list(adapter._trade_times) == [30.0]
