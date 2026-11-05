"""Automatic overnight routing and exact-order cancellation readback."""
from datetime import datetime
import json
from unittest.mock import Mock, patch

import pytest

from bitget.adapter import Adapter, PREFIX
from common.commands import normalize
from common.errors import BrokerError, ConfigurationError, OutcomeUnknown, UnsupportedOrder
from test_bitget import command, instrument, order


@pytest.mark.parametrize("stamp,expected", [
    ("2026-11-05T00:59:59+00:00", "AnyTime"),
    ("2026-11-05T01:00:00+00:00", "Overnight"),
    ("2026-11-05T08:59:59+00:00", "Overnight"),
    ("2026-11-05T09:00:00+00:00", "AnyTime"),
    ("2026-07-08T23:59:59+00:00", "AnyTime"),
    ("2026-07-09T00:00:00+00:00", "Overnight"),
    ("2026-07-09T07:59:59+00:00", "Overnight"),
    ("2026-07-09T08:00:00+00:00", "AnyTime"),
    ("2026-03-08T06:30:00+00:00", "Overnight"),
    ("2026-03-08T07:30:00+00:00", "Overnight"),
    ("2026-11-01T05:30:00+00:00", "Overnight"),
    ("2026-11-01T06:30:00+00:00", "Overnight"),
])
@pytest.mark.parametrize("kind", ["market", "limit"])
def test_automatic_routing_follows_new_york_boundaries_and_dst(stamp, expected, kind):
    adapter = Adapter({"default_outside_rth": "Auto", "timezone": "Asia/Singapore"})
    with patch("bitget.adapter.datetime") as clock:
        clock.now.return_value = datetime.fromisoformat(stamp)
        if kind == "market" and expected == "Overnight":
            with pytest.raises(UnsupportedOrder, match="Overnight trading requires a limit order"):
                adapter.build_order(command(order_type=kind, limit_price=None))
            return
        result = adapter.build_order(command(order_type=kind, limit_price="190.5" if kind == "limit" else None))
    assert result["outsideRth"] == expected
    assert result["orderType"] == ("LO" if kind == "limit" else "MO")


@pytest.mark.parametrize("options,expected", [
    ({"outside_rth": "RTHOnly"}, "RTHOnly"),
    ({"extended_hours": False}, "RTHOnly"),
    ({"outside_rth": "AnyTime"}, "AnyTime"),
    ({"extended_hours": True}, "Overnight"),
    ({"outside_rth": "Overnight", "extended_hours": True}, "Overnight"),
    ({"outside_rth": "AUTO"}, "Overnight"),
])
def test_explicit_session_intent_and_extended_hours_are_preserved(options, expected):
    with patch("bitget.adapter.datetime") as clock:
        clock.now.return_value = datetime.fromisoformat("2026-11-05T02:00:00+00:00")
        result = Adapter({"default_outside_rth": "Auto"}).build_order(command(options=options))
    assert result["outsideRth"] == expected


def test_overnight_payload_reaches_transport_after_ktrader_normalization():
    adapter = Adapter({"default_outside_rth": "Auto", "api_key": "key", "api_secret": "secret", "passphrase": "pass"})
    static = Mock(status_code=200)
    static.json.return_value = {"code": "00000", "data": instrument()}
    ack = Mock(status_code=200)
    ack.json.return_value = {"code": "00000", "data": {"orderId": "001"}}
    payload = {"id": "overnight", "type": "LIMIT_ORDER", "symbol": "AAPL", "side": "BUY",
               "qty_shares": "1", "limit_price": "190.50", "allowPrePost": True}
    with patch("bitget.adapter.datetime") as clock, patch("requests.request", side_effect=[static, ack]) as request:
        clock.now.return_value = datetime.fromisoformat("2026-11-05T02:00:00+00:00")
        adapter.place_order(normalize(payload, "bitget", {}))
    assert request.call_args.args == ("POST", "https://api.bitget.com" + PREFIX + "/trade/place-order")
    body = json.loads(request.call_args.kwargs["data"])
    assert body["outsideRth"] == "Overnight" and body["timeInForce"] == "Day"
    assert body["submittedPrice"] == "190.5" and body["submittedQuantity"] == "1"


def test_auto_never_overrides_luld_regular_hours():
    adapter = Adapter({"default_outside_rth": "Auto"})
    assert adapter.build_order(command(auction_intent="luld_reopening"))["outsideRth"] == "RTHOnly"
    with pytest.raises(UnsupportedOrder, match="LULD"):
        adapter.build_order(command(auction_intent="luld_reopening", options={"outside_rth": "Auto"}))


@pytest.mark.parametrize("value", ["invalid", [], None, True])
def test_invalid_config_session_fails_before_dispatch(value):
    with pytest.raises(ConfigurationError, match="default_outside_rth"):
        Adapter({"default_outside_rth": value})


@pytest.mark.parametrize("ack", [{}, None, {"orderId": "other"}])
def test_bad_cancel_ack_is_resolved_only_by_exact_terminal_readback(ack):
    adapter = Adapter({})
    adapter._request = Mock(side_effect=[order(), instrument(), ack, order(status="Canceled")])
    result = adapter.cancel_stock_order("001")
    assert result["terminal_confirmed"] is True and result["status"] == "Canceled"
    assert result["acknowledgement_verified"] is False
    assert [call.args[0] for call in adapter._request.call_args_list] == ["GET", "GET", "POST", "GET"]
    assert adapter._request.call_args.args == ("GET", PREFIX + "/trade/order-detail", {"orderId": "001"})


@pytest.mark.parametrize("readback", [order(), order(identity="other", status="Canceled"), BrokerError("read failed")])
def test_bad_ack_with_unproven_target_outcome_stays_unknown(readback):
    adapter = Adapter({})
    adapter._request = Mock(side_effect=[order(), instrument(), {}, readback])
    with pytest.raises(OutcomeUnknown):
        adapter.cancel_stock_order("001")
    assert sum(call.args[0] == "POST" for call in adapter._request.call_args_list) == 1
