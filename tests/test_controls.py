from unittest.mock import Mock
import pytest
from common.commands import normalize, validate_age
from common.errors import AuthenticationError, BrokerError, CommandError, ConfigurationError
from common.runner import Runner


def test_ts_preserves_gigamoney_staleness():
    with pytest.raises(CommandError):
        validate_age({"ts": "2020-01-01T00:00:00Z"}, kafka_timestamp_ms=1800000000000, now=1800000000)


def test_cancel_unfilled_never_becomes_resting_limit():
    raw = {"id":"x", "type":"LIMIT_ORDER", "symbol":"AAPL", "side":"buy", "qty_shares":1, "limit_price":"10", "cancel_unfilled":True}
    assert normalize(raw, "alpaca", {})["time_in_force"] == "fok"
    with pytest.raises(CommandError):
        normalize({**raw, "time_in_force":"day"}, "alpaca", {})
    with pytest.raises(CommandError):
        normalize({**raw, "cancel_unfilled":"false"}, "alpaca", {})


def test_controls_normalize_and_conflicts_reject():
    assert normalize({"id":"x","type":"CANCEL_OPEN_ORDERS"},"alpaca",{})["action"] == "cancel_open_orders"
    result = normalize({"id":"x","type":"CANCEL_ORDER","orderId":"123"},"alpaca",{})
    assert result["order_id"] == "123"
    for raw in ({"type":"CANCEL_ORDER"}, {"type":"CANCEL_OPEN_ORDERS","symbol":"AAPL"}, {"type":"SET_TRADING_ENABLED","trading_enabled":"false"}):
        with pytest.raises(CommandError):
            normalize({"id":"x",**raw},"alpaca",{})


@pytest.mark.parametrize("selector", ["symbol", "ticker", "code"])
def test_bitget_smart_cancel_preserves_symbol_scope(selector):
    command = normalize({"command_id": "smart-cancel", "type": "CANCEL_OPEN_ORDERS",
                         "account_id": "BITGET_WXL", selector: "POM",
                         "parent_id": "parent", "trading_mode": "H"},
                        "bitget", {"routing": {"account_id": "BITGET_WXL"}}, require_account=True)
    assert command == {"id": "smart-cancel", "broker": "bitget",
                       "action": "cancel_open_orders", "symbol": "POM.US"}
    with pytest.raises(CommandError, match="use cancel_order"):
        normalize({"id": "x", "type": "CANCEL_OPEN_ORDERS", selector: "POM", "order_id": "a"},
                  "bitget", {})


def test_persistent_disable_survives_restart_and_old_enable_replay(tmp_path,monkeypatch):
    adapter=Mock()
    monkeypatch.setattr("common.runner.adapter_for",lambda *_:adapter)
    config={"trading_enabled":True,"journal_account":"paper-account"}
    path=tmp_path/"commands.sqlite3"
    runner=Runner("alpaca",config,True,path)
    enable={"id":"enable-1","type":"SET_TRADING_ENABLED","trading_enabled":True}
    disable={"id":"disable-1","type":"SET_TRADING_ENABLED","trading_enabled":False}
    assert runner.process(enable)["status"]=="ok"
    assert runner.process(disable)["data"]["trading_enabled"] is False
    assert runner.process(enable)["duplicate"] is True
    assert runner.trading_enabled() is False
    runner.close()
    runner=Runner("alpaca",config,True,path)
    assert runner.trading_enabled() is False
    with pytest.raises(ConfigurationError):
        runner.process({"id":"order-1","type":"MARKET_ORDER","symbol":"AAPL","qty_shares":1,"side":"buy"})
    adapter.place_order.assert_not_called()
    runner.close()


def test_remote_control_cannot_override_local_disabled_config(tmp_path,monkeypatch):
    monkeypatch.setattr("common.runner.adapter_for",lambda *_:Mock())
    runner=Runner("alpaca",{"trading_enabled":False,"journal_account":"paper"},True,tmp_path/"state.sqlite3")
    with pytest.raises(ConfigurationError):
        runner.process({"id":"enable","type":"SET_TRADING_ENABLED","trading_enabled":True})
    assert runner.process({"id":"disable","type":"SET_TRADING_ENABLED","trading_enabled":False})["status"]=="ok"
    runner.close()


def test_uncertain_account_action_blocks_new_ids_until_reviewed(tmp_path,monkeypatch):
    adapter=Mock();adapter.place_order.side_effect=TimeoutError()
    monkeypatch.setattr("common.runner.adapter_for",lambda *_:adapter)
    runner=Runner("alpaca",{"trading_enabled":True,"journal_account":"paper"},True,tmp_path/"state.sqlite3")
    order={"id":"old","type":"MARKET_ORDER","symbol":"AAPL","qty_shares":1,"side":"buy"}
    assert runner.process(order)["status"]=="unknown"
    with pytest.raises(ConfigurationError):runner.process({**order,"id":"new"})
    rows=runner.journal.unresolved(runner.scope)
    assert rows[0]["command"]["symbol"]=="AAPL"
    assert runner.process({"id":"disable","type":"SET_TRADING_ENABLED","trading_enabled":False})["status"]=="ok"
    runner.journal.reconcile(runner.scope,"old","found","Verified exact client ID with broker order history","broker-123")
    assert runner.journal.unresolved(runner.scope)==[]
    assert runner.process(order)["status"]=="reconciled"
    adapter.place_order.assert_called_once()
    runner.close()


def test_proven_unaccepted_auth_releases_reservation_for_repaired_credentials(tmp_path,monkeypatch):
    adapter=Mock();adapter.place_order.side_effect=AuthenticationError("denied",no_submission=True)
    monkeypatch.setattr("common.runner.adapter_for",lambda *_:adapter)
    runner=Runner("alpaca",{"trading_enabled":True,"journal_account":"paper"},True,tmp_path/"state.sqlite3")
    order={"id":"order","type":"MARKET_ORDER","symbol":"AAPL","qty_shares":1,"side":"buy"}
    first=runner.process(order)
    assert first["status"]=="authentication_required" and first["submission_outcome"]=="not_submitted"
    assert runner.journal.unresolved(runner.scope)==[]
    adapter.place_order.side_effect=None;adapter.place_order.return_value={"id":"broker"}
    assert runner.process(order)["status"]=="submitted"
    runner.close()


def test_auth_without_proof_keeps_ambiguous_command_reserved(tmp_path,monkeypatch):
    adapter=Mock();adapter.place_order.side_effect=AuthenticationError("auth after uncertain step")
    monkeypatch.setattr("common.runner.adapter_for",lambda *_:adapter)
    runner=Runner("alpaca",{"trading_enabled":True,"journal_account":"paper"},True,tmp_path/"state.sqlite3")
    result=runner.process({"id":"order","type":"MARKET_ORDER","symbol":"AAPL","qty_shares":1,"side":"buy"})
    assert result["submission_outcome"]=="unknown"
    assert runner.journal.unresolved(runner.scope)[0]["state"]=="unknown"
    runner.close()


@pytest.mark.parametrize("broker,symbol,asset,expected", [("alpaca","BTC/USD",None,"gtc"),("webull","BTCUSD","crypto","ioc")])
def test_nonstock_market_requests_are_rejected(broker,symbol,asset,expected):
    payload={"id":"x","type":"MARKET_ORDER","symbol":symbol,"side":"buy","quantity":"1"}
    if asset:payload["asset_class"]=asset
    with pytest.raises(BrokerError):
        normalize(payload,broker,{})


def test_nonstock_config_is_rejected_before_submission():
    with pytest.raises(BrokerError):
        normalize({"id":"x","type":"MARKET_ORDER","symbol":"ESH27","side":"buy","quantity":"1"},"webull",{"asset_class":"futures"})
