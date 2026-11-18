from datetime import datetime,timezone
from unittest.mock import Mock
import pytest
from common.snapshots import collect_account,collect_orders,map_order
from common.publication import validate_order_snapshot
from common.registry import adapter_for
from common.errors import BrokerError,ConfigurationError

NOW=datetime(2026,10,25,15,tzinfo=timezone.utc)
STAMP="2026-10-25T14:00:00Z"


def canonical(row):
    return validate_order_snapshot({**row,"schema_version":1,"account_id":"ACC"})


def test_alpaca_account_and_negative_short_position():
    adapter=Mock()
    adapter.positions.return_value=[{"symbol":"AAPL","qty":"-3","avg_entry_price":"12.5","market_value":"-42"}]
    adapter.http.request.return_value={"currency":"USD","cash":"0"}
    data=collect_account(adapter,"alpaca")
    assert data["cash"]==0 and data["positions"][0]["qty"]==-3
    assert data["positions"][0]["marketValue"]==-42


def test_trade_station_cash_and_account_isolation():
    adapter=Mock(config={"account_id":"a"})
    adapter._accounts.return_value=["a"]
    adapter._request.return_value={"Balances":[{"AccountID":"a","CashBalance":"42.5"}]}
    adapter.positions.return_value=[{"AccountID":"a","Symbol":"AAPL","Quantity":"2","AveragePrice":"5"}]
    assert collect_account(adapter,"tradestation")["cash"]==42.5
    adapter._accounts.return_value=["a","b"]
    with pytest.raises(ConfigurationError):collect_account(adapter,"tradestation")


def test_stockplus_account_snapshot_hook_preserves_stock_cash_and_positions():
    adapter=adapter_for("bitget",{})
    adapter.positions=Mock(return_value=[{"symbol":"AAPL.US","quantity":"2","costPrice":"100","availableQuantity":"1","accountChannel":"stock","currency":"USD"}])
    adapter._request=Mock(return_value={"list":[{"currency":"USD","totalCash":"42","cashInfos":[{"currency":"USD","availableCash":"30"}]}]})
    data=collect_account(adapter,"bitget")
    assert data["cash"]==30 and data["positions"][0]["symbol"]=="AAPL"
    assert data["positions"][0]["qty"]==2


def test_robinhood_stock_snapshot_uses_portfolio_cash_not_crypto_proxy():
    adapter=adapter_for("robinhood",{"account_number":"a"})
    adapter._account_record=Mock(return_value={"account_number":"a"})
    adapter._call_tool=Mock(return_value={"currency":"USD","cash":"42","buying_power":{"display_currency":"USD","buying_power":"30"}})
    adapter.positions=Mock(return_value=[{"symbol":"AAPL","quantity":"2","average_buy_price":"100","shares_available_for_sells":"1"}])
    data=collect_account(adapter,"robinhood")
    assert data["cash"]==42 and data["positions"][0]["avg_price"]==100
    assert data["buying_power_by_currency"]["USD"]==30


def test_zhuorui_bridge_decimals_are_json_numbers_without_id_conversion():
    adapter=Mock(config={})
    adapter._bridge.return_value={"cash":"10.12","cash_by_currency":{"USD":"10.12"},"positions":[{"symbol":"AAPL","qty":"3","avg_price":"-2"}]}
    data=collect_account(adapter,"zhuorui")
    assert data["cash"]==10.12 and data["positions"][0]["avg_price"]==-2
    adapter._bridge.return_value=[{"order_id":"0000123","created_at":STAMP,"updated_at":STAMP,"symbol":"AAPL","side":"BUY","order_type":"LIMIT","status":"PARTIALLY_FILLED","quantity":"3","filled_quantity":"1","average_fill_price":"2.5","limit_price":"3"}]
    rows=collect_orders(adapter,"zhuorui",NOW,NOW)
    assert rows[0]["order_id"]=="0000123" and canonical(rows[0])["filled_quantity"]==1


def test_alpaca_order_cumulative_fills_and_notional():
    raw={"id":"001","submitted_at":STAMP,"updated_at":STAMP,"symbol":"AAPL","side":"buy","type":"limit","status":"canceled","qty":"3","filled_qty":"1","filled_avg_price":"10","limit_price":"11"}
    data=canonical(map_order("alpaca",raw,NOW))
    assert data["status"]=="CANCELED" and data["filled_quantity"]==1
    with pytest.raises(BrokerError):map_order("alpaca",{**raw,"filled_avg_price":None},NOW)
    data=canonical(map_order("alpaca",{**raw,"type":"market","qty":None,"notional":"100"},NOW))
    assert data["quantity"] is None and data["notional"]==100


def test_trade_station_single_leg_uses_top_level_average_not_last_execution():
    raw={"OrderID":"a","OpenedDateTime":STAMP,"OrderType":"Limit","Status":"FLP","FilledPrice":"10.4","LimitPrice":"11","Legs":[{"Symbol":"AAPL","BuyOrSell":"SellShort","QuantityOrdered":"3","ExecQuantity":"2","ExecutionPrice":"10.1"}]}
    data=canonical(map_order("tradestation",raw,NOW))
    assert data["average_fill_price"]==10.4 and data["status"]=="CANCELED" and data["side"]=="SELL"
    with pytest.raises(BrokerError):map_order("tradestation",{**raw,"Legs":raw["Legs"]*2},NOW)


def test_bitget_stock_market_buy_quantity_is_shares():
    raw={"orderId":"123","currency":"USD","submittedAt":STAMP,"symbol":"AAPL.US","side":"Buy","orderType":"MO","status":"Filled","quantity":"2","executedQuantity":"2","executedPrice":"100"}
    data=canonical(map_order("bitget",raw,NOW))
    assert data["quantity"]==2 and data["filled_quantity"]==2 and data["currency"]=="USD"
    assert "notional" not in data


def test_robinhood_stock_order_maps_native_cumulative_fields():
    raw={"id":"id","created_at":STAMP,"symbol":"AAPL","type":"limit","side":"buy","state":"partially_filled","quantity":"2","price":"100","cumulative_quantity":"1","average_price":"99"}
    data=canonical(map_order("robinhood",raw,NOW))
    assert data["filled_quantity"]==1 and data["average_fill_price"]==99 and data["quantity"]==2
