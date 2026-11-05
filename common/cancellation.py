"""Account-scoped cancellation requests. No write is retried or called a fill result.

The caller owns durable command claiming and explicit execution authorization.
Open-order discovery has no creation-date filter; it is deliberately separate
from orders_today(). Acknowledgement is not final cancellation confirmation.
"""
from __future__ import annotations

import re
import time
from urllib.parse import quote, urlencode

from .errors import AuthenticationError, BrokerError, CancellationDiscoveryError, CommandError, OutcomeUnknown


LONG_OPEN = {
    "NotReported", "ReplacedNotReported", "ProtectedNotReported", "VarietiesNotReported",
    "WaitToNew", "NewStatus", "WaitToReplace", "PendingReplaceStatus", "ReplacedStatus",
    "PartialFilledStatus",
}
LONG_CLOSED = {"FilledStatus", "RejectedStatus", "CanceledStatus", "ExpiredStatus", "PartialWithdrawal"}
LONG_PENDING = {"WaitToCancel", "PendingCancelStatus"}
TS_OPEN = {
    "ACK", "ASS", "BRO", "CHG", "CND", "COR", "DIS", "DON", "EXE", "FPR", "LAT",
    "OPN", "OSO", "PLA", "RJC", "RPD", "RSN", "STP", "STT", "SUS",
}
TS_CLOSED = {"BRC", "BRF", "DOA", "CAN", "EXP", "OUT", "RJR", "SCN", "TSC", "UCH", "REJ", "FLL", "FLP"}
TS_PENDING = {"CSN", "UCN", "ECN", "REC"}


def _id(value):
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > 256 or any(
        char.isspace() or ord(char) < 32 or ord(char) >= 127 for char in value
    ):
        raise CommandError("Cancellation requires a nonempty string order_id without whitespace")
    return value


def _rows(value, label):
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise BrokerError(f"Invalid {label} order list; no cancellations sent")
    return value


def _is_open(row, field, opened, closed, pending=()):
    state = row.get(field)
    if state in opened:
        return True
    if state in closed or state in pending:
        return False
    raise BrokerError(f"Unrecognized cancellation state {state!r}; no cancellations sent")


def _unique(rows, key):
    unique = {}
    for row in rows:
        identity = _id(row.get(key))
        if identity in unique and unique[identity] != row:
            raise BrokerError("Conflicting duplicate open orders; no cancellations sent")
        unique[identity] = row
    return list(unique.values())


def _result(broker, requests, scope=None):
    states = {row["status"] for row in requests}
    status = ("outcome_unknown" if "outcome_unknown" in states else
              "cancel_rejected" if "cancel_rejected" in states else
              "cancel_requested" if "cancel_requested" in states else "no_action")
    return {"broker": broker, "status": status, "cancellation_confirmed": False,
            "requests": requests,
            "cancellations_requested": sum(row["status"] == "cancel_requested" for row in requests),
            "scope": scope or "Configured account; open orders from all creation dates",
            "message": "Requests are acknowledgements only. Read order status to establish the final result."}


def _requests(broker, rows, send):
    """Retain earlier results on a partial failure; never resend an uncertain write."""
    results = []
    for row in rows:
        identity = row["order_id"]
        target = {"order_id": identity}
        if broker == "bitget" and row.get("symbol"):
            target["symbol"] = row["symbol"]
        try:
            result = send(row)
            results.append({**target, "status": "no_action" if isinstance(result,dict) and (result.get("already_terminal") or result.get("cancel_requested") is False) else "cancel_requested", "raw": result})
        except OutcomeUnknown as exc:
            results.append({**target, "status": "outcome_unknown", "error": str(exc)})
            break
        except BrokerError as exc:
            results.append({**target, "status": "cancel_rejected", "error": str(exc)})
            break
    response = _result(broker, results)
    response["not_requested_order_ids"] = [row["order_id"] for row in rows[len(results):]]
    return response


def _rh_orders(adapter):
    return _unique(_rows(adapter.open_stock_orders(), "Robinhood US stock"), "order_id")


def _ts_orders(adapter):
    accounts = adapter._accounts()
    rows = []
    for offset in range(0, len(accounts), 10):
        segment = ",".join(quote(account, safe="") for account in accounts[offset:offset + 10])
        rows.extend(adapter._orders(f"/brokerage/accounts/{segment}/orders", {"status": "Open"}))
    for row in _rows(rows, "TradeStation"):
        if row.get("AccountID") not in accounts:
            raise BrokerError("TradeStation cancellation read returned another account")
    return [{**row, "order_id": row["OrderID"]} for row in _unique(rows, "OrderID")
            if adapter.is_us_stock_order(row) and _is_open(row, "Status", TS_OPEN, TS_CLOSED, TS_PENDING)]


def _long_orders(adapter):
    # Today is not documented to include older GTC orders. Explicit epoch-to-now
    # history filters avoid the endpoint's implicit 90-day lookback. has_more and
    # the 1000-record ceiling require interval subdivision, before any writes.
    calls = 0
    def history(lower, upper):
        nonlocal calls
        calls += 1
        if calls > int(adapter.config.get("max_pages", 10000)):
            raise BrokerError("Longbridge cancellation discovery limit reached; no cancellations sent")
        params = [("start_at", str(lower)), ("end_at", str(upper))]
        params += [("status", state) for state in sorted(LONG_OPEN)]
        response = adapter._request("get", "/v1/trade/order/history?" + urlencode(params))
        if not isinstance(response, dict) or type(response.get("has_more", False)) is not bool:
            raise BrokerError("Invalid Longbridge cancellation history response")
        rows = _rows(response.get("orders"), "Longbridge")
        if response.get("has_more") or len(rows) >= 1000:
            if upper - lower <= 1:
                raise BrokerError("Longbridge open orders exceed the API ceiling in one second; no cancellations sent")
            midpoint = (lower + upper) // 2
            return history(lower, midpoint) + history(midpoint, upper)
        return rows
    rows = history(0, int(time.time()))
    today = adapter._request("get", "/v1/trade/order/today")
    if not isinstance(today, dict) or today.get("has_more"):
        raise BrokerError("Incomplete Longbridge current-order response")
    # Today's state is newer and overrides a historical state of the same ID.
    by_id = {_id(row.get("order_id")): row for row in rows}
    for row in _rows(today.get("orders"), "Longbridge"):
        by_id[_id(row.get("order_id"))] = row
    return [row for row in by_id.values()
            if adapter.is_us_stock_order(row) and _is_open(row, "status", LONG_OPEN, LONG_CLOSED, LONG_PENDING)]


def _webull_orders(adapter):
    account = adapter._account_id()
    groups = list(adapter._pages(adapter._client().order_v3.list_order_open, account_id=account))
    rows = []
    for group in _rows(groups, "Webull"):
        if group.get("account_id", account) != account:
            raise BrokerError("Webull cancellation read returned another account")
        for row in _rows(group.get("orders"), "Webull"):
            if row.get("account_id", account) != account:
                raise BrokerError("Webull cancellation read returned another account")
            if adapter.is_us_stock_order(row) and _is_open(row, "status", {"PENDING", "SUBMITTED", "PARTIAL_FILLED", "PARTIAL FILLED"},
                        {"FILLED", "CANCELLED", "FAILED", "EXPIRED", "REJECTED"}, {"PENDING_CANCEL"}):
                # SDK v3 accepts client_order_id, never the broker order_id.
                _id(row.get("client_order_id"))
                rows.append({**row, "order_id": row.get("order_id") or row["client_order_id"]})
    return _unique(rows, "order_id")


def _bitget_orders(adapter, command=None):
    if command:
        row=adapter.stock_order_detail(command["order_id"])
        if command.get("symbol") is not None and adapter.stock_symbol(command["symbol"]) != row["symbol"]:
            raise BrokerError("Stock+ cancellation symbol mismatch")
        rows=[{**row,"order_id":row["orderId"]}]
    else:
        rows=_rows(adapter.open_stock_orders(),"Bitget Stock+")
        rows=[{**row,"order_id":row.get("order_id") or row["orderId"]} for row in rows]
    opened={"NotReported","ReplacedNotReported","ProtectedNotReported","VarietiesNotReported","WaitToNew","New","WaitToReplace","PendingReplace","Replaced","PartialFilled"}
    closed={"Filled","Rejected","Canceled","Expired","PartialWithdrawal"}
    return _unique([row for row in rows if _is_open(row,"status",opened,closed,{"WaitToCancel","PendingCancel"})],"order_id")


def _send(adapter, broker, row):
    identity = row["order_id"]
    if broker in {"bitget","robinhood"}:
        return adapter.cancel_stock_order(identity, symbol=row.get("symbol"))
    if broker == "alpaca":
        return adapter.http.request("DELETE", "/v2/orders/" + quote(identity,safe=""))
    if broker == "tradestation":
        result = adapter._request("DELETE", "/orderexecution/orders/" + quote(identity, safe=""))
        if result.get("OrderID") != identity:
            raise OutcomeUnknown("TradeStation cancellation acknowledgement omitted the matching order ID")
        return result
    if broker == "longbridge":
        result = adapter._request("delete", "/v1/trade/order?" + urlencode({"order_id": identity}))
        if result not in ({}, None):
            raise OutcomeUnknown("Unexpected Longbridge cancellation acknowledgement")
        return result
    if broker == "webull":
        result = adapter._call(adapter._client().order_v3.cancel_order, mutation=True,
                               account_id=adapter._account_id(), client_order_id=row["client_order_id"])
        if isinstance(result, dict) and (result.get("error") or result.get("error_code")):
            raise BrokerError("Webull cancellation returned a broker error")
        if not isinstance(result, dict) or not (
            result.get("client_order_id") == row["client_order_id"] or result.get("order_id") == identity
        ):
            raise OutcomeUnknown("Webull cancellation acknowledgement omitted the matching order ID")
        return result
    raise CommandError("Unknown cancellation broker")


def _bitget_bulk(adapter, rows):
    # Stock+ does not document crypto-style batch cancellation. Use exact IDs.
    return _requests("bitget",rows,lambda row:_send(adapter,"bitget",row))


def cancel_order(adapter, broker, command):
    """Cancel one verified US-stock order; never an unrelated asset class."""
    identity=_id(command.get("order_id"))
    if broker=="zhuorui":return _zhuorui(adapter,"cancel_order",command)
    if broker in {"bitget","robinhood"}:
        if broker=="bitget":rows=_bitget_orders(adapter,command)
        else:
            row=adapter.stock_order_detail(identity)
            state=row.get("state")
            opened={"new","queued","confirmed","unconfirmed","partially_filled"}
            closed={"filled","cancelled","partially_filled_rest_cancelled","rejected","failed","voided"}
            rows=[{**row,"order_id":identity}] if _is_open(row,"state",opened,closed,{"pending_cancelled"}) else []
        return _requests(broker,rows,lambda row:_send(adapter,broker,row))
    if broker=="alpaca":
        row=adapter.http.request("GET","/v2/orders/"+quote(identity,safe=""))
        if not isinstance(row,dict) or row.get("id")!=identity:raise BrokerError("Alpaca order identity mismatch")
        if not adapter.is_us_stock_order(row):raise CommandError("Cancellation is restricted to US stock orders")
        opened={"new","partially_filled","accepted","pending_new","accepted_for_bidding","stopped","suspended","held","pending_replace"}
        closed={"filled","canceled","expired","replaced","rejected","done_for_day","calculated"}
        rows=[{**row,"order_id":identity}] if _is_open(row,"status",opened,closed,{"pending_cancel"}) else []
    elif broker=="tradestation":rows=_ts_orders(adapter)
    elif broker=="longbridge":
        row=adapter._request("get","/v1/trade/order?"+urlencode({"order_id":identity}))
        if not isinstance(row,dict) or row.get("order_id")!=identity:raise BrokerError("Longbridge order identity mismatch")
        if not adapter.is_us_stock_order(row):raise CommandError("Cancellation is restricted to US stock orders")
        rows=[row] if _is_open(row,"status",LONG_OPEN,LONG_CLOSED,LONG_PENDING) else []
    elif broker=="webull":rows=_webull_orders(adapter)
    else:raise CommandError("Unknown cancellation broker")
    rows=[row for row in rows if identity in {row["order_id"],row.get("client_order_id")}]
    if len(rows)>1:raise BrokerError("Cancellation ID ambiguously matches multiple orders")
    return _requests(broker,rows,lambda row:_send(adapter,broker,row))


def _zhuorui(adapter, operation, command):
    if not command or not command.get("id"):
        raise CommandError("Zhuorui cancellation requires its durable command ID")
    result = adapter._bridge(operation, {"command_id": command["id"], "order_id": command.get("order_id")})
    if not isinstance(result, dict):
        raise OutcomeUnknown("Zhuorui cancellation response is not an object")
    if result.get("status") in {"unknown", "cancel_unknown"}:
        raise OutcomeUnknown(result.get("message", "Zhuorui cancellation outcome unknown"))
    if result.get("status") in {"rejected", "blocked", "disabled", "cancel_rejected"}:
        raise BrokerError(result.get("message", "Zhuorui cancellation not accepted"))
    if result.get("status") not in {"cancel_requested", "no_action", "duplicate"}:
        raise OutcomeUnknown("Zhuorui cancellation returned an unrecognized status")
    if result.get("status") == "duplicate":
        # The upstream journal did not resend this command. An older unknown or
        # rejected outcome cannot become an acknowledgement in the new runner.
        previous = result.get("previous_state")
        if previous == "cancel_requested":
            result = {**result, "status": "cancel_requested", "duplicate": True}
        elif previous == "no_action":
            result = {**result, "status": "no_action", "duplicate": True}
        elif previous in {"rejected", "blocked", "disabled"}:
            raise BrokerError("Previously recorded Zhuorui cancellation was not accepted")
        else:
            raise OutcomeUnknown("Previously recorded Zhuorui cancellation requires reconciliation")
    return {**result, "broker": "zhuorui", "cancellation_confirmed": False,
            "scope": "Configured account's US orders returned by the upstream current-orders endpoint"}


def _discover_open_orders(adapter, broker):
    """Read and validate the entire cancellation scope before any writes."""
    if broker=="alpaca":
        rows=[];seen=set();params={"status":"open","limit":500,"direction":"desc","nested":"false"}
        for _ in range(int(adapter.config.get("max_pages",10000))):
            page=_rows(adapter.http.request("GET","/v2/orders",params=params),"Alpaca")
            rows.extend({**row,"order_id":_id(row.get("id"))} for row in page if adapter.is_us_stock_order(row))
            if len(page)<500:break
            cursor=_id(page[-1].get("id"))
            if cursor in seen:raise BrokerError("Repeated Alpaca open-order cursor; no cancellations sent")
            seen.add(cursor);params={**params,"before_order_id":cursor}
        else:raise BrokerError("Alpaca open-order pagination limit; no cancellations sent")
        rows=_unique(rows,"order_id")
    else:
        loaders={"robinhood":_rh_orders,"tradestation":_ts_orders,"longbridge":_long_orders,"webull":_webull_orders,"bitget":_bitget_orders}
        if broker not in loaders:raise CommandError("Unknown cancellation broker")
        rows=loaders[broker](adapter)
    return rows


def cancel_open_orders(adapter, broker, command=None):
    """Cancel discovered US-stock orders only; account-wide bulk delete is unsafe."""
    if broker=="zhuorui":return _zhuorui(adapter,"cancel_open_orders",command)
    try:
        rows = _discover_open_orders(adapter, broker)
    except (AuthenticationError, CommandError):
        raise
    except Exception as exc:
        # This boundary contains reads only. In particular, a transport timeout
        # here cannot mean that a cancellation was accepted. Once _requests
        # starts, its partial/uncertain write results must retain their meaning.
        message = str(exc) if isinstance(exc, BrokerError) else "Open-order discovery failed"
        raise CancellationDiscoveryError(message + "; no cancellations sent") from exc
    return _requests(broker,rows,lambda row:_send(adapter,broker,row))
