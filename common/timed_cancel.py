"""Explicit order-then-cancel execution, with durable recovery and no write replay.

Callers must hold the account operation lock and authorize execution. Native
FOK remains the default. This workflow can partially fill and can miss its
cancellation deadline; it never offers exchange-atomic fill-or-kill semantics.
"""
from __future__ import annotations

from copy import deepcopy
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import time
from urllib.parse import quote, urlencode

from .cancellation import _send as send_confirmed_cancel, TS_CLOSED, LONG_CLOSED, TS_OPEN, LONG_OPEN
from .errors import AuthenticationError, BrokerError, CommandError, ConfigurationError, OutcomeUnknown, UnsupportedOrder, RequestRejected
from .journal import encoded
from .equity_scope import require_us_equity, us_stock_symbol


BROKERS = {"alpaca", "bitget", "robinhood", "tradestation", "longbridge", "webull"}
CLIENT_FIELDS = {"alpaca": "client_order_id", "bitget": "remark", "robinhood": "ref_id",
                 "tradestation": "OrderConfirmID", "longbridge": "client_request_id", "webull": "client_order_id"}


def _seconds(value, name, maximum):
    if isinstance(value, bool):
        raise ConfigurationError(f"{name} must be a finite number between zero and {maximum}")
    try:
        value = float(value)
    except (TypeError, ValueError):
        raise ConfigurationError(f"{name} must be a finite number") from None
    if not math.isfinite(value) or not 0 <= value <= maximum:
        raise ConfigurationError(f"{name} must be between zero and {maximum}")
    return value


def _delays(adapter):
    values = adapter.config.get("cancel_confirmation_delays_seconds", [0, 1, 1, 2])
    if not isinstance(values, list) or not values or len(values) > 10:
        raise ConfigurationError("cancel_confirmation_delays_seconds must contain 1 to 10 delays")
    values = [_seconds(value, "cancel_confirmation_delays_seconds", 30) for value in values]
    if sum(values) > 60:
        raise ConfigurationError("Cancellation confirmation delays must total at most 60 seconds")
    return values


def is_enabled(broker, config, command):
    policy = config.get("fok_policy", "native")
    if policy not in {"native", "timed_cancel"}:
        raise ConfigurationError("fok_policy must be native or timed_cancel")
    if broker == "bitget" and command.get("time_in_force") == "ioc":
        policy = config.get("ioc_policy", "native")
        if policy not in {"native", "timed_cancel"}:
            raise ConfigurationError("ioc_policy must be native or timed_cancel")
        return policy == "timed_cancel"
    return policy == "timed_cancel" and command.get("time_in_force") == "fok" and broker != "zhuorui"


def prepare(adapter, broker, command):
    """Pure preview: return a copied command and its explicit execution plan."""
    require_us_equity(command,adapter.config)
    us_stock_symbol(command.get("symbol"))
    converted = deepcopy(command)
    enabled = is_enabled(broker, adapter.config, command)
    ioc = broker == "bitget" and command.get("time_in_force") == "ioc"
    if enabled:
        if broker not in BROKERS:
            raise UnsupportedOrder("Timed cancellation is unavailable for this broker")
        if command.get("order_type") != "limit" and not (ioc and command.get("order_type") == "market"):
            raise UnsupportedOrder("Timed cancellation requires an explicit limit order")
        if command.get("auction_intent"):
            raise UnsupportedOrder("Timed cancellation cannot preserve LULD reopening-auction intent")
        delay_key = "ioc_cancel_after_seconds" if ioc else "cancel_after_seconds"
        delay = _seconds(adapter.config.get(delay_key, 0.5 if ioc else 1), delay_key, 300)
        _delays(adapter)
        tif = "day"
        converted["time_in_force"] = tif
    request = adapter.build_order(converted)
    plan = {"enabled": enabled, "request": request}
    if enabled:
        client_id = request.get(CLIENT_FIELDS[broker])
        if not isinstance(client_id, str) or not client_id:
            raise ConfigurationError("Timed cancellation requires a deterministic broker client reference")
        plan.update(native_fok=False, execution_semantics="order_then_timed_cancel" if ioc else "limit_then_timed_cancel", partial_fills_possible=True,
                    requested_time_in_force=command["time_in_force"], submitted_time_in_force=converted["time_in_force"],
                    cancel_after_seconds=delay, client_order_id=client_id,
                    timing=("Deadline starts at successful submission acknowledgement; network delays can make cancellation late" if ioc else
                            "Deadline starts at submission dispatch; network delays can make cancellation late"),
                    client_reference_kind="remark" if broker=="bitget" else "client_order_id",
                    server_idempotency_confirmed=False if broker=="bitget" else None)
        if ioc:
            plan["native_ioc"] = False
    return converted, plan


def _ensure(journal):
    if journal is None or not hasattr(journal, "db"):
        raise ConfigurationError("Timed cancellation requires a persistent journal")
    journal.db.execute("""CREATE TABLE IF NOT EXISTS timed_cancellations (
        scope TEXT NOT NULL, command_id TEXT NOT NULL, broker TEXT NOT NULL,
        fingerprint TEXT NOT NULL, state TEXT NOT NULL, record TEXT NOT NULL,
        PRIMARY KEY(scope, command_id))""")
    journal.db.commit()


def _save(journal, scope, record, state):
    record["state"] = state
    with journal.db:
        journal.db.execute("UPDATE timed_cancellations SET state=?,record=? WHERE scope=? AND command_id=?",
                           (state, encoded(record), scope, record["command_id"]))


def unresolved(journal, scope):
    """Outstanding responsibility; callers should block new submissions on these."""
    _ensure(journal)
    rows = journal.db.execute("SELECT record FROM timed_cancellations WHERE scope=? AND state NOT IN ('complete','authentication_rejected') ORDER BY rowid", (scope,)).fetchall()
    return [{key: value for key, value in json.loads(row[0]).items()
             if key in {"command_id", "broker", "state", "order_id", "client_order_id", "deadline_at"}} for row in rows]


def _identity(value):
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > 256 or any(
        char.isspace() or ord(char) < 32 or ord(char) >= 127 for char in value
    ):
        raise OutcomeUnknown("Order acknowledgement lacks an exact string broker reference")
    return value


def _ack_reference(broker, raw, plan, adapter):
    if not isinstance(raw, dict):
        raise OutcomeUnknown("Submission acknowledgement is not an object")
    if raw.get("error") or raw.get("error_code") or raw.get("Error") or raw.get("Errors"):
        raise OutcomeUnknown("Submission did not provide an unambiguous accepted order")
    if broker == "tradestation":
        orders = raw.get("Orders")
        if not isinstance(orders, list) or len(orders) != 1 or not isinstance(orders[0], dict) or orders[0].get("Error"):
            raise OutcomeUnknown("TradeStation acknowledgement does not identify one accepted order")
        return _identity(orders[0].get("OrderID"))
    expected = plan["client_order_id"]
    field = CLIENT_FIELDS[broker]
    if raw.get(field) is not None and raw[field] != expected:
        raise OutcomeUnknown("Submission acknowledgement returned a different client reference")
    if broker == "robinhood" and raw.get("account_number", adapter.config.get("account_number")) != adapter.config.get("account_number"):
        raise OutcomeUnknown("Robinhood submission acknowledgement returned another account")
    field = {"alpaca": "id", "bitget": "orderId", "robinhood": "id", "longbridge": "order_id", "webull": "order_id"}[broker]
    if broker == "webull" and raw.get(field) is None and raw.get("client_order_id") == expected:
        return expected
    return _identity(raw.get(field))


def _single(rows, field, identity):
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise BrokerError("Order detail did not return a valid record list")
    matches = [row for row in rows if row.get(field) == identity]
    if len(matches) != 1:
        raise BrokerError("The exact submitted order is not uniquely visible; absence is not cancellation")
    return matches[0]


def _validate_ticket(broker, record, row):
    """The exact ID must also describe the intended ticket before a cancel."""
    request = record["request"]
    client_field = CLIENT_FIELDS[broker]
    if row.get(client_field) is not None and row[client_field] != record["client_order_id"]:
        raise BrokerError("Order detail returned a different deterministic client reference")
    if broker == "tradestation":
        legs = row.get("Legs")
        if not isinstance(legs, list) or len(legs) != 1 or not isinstance(legs[0], dict):
            raise BrokerError("TradeStation detail does not describe the submitted single-leg ticket")
        action = str(legs[0].get("BuyOrSell", "")).lower().replace(" ", "")
        direction = {"buy": "buy", "buytocover": "buy", "buytoopen": "buy", "buytoclose": "buy",
                     "sell": "sell", "sellshort": "sell", "selltoopen": "sell", "selltoclose": "sell"}.get(action)
        actual = (legs[0].get("Symbol"), direction, row.get("OrderType"),
                  legs[0].get("QuantityOrdered"), row.get("LimitPrice"))
        expected = (request["Symbol"], "buy" if request["TradeAction"].startswith("BUY") else "sell",
                    request["OrderType"], request["Quantity"], request["LimitPrice"])
    elif broker == "robinhood":
        actual = (row.get("symbol"),row.get("side"),row.get("type"),row.get("quantity"),row.get("price",row.get("limit_price")))
        expected = (request["symbol"],request["side"],request["type"],request["quantity"],request["limit_price"])
    else:
        fields = {"alpaca": ("symbol", "side", "type", "qty", "limit_price"),
                  "bitget": ("symbol", "side", "orderType", "quantity", "price"),
                  "longbridge": ("symbol", "side", "order_type", "quantity", "price"),
                  "webull": ("symbol", "side", "order_type", "total_quantity", "limit_price")}[broker]
        request_fields = {"bitget": ("symbol", "side", "orderType", "submittedQuantity", "submittedPrice"),
                          "longbridge": ("symbol", "side", "order_type", "submitted_quantity", "submitted_price"),
                          "webull": ("symbol", "side", "order_type", "quantity", "limit_price")}.get(broker, fields)
        actual = tuple(row.get(key) for key in fields)
        expected = tuple(request.get(key) for key in request_fields)
    if actual[:3] != expected[:3]:
        raise BrokerError("Order detail symbol, side or type differs from the submitted ticket")
    if broker == "bitget" and request["orderType"] == "MO":
        # A market order has no submitted limit. Its reported price may be null
        # or an execution price; identity, symbol, side, type and shares still match.
        actual, expected = actual[:4], expected[:4]
    try:
        numeric_actual = [Decimal(str(value)) for value in actual[3:]]
        numeric_expected = [Decimal(str(value)) for value in expected[3:]]
        if not all(value.is_finite() for value in numeric_actual) or numeric_actual != numeric_expected:
            raise ValueError
    except (ValueError, InvalidOperation):
        raise BrokerError("Order detail quantity or limit differs from the submitted ticket") from None


def _detail(adapter, broker, record):
    """Read only exact persisted identity, with account scoping where required."""
    identity = record["order_id"]
    if broker == "alpaca":
        row = adapter.http.request("GET", "/v2/orders/" + quote(identity, safe=""))
        field, state = "id", "status"
    elif broker in {"bitget","robinhood"}:
        row=adapter.stock_order_detail(identity)
        field,state=("orderId","status") if broker=="bitget" else ("id","state")
    elif broker == "tradestation":
        account = adapter.config.get("account_id")
        if not isinstance(account, str) or not account:
            raise ConfigurationError("TradeStation timed cancellation requires account_id")
        response = adapter._request("GET", "/brokerage/accounts/" + quote(account, safe="") + "/orders/" + quote(identity, safe=""))
        row = _single(response.get("Orders"), "OrderID", identity)
        if row.get("AccountID") != account:
            raise BrokerError("TradeStation detail does not match the configured account")
        field, state = "OrderID", "Status"
    elif broker == "longbridge":
        row = adapter._request("get", "/v1/trade/order?" + urlencode({"order_id": identity}))
        field, state = "order_id", "status"
    elif broker == "webull":
        response = adapter._call(adapter._client().order_v3.get_order_detail,
                                 account_id=adapter._account_id(), client_order_id=record["client_order_id"])
        if not isinstance(response, dict):
            raise BrokerError("Webull detail is not an object")
        row = _single(response.get("orders"), "client_order_id", record["client_order_id"])
        if row.get("account_id", adapter._account_id()) != adapter._account_id():
            raise BrokerError("Webull detail does not match the configured account")
        if identity not in {row.get("order_id"), row.get("client_order_id")}:
            raise BrokerError("Webull detail returned a different broker reference")
        if not adapter.is_us_stock_order(row):
            raise BrokerError("Persisted order is outside the US-stock scope; no cancellation sent")
        _validate_ticket(broker, record, row)
        return row, row.get("status")
    else:
        raise ConfigurationError("No timed-cancellation detail API for this broker")
    if not isinstance(row, dict) or row.get(field) != identity:
        raise BrokerError("Order detail does not match the submitted broker reference")
    if broker not in {"bitget","robinhood"} and not adapter.is_us_stock_order(row):
        raise BrokerError("Persisted order is outside the US-stock scope; no cancellation sent")
    _validate_ticket(broker, record, row)
    return row, row.get(state)


def _terminal(broker, state):
    cancelled = {"alpaca": {"canceled"}, "bitget": {"Canceled", "PartialWithdrawal"}, "robinhood": {"cancelled", "partially_filled_rest_cancelled"},
                 "tradestation": {"CAN", "OUT", "TSC", "SCN"}, "longbridge": {"CanceledStatus", "PartialWithdrawal"},
                 "webull": {"CANCELLED"}}
    terminals = {"alpaca": {"filled", "canceled", "expired", "rejected"},
                 "bitget": {"Filled", "Rejected", "Canceled", "Expired", "PartialWithdrawal"}, "robinhood": {"filled", "cancelled", "partially_filled_rest_cancelled", "rejected", "failed", "voided"},
                 "tradestation": TS_CLOSED - {"UCH", "RJR"}, "longbridge": LONG_CLOSED,
                 "webull": {"FILLED", "CANCELLED", "FAILED", "EXPIRED", "REJECTED"}}
    return isinstance(state, str) and state in terminals[broker], isinstance(state, str) and state in cancelled[broker]


def _write_cancel(adapter, broker, record, detail):
    """Reuse the cancellation transport for the already validated exact ticket."""
    if broker == "alpaca":
        return adapter.http.request("DELETE", "/v2/orders/" + quote(record["order_id"], safe=""))
    target = {**detail, "order_id": record["order_id"], "client_order_id": record["client_order_id"]}
    return send_confirmed_cancel(adapter, broker, target)


def _result(record, status, **details):
    requested_tif = record.get("requested_time_in_force", "fok")
    return {"status": status, "command_id": record["command_id"], "broker": record["broker"],
            "native_fok": False, "execution_semantics": "order_then_timed_cancel" if requested_tif == "ioc" else "limit_then_timed_cancel", "partial_fills_possible": True,
            "requested_time_in_force": requested_tif,
            **({"native_ioc": False} if requested_tif == "ioc" else {}),
            "order_id": record.get("order_id"), "client_order_id": record["client_order_id"],
            "submission": record.get("submission"), "submission_confirmed": bool(record.get("order_id")),
            "cancellation": record.get("cancellation"), "cancellation_confirmed": False,
            "dispatch_at": record["dispatch_at"], "deadline_at": record["deadline_at"],
            "authentication_error": record.get("authentication_error", False),
            "cancellation_deadline_missed": record.get("cancellation_deadline_missed", False), **details}


def _poll(adapter, broker, journal, scope, record):
    last, last_state, read_error = None, None, None
    for delay in _delays(adapter):
        if delay:
            time.sleep(delay)
        try:
            last, last_state = _detail(adapter, broker, record)
            terminal, cancelled = _terminal(broker, last_state)
            if terminal:
                result = _result(record, "cancel_requested" if cancelled else "no_action",
                                 terminal_confirmed=True, terminal_state=last_state,
                                 cancellation_confirmed=cancelled, order=last,
                                 message="Exact order detail confirms a terminal state; any partial fills remain effective.")
                record["result"] = result
                _save(journal, scope, record, "complete")
                return result
        except BrokerError as exc:
            read_error = str(exc)
            if isinstance(exc, AuthenticationError):
                record["authentication_error"] = True
    result = _result(record, "outcome_unknown", terminal_confirmed=False, terminal_state=last_state, order=last,
                     read_error=read_error, message="The order is not confirmed terminal. Recovery can read its exact identity but will not repeat a cancellation.")
    record["result"] = result
    _save(journal, scope, record, record["state"])
    return result


def _finish_pending(adapter, broker, journal, scope, record, *, remaining=None):
    if remaining is None:
        remaining = max(0, record["deadline_at"] - time.time())
    if remaining > 300:
        raise OutcomeUnknown("Stored cancellation deadline is too far in the future; check the system clock")
    if remaining > 0:
        time.sleep(remaining)
    record["cancellation_deadline_missed"] = time.time() > record["deadline_at"] + 0.05
    # Cancellation must target the exact intended ticket. A failed/lagging read
    # leaves a never-dispatched cancel pending, safe for later recovery.
    try:
        detail, state = _detail(adapter, broker, record)
        terminal, cancelled = _terminal(broker, state)
        if terminal:
            result = _result(record, "cancel_requested" if cancelled else "no_action", terminal_confirmed=True,
                             terminal_state=state, cancellation_confirmed=cancelled, order=detail)
            record["result"] = result
            _save(journal, scope, record, "complete")
            return result
    except BrokerError as exc:
        if isinstance(exc, AuthenticationError):
            record["authentication_error"] = True
        result = _result(record, "outcome_unknown", terminal_confirmed=False, read_error=str(exc),
                         message="The exact submitted ticket could not be verified; cancellation has not been sent. Recovery may re-read and cancel this same reference.")
        record["result"] = result
        _save(journal, scope, record, "cancel_pending")
        return result
    opened = {"alpaca": {"new", "partially_filled", "accepted", "pending_new", "accepted_for_bidding", "stopped", "suspended", "held", "pending_replace"},
              "bitget": {"NotReported", "ReplacedNotReported", "ProtectedNotReported", "VarietiesNotReported", "WaitToNew", "New", "WaitToReplace", "PendingReplace", "Replaced", "PartialFilled"}, "robinhood": {"new", "queued", "confirmed", "unconfirmed", "partially_filled"},
              "tradestation": TS_OPEN, "longbridge": LONG_OPEN,
              "webull": {"PENDING", "SUBMITTED", "PARTIAL_FILLED", "PARTIAL FILLED"}}[broker]
    pending = {"alpaca": {"pending_cancel"}, "bitget": {"WaitToCancel", "PendingCancel"}, "robinhood": {"pending_cancelled"},
               "tradestation": {"CSN", "UCN", "ECN", "REC"}, "longbridge": {"WaitToCancel", "PendingCancelStatus"},
               "webull": {"PENDING_CANCEL"}}[broker]
    if not isinstance(state, str) or state not in opened:
        if isinstance(state, str) and state in pending:
            _save(journal, scope, record, "cancel_unconfirmed")
            return _poll(adapter, broker, journal, scope, record)
        result = _result(record, "outcome_unknown", terminal_confirmed=False, order=detail,
                         message="The exact order has an unrecognized active state. No cancellation was sent; recovery may re-read it.")
        record["result"] = result
        _save(journal, scope, record, "cancel_pending")
        return result
    # This durable marker precedes the helper's reads AND write. A crash here
    # conservatively disables another cancel, even if dispatch never happened.
    _save(journal, scope, record, "cancel_dispatching")
    try:
        raw = _write_cancel(adapter, broker, record, detail)
        record["cancellation"] = {"status": "cancel_requested", "raw": raw}
        next_state = "cancel_requested"
    except Exception as exc:
        record["cancellation"] = {"status": "outcome_unknown", "error": str(exc) if isinstance(exc, BrokerError) else type(exc).__name__}
        if isinstance(exc, AuthenticationError):
            record["authentication_error"] = True
        next_state = "cancel_unknown"
    _save(journal, scope, record, next_state)
    return _poll(adapter, broker, journal, scope, record)


def execute(adapter, broker, command, journal, scope):
    """Submit once, then request cancellation at the deadline and confirm detail."""
    converted, plan = prepare(adapter, broker, command)
    if not plan["enabled"]:
        raise ConfigurationError("Timed execution requires an explicit timed_cancel plan")
    _ensure(journal)
    identity = _identity(command.get("id"))
    digest = hashlib.sha256(encoded(command).encode()).hexdigest()
    existing = journal.db.execute("SELECT fingerprint,record FROM timed_cancellations WHERE scope=? AND command_id=?", (scope, identity)).fetchone()
    if existing:
        if existing[0] != digest:
            raise CommandError("Timed command ID was already used with another payload")
        previous = json.loads(existing[1])
        if previous["state"] == "complete":
            return {**previous["result"], "duplicate": True}
        if previous["state"] != "authentication_rejected":
            raise OutcomeUnknown("This timed order already dispatched; use recovery, never resubmit")
        # A prior HTTP authentication refusal proves the order was not accepted.
        # This is the sole retryable submission state, after credentials repair.
        with journal.db:
            journal.db.execute("DELETE FROM timed_cancellations WHERE scope=? AND command_id=?", (scope, identity))
    if unresolved(journal, scope):
        raise OutcomeUnknown("An earlier timed order requires recovery or reconciliation before new submissions")
    dispatch_at = time.time()
    record = {"command_id": identity, "broker": broker, "command": converted, "request": plan["request"],
              "client_order_id": plan["client_order_id"], "state": "submission_pending",
              "requested_time_in_force": plan["requested_time_in_force"],
              "dispatch_at": dispatch_at, "deadline_at": dispatch_at + plan["cancel_after_seconds"]}
    with journal.db:
        journal.db.execute("INSERT INTO timed_cancellations VALUES (?,?,?,?,?,?)",
                           (scope, identity, broker, digest, record["state"], encoded(record)))
    # The clock starts immediately before submission, after durable reservation.
    dispatched = time.monotonic()
    try:
        record["submission"] = adapter.place_order(converted)
        record["order_id"] = _ack_reference(broker, record["submission"], plan, adapter)
    except Exception as exc:
        if isinstance(exc, RequestRejected):
            record["result"] = _result(record, "rejected", terminal_confirmed=False,
                submission_outcome="not_submitted", requests_sent=exc.requests_sent,
                broker_error_code=exc.code, http_status=exc.http_status, message=str(exc))
            _save(journal, scope, record, "complete")
            raise
        if isinstance(exc, AuthenticationError):
            record["authentication_error"] = True
            if exc.no_submission:
                record["result"] = _result(record, "cancel_rejected", terminal_confirmed=False,
                                            message="Authentication rejected the submission before acceptance.")
                _save(journal, scope, record, "authentication_rejected")
                raise
        result = _result(record, "outcome_unknown", terminal_confirmed=False,
                         error=str(exc) if isinstance(exc, BrokerError) else type(exc).__name__,
                         message="Submission may have reached the broker. No cancellation reference was inferred and no submission will be replayed.")
        record["result"] = result
        _save(journal, scope, record, "submission_unknown")
        return result
    if plan["requested_time_in_force"] == "ioc":
        # IOC emulation waits after successful acceptance. Persist the deadline
        # so recovery cancels this same order without creating a replacement.
        dispatched = time.monotonic()
        record["accepted_at"] = time.time()
        record["deadline_at"] = record["accepted_at"] + plan["cancel_after_seconds"]
    _save(journal, scope, record, "cancel_pending")
    remaining = max(0, plan["cancel_after_seconds"] - (time.monotonic() - dispatched))
    return _finish_pending(adapter, broker, journal, scope, record, remaining=remaining)


def recover(adapter, broker, journal, scope):
    """Resume only confirmed, never-cancelled references; otherwise read only."""
    if broker not in BROKERS:
        raise ConfigurationError("Use Zhuorui's own pending-cancellation recovery for that broker")
    _ensure(journal)
    rows = journal.db.execute("SELECT record FROM timed_cancellations WHERE scope=? AND state NOT IN ('complete','authentication_rejected') ORDER BY rowid", (scope,)).fetchall()
    results = []
    for row in rows:
        record = json.loads(row[0])
        require_us_equity(record["command"],adapter.config)
        us_stock_symbol(record["command"].get("symbol"))
        if record["broker"] != broker:
            raise ConfigurationError("Timed recovery broker differs from the persisted account scope")
        if record["state"] in {"submission_pending", "submission_unknown"} or not record.get("order_id"):
            results.append(_result(record, "outcome_unknown", terminal_confirmed=False,
                                   message="No confirmed broker reference is persisted. Reconcile this command manually; no submission or cancellation was sent."))
        elif record["state"] == "cancel_pending":
            results.append(_finish_pending(adapter, broker, journal, scope, record))
        else:
            results.append(_poll(adapter, broker, journal, scope, record))
    return results
