"""Strict JSON normalization, including the KTrader/Zhuorui command vocabulary."""
from decimal import Decimal
import json
import math
import re
import time
from .errors import CommandError, UnsupportedOrder
from .utils import decimal_string, parse_time
from .equity_scope import require_us_equity, us_stock_symbol

ACCOUNT_KEYS = ("account_id", "accountId", "account")
NUM_KEYS = ("account_num_id", "accountNumId", "account_numeric_id", "accountNumericId", "account_num", "accountNum")
SERVER_KEYS = ("server_id", "serverId", "target_server_id", "targetServerId")
ACTION_KEYS = ("action", "cmd", "command", "command_type", "commandType", "event", "event_type", "eventType", "request_type", "requestType")
TYPE_KEYS = ("order_type", "orderType", "type")
SYMBOL_KEYS = ("symbol", "ticker", "code")
ORDER_REFERENCE_KEYS = ("order_id", "orderId", "order_reference", "orderReference", "orderTxnReference")
SUBMISSION_KEYS = ("side", "direction", "quantity", "qty_shares", "qty", "shares", "limit_price", "limitPrice", "price", "limit",
                   "notional", "notional_usd", "notionalUsd", "dollar_amount", "dollars", "cancel_unfilled", "time_in_force", "timeInForce", "tif", "options", "auction_intent")
ORDER_TYPES = {"market": "market", "market_order": "market", "limit": "limit", "limit_order": "limit",
               "fok": "fok", "fill_or_kill": "fok", "limit_order_fok": "fok",
               "ioc": "ioc", "immediate_or_cancel": "ioc", "limit_order_ioc": "ioc"}
CANCEL_TYPES = {"cancel_order": "cancel_order", "cancel": "cancel_order",
                "cancel_open_orders": "cancel_open_orders", "cancel_all": "cancel_open_orders",
                "cancel_all_orders": "cancel_open_orders", "cancel_pending_orders": "cancel_open_orders"}
QUERIES = {"positions": "positions", "get_positions": "positions", "query_positions": "positions",
           "all_positions": "positions", "holdings": "positions", "query_holdings": "positions",
           "orders_today": "orders_today", "today_orders": "orders_today", "get_orders": "orders_today",
           "query_orders": "orders_today", "orders": "orders_today", "query_today_orders": "orders_today",
           "get_today_orders": "orders_today", "capabilities": "capabilities", "get_capabilities": "capabilities",
           "tradability": "tradability", "tradable": "tradability", "test_tradable": "tradability",
           "check_symbol": "tradability", "is_tradable": "tradability",
           "borrow_availability": "borrow_availability", "query_borrow_availability": "borrow_availability",
           "get_borrow_availability": "borrow_availability", "short_availability": "borrow_availability"}


def decode_command(raw):
    if not isinstance(raw, (str, bytes)) or len(raw) > 65536:
        raise CommandError("Command must be a JSON object no larger than 64 KiB")
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise CommandError("Duplicate JSON key")
            result[key] = value
        return result
    def nonfinite(_):
        raise CommandError("Nonfinite JSON number")
    try:
        value = json.loads(raw, parse_float=Decimal, object_pairs_hook=pairs, parse_constant=nonfinite)
    except (ValueError, UnicodeError, RecursionError):
        raise CommandError("Invalid JSON command") from None
    if not isinstance(value, dict):
        raise CommandError("Command must be a JSON object")
    return value


def text(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise CommandError("Identifier must be text or integer")
    result = str(value).strip()
    if not result or len(result) > 256 or any(ord(c) < 32 for c in result):
        raise CommandError("Invalid identifier")
    return result


def token(value):
    value = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", text(value))
    return re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").lower()


def alias(payload, keys, convert=text, default=None):
    values = [convert(payload[key]) for key in keys if key in payload and payload[key] is not None and payload[key] != ""]
    if not values:
        return default
    if any(value != values[0] for value in values):
        raise CommandError(f"Conflicting aliases for {keys[0]}")
    return values[0]


def targets(payload, broker, routing, require_account=False):
    if "broker" in payload and token(payload["broker"]) != broker:
        return False
    account_values = [text(payload[k]) for k in ACCOUNT_KEYS if payload.get(k) is not None]
    num_values = [text(payload[k]) for k in NUM_KEYS if payload.get(k) is not None]
    if require_account and not account_values and not num_values:
        raise CommandError("Kafka commands require account_id or account_num_id")
    for value in account_values:
        expected = {str(routing[k]) for k in ("account_id", "account_num_id") if routing.get(k) is not None}
        if not expected:
            raise CommandError("Configure routing.account_id/account_num_id for account-addressed commands")
        if value not in expected:
            return False
    for value in num_values:
        if not value.isdigit() or int(value) <= 0:
            raise CommandError("Invalid numeric account selector")
        if routing.get("account_num_id") is None:
            raise CommandError("Configure routing.account_num_id")
        if int(value) != int(routing["account_num_id"]):
            return False
    for key in SERVER_KEYS:
        if payload.get(key) is not None:
            if routing.get("server_id") is None:
                raise CommandError("Configure routing.server_id")
            if text(payload[key]) != str(routing["server_id"]):
                return False
    return True


def normalize(payload, broker, config, message_id=None, require_account=False):
    if not targets(payload, broker, config.get("routing", {}), require_account):
        return None
    try:
        require_us_equity(payload, config)
    except UnsupportedOrder as exc:
        raise CommandError(str(exc)) from None
    command_id = alias(payload, ("id", "command_id", "commandId"), default=message_id)
    if not command_id:
        raise CommandError("Provide a stable id/command_id (Kafka can use topic:partition:offset)")
    action_values = [token(payload[k]) for k in ACTION_KEYS if payload.get(k) is not None]
    type_values = [token(payload[k]) for k in TYPE_KEYS if payload.get(k) is not None]
    all_values = action_values + type_values
    result = {"id": command_id, "broker": broker}
    controls = [v for v in all_values if v in CANCEL_TYPES or v == "set_trading_enabled"]
    if controls:
        mapped = [CANCEL_TYPES.get(v, v) for v in controls]
        if len(set(mapped)) != 1 or len(controls) != len(all_values):
            raise CommandError("Conflicting control/order actions")
        result = {**result, "action": mapped[0]}
        if any(k in payload for k in SUBMISSION_KEYS):
            raise CommandError("Control command must not contain submission fields")
        if result["action"] == "set_trading_enabled":
            enabled = payload.get("trading_enabled")
            if not isinstance(enabled, bool):
                raise CommandError("trading_enabled must be a JSON boolean")
            if any(k in payload for k in SYMBOL_KEYS + ORDER_REFERENCE_KEYS):
                raise CommandError("Trading-state control must not contain an order selector")
            result["trading_enabled"] = enabled
        else:
            reference = alias(payload, ORDER_REFERENCE_KEYS)
            symbol = alias(payload, ("symbol", "ticker", "code"), lambda v: us_stock_symbol(text(v), qualified=broker in {"bitget", "longbridge"}))
            if result["action"] == "cancel_order" and reference is None:
                raise CommandError("cancel_order requires an explicit broker order_id")
            if result["action"] == "cancel_open_orders" and (reference is not None or symbol is not None):
                raise CommandError("cancel_open_orders targets the configured account; omit symbol and order_id")
            if reference is not None:
                result["order_id"] = reference
            if symbol is not None:
                result["symbol"] = symbol
        return result
    queries = [QUERIES[value] for value in all_values if value in QUERIES]
    if queries:
        if len(set(queries)) != 1 or any(v not in QUERIES for v in all_values):
            raise CommandError("Conflicting query/order actions")
        if any(k in payload for k in SUBMISSION_KEYS + ORDER_REFERENCE_KEYS):
            raise CommandError("Query command cannot contain order fields")
        result["action"] = queries[0]
        if queries[0] in {"tradability", "borrow_availability"}:
            result["symbol"] = alias(payload, ("symbol", "ticker", "code"), lambda v: us_stock_symbol(text(v), qualified=broker in {"bitget", "longbridge"}))
            if not result["symbol"]:
                raise CommandError("Symbol check requires symbol")
        elif any(k in payload for k in ("symbol", "ticker", "code")):
            raise CommandError("This query returns all symbols; omit symbol")
        return result
    if any(v not in ORDER_TYPES and v not in {"order", "place_order", "submit_order", "trade"} for v in all_values):
        raise CommandError("Unsupported command action; use market/limit/FOK/IOC, positions, or orders_today")
    kinds = [ORDER_TYPES[v] for v in all_values if v in ORDER_TYPES]
    if not kinds:
        raise CommandError("Explicit order_type/type is required")
    if len(set(kinds)) > 1:
        raise CommandError("Conflicting order types")
    kind = kinds[0]
    if "cancel_unfilled" in payload:
        if not isinstance(payload["cancel_unfilled"], bool):
            raise CommandError("cancel_unfilled must be a JSON boolean")
        if payload["cancel_unfilled"]:
            if kind == "ioc":
                raise CommandError("cancel_unfilled=true requests FOK and conflicts with the IOC order type")
            if kind == "market":
                raise CommandError("cancel_unfilled requires a limit order")
            kind = "fok"
        elif kind == "fok":
            raise CommandError("cancel_unfilled=false conflicts with the FOK order type")
    configured_asset = "us_equity"
    default_tif = config.get("default_time_in_force", "day")
    tif = alias(payload, ("time_in_force", "timeInForce", "tif"), token, kind if kind in {"fok", "ioc"} else default_tif)
    tif = {"fill_or_kill": "fok", "immediate_or_cancel": "ioc"}.get(tif, tif)
    if kind in {"fok", "ioc"} and tif != kind:
        raise CommandError(f"{kind.upper()} order conflicts with time_in_force")
    if tif not in {"day", "gtc", "gtd", "ioc", "fok"}:
        raise CommandError("Unsupported time_in_force")
    kind = "limit" if kind in {"fok", "ioc"} else kind
    symbol = alias(payload, ("symbol", "ticker", "code"), lambda v: us_stock_symbol(text(v), qualified=broker in {"bitget", "longbridge"}))
    side = alias(payload, ("side", "direction"), token)
    if symbol is None or len(symbol) > 64 or (broker != "tradestation" and any(c.isspace() for c in symbol)) or (symbol and any(c in symbol for c in "\t\r\n")):
        raise CommandError("A valid symbol is required")
    if side not in {"buy", "sell"}:
        raise CommandError("Side must be buy or sell")
    quantity = alias(payload, ("quantity", "qty_shares", "qty", "shares"), decimal_string)
    if quantity is None:
        raise CommandError("Explicit quantity/qty_shares is required; notional sizing is not implemented")
    if any(k in payload for k in ("notional", "notional_usd", "notionalUsd", "dollar_amount", "dollars")):
        raise CommandError("Use explicit quantity; notional instructions are not silently ignored")
    price = alias(payload, ("limit_price", "limitPrice", "price", "limit"), decimal_string)
    if kind == "limit" and price is None:
        raise CommandError("Limit/FOK order requires limit_price")
    if kind == "market" and price is not None:
        raise CommandError("Market order cannot include limit_price")
    options = payload.get("options", {})
    if not isinstance(options, dict):
        raise CommandError("options must be an object")
    options = dict(options)
    extended = alias(payload, ("allow_pre_post", "allowPrePost", "extended_hours", "extendedHours"),
                     lambda v: v if isinstance(v, bool) else {"true": True, "false": False, "y": True, "n": False}.get(str(v).lower(), "invalid"))
    if extended == "invalid":
        raise CommandError("Extended-hours flag must be boolean")
    if extended is not None:
        key = "allow_pre_post" if broker == "zhuorui" else "extended_hours"
        if key in options and options[key] != extended:
            raise CommandError("Conflicting extended-hours flags")
        options[key] = extended
    result.update(action="place_order", symbol=symbol, side=side, quantity=quantity,
                  order_type=kind, time_in_force=tif, options=options)
    if price is not None:
        result["limit_price"] = price
    if configured_asset is not None:
        result["asset_class"] = token(configured_asset)
    if payload.get("auction_intent") is not None:
        result["auction_intent"] = token(payload["auction_intent"])
        if result["auction_intent"] != "luld_reopening":
            raise CommandError("Only auction_intent=luld_reopening is recognized")
        effective_asset = result.get("asset_class") or config.get("asset_class")
        if effective_asset is None:
            raise CommandError("LULD intent requires an explicit US equity asset_class in the command or broker config")
        if effective_asset not in {"stock", "equity", "us_equity"}:
            raise CommandError("LULD intent applies only to US equities")
        if kind != "limit" or tif != "day":
            raise CommandError("LULD intent requires limit DAY; IOC/FOK cannot wait for a reopening")
        if options.get("extended_hours") or options.get("allow_pre_post"):
            raise CommandError("LULD intent is for the regular session; extended-hours must be false")
        if broker == "longbridge" and not symbol.endswith(".US"):
            raise CommandError("LULD intent requires a US symbol")
        if broker == "zhuorui":
            result["options"]["allow_pre_post"] = False
    return result


def validate_age(payload, kafka_timestamp_ms=None, max_age_seconds=120, now=None):
    now = time.time() if now is None else now
    if isinstance(max_age_seconds, bool) or not math.isfinite(max_age_seconds) or max_age_seconds <= 0:
        raise CommandError("command_max_age_seconds must be finite and positive")
    times = [payload[k] for k in ("ts", "timestamp", "timeStamp", "created_at", "createdAt", "issued_at", "issuedAt") if payload.get(k) is not None]
    if kafka_timestamp_ms is not None:
        times.append(kafka_timestamp_ms / 1000)
    for value in times:
        try:
            stamp = parse_time(value).timestamp()
        except (ValueError, OverflowError, TypeError):
            raise CommandError("Invalid command timestamp") from None
        if stamp > now + 60 or now - stamp > max_age_seconds:
            raise CommandError("Command is stale or timestamp is in the future")
    for key in ("expires_at", "expiresAt", "expiry", "expiration"):
        if payload.get(key) is not None:
            try:
                expiry = parse_time(payload[key]).timestamp()
            except (ValueError, OverflowError, TypeError, OSError):
                raise CommandError("Invalid expiration timestamp") from None
            if expiry <= now:
                raise CommandError("Command has expired")
