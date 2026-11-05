"""Bitget Stock+ US equities; separate from crypto, Reality tokens and CFDs.

Official contracts: https://www.bitget.com/docs/catalog/stock-plus/{orders,assets,stock-quotes}
The local journal protects command replay. A remark is correlation, not idempotency.
"""
from __future__ import annotations

import base64
from collections import deque
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import hmac
import json
import math
import re
from pathlib import Path
import threading
import time
from urllib.parse import urlencode, urlsplit
from zoneinfo import ZoneInfo

from common.errors import AuthenticationError, BrokerError, CommandError, ConfigurationError, UnsupportedOrder, OutcomeUnknown, RequestRejected
from common.utils import decimal_string, iso_utc, parse_time
from common.equity_scope import require_us_equity, us_stock_symbol

AUTH_ERROR_CODES = {"40001", "40002", "40003", "40006", "40009", "40011", "40012",
                    "40014", "40018", "40025", "40030", "40036", "40037", "40038",
                    "40039", "40040", "40041", "40065", "40084"}
# Bitget explicitly requires reconciliation for these errors, even if HTTP 400.
UNCERTAIN_ERROR_CODES = {"40010", "40725", "45001", "25000", "25001"}
REJECTION_CODES = {"429", "25002", "25004", "25012", "25013", "25100", "25101", "25102", "25104", "25105",
                   "25200", "25201", "25202", "25203", "25204", "25205", "25206", "25207", "25208", "25209",
                   "25210", "25211", "25213", "25214", "25215", "25216", "25225", "25226", "25227", "25229", "25230",
                   "40017", "40019", "40020", "40034", "40053", "40057", "40058", "40059", "40064", "40102",
                   "40500", "40706", "40721", "40724", "40754", "40755", "40757", "40761", "40762"}
PREFIX = "/api/v3/stockplus"
HISTORY_PAGE_LIMIT = 1000
TERMINAL = {"Filled", "Rejected", "Canceled", "Expired", "PartialWithdrawal"}
ACTIVE = {"NotReported", "ReplacedNotReported", "ProtectedNotReported", "VarietiesNotReported",
          "WaitToNew", "New", "WaitToReplace", "PendingReplace", "Replaced", "PartialFilled",
          "WaitToCancel", "PendingCancel"}


class Adapter:
    """One authenticated Stock+ account, restricted to US stock instruments."""

    def __init__(self, config: dict):
        self.config = dict(config)
        self.base_url = self.config.get("base_url", "https://api.bitget.com").rstrip("/")
        parsed = urlsplit(self.base_url)
        if parsed.scheme != "https" or not parsed.netloc or parsed.path or parsed.query or parsed.fragment or parsed.username or parsed.password:
            raise ConfigurationError("Bitget base_url must be an HTTPS origin")
        if self.config.get("account_type", "stock_plus") != "stock_plus":
            raise ConfigurationError("Bitget US stocks require account_type=stock_plus")
        try:
            require_us_equity({}, self.config)
        except UnsupportedOrder as exc:
            raise ConfigurationError("Bitget adapter supports US stocks only") from exc
        self._trade_times = deque()
        self._request_context = threading.local()
        self.request_observer = None
        self._limiter = None
        monitor = self.config.get("monitor", {})
        if not isinstance(monitor, dict) or type(monitor.get("enabled", False)) is not bool:
            raise ConfigurationError("Bitget monitor must be an object with a boolean enabled flag")
        if monitor.get("enabled"):
            from bitget.rate_limit import TradeRateLimiter
            account = self.config.get("journal_account")
            if not isinstance(account, str) or not account.strip():
                raise ConfigurationError("Bitget monitoring requires a stable journal_account")
            scope = hashlib.sha256((self.base_url + ':' + account).encode()).hexdigest()[:16]
            default_path = Path(__file__).resolve().parents[1] / '.state' / f'bitget.{scope}.quota.json'
            self._limiter = TradeRateLimiter(monitor.get('rate_limit_path', default_path),
                                            monitor.get('command_reserve', 5))
        try:
            self._session_name(self.config.get("default_outside_rth", "AnyTime"))
        except UnsupportedOrder as exc:
            raise ConfigurationError("Bitget default_outside_rth must be Auto, RTHOnly, AnyTime or Overnight") from exc
        if self.config.get("borrow_scope", "none") != "none" or self.config.get("borrow_coin_map"):
            raise ConfigurationError("Crypto borrow settings cannot be used for Stock+ US stocks")

    @staticmethod
    def stock_symbol(symbol: str) -> str:
        return us_stock_symbol(symbol, qualified=True)

    @staticmethod
    def _positive(value, name: str) -> str:
        try:
            return decimal_string(value)
        except (CommandError, ValueError, TypeError, ArithmeticError) as exc:
            raise UnsupportedOrder(f"{name} must be a positive decimal") from exc

    @staticmethod
    def correlation(command_id) -> str:
        return "cmd:" + hashlib.sha256(str(command_id).encode()).hexdigest()[:60]

    @staticmethod
    def _session_name(value):
        if not isinstance(value, str):
            raise UnsupportedOrder("Invalid Stock+ outside_rth")
        value = {"AUTO": "Auto", "RTH_ONLY": "RTHOnly", "ANY_TIME": "AnyTime",
                 "OVERNIGHT": "Overnight"}.get(value, value)
        if value not in {"Auto", "RTHOnly", "AnyTime", "Overnight"}:
            raise UnsupportedOrder("Invalid Stock+ outside_rth")
        return value

    @staticmethod
    def _automatic_session():
        # This selects a session, not symbol eligibility or a market-open state.
        hour = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York")).hour
        return "Overnight" if hour >= 20 or hour < 4 else "AnyTime"

    def build_order(self, command: dict) -> dict:
        require_us_equity(command, self.config)
        options = command.get("options") or {}
        if not isinstance(options, dict) or set(options) - {"outside_rth", "extended_hours", "expire_date"}:
            raise UnsupportedOrder("Bitget Stock+ options are outside_rth, extended_hours and expire_date")
        side = command.get("side")
        kind = command.get("order_type")
        tif = command.get("time_in_force", "day")
        if side not in {"buy", "sell"} or kind not in {"market", "limit"}:
            raise UnsupportedOrder("Bitget Stock+ requires buy/sell and market/limit")
        if tif not in {"day", "gtc", "gtd"}:
            raise UnsupportedOrder("Bitget Stock+ supports DAY/GTC/GTD; native FOK and IOC are not documented")
        default_session = self._session_name(self.config.get("default_outside_rth", "AnyTime"))
        outside = self._session_name(options.get("outside_rth", "RTHOnly" if command.get("auction_intent") else default_session))
        if "extended_hours" in options:
            if type(options["extended_hours"]) is not bool:
                raise UnsupportedOrder("extended_hours must be boolean")
            if "outside_rth" in options:
                if (outside == "RTHOnly") == options["extended_hours"]:
                    raise UnsupportedOrder("Conflicting outside_rth and extended_hours options")
            else:
                outside = ("Auto" if default_session == "Auto" else "AnyTime") if options["extended_hours"] else "RTHOnly"
        if command.get("auction_intent") and outside != "RTHOnly":
            raise UnsupportedOrder("LULD intent requires regular trading hours")
        if outside == "Auto":
            outside = self._automatic_session()
        if outside == "Overnight" and kind == "market":
            raise UnsupportedOrder("Overnight trading requires a limit order in this adapter; use LIMIT_ORDER with an explicit limit_price")
        order = {"symbol": self.stock_symbol(command.get("symbol")), "side": "Buy" if side == "buy" else "Sell",
                 "orderType": "MO" if kind == "market" else "LO",
                 "submittedQuantity": self._positive(command.get("quantity"), "quantity"),
                 "timeInForce": {"day": "Day", "gtc": "GoodTilCanceled", "gtd": "GoodTilDate"}[tif],
                 "outsideRth": outside, "remark": self.correlation(command["id"])}
        if kind == "limit":
            order["submittedPrice"] = self._positive(command.get("limit_price"), "limit_price")
        elif command.get("limit_price") is not None:
            raise UnsupportedOrder("A market order cannot include limit_price")
        expiry = options.get("expire_date")
        if tif == "gtd":
            try:
                if not isinstance(expiry, str) or date.fromisoformat(expiry).isoformat() != expiry:
                    raise ValueError
            except (ValueError, TypeError):
                raise UnsupportedOrder("GTD requires options.expire_date in YYYY-MM-DD format") from None
            order["expireDate"] = expiry
        elif expiry is not None:
            raise UnsupportedOrder("expire_date requires time_in_force=gtd")
        return order

    @staticmethod
    def signature(secret: str, timestamp: str, method: str, path: str, body: str = "") -> str:
        message = f"{timestamp}{method.upper()}{path}{body}".encode()
        return base64.b64encode(hmac.new(secret.encode(), message, hashlib.sha256).digest()).decode()

    @staticmethod
    def _access_error(code):
        if str(code) == "40084":
            return AuthenticationError(
                "Bitget rejected account mode (code 40084): this key belongs to a Classic Account; "
                "the requested API requires Unified Account access", no_submission=True)
        return AuthenticationError(f"Bitget rejected API credentials/access (code {code})", no_submission=True)

    def _response_error(self, status, result):
        """Classify broker evidence; an HTTP client error alone is insufficient."""
        raw_code = result.get("code") if isinstance(result, dict) else None
        code = str(raw_code) if isinstance(raw_code, (str, int)) and not isinstance(raw_code, bool) else None
        if code is not None and not re.fullmatch(r"[0-9]{3,6}", code):
            code = None
        raw_message = result.get("msg", result.get("message")) if isinstance(result, dict) else None
        message = raw_message if isinstance(raw_message, str) else "Broker response has no error message"
        for key in ("api_key", "api_secret", "passphrase"):
            secret = self.config.get(key)
            if isinstance(secret, str) and secret:
                message = message.replace(secret, "[redacted]")
        message = " ".join(message.split())[:1000]
        description = f"Bitget HTTP {status}, code {code or 'unavailable'}: {message}"
        # Server/timeout/unknown outcomes override apparent client-side failure.
        if status >= 500 or status < 200 or 300 <= status < 400 or code in UNCERTAIN_ERROR_CODES:
            return OutcomeUnknown(description)
        data = result.get("data") if isinstance(result, dict) else None
        def reference(value):
            if isinstance(value, dict):
                return any(value.get(k) not in (None, "", 0) for k in ("orderId", "order_id")) or any(reference(v) for v in value.values())
            return isinstance(value, list) and any(reference(v) for v in value)
        if reference(data):
            return OutcomeUnknown(description)
        if status in {401, 403}:
            return AuthenticationError(description, no_submission=True)
        if code in AUTH_ERROR_CODES:
            return self._access_error(code)
        if code in {None, "00000", "0", "25212"}:
            return OutcomeUnknown(description)
        if re.search(r"timed?\s*out|timeout|unknown\s+error|internal\s+(?:server\s+)?error|system\s+error|service\s+(?:return\s+an\s+)?error", message, re.I):
            return OutcomeUnknown(description)
        if code in REJECTION_CODES or (status in {400, 404, 405, 415, 422, 429}
                                      and isinstance(raw_message, str) and bool(raw_message.strip())):
            return RequestRejected(description, code=code, http_status=status)
        return OutcomeUnknown(description)

    @contextmanager
    def monitoring_requests(self, lane, stop):
        self._request_context.lane = lane
        self._request_context.stop = stop
        try:
            yield
        finally:
            session = getattr(self._request_context, 'session', None)
            if session is not None:
                session.close()
                del self._request_context.session
            self._request_context.lane = 'command'
            self._request_context.stop = None

    def _request(self, method: str, path: str, params=None, payload=None):
        if self._limiter is None:
            return self._raw_request(method, path, params, payload)
        from bitget.rate_limit import MonitorStopped
        lane = getattr(self._request_context, 'lane', 'command')
        stop = getattr(self._request_context, 'stop', None)
        if stop is not None and stop.is_set():
            raise MonitorStopped("Stock+ monitor stopped")
        started = time.monotonic()
        wait_ms = 0.0
        sent = None
        error = None
        self._request_context.http_status = None
        self._request_context.broker_code = None
        try:
            if path.startswith(PREFIX + '/trade/'):
                wait_ms = self._limiter.acquire(background=lane != 'command',
                                                low_priority=lane == 'reconcile', stop=stop)
            else:
                wait_ms = self._limiter.wait_cooldown(stop)
            sent = time.monotonic()
            return self._raw_request(method, path, params, payload)
        except Exception as exc:
            error = type(exc).__name__
            raise
        finally:
            if self.request_observer is not None:
                ended = time.monotonic()
                # No URL query, payload, signed headers, or broker error text.
                try:
                    self.request_observer('monitor_request', lane=lane, method=method, endpoint=path,
                                          http_status=self._request_context.http_status,
                                          broker_code=self._request_context.broker_code, error_type=error,
                                          rate_limit_wait_ms=round(wait_ms, 3),
                                          http_ms=round((ended - sent) * 1000, 3) if sent is not None else None,
                                          total_ms=round((ended - started) * 1000, 3))
                except Exception:
                    print(json.dumps({'event': 'monitor_telemetry_error', 'component': 'request'}), flush=True)

    def _raw_request(self, method: str, path: str, params=None, payload=None):
        import requests

        for key in ("api_key", "api_secret", "passphrase"):
            if not self.config.get(key):
                raise ConfigurationError(f"Bitget config requires {key}")
        if self._limiter is None and path.startswith(PREFIX + "/trade/"):
            # Stock+ has its own quota, not UTA/crypto's higher rate. Pace all
            # trade calls conservatively within this adapter process.
            now = time.monotonic()
            while self._trade_times and now - self._trade_times[0] >= 30:
                self._trade_times.popleft()
            delay = max(0, (self._trade_times[-1] + .02 - now) if self._trade_times else 0,
                        (self._trade_times[0] + 30 - now) if len(self._trade_times) >= 30 else 0)
            if delay:
                time.sleep(delay)
                now = time.monotonic()
                while self._trade_times and now - self._trade_times[0] >= 30:
                    self._trade_times.popleft()
            self._trade_times.append(now)
        signing_path = path
        if params:
            signing_params = params
            if (method.upper() == "GET" and path in {
                    PREFIX + "/trade/today-orders", PREFIX + "/trade/history-orders"}
                    and isinstance(params.get("status"), (list, tuple)) and params["status"]):
                # Stock+ authenticates the first repeated status value, while
                # its order query uses all values. Signing the full repeated
                # query returns 40009 (verified against both endpoints).
                signing_params = {**params, "status": params["status"][0]}
            signing_path += "?" + urlencode(signing_params, doseq=True)
            path += "?" + urlencode(params, doseq=True)
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True) if payload is not None else ""
        timestamp = str(time.time_ns() // 1_000_000)
        headers = {
            "ACCESS-KEY": self.config["api_key"],
            "ACCESS-PASSPHRASE": self.config["passphrase"],
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-SIGN": self.signature(self.config["api_secret"], timestamp, method, signing_path, body),
            "Content-Type": "application/json",
            "locale": "en-US",
        }
        try:
            request = requests.request
            timeout = self.config.get("timeout", 20)
            if self._limiter is not None:
                if not hasattr(self._request_context, 'session'):
                    self._request_context.session = requests.Session()
                request = self._request_context.session.request
                if getattr(self._request_context, 'lane', 'command') != 'command':
                    timeout = (3.05, 5)
            response = request(
                method, self.base_url + path, headers=headers, data=body or None,
                timeout=timeout, allow_redirects=False,
            )
        except requests.RequestException as exc:
            raise OutcomeUnknown("Bitget request failed; an order submission may have succeeded. Reconcile before retrying.") from exc
        try:
            result = response.json()
        except ValueError:
            result = None
        if self._limiter is not None:
            self._request_context.http_status = response.status_code
            self._request_context.broker_code = str(result.get('code')) if isinstance(result, dict) else None
            if response.status_code == 429 or self._request_context.broker_code == '429':
                self._limiter.throttle(response.headers.get('Retry-After'))
        if not 200 <= response.status_code < 300 or not isinstance(result, dict) or str(result.get("code")) != "00000":
            raise self._response_error(response.status_code, result)
        if "data" not in result:
            raise BrokerError("Bitget response omitted data")
        return result["data"]

    @staticmethod
    def _list(data, field):
        if not isinstance(data, dict) or not isinstance(data.get(field), list):
            raise BrokerError(f"Stock+ response omitted {field} list")
        if any(not isinstance(row, dict) for row in data[field]):
            raise BrokerError(f"Stock+ {field} contains an invalid row")
        return data[field]

    @classmethod
    def _is_stock(cls, row):
        if not isinstance(row, dict) or not isinstance(row.get("symbol"), str):
            raise BrokerError("Stock+ record omitted its symbol")
        symbol = row["symbol"]
        if not symbol.endswith(".US") or row.get("market", "US") != "US":
            return False
        if row.get("asset_class", "us_equity") not in {"us_equity", "equity", "stock"}:
            return False
        try:
            return cls.stock_symbol(symbol) == symbol
        except UnsupportedOrder:
            return False

    @staticmethod
    def _identity(row):
        identity = row.get("orderId")
        if not isinstance(identity, str) or not identity or len(identity) > 128 or not re.fullmatch(r"[A-Za-z0-9_-]+", identity):
            raise BrokerError("Stock+ order omitted an exact valid string orderId")
        return identity

    @staticmethod
    def _created(row):
        try:
            return parse_time(row["submittedAt"])
        except (KeyError, ValueError, TypeError, OverflowError, OSError, CommandError):
            raise BrokerError("Stock+ order omitted a valid timezone-aware submittedAt") from None

    def _stock_instrument(self, symbol):
        rows = self._list(self._request("GET", PREFIX + "/market/static", {"symbol": symbol}), "list")
        matches = [row for row in rows if row.get("symbol") == symbol]
        if not matches:
            return None
        if len(matches) != 1:
            raise BrokerError("Stock+ instrument identity is ambiguous")
        row = matches[0]
        # board=USMain is the documented US stock example. Unknown, index and
        # option classifications never establish stock eligibility.
        if row.get("board") not in {"USMain", "USPink"} or row.get("currency") != "USD":
            raise UnsupportedOrder("Stock+ instrument is not a verified US stock board in USD")
        return row

    def place_order(self, command: dict):
        payload = self.build_order(command)
        try:
            if self._stock_instrument(payload["symbol"]) is None:
                raise UnsupportedOrder("Stock+ did not return the requested US stock instrument")
        except AuthenticationError:
            raise
        except BrokerError as exc:
            raise RequestRejected(str(exc), code=getattr(exc, "code", None),
                                  http_status=getattr(exc, "http_status", None), requests_sent=0) from exc
        result = self._request("POST", PREFIX + "/trade/place-order", payload=payload)
        if not isinstance(result, dict) or not isinstance(result.get("orderId"), str) or not result["orderId"]:
            raise OutcomeUnknown("Stock+ placement response omitted orderId; reconcile before retrying")
        if result.get("symbol") not in (None, payload["symbol"]):
            raise OutcomeUnknown("Stock+ placement response symbol differs; reconcile before retrying")
        return result

    def positions(self):
        channels = self._list(self._request("GET", PREFIX + "/asset/stock-position"), "channels")
        output = []
        seen = set()
        for channel in channels:
            identity = channel.get("accountChannel")
            if not isinstance(identity, str) or not identity or identity in seen:
                raise BrokerError("Stock+ position account channel is missing or duplicated")
            seen.add(identity)
            for row in self._list(channel, "positions"):
                if self._is_stock(row):
                    output.append({**row, "accountChannel": identity})
        return output

    def _today(self, statuses=None):
        params = {"market": "US"}
        if statuses is not None:
            params["status"] = sorted(statuses)
        data = self._request("GET", PREFIX + "/trade/today-orders", params)
        if isinstance(data, dict) and data.get("hasMore") not in (None, False):
            raise BrokerError("Stock+ today-orders unexpectedly reports truncation")
        return self._list(data, "orders")

    def _history(self, start_seconds, end_seconds, statuses=None):
        """Inclusive integer-second windows; split hasMore, never drop a boundary."""
        if end_seconds < start_seconds:
            return []
        pending = [(start_seconds, end_seconds)]
        output = []
        calls = 0
        while pending:
            low, high = pending.pop()
            calls += 1
            if calls > 10000:
                raise BrokerError("Stock+ history query exceeds 10000 windows; result is incomplete")
            params = {"market": "US", "startAt": low, "endAt": high}
            if statuses is not None:
                params["status"] = sorted(statuses)
            data = self._request("GET", PREFIX + "/trade/history-orders", params)
            rows = self._list(data, "orders")
            # Live Stock+ omits hasMore on nonempty pages too. Its documented
            # cap is 1000 orders: a shorter page is complete, but a full page
            # without the flag must be split rather than silently truncated.
            if "hasMore" in data and type(data["hasMore"]) is not bool:
                raise BrokerError("Stock+ history returned invalid boolean hasMore; completeness is unknown")
            has_more = data.get("hasMore", len(rows) >= HISTORY_PAGE_LIMIT)
            if has_more:
                if low == high:
                    raise BrokerError("Stock+ history cannot establish completeness within one second")
                middle = (low + high) // 2
                pending.extend(((middle + 1, high), (low, middle)))
            else:
                output.extend(rows)
        return output

    def _merge_orders(self, *batches):
        orders = {}
        for batch in batches:
            for row in batch:
                if not self._is_stock(row):
                    continue
                identity = self._identity(row)
                self._created(row)
                if row.get("status") not in ACTIVE | TERMINAL:
                    raise BrokerError("Stock+ order has an unknown status; cannot establish active orders")
                previous = orders.get(identity)
                if previous and previous["symbol"] != row["symbol"]:
                    raise BrokerError("Stock+ order identity changed symbol")
                # History queried after today captures pending -> terminal
                # transitions. Retain a newer explicit broker update if supplied.
                if previous and previous.get("updatedAt") and row.get("updatedAt"):
                    try:
                        if parse_time(previous["updatedAt"]) > parse_time(row["updatedAt"]):
                            continue
                    except (ValueError, TypeError, CommandError):
                        raise BrokerError("Stock+ order has an invalid updatedAt") from None
                orders[identity] = row
        return sorted(orders.values(), key=lambda row: (self._created(row), row["orderId"]))

    def orders_today(self, start: datetime, end: datetime):
        start, end = parse_time(start), parse_time(end)
        if start >= end:
            raise BrokerError("Stock+ order query requires start before end")
        today = self._today()
        history = self._history(math.floor(start.timestamp()), math.ceil(end.timestamp()) - 1)
        return [row for row in self._merge_orders(today, history) if start <= self._created(row) < end]

    def open_stock_orders(self):
        # No date cutoff: preserve older GTC orders. The broker may reject a
        # historical range outside retention; propagate that instead of claiming
        # a complete cancellation set from a shorter successful query.
        today = self._today()
        history = self._history(0, int(time.time()))
        return [row for row in self._merge_orders(today, history) if row["status"] in ACTIVE]

    def stock_order_detail(self, order_id):
        identity = self._identity({"orderId": order_id})
        row = self._request("GET", PREFIX + "/trade/order-detail", {"orderId": identity})
        if not isinstance(row, dict) or self._identity(row) != identity:
            raise BrokerError("Stock+ order detail identity mismatch")
        if not self._is_stock(row):
            raise UnsupportedOrder("Stock+ order is not a US stock order")
        self._created(row)
        if row.get("status") not in ACTIVE | TERMINAL:
            raise BrokerError("Stock+ order detail has an unknown status")
        return row

    def cancel_stock_order(self, order_id, symbol=None):
        row = self.stock_order_detail(order_id)
        if symbol is not None and self.stock_symbol(symbol) != row["symbol"]:
            raise BrokerError("Stock+ cancel symbol differs from the requested order")
        if self._stock_instrument(row["symbol"]) is None:
            raise UnsupportedOrder("Stock+ cannot verify the order's US stock instrument")
        if row["status"] in TERMINAL:
            return {"orderId": row["orderId"], "symbol": row["symbol"], "status": row["status"],
                    "already_terminal": True, "cancel_requested": False}
        result = self._request("POST", PREFIX + "/trade/cancel-order",
                               payload={"symbol": row["symbol"], "orderId": row["orderId"]})
        if not isinstance(result, dict) or result.get("orderId") != row["orderId"] or result.get("symbol") not in (None, row["symbol"]):
            # A read can prove the exact target is terminal even when the live
            # acknowledgement is incomplete. Never retry the cancellation.
            try:
                observed = self.stock_order_detail(row["orderId"])
            except BrokerError:
                observed = None
            if observed is not None and observed["status"] in TERMINAL and observed["symbol"] == row["symbol"]:
                return {"orderId": observed["orderId"], "symbol": observed["symbol"], "status": observed["status"],
                        "already_terminal": True, "cancel_requested": False, "terminal_confirmed": True,
                        "acknowledgement_verified": False}
            raise OutcomeUnknown("Stock+ cancellation acknowledgement identity mismatch; query order detail")
        return result

    def tradability(self, symbol: str):
        symbol = self.stock_symbol(symbol)
        instrument = self._stock_instrument(symbol)
        base = {"symbol": symbol, "account_tradable": None, "luld_applicable": True,
                "scope": "Stock+ instrument listing and market status; account permission remains unknown"}
        if instrument is None:
            return {**base, "found": False, "tradable": False, "halted": None,
                    "reason": "US stock was not returned by Stock+ instrument lookup"}
        quotes = self._list(self._request("GET", PREFIX + "/market/quote", {"symbol": symbol}), "list")
        quotes = [row for row in quotes if row.get("symbol") == symbol]
        if len(quotes) != 1:
            raise BrokerError("Stock+ quote identity is missing or ambiguous")
        quote = quotes[0]
        status = quote.get("tradeStatus")
        tradable = True if status == "Normal" else None
        # Unknown statuses are retained; the docs do not enumerate every halt
        # reason. A quote never establishes LULD reopening-auction eligibility.
        return {**base, "found": True, "tradable": tradable,
                "halted": False if status == "Normal" else None,
                "status": status, "observed_at": quote.get("timestamp"), "instrument": instrument,
                "reason": "Market quote status only; no account-specific order-validation endpoint is documented"}

    def borrow_availability(self, symbol: str):
        return {"symbol": self.stock_symbol(symbol), "supported": False, "status": "unavailable",
                "shortable": None, "easy_to_borrow": None, "available_quantity": None,
                "fee_rate": None, "locate_required": None, "borrowable": None,
                "scope": "US stock borrowing in Bitget Stock+", "quantity_unit": "shares",
                "reason": "Stock+ supports ETB stock short selling, but its documented API exposes no stock borrow inventory, rate or shortable query. Crypto borrowing endpoints do not answer this question."}

    def capabilities(self):
        return {"broker": "bitget", "scope": "Bitget Stock+ US stocks only", "asset_classes": ["us_equity"],
                "order_types": ["market", "limit"], "implemented_order_types": ["market", "limit"],
                "broker_order_types": ["MO", "LO", "LIT", "MIT", "TSLPAMT", "TSLPPCT"],
                "time_in_force": ["day", "gtc", "gtd"], "limit_time_in_force": ["day", "gtc", "gtd"],
                "native_fok": False, "native_ioc": False, "luld_applicable": True,
                "luld_auction_participation": "unverified; no dedicated auction route is documented",
                "borrow_availability": "No documented Stock+ stock borrow availability endpoint",
                "tradability_scope": "US stock listing and quote status; account permission is separate",
                "outside_rth": ["Auto", "RTHOnly", "AnyTime", "Overnight"],
                "default_outside_rth": self.config.get("default_outside_rth", "AnyTime"),
                "automatic_session_timezone": "America/New_York",
                "automatic_overnight_hours": "20:00-04:00 America/New_York; broker eligibility applies",
                "overnight_order_types": ["limit"],
                "server_idempotency": False, "client_correlation": "remark (correlation only)",
                "sources": ["https://www.bitget.com/docs/catalog/stock-plus/orders",
                            "https://www.bitget.com/docs/catalog/stock-plus/assets",
                            "https://www.bitget.com/docs/catalog/stock-plus/stock-quotes"]}

    def snapshot_account(self):
        from common.snapshots import number, position
        rows = self._list(self._request("GET", PREFIX + "/asset/account", {"currency": "USD"}), "list")
        usd = [row for row in rows if row.get("currency") == "USD"]
        if len(usd) != 1:
            raise BrokerError("Stock+ USD account balance is missing or ambiguous")
        account = usd[0]
        cash = number(account.get("totalCash"), "cash")
        infos = self._list(account, "cashInfos")
        usd_infos = [row for row in infos if row.get("currency") == "USD"]
        if len(usd_infos) != 1:
            raise BrokerError("Stock+ USD available cash is missing or ambiguous")
        available = number(usd_infos[0].get("availableCash"), "available cash")
        stock_positions = self.positions()
        if any(row.get("currency") != "USD" for row in stock_positions):
            raise BrokerError("Stock+ US stock position has a missing or unexpected currency")
        mapped = [position(row["symbol"][:-3], row.get("quantity"), row.get("costPrice"),
                           available_qty=number(row.get("availableQuantity"), "available quantity"),
                           currency="USD", account_channel=row["accountChannel"])
                  for row in stock_positions]
        return {"cash": cash, "cash_by_currency": {"USD": cash}, "available_cash_by_currency": {"USD": available},
                "cash_basis": "Stock+ totalCash USD", "positions": mapped}

    def snapshot_order(self, row, observed_at):
        from common.snapshots import base_order, number
        if not self._is_stock(row):
            raise UnsupportedOrder("Stock+ snapshot requires a US stock order")
        if row.get("currency") != "USD":
            raise BrokerError("Stock+ US stock order has a missing or unexpected currency")
        statuses = {**{key: "PENDING_NEW" for key in ("NotReported", "ReplacedNotReported", "ProtectedNotReported", "VarietiesNotReported", "WaitToNew")},
                    "New": "NEW", "Replaced": "NEW", "WaitToReplace": "NEW", "PendingReplace": "NEW",
                    "PartialFilled": "PARTIALLY_FILLED", "WaitToCancel": "PENDING_CANCEL", "PendingCancel": "PENDING_CANCEL",
                    "Filled": "FILLED", "Rejected": "REJECTED", "Canceled": "CANCELED", "Expired": "EXPIRED", "PartialWithdrawal": "CANCELED"}
        if row.get("status") not in statuses:
            raise BrokerError("Stock+ snapshot has an unknown order status")
        kind = {"MO": "MARKET", "LO": "LIMIT", "MIT": "STOP", "LIT": "STOP_LIMIT",
                "TSLPAMT": "TRAILING_STOP", "TSLPPCT": "TRAILING_STOP"}.get(row.get("orderType"), "OTHER")
        updated = row.get("updatedAt") or (iso_utc(observed_at) if isinstance(observed_at, datetime) else observed_at)
        result = base_order(self._identity(row), row.get("submittedAt"), updated, row["symbol"][:-3],
                            str(row.get("side", "")).upper(), kind, statuses[row["status"]],
                            row.get("quantity"), row.get("executedQuantity"), row.get("executedPrice"))
        if row.get("orderType") in {"LO", "LIT", "TSLPAMT", "TSLPPCT"} and row.get("price") is not None:
            result["limit_price"] = number(row["price"], "limit price")
        if row.get("triggerPrice") is not None:
            result["stop_price"] = number(row["triggerPrice"], "trigger price")
        if row.get("timeInForce"):
            result["time_in_force"] = {"Day": "day", "GoodTilCanceled": "gtc", "GoodTilDate": "gtd"}.get(row["timeInForce"], row["timeInForce"])
        result["currency"] = "USD"
        messages = [str(row["msg"])] if row.get("msg") else []
        if row.get("remark"):
            messages.append("Correlation: " + str(row["remark"]))
        if messages:
            result["message"] = "; ".join(messages)[:2000]
        return result
