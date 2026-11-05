"""Acknowledged account snapshots and a durable KTrader order-snapshot outbox.

The broker mappers supply full canonical snapshots. This module never guesses a
broker state, fill, balance or timestamp. Keep this database across restarts; it
contains publication sequences, separately from the financial command journal.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import threading
from zoneinfo import ZoneInfo

from .errors import BrokerError, ConfigurationError


class PublicationError(BrokerError):
    """A snapshot is invalid or remains undelivered; no trading action is retried."""


MAX_SEQUENCE = 2**53 - 1
ORDER_REQUIRED = {
    "schema_version", "account_id", "order_id", "created_at", "updated_at",
    "symbol", "side", "order_type", "status", "quantity", "filled_quantity",
    "average_fill_price",
}
ORDER_OPTIONAL = {
    "account_num_id", "notional", "limit_price", "stop_price", "currency",
    "time_in_force", "command_id", "client_order_id", "message",
}
STATUSES = {"PENDING_NEW", "NEW", "PARTIALLY_FILLED", "PENDING_CANCEL", "FILLED",
            "CANCELED", "REJECTED", "EXPIRED", "UNKNOWN"}
ORDER_TYPES = {"MARKET", "LIMIT", "STOP", "STOP_LIMIT", "TRAILING_STOP", "OTHER"}


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _identifier(value, name, maximum):
    if (not isinstance(value, str) or not value or value != value.strip()
            or len(value) > maximum or any(ord(char) < 32 for char in value)):
        raise PublicationError(f"Invalid snapshot {name}")
    return value


def _timestamp(value, name):
    if not isinstance(value, str) or len(value) > 40 or "T" not in value:
        raise PublicationError(f"Snapshot {name} must be an offset-bearing ISO timestamp")
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.utcoffset() is None:
            raise ValueError
        return stamp.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        raise PublicationError(f"Invalid snapshot {name}") from None


def _iso(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _number(value, name, *, nullable=False, minimum=None, positive=False):
    if value is None and nullable:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise PublicationError(f"Snapshot {name} must be a JSON number")
    try:
        decimal = Decimal(str(value))
        result = int(decimal) if decimal.is_finite() and decimal == decimal.to_integral_value() else float(decimal)
        if (not decimal.is_finite() or not math.isfinite(result)
                or (positive and decimal <= 0) or (minimum is not None and decimal < minimum)
                or (decimal != 0 and result == 0)):
            raise ValueError
    except (ValueError, OverflowError):
        raise PublicationError(f"Invalid snapshot {name}") from None
    return result


def validate_order_snapshot(snapshot, *, sequenced=False):
    """Validate KTrader v1 fields and execution consistency, returning plain JSON.

    Input numbers must already be numbers, not broker decimal strings. A mapper
    must retain unknown average prices as null rather than inventing a fill.
    """
    required = ORDER_REQUIRED | ({"sequence"} if sequenced else set())
    permitted = required | ORDER_OPTIONAL
    if not isinstance(snapshot, dict) or not required <= snapshot.keys() or snapshot.keys() - permitted:
        raise PublicationError("Order snapshot has missing or unsupported fields")
    result = dict(snapshot)
    if type(result["schema_version"]) is not int or result["schema_version"] != 1:
        raise PublicationError("Order snapshot schema_version must be 1")
    for name, maximum in (("account_id", 128), ("order_id", 256), ("symbol", 64),
                          ("time_in_force", 32), ("command_id", 256), ("client_order_id", 256)):
        if name in result:
            _identifier(result[name], name, maximum)
    if re.search(r"\s", result["symbol"]):
        raise PublicationError("Snapshot symbol cannot contain whitespace")
    if "account_num_id" in result and (type(result["account_num_id"]) is not int or result["account_num_id"] <= 0):
        raise PublicationError("Snapshot account_num_id must be a positive integer")
    if sequenced and (type(result["sequence"]) is not int or not 0 <= result["sequence"] <= MAX_SEQUENCE):
        raise PublicationError("Invalid order publication sequence")
    if (not all(isinstance(result[key], str) for key in ("side", "order_type", "status"))
            or result["side"] not in ("BUY", "SELL") or result["order_type"] not in ORDER_TYPES or result["status"] not in STATUSES):
        raise PublicationError("Invalid canonical order side, type or status")
    created = _timestamp(result["created_at"], "created_at")
    updated = _timestamp(result["updated_at"], "updated_at")
    if updated < created:
        raise PublicationError("Order updated_at precedes created_at")
    result.update(created_at=_iso(created), updated_at=_iso(updated))
    result["quantity"] = _number(result["quantity"], "quantity", nullable=True, positive=True)
    result["filled_quantity"] = _number(result["filled_quantity"], "filled_quantity", minimum=0)
    result["average_fill_price"] = _number(result["average_fill_price"], "average_fill_price", nullable=True, minimum=0)
    for name in ("limit_price", "stop_price"):
        if name in result:
            result[name] = _number(result[name], name, nullable=True, minimum=0)
    if "notional" in result:
        result["notional"] = _number(result["notional"], "notional", positive=True)
    if result["quantity"] is None and "notional" not in result:
        raise PublicationError("Unknown order quantity requires a positive known notional")
    for kinds, field in (({"LIMIT", "STOP_LIMIT"}, "limit_price"), ({"STOP", "STOP_LIMIT"}, "stop_price")):
        if result["order_type"] in kinds and result.get(field) is None:
            raise PublicationError(f"Order type requires {field}")
    quantity, filled = result["quantity"], result["filled_quantity"]
    if quantity is not None and filled > quantity:
        raise PublicationError("Order filled_quantity exceeds quantity")
    if result["status"] == "FILLED" and (filled <= 0 or (quantity is not None and filled != quantity)):
        raise PublicationError("FILLED order does not contain a complete positive fill")
    if result["status"] == "PARTIALLY_FILLED" and (filled <= 0 or (quantity is not None and filled >= quantity)):
        raise PublicationError("PARTIALLY_FILLED order has inconsistent fill quantities")
    if filled == 0 and result["average_fill_price"] not in (None, 0):
        raise PublicationError("Unfilled order cannot have a positive average fill price")
    if "currency" in result and (not isinstance(result["currency"], str) or not re.fullmatch(r"[A-Z]{3}", result["currency"])):
        raise PublicationError("Invalid order currency")
    if "message" in result and (not isinstance(result["message"], str) or len(result["message"]) > 2000):
        raise PublicationError("Invalid order message")
    if len(_json(result).encode()) > 65536:
        raise PublicationError("Order snapshot exceeds 64 KiB")
    return result


def _state(snapshot):
    return {key: value for key, value in snapshot.items() if key not in {"sequence", "updated_at"}}


class SnapshotPublisher:
    """Send canonical snapshots through kafka-python's send(...).get(timeout).

    routing needs account_id and may contain account_num_id and timezone. By
    default only orders created today are published or retried. The independent
    Bitget monitor opts into allow_prior_orders for carried GTC orders.
    Prior/future-day pending rows remain retained for inspection, never replayed.
    Independent failures appear in publish_orders' result; callers can still
    publish the account snapshot. Account snapshots have no order sequence.
    """
    def __init__(self, path, routing, producer, account_topic="account-details",
                 order_topic="order-status", timeout=30, allow_prior_orders=False,
                 republish_on_restart=True):
        self.path = Path(path).expanduser()
        self.account = _identifier(routing.get("account_id"), "account_id", 128)
        self.account_num_id = routing.get("account_num_id")
        if self.account_num_id is not None and (type(self.account_num_id) is not int or self.account_num_id <= 0):
            raise ConfigurationError("Publication account_num_id must be a positive integer")
        try:
            self.zone = ZoneInfo(routing.get("timezone", "America/New_York"))
        except (ValueError, TypeError, KeyError):
            raise ConfigurationError("Invalid publication timezone") from None
        if not isinstance(account_topic, str) or not account_topic.strip() or not isinstance(order_topic, str) or not order_topic.strip() or account_topic == order_topic:
            raise ConfigurationError("Account and order publication topics must be distinct nonempty names")
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
            raise ConfigurationError("Publication timeout must be positive and finite")
        self.producer, self.account_topic, self.order_topic, self.timeout = producer, account_topic, order_topic, timeout
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(self.path, timeout=timeout, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS snapshot_publications (account_id TEXT NOT NULL, order_id TEXT NOT NULL, published TEXT, pending TEXT, acknowledgement TEXT, PRIMARY KEY(account_id,order_id))")
        self.db.commit()
        self._seen = set()
        self._thread_lock = threading.RLock()
        self.allow_prior_orders = allow_prior_orders
        self.republish_on_restart = republish_on_restart
        self.delivery_observer = None

    def known_orders(self):
        """Validated durable observations, for restoring a monitor after restart."""
        with self._locked():
            identities = [row[0] for row in self.db.execute(
                'SELECT order_id FROM snapshot_publications WHERE account_id=?', (self.account,))]
            return [pending or published for published, pending in map(self._read, identities)]

    def _eligible(self, snapshot, today):
        created = _timestamp(snapshot['created_at'], 'created_at').astimezone(self.zone).date()
        return created <= today if self.allow_prior_orders else created == today

    @contextmanager
    def _locked(self):
        # The pending commit must survive a process death before Kafka send.
        # A separate OS lock permits committing SQLite while serializing other
        # publishers sharing this database across the network acknowledgement.
        with self._thread_lock, self.path.with_suffix(self.path.suffix + ".lock").open("a+b") as handle:
            if os.name == "nt":
                import msvcrt
                handle.seek(0)
                if not handle.read(1):
                    handle.write(b"0")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                try:
                    yield
                finally:
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _route(self, snapshot):
        if snapshot["account_id"] != self.account:
            raise PublicationError("Snapshot account_id differs from publisher routing")
        numeric = snapshot.get("account_num_id")
        if numeric is not None and (type(numeric) is not int or numeric != self.account_num_id):
            raise PublicationError("Snapshot account_num_id differs from publisher routing")

    def _send(self, topic, key, snapshot):
        metadata = self.producer.send(topic, key=key, value=_json(snapshot).encode()).get(timeout=self.timeout)
        if metadata is None or type(getattr(metadata, "partition", None)) is not int or type(getattr(metadata, "offset", None)) is not int or metadata.partition < 0 or metadata.offset < 0:
            raise PublicationError("Kafka returned no valid snapshot delivery acknowledgement")
        return {"topic": topic, "partition": metadata.partition, "offset": metadata.offset}

    def publish_account(self, snapshot):
        """Publish current canonical holdings; errors propagate to the caller."""
        if not isinstance(snapshot, dict) or not {"account_id", "cash", "positions", "ts", "trading_enabled"} <= snapshot.keys():
            raise PublicationError("Account snapshot has missing fields")
        value = dict(snapshot)
        self._route(value)
        if type(value["trading_enabled"]) is not bool or not isinstance(value["positions"], list):
            raise PublicationError("Invalid account trading state or positions")
        value["ts"] = _iso(_timestamp(value["ts"], "ts"))
        # KTrader casts cash with float(...); null rejects the whole snapshot.
        # The mapper must supply a known numeric cash amount or an explicitly
        # configured, clearly labelled alternative basis. Never invent zero.
        value["cash"] = _number(value["cash"], "cash")
        if "cash_by_currency" in value:
            if not isinstance(value["cash_by_currency"], dict):
                raise PublicationError("cash_by_currency must be an object")
            value["cash_by_currency"] = dict(value["cash_by_currency"])
            for currency, amount in value["cash_by_currency"].items():
                if not isinstance(currency, str) or not re.fullmatch(r"[A-Z]{3}", currency):
                    raise PublicationError("Invalid cash currency")
                value["cash_by_currency"][currency] = _number(amount, "cash_by_currency")
        positions = []
        for position in value["positions"]:
            if not isinstance(position, dict) or not {"symbol", "qty", "avg_price"} <= position.keys():
                raise PublicationError("Account position has missing canonical fields")
            position = dict(position)
            _identifier(position["symbol"], "symbol", 64)
            if re.search(r"\s", position["symbol"]):
                raise PublicationError("Position symbol contains whitespace")
            position["qty"] = _number(position["qty"], "qty")
            # Diluted position cost may be negative after realized proceeds;
            # it is not an order execution price and must retain broker meaning.
            position["avg_price"] = _number(position["avg_price"], "avg_price", nullable=True)
            for field in ("available_qty", "marketPrice", "marketValue", "dailyPL", "dailyPLPercent"):
                if field in position:
                    position[field] = _number(position[field], field, nullable=True, minimum=0 if field == "marketPrice" else None)
            positions.append(position)
        value["positions"] = positions
        # The existing account-details topic expects an empty key, not null.
        return self._send(self.account_topic, b"", value)

    def _read(self, order_id):
        row = self.db.execute("SELECT published,pending FROM snapshot_publications WHERE account_id=? AND order_id=?", (self.account, order_id)).fetchone()
        if row is None:
            return None, None
        try:
            published, pending = [validate_order_snapshot(json.loads(item), sequenced=True) if item is not None else None for item in row]
            if published is None and pending is None:
                raise ValueError
            for item in (published, pending):
                if item is not None:
                    self._route(item)
                    if item["order_id"] != order_id:
                        raise ValueError
            if published is None and pending["sequence"] != 0:
                raise ValueError
            if published is not None and pending is not None:
                if (pending["created_at"] != published["created_at"]
                        or _timestamp(pending["updated_at"], "updated_at") < _timestamp(published["updated_at"], "updated_at")
                        or pending["sequence"] not in (published["sequence"], published["sequence"] + 1)
                        or (pending["sequence"] == published["sequence"] and pending != published)):
                    raise ValueError
            return published, pending
        except (ValueError, TypeError, KeyError, PublicationError):
            raise PublicationError("Corrupt publication state; preserve database for sequence recovery") from None

    def _pending(self, snapshot):
        with self.db:
            self.db.execute("INSERT INTO snapshot_publications(account_id,order_id,pending) VALUES (?,?,?) ON CONFLICT(account_id,order_id) DO UPDATE SET pending=excluded.pending", (self.account, snapshot["order_id"], _json(snapshot)))

    def _deliver(self, snapshot):
        key = json.dumps([self.account, snapshot["order_id"]], separators=(",", ":")).encode()
        acknowledgement = self._send(self.order_topic, key, snapshot)
        with self.db:
            cursor = self.db.execute("UPDATE snapshot_publications SET published=?,pending=NULL,acknowledgement=? WHERE account_id=? AND order_id=? AND pending=?", (_json(snapshot), _json(acknowledgement), self.account, snapshot["order_id"], _json(snapshot)))
            if cursor.rowcount != 1:
                raise PublicationError("Publication state changed during acknowledgement; preserve database")
        self._seen.add(snapshot["order_id"])
        if self.delivery_observer is not None:
            try:
                self.delivery_observer(snapshot, acknowledgement)
            except Exception:
                # A telemetry failure must never turn an acknowledged delivery
                # into a publication failure or replay a financial command.
                print(_json({'event': 'monitor_telemetry_error', 'component': 'delivery'}), flush=True)

    def publish_orders(self, snapshots, *, now=None):
        """Retry today's pending messages, then publish observed changes.

        Returns delivered/failed/skipped counts and per-order errors. Delivery
        failures retain the exact pending payload and sequence. Snapshots from
        prior/future calendar dates are retained in the DB if pending but never
        sent unless allow_prior_orders is enabled. Unchanged orders are
        republished once after restart only when republish_on_restart is true.
        Updated observation times alone are suppressed.
        """
        current = datetime.now(timezone.utc) if now is None else now
        if not isinstance(current, datetime) or current.utcoffset() is None:
            raise PublicationError("Publication now must be a timezone-aware datetime")
        if not isinstance(snapshots, list):
            raise PublicationError("Order snapshots must be a list")
        today = current.astimezone(self.zone).date()
        result = {"delivered": 0, "failed": 0, "skipped": 0, "errors": []}
        failed_ids = set()

        def failure(identifier, error):
            result["failed"] += 1
            result["errors"].append({"order_id": identifier, "error_type": type(error).__name__,
                                     "error": str(error) if isinstance(error, PublicationError) else "Snapshot delivery failed; pending payload retained"})
            failed_ids.add(identifier)

        with self._locked():
            pending_ids = [row[0] for row in self.db.execute("SELECT order_id FROM snapshot_publications WHERE account_id=? AND pending IS NOT NULL ORDER BY order_id", (self.account,))]
            for identifier in pending_ids:
                try:
                    _, pending = self._read(identifier)
                    if not self._eligible(pending, today):
                        result["skipped"] += 1
                        continue
                    self._deliver(pending)
                    result["delivered"] += 1
                except Exception as error:
                    failure(identifier, error)
            prepared = {}
            duplicates = set()
            for raw in snapshots:
                identifier = raw.get("order_id") if isinstance(raw, dict) else None
                if not isinstance(identifier, str):
                    identifier = None
                try:
                    snapshot = validate_order_snapshot(raw)
                    self._route(snapshot)
                    identifier = snapshot["order_id"]
                    if not self._eligible(snapshot, today):
                        result["skipped"] += 1
                        continue
                    if identifier in prepared and prepared[identifier] != snapshot:
                        duplicates.add(identifier)
                        raise PublicationError("Conflicting full snapshots for the same order in one refresh")
                    prepared[identifier] = snapshot
                except Exception as error:
                    failure(identifier, error)
            for identifier, snapshot in prepared.items():
                if identifier in failed_ids or identifier in duplicates:
                    continue
                try:
                    previous, pending = self._read(identifier)
                    if pending is not None:
                        # Only prior/future-day pending data can remain here. Do
                        # not overwrite it with a conflicting creation date.
                        raise PublicationError("Existing pending snapshot prevents replacing this order")
                    if previous is not None and previous["created_at"] != snapshot["created_at"]:
                        raise PublicationError("Order created_at changed for an existing order_id")
                    changed = previous is None or _state(previous) != _state(snapshot)
                    if not changed and (not self.republish_on_restart or identifier in self._seen):
                        result["skipped"] += 1
                        continue
                    if changed:
                        sequence = previous["sequence"] + 1 if previous is not None else 0
                        if sequence > MAX_SEQUENCE:
                            raise PublicationError("Order publication sequence exhausted")
                        update = dict(snapshot, sequence=sequence)
                        if previous is not None:
                            update["updated_at"] = _iso(max(_timestamp(previous["updated_at"], "updated_at"), _timestamp(update["updated_at"], "updated_at")))
                    else:
                        update = previous
                    # This commit occurs before send; any crash retries it exactly.
                    self._pending(update)
                    self._deliver(update)
                    result["delivered"] += 1
                except Exception as error:
                    failure(identifier, error)
        return result

    def close(self):
        with self._thread_lock:
            self.db.close()
