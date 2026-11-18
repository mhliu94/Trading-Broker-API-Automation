"""Worker-observed UTC epoch milliseconds and monotonic stage durations."""
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import hashlib
import json
import logging
import time

_log = logging.Logger("broker.latency", level=logging.INFO)
_log.addHandler(logging.StreamHandler())
_current = ContextVar("command_timing_id", default=None)


def now_ms():
    return time.time_ns() // 1_000_000


def _tag(value):
    if not isinstance(value, (str, int)):
        return None
    return hashlib.sha256(str(value).encode()).hexdigest()[:12]


def _emit(stage, **fields):
    # Observability must never affect submission, cancellation or acknowledgement.
    try:
        _log.info("%s", json.dumps({"event": "broker_latency", "stage": stage, **fields},
                                   separators=(",", ":"), allow_nan=False))
    except Exception:
        pass


def received(command, message, at_ms):
    try:
        stamp = getattr(message, "timestamp", None)
        if callable(stamp):
            stamp = stamp()[1]
        stamp = stamp if isinstance(stamp, int) and stamp >= 0 else None
        _emit("command_received", command_tag=_tag(command.get("command_id") or command.get("id")),
              received_ms=at_ms, kafka_record_ms=stamp, pickup_ms=at_ms-stamp if stamp is not None else None)
    except Exception:
        pass


@contextmanager
def span(stage, operation, record=None):
    reference = _current.get()
    ticket = None
    if isinstance(record, dict):
        reference = _tag(record.get("command_id") or record.get("id")) or reference
        ticket = _tag(record.get("order_id") or record.get("order_reference"))
    token = _current.set(reference)
    start_ms, start_ns = now_ms(), time.perf_counter_ns()
    outcome = "ok"
    try:
        yield
    except BaseException as exc:
        outcome = type(exc).__name__
        raise
    finally:
        end_ns = time.perf_counter_ns()
        _current.reset(token)
        _emit(stage, operation=operation, command_tag=reference, ticket_tag=ticket,
              start_ms=start_ms, end_ms=now_ms(), elapsed_ms=round((end_ns-start_ns)/1_000_000, 3),
              outcome=outcome)


def timed(stage, operation, *, arg=None, payload=None):
    """Instrument a broker request or a report-delivery boundary; never log its contents."""
    def decorate(method):
        @wraps(method)
        def measured(*args, **kwargs):
            name, record = operation, None
            if arg is not None and len(args) > arg and isinstance(args[arg], str):
                value = args[arg].split("?", 1)[0]
                if len(value) < 100 and all(c.isalnum() or c in "/_-" for c in value):
                    name = value
            if payload is not None and len(args) > payload:
                record = args[payload]
                if isinstance(record, str):
                    try:
                        record = json.loads(record)
                    except (TypeError, ValueError):
                        record = None
                if isinstance(record, dict) and isinstance(record.get("pending"), dict):
                    record = record["pending"]
            with span(stage, name, record):
                return method(*args, **kwargs)
        return measured
    return decorate
