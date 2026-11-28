"""Isolated integration worker. All upstream imports resolve to the configured checkout."""
from __future__ import annotations

import contextlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import io
import json
import re
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo


def session_authentication_error(error):
    """Recognize only the pinned runtime's explicit session-denial classes."""
    return (type(error).__module__ == "zhuorui.api.errors" and
            type(error).__name__ in {"SessionExpired", "LoggedInElsewhere"})


def verify_checkout(repo: Path, expected: str):
    revision = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"],
                              capture_output=True, text=True, check=True, timeout=10).stdout.strip()
    if revision != expected:
        raise ValueError("Zhuorui checkout differs from the pinned commit; see the setup instructions")
    clean = subprocess.run(["git", "-C", str(repo), "diff", "--quiet", "HEAD", "--", "zhuorui"],
                           capture_output=True, check=False, timeout=10)
    if clean.returncode:
        raise ValueError("Zhuorui runtime has tracked changes; use a clean pinned checkout")


def _rows(response):
    if not isinstance(response, dict) or response.get("code") != "000000":
        raise ValueError("Zhuorui order read did not succeed")
    rows = response.get("data")
    if rows is None:
        return []
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("Zhuorui order response is not a complete record list")
    return rows


def is_us_stock_row(row):
    """The pinned account reads are unfiltered; ts must explicitly identify US."""
    if not isinstance(row, dict) or not row.get("ts"):
        raise ValueError("Zhuorui record omitted its market; US stock scope is unknown")
    if row["ts"] != "US":
        return False
    for key in ("asset_class", "instrument_type", "security_type"):
        if row.get(key) is not None and str(row[key]).lower() not in {"us_equity", "equity", "stock"}:
            return False
    if not row.get("code"):
        raise ValueError("Zhuorui record omitted its stock code")
    symbol = str(row["code"]).upper()
    if symbol.endswith((".US", ".HK", ".SG", ".SH", ".SS", ".SZ", ".JP", ".T", ".TO", ".L", ".AX", ".DE", ".PA", ".SI", ".KS", ".KQ", ".TW")):
        return False
    return bool(re.fullmatch(r"[A-Z]{1,6}(?:[.-][A-Z]{1,2})?", symbol))


def stock_holdings(response):
    if not isinstance(response, dict) or response.get("code") != "000000":
        raise ValueError("Zhuorui holdings read did not succeed")
    data = response.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("holdList"), list):
        raise ValueError("Zhuorui holdings response omitted holdList")
    return {**response, "data": {**data, "holdList": [row for row in data["holdList"] if is_us_stock_row(row)]}}


def read_orders(client, start: datetime, end: datetime):
    if start.tzinfo is None or end.tzinfo is None or start >= end:
        raise ValueError("Order window needs increasing timezone-aware boundaries")
    eastern = ZoneInfo("America/New_York")
    day = start.astimezone(eastern).date()
    last = (end - timedelta(microseconds=1)).astimezone(eastern).date()
    batches = [_rows(client.query("orders"))]
    while day <= last:
        batches.append(_rows(client.query_orders_for_date(day)))
        day += timedelta(days=1)
    result = {}
    for batch in batches:
        for row in batch:
            if not is_us_stock_row(row):
                continue
            stamp, reference = row.get("entrustTime"), row.get("orderTxnReference")
            if isinstance(stamp, bool) or not isinstance(stamp, (str, int, Decimal)) or not reference:
                raise ValueError("Zhuorui order omitted identity or creation time")
            created = datetime.fromtimestamp(int(stamp) / 1000, timezone.utc)
            if start <= created < end:
                result[str(reference)] = row
    return sorted(result.values(), key=lambda row: (int(row["entrustTime"]), str(row["orderTxnReference"])))


def read_tradability(client, symbol: str, settings, runtime):
    """New read-only symbol diagnostic, with unknown account/auction eligibility explicit."""
    quote = client._request(runtime.QUOTE_PATH, {"stockVos": [{"code": symbol, "ts": "US"}]})
    rows = quote.get("data")
    if not isinstance(rows, list):
        raise ValueError("Zhuorui did not return a symbol quote list")
    matches = [row for row in rows if isinstance(row, dict) and row.get("code") == symbol and row.get("ts") == "US"]
    if len(matches) > 1:
        raise ValueError("Zhuorui quote does not uniquely identify the symbol")
    row = matches[0] if matches else None
    suspension = row.get("suspension") if row else None
    # Upstream recognizes only 1 and 3 as active. No other enum is guessed.
    stamp = row.get("time") if row else None
    fresh = (type(stamp) is int and -5 <= client.now() - stamp / 1000 <= settings.quote_max_age_seconds
             and row.get("delay") is False)
    active = True if fresh and type(suspension) is int and suspension in (1, 3) else None
    started = client.now()
    status = client._request(runtime.MARKET_STATUS_PATH, {})
    session_name = None
    session_reason = None
    try:
        session_name = runtime.parse_session(status, started=started, now=client.now(),
                                            max_age=settings.quote_max_age_seconds).name
    except runtime.ApiError as exc:
        if session_authentication_error(exc):
            raise
        session_reason = "Session is closed, stale, halted, or otherwise unverified"
    return {"symbol": symbol, "found": bool(matches), "tradable": None, "account_tradable": None,
            "halted": False if active else None, "quote_reports_active": active,
            "quote_is_fresh_realtime": bool(fresh), "suspension_code": suspension,
            "market_session": session_name, "session_reason": session_reason,
            "luld_applicable": True, "luld_auction_participation": None,
            "reason": "A quote proves symbol visibility only; account trade permission, halt cause, and auction routing are not established",
            "quote": row}


class NativeMarketClient:
    def __init__(self, client, allow_synthetic, error_type):
        self.client, self.allow_synthetic, self.error_type = client, allow_synthetic, error_type

    def __getattr__(self, name):
        return getattr(self.client, name)

    def query(self, name):
        response = self.client.query(name)
        if name == "orders":
            return {**response, "data": [row for row in _rows(response) if is_us_stock_row(row)]}
        if name == "holdings":
            return stock_holdings(response)
        return response

    def query_order_detail(self, *args, **kwargs):
        response = self.client.query_order_detail(*args, **kwargs)
        if not isinstance(response, dict) or response.get("code") != "000000" or not is_us_stock_row(response.get("data")):
            raise self.error_type("Zhuorui order detail is not a verified US stock")
        return response

    def prepare_market_order(self, *args, **kwargs):
        prepared = self.client.prepare_market_order(*args, **kwargs)
        if prepared[0] != "market" and not self.allow_synthetic:
            raise self.error_type("Native Market is unavailable in this session; send an explicit limit or enable synthetic extended-hours market routing")
        return prepared


def execute_order(request, config, settings, client, runtime):
    listener_settings = runtime.load_listener_settings(request["config"], config)
    path = listener_settings.journal_file
    reports = []
    def emit(command, status, message, **details):
        reports.append({"status": status, "message": message, **details})
    with runtime.InstanceLock(path.with_suffix(path.suffix + ".lock")):
        journal = runtime.CommandJournal(path, runtime.binding(config))
        try:
            guarded = NativeMarketClient(client, request.get("allow_synthetic_extended_hours_market", False), runtime.ApiError)
            executor = runtime.CommandExecutor(listener_settings, settings, journal, lambda: guarded,
                                                SimpleNamespace(request=lambda *a, **kw: None), emit, config=config)
            if request["operation"] == "recover_cancellations":
                executor.recover_pending_cancellations()
                return {"reports": reports, "unresolved_count": len(journal.unresolved()),
                        "pending_cancellations_count": len(journal.pending_cancellations()), "orders_replayed": False}
            if journal.pending_cancellations():
                raise runtime.ApiError("Earlier timed cancellation requires recovery; run recover_cancellations.py before another order")
            payload = request["payload"]
            if request["operation"] in ("cancel_order", "cancel_open_orders"):
                command = runtime.CancelCommand(command_id=payload["command_id"],
                    order_reference=payload.get("order_id"),
                    cancel_all=request["operation"] == "cancel_open_orders")
                executor.execute(command)
                if not reports:
                    raise runtime.ApiError("Zhuorui cancellation returned no outcome; inspect its journal")
                return {**reports[-1], "command_id": command.command_id,
                        "cancellation_confirmed": False,
                        "scope": "Configured account's US current orders; upstream validates cancellable states"}
            command = runtime.TradingCommand(command_id=payload["command_id"], symbol=payload["symbol"],
                side=payload["side"], quantity=payload["quantity"], order_type=payload["order_type"],
                limit_price=Decimal(payload["limit_price"]) if payload.get("limit_price") else None,
                allow_pre_post=payload["allow_pre_post"])
            executor.execute(command)
            if not reports:
                raise runtime.ApiError("Zhuorui execution returned no outcome; inspect its journal")
            return {**reports[-1], "command_id": command.command_id, "native_fok": False,
                    "execution_semantics": payload["execution_semantics"],
                    "partial_fills_possible": payload["timed_cancel"]}
        finally:
            journal.close()


def collect_order_snapshots(config, client, payload, order_mapper, detail_mapper, now=None):
    """Isolate record mapping/detail failures without masking failed list reads."""
    if "report_errors" in payload and not isinstance(payload["report_errors"], bool):
        raise ValueError("report_errors must be boolean")
    report_errors = payload.get("report_errors", False)
    now = now or datetime.now(timezone.utc)
    rows = read_orders(client, datetime.fromisoformat(payload["start"].replace("Z", "+00:00")),
                       datetime.fromisoformat(payload["end"].replace("Z", "+00:00")))
    snapshots, errors = [], []
    for raw in rows:
        try:
            mapped = order_mapper(config, {"code": "000000", "data": [raw]}, now=now)
            complete = []
            for snapshot in mapped:
                if str(snapshot["order_id"]) != str(raw["orderTxnReference"]):
                    raise ValueError("Zhuorui snapshot identity differs from its source order")
                if snapshot["filled_quantity"] > 0 and snapshot["average_fill_price"] is None:
                    detail = client.query_order_detail(snapshot["order_id"], raw["entrustTime"])
                    snapshot = detail_mapper(config, detail, snapshot, now=now)
                complete.append(snapshot)
            snapshots.extend(complete)
        except Exception as exc:
            if session_authentication_error(exc):
                raise
            if not report_errors:
                raise
            identity = raw.get("orderTxnReference") if isinstance(raw, dict) else None
            # The external runtime/transport exception can embed private
            # response data; return a diagnostic category without its text.
            errors.append({"order_id": str(identity) if isinstance(identity, (str, int)) and not isinstance(identity, bool) else None,
                           "error_type": type(exc).__name__,
                           "error": "Zhuorui order snapshot mapping or detail lookup failed"})
    return {"orders": snapshots, "errors": errors} if report_errors else snapshots


def run(request):
    repo = Path(request["repo"]).resolve()
    verify_checkout(repo, request["expected_commit"])
    sys.path.insert(0, str(repo))
    from zhuorui.api.config import load_settings
    from zhuorui.api.session import load_session, binding
    from zhuorui.api.client import ApiClient, QUOTE_PATH
    from zhuorui.api.errors import ApiError
    from zhuorui.api.market_data import MARKET_STATUS_PATH, parse_session
    from zhuorui.api.commands import TradingCommand, CancelCommand
    from zhuorui.api.execution import CommandExecutor
    from zhuorui.api.journal import CommandJournal, InstanceLock
    from zhuorui.api.listener_config import load_listener_settings
    runtime = SimpleNamespace(**{name: value for name, value in locals().copy().items() if name != "runtime"})
    config, settings = load_settings(request["config"])
    actual_binding = binding(config)
    for key, value in request.get("expected_binding", {}).items():
        if str(actual_binding.get(key)) != str(value):
            raise ApiError("Zhuorui upstream account/server selectors differ from the shared broker config")
    client = ApiClient(load_session(settings.session_file, config), settings)
    operation, payload = request["operation"], request.get("payload", {})
    if operation == "working_order_dates":
        return [[str(row["orderTxnReference"]), int(row["entrustTime"])] for row in _rows(client.query("orders"))
                if is_us_stock_row(row) and row.get("orderTxnReference") and row.get("entrustTime")]
    if operation == "account_snapshot":
        from zhuorui.api.snapshot import account_snapshot
        return account_snapshot(config, stock_holdings(client.query("holdings")), client.query("cash"), client.query("account"))
    if operation == "order_snapshots":
        from zhuorui.api.order_snapshot import order_snapshots, order_detail_snapshot
        return collect_order_snapshots(config, client, payload, order_snapshots, order_detail_snapshot)
    if operation == "positions":
        response = stock_holdings(client.query("holdings"))
        data = response.get("data")
        if not isinstance(data, dict) or not isinstance(data.get("holdList"), list):
            raise ApiError("Zhuorui holdings response omitted holdList; no empty portfolio inferred")
        return data["holdList"]
    if operation == "orders_today":
        return read_orders(client, datetime.fromisoformat(payload["start"].replace("Z", "+00:00")),
                           datetime.fromisoformat(payload["end"].replace("Z", "+00:00")))
    if operation == "tradability":
        return read_tradability(client, payload["symbol"], settings, runtime)
    if operation in ("place_order", "recover_cancellations", "cancel_order", "cancel_open_orders"):
        return execute_order(request, config, settings, client, runtime)
    raise ApiError("Unsupported Zhuorui bridge action")


def main():
    operation = None
    try:
        request = json.load(sys.stdin)
        operation = request.get("operation")
        # Upstream diagnostics cannot interleave with the machine-readable protocol.
        with contextlib.redirect_stdout(io.StringIO()):
            result = run(request)
        response = {"ok": True, "result": result}
        exit_code = 0
    except Exception as exc:
        safe = type(exc).__module__.startswith("zhuorui.") or isinstance(exc, ValueError)
        message = str(exc) if safe else "Zhuorui bridge setup or execution failed; verify pinned checkout, dependencies, Windows session, and journal"
        # Executions can catch session failures after submission or during
        # timed cancellation. No mutation retry guarantee is inferred here.
        auth_failure = session_authentication_error(exc) and operation in {
            "positions", "orders_today", "tradability", "account_snapshot", "order_snapshots", "working_order_dates"
        }
        if auth_failure:
            message = "Zhuorui session expired or was displaced; refresh the session before continuing"
        response = {"ok": False, "error": message,
                    "authentication_error": auth_failure, "no_submission": auth_failure,
                    "outcome_unknown": type(exc).__name__ == "OrderOutcomeUnknown" or
                    (operation in ("place_order", "cancel_order", "cancel_open_orders") and not safe)}
        exit_code = 2
    print(json.dumps(response, default=str, allow_nan=False))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
