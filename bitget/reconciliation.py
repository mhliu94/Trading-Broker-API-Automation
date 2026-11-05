"""Resolve uncertain outcomes from exact order reads, without repeating writes."""
import json
from datetime import datetime, timezone

from bitget.adapter import TERMINAL
from common.errors import AuthenticationError, BrokerError
from common.journal import encoded
from common.utils import iso_utc


def _outcome(states):
    if all(state in {"Canceled", "PartialWithdrawal"} for state in states):
        return "canceled"
    if all(state == "Filled" for state in states):
        return "filled"
    if all(state == "Rejected" for state in states):
        return "rejected"
    return "found"


def reconcile(runner):
    journal, scope = runner.journal, runner.scope
    waiting = {item["id"]: item for item in journal.unresolved(scope)}
    timed = {}
    if journal.db.execute("SELECT 1 FROM sqlite_master WHERE name='timed_cancellations'").fetchone():
        timed = {command_id: json.loads(raw) for command_id, raw in journal.db.execute(
            "SELECT command_id,record FROM timed_cancellations WHERE scope=? "
            "AND state NOT IN ('complete','authentication_rejected')", (scope,))}
    resolved, pending = [], []
    for command_id in dict.fromkeys([*waiting, *timed]):
        item = waiting.get(command_id, {})
        command = item.get("command") or {}
        record = timed.get(command_id)
        if record and record.get("broker") != "bitget":
            pending.append({"command_id": command_id, "reason": "timed_ticket_broker_mismatch"})
            continue
        targets = []
        if record and record.get("order_id"):
            targets = [{"order_id": record["order_id"]}]
        elif not record and command.get("action") in {"cancel_order", "cancel_open_orders"}:
            requests = (item.get("result") or {}).get("data", {}).get("requests", [])
            targets = [target for target in requests
                       if target.get("status") in {"outcome_unknown", "cancel_requested"}]
            if not targets and command.get("action") == "cancel_order" and command.get("order_id"):
                targets = [{"order_id": command["order_id"], "symbol": command.get("symbol")}]
        observations = []
        try:
            for target in targets:
                if record:
                    from common.timed_cancel import _detail
                    row, state = _detail(runner.adapter, "bitget", record)
                else:
                    row = runner.adapter.stock_order_detail(target["order_id"])
                    if row.get("orderId") != target["order_id"]:
                        raise BrokerError("Reconciliation order identity differs from the cancellation target")
                    state = row["status"]
                    symbol = target.get("symbol") or command.get("symbol")
                    if symbol and runner.adapter.stock_symbol(symbol) != row["symbol"]:
                        raise BrokerError("Reconciliation order symbol differs from the cancellation target")
                observations.append({"order_id": target["order_id"], "symbol": row["symbol"],
                                     "status": state, "executed_quantity": row.get("executedQuantity")})
            if not observations or any(row["status"] not in TERMINAL for row in observations):
                pending.append({"command_id": command_id, "orders": observations,
                                "reason": "awaiting_terminal_status" if observations else "no_confirmed_order_reference"})
                continue
        except AuthenticationError:
            raise
        except Exception as exc:
            pending.append({"command_id": command_id, "orders": observations,
                            "error_type": type(exc).__name__,
                            "error": str(exc) if isinstance(exc, BrokerError) else "Order status read failed"})
            continue
        evidence = {"source": "Stock+ exact order detail", "observed_at": iso_utc(datetime.now(timezone.utc)),
                    "orders": observations, "writes_sent": 0}
        unrequested = ((item.get("result") or {}).get("data") or {}).get("not_requested_order_ids", [])
        if unrequested:
            evidence["not_requested_order_ids"] = unrequested
        if record:
            from common.timed_cancel import _save, _result
            state = observations[0]["status"]
            record["result"] = _result(record, "no_action", terminal_confirmed=True,
                terminal_state=state, cancellation_confirmed=state in {"Canceled", "PartialWithdrawal"}, order=row)
            _save(journal, scope, record, "complete")
        if command_id in waiting:
            journal.reconcile(scope, command_id, "found" if unrequested else _outcome([row["status"] for row in observations]),
                              encoded(evidence), observations[0]["order_id"])
        resolved.append({"command_id": command_id, **evidence})
    return {"status": "pending" if pending else "ok", "resolved": resolved,
            "pending": pending, "orders_replayed": False, "writes_sent": 0}
