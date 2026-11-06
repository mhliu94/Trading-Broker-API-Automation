from datetime import datetime, timezone
import hashlib
import math
import time
from pathlib import Path
from .commands import normalize, validate_age
from .errors import AuthenticationError, BrokerError, CancellationDiscoveryError, CommandError, ConfigurationError, OutcomeUnknown, RequestRejected, UnsupportedOrder
from .journal import Journal
from .locking import operation_lock
from .registry import adapter_for
from .utils import today_window, iso_utc


class Runner:
    def __init__(self, broker, config, execute=False, journal_path=None):
        self.broker, self.config, self.execute = broker, config, execute
        self.adapter = adapter_for(broker, config)
        self.journal = Journal(journal_path) if execute and journal_path else None
        # Stable, explicit account/environment label. Routing metadata and rotated
        # API keys must never create a new deduplication namespace for old orders.
        identity = config.get("journal_account") or "read-only-unconfigured"
        self.scope = broker + ":" + str(identity)
        self.journal_path = Path(journal_path) if journal_path else None
        self.lock_path = self.journal_path.with_suffix("." + hashlib.sha256(self.scope.encode()).hexdigest()[:16] + ".lock") if self.journal_path else None

    def unresolved_timed(self):
        if not self.journal or self.broker == "zhuorui":
            return []
        from .timed_cancel import unresolved
        return unresolved(self.journal, self.scope)

    def trading_enabled(self):
        return self.config.get("trading_enabled") is True and (not self.journal or (
            self.journal.trading_enabled(self.scope) and not self.journal.unresolved(self.scope)
            and not self.unresolved_timed()))

    def recover_pending(self):
        """Recover confirmed cancellation work only; never replay submissions."""
        if not self.execute:
            return {"status": "dry_run", "broker": self.broker, "requests_sent": 0,
                    "orders_replayed": False}
        if not self.journal or not self.lock_path or not self.config.get("journal_account"):
            raise ConfigurationError("Recovery requires the original journal and stable journal_account")
        with operation_lock(self.lock_path, timeout=float(self.config.get("lock_timeout_seconds", 30))):
            if self.broker == "zhuorui":
                data = self.adapter.recover_cancellations()
                outstanding = bool(data.get("unresolved_count") or data.get("pending_cancellations_count"))
                return {"status": "reconciliation_required" if outstanding else "ok",
                        "broker": self.broker, "data": data, "orders_replayed": False}
            from .timed_cancel import recover
            import json
            results = recover(self.adapter, self.broker, self.journal, self.scope)
            waiting = {row["id"] for row in self.journal.unresolved(self.scope)}
            # A crash can occur after the timed workflow persisted terminal
            # evidence but before the outer command result was committed.
            completed = self.journal.db.execute(
                "SELECT command_id,record FROM timed_cancellations WHERE scope=? AND state='complete'",
                (self.scope,)).fetchall()
            for command_id, raw in completed:
                if command_id not in waiting:
                    continue
                record = json.loads(raw)
                outcome = record.get("result", {})
                if outcome.get("terminal_confirmed") and record.get("order_id"):
                    self.journal.reconcile(self.scope, command_id, "found",
                        "Durable timed-cancellation record contains exact broker terminal-state confirmation",
                        record["order_id"])
                elif outcome.get("submission_outcome") == "not_submitted" and not record.get("order_id"):
                    self.journal.reconcile(self.scope, command_id, "not_submitted",
                        "Durable timed workflow contains verified request rejection before acceptance; " + str(outcome.get("message", "")))
            outstanding = self.unresolved_timed()
            return {"status": "reconciliation_required" if outstanding else "ok",
                    "broker": self.broker, "results": results, "unresolved": outstanding,
                    "orders_replayed": False}

    def process(self, payload, message_id=None, kafka_timestamp_ms=None, require_account=False):
        if self.execute and self.lock_path:
            with operation_lock(self.lock_path, timeout=float(self.config.get("lock_timeout_seconds", 30))):
                return self._process(payload, message_id, kafka_timestamp_ms, require_account)
        return self._process(payload, message_id, kafka_timestamp_ms, require_account)

    def reconcile_pending(self):
        """Read exact Bitget order references and persist terminal evidence."""
        if not self.execute or self.broker != "bitget" or not self.journal or not self.lock_path:
            raise ConfigurationError("Automatic reconciliation requires a live Bitget journal")
        from bitget.reconciliation import reconcile
        with operation_lock(self.lock_path, timeout=float(self.config.get("lock_timeout_seconds", 30))):
            return reconcile(self)

    def record_preflight_rejection(self, payload, exc, message_id, kafka_timestamp_ms):
        """Keep addressed, valid order rejections for durable control feedback.

        Called only when process raised before dispatch. Existing journal rows
        (including pending writes and ID conflicts) can never be overwritten.
        """
        if (self.broker != 'bitget' or not self.execute or not self.journal
                or not isinstance(exc, (CommandError, ConfigurationError, UnsupportedOrder))):
            return None
        try:
            command = normalize(payload, self.broker, self.config, message_id, True)
        except BrokerError:
            return None
        if command is None or command.get('action') != 'place_order':
            return None
        from bitget.feedback import command_time
        result = {'id': command['id'], 'broker': self.broker, 'action': 'place_order',
                  'status': 'rejected', 'submission_outcome': 'not_submitted',
                  'requests_sent': 0, 'error_type': type(exc).__name__, 'error': str(exc),
                  'observed_at': iso_utc(datetime.now(timezone.utc)),
                  'feedback_created_at': command_time(payload, kafka_timestamp_ms)}
        with operation_lock(self.lock_path, timeout=float(self.config.get('lock_timeout_seconds', 30))):
            return result if self.journal.reject_unsubmitted(self.scope, command, result) else None

    def _process(self, payload, message_id=None, kafka_timestamp_ms=None, require_account=False):
        command = normalize(payload, self.broker, self.config, message_id, require_account)
        if command is None:
            return {"status": "ignored", "broker": self.broker, "reason": "Different account, server or broker"}
        base = {"id": command["id"], "broker": self.broker, "action": command["action"]}
        action = command["action"]
        mutation = action in {"place_order", "cancel_order", "cancel_open_orders", "set_trading_enabled"}
        journal_order = self.journal is not None and mutation
        if journal_order:
            cached = self.journal.lookup(self.scope, command)
            if cached is not None:
                return {**cached, "duplicate": True}
        if action == "place_order":
            if self.execute and self.journal and (self.journal.unresolved(self.scope) or self.unresolved_timed()):
                raise ConfigurationError("An earlier account operation is unresolved; inspect/reconcile the journal before new submissions")
            validate_age(payload, kafka_timestamp_ms, float(self.config.get("command_max_age_seconds", 120)))
            from .timed_cancel import is_enabled, prepare
            timed_plan = None
            if is_enabled(self.broker, self.config, command):
                _, timed_plan = prepare(self.adapter, self.broker, command)
                plan = timed_plan["request"]
            else:
                plan = self.adapter.build_order(command)
            if not self.execute:
                result = {**base, "status": "dry_run", "request": plan,
                          "auction_intent": command.get("auction_intent"), "requests_sent": 0}
                if timed_plan:
                    result["execution_plan"] = timed_plan
                return result
            if not self.trading_enabled():
                raise ConfigurationError("Trading is disabled in configuration or persistent control state")
            if not self.journal:
                raise ConfigurationError("Live order submissions require a persistent journal")
            if not isinstance(self.config.get("journal_account"), str) or not self.config["journal_account"].strip():
                raise ConfigurationError("Set a stable journal_account identifying this exact broker account and paper/live environment")
        elif mutation:
            validate_age(payload, kafka_timestamp_ms, float(self.config.get("command_max_age_seconds", 120)))
            if not self.execute:
                return {**base, "status": "dry_run", "request": command, "requests_sent": 0}
            if not self.journal or not self.config.get("journal_account"):
                raise ConfigurationError("Controls/cancellations require a persistent journal and stable journal_account")
            if action == "set_trading_enabled" and command["trading_enabled"] and self.config.get("trading_enabled") is not True:
                raise ConfigurationError("Remote enable cannot override trading_enabled=false in local configuration")
        elif action == "capabilities":
            data = self.adapter.capabilities()
            data["runtime"] = {"cancel_order": True, "cancel_open_orders": True,
                "persistent_trading_control": True, "account_order_snapshots": True,
                "fok_policy": self.config.get("fok_policy", "native"),
                "timed_cancel_requires_opt_in": True,
                "timed_cancel_is_native_fok": False}
            if self.broker == "bitget":
                data["runtime"].update(ioc_policy=self.config.get("ioc_policy", "native"),
                    ioc_cancel_after_seconds=self.config.get("ioc_cancel_after_seconds", 0.5),
                    timed_cancel_is_native_ioc=False)
            return {**base, "status": "ok", "data": data}
        elif not self.execute:
            return {**base, "status": "dry_run", "request": command, "requests_sent": 0}
        if journal_order:
            cached = self.journal.reserve(self.scope, command)
            if cached is not None:
                return {**cached, "duplicate": True}
        try:
            if action in {"place_order", "cancel_order", "cancel_open_orders"}:
                delay = float(self.config.get("action_delay_seconds", 0))
                if not math.isfinite(delay) or not 0 <= delay <= 300:
                    raise CommandError("action_delay_seconds must be between zero and 300")
                if delay:
                    time.sleep(delay)
            if mutation:
                validate_age(payload, kafka_timestamp_ms, float(self.config.get("command_max_age_seconds", 120)))
            if action == "place_order":
                # SQLite reservation can wait behind another process. Recheck
                # freshness immediately before dispatching a new submission.
                validate_age(payload, kafka_timestamp_ms, float(self.config.get("command_max_age_seconds", 120)))
                if timed_plan and timed_plan["enabled"]:
                    from .timed_cancel import execute
                    data = execute(self.adapter, self.broker, command, self.journal, self.scope)
                else:
                    data = self.adapter.place_order(command)
            elif action in {"cancel_order", "cancel_open_orders"}:
                from .cancellation import cancel_order, cancel_open_orders
                data = cancel_order(self.adapter, self.broker, command) if action == "cancel_order" else cancel_open_orders(self.adapter, self.broker, command)
            elif action == "set_trading_enabled":
                data = {"trading_enabled": command["trading_enabled"]}
                result = {**base, "status": "ok", "data": data, "observed_at": iso_utc(datetime.now(timezone.utc))}
                self.journal.set_trading_enabled(self.scope, command["id"], command["trading_enabled"], result)
                return result
            elif action == "positions":
                data = self.adapter.positions()
            elif action == "tradability":
                data = self.adapter.tradability(command["symbol"])
            elif action == "borrow_availability":
                data = self.adapter.borrow_availability(command["symbol"])
                if not isinstance(data, dict):
                    raise BrokerError("Unexpected borrow availability response")
                data = dict(data)
                # Common control fields, while retaining broker-specific evidence.
                data.setdefault("available_quantity", data.get("quantity"))
                for field in ("shortable", "easy_to_borrow", "fee_rate", "locate_required"):
                    data.setdefault(field, None)
                data.setdefault("inventory_reserved", False)
                applicable = data.get("support", data.get("status")) != "not_applicable"
                data.setdefault("quantity_unit", "shares" if applicable else None)
            elif action == "orders_today":
                start, end = today_window(self.config.get("timezone", "America/New_York"))
                data = self.adapter.orders_today(start, end)
                base["window"] = {"start": iso_utc(start), "end_exclusive": iso_utc(end), "timezone": self.config.get("timezone", "America/New_York")}
            else:
                raise CommandError("Unknown action")
            result = {**base, "status": "submitted" if action == "place_order" else ("cancel_requested" if action in {"cancel_order", "cancel_open_orders"} else "ok"), "data": data,
                      "observed_at": iso_utc(datetime.now(timezone.utc))}
            if action == "place_order" and timed_plan:
                if data.get("status") == "outcome_unknown":
                    result["status"] = "authentication_required" if data.get("authentication_error") else "unknown"
                    result["submission_outcome"] = "unknown"
                elif not data.get("terminal_confirmed"):
                    result["status"] = "unknown"
            if action in {"cancel_order", "cancel_open_orders"}:
                result["status"] = {"cancel_requested":"cancel_requested", "no_action":"ok", "cancel_rejected":"rejected", "outcome_unknown":"unknown"}.get(data.get("status"), "unknown")
            if command.get("auction_intent"):
                result["auction_intent"] = command["auction_intent"]
                result["auction_participation_confirmed"] = False
                result["note"] = "Ordinary limit DAY submitted. Broker acknowledgement is not confirmation of routing to or filling in a LULD auction."
        except Exception as exc:
            # A failed write can be ambiguous, including adapter decoding failures.
            # Never automatically retry any recorded submission attempt.
            result = {**base, "status": "rejected" if isinstance(exc, (CommandError, CancellationDiscoveryError)) else ("unknown" if mutation else "error"),
                      "error_type": type(exc).__name__,
                      "error": str(exc) if isinstance(exc, BrokerError) else "Broker operation failed; inspect local configuration and reconcile before retrying"}
            if isinstance(exc, CancellationDiscoveryError):
                result.update(submission_outcome="not_submitted", requests_sent=0)
            if isinstance(exc, RequestRejected) and mutation:
                result.update(status="rejected", submission_outcome="not_submitted", requests_sent=exc.requests_sent,
                              broker_error_code=exc.code, http_status=exc.http_status)
            if isinstance(exc,AuthenticationError):
                result["status"]="authentication_required"
                if action=="place_order" and exc.no_submission and self.journal:
                    self.journal.release_unaccepted_auth(self.scope,command["id"])
                    result["submission_outcome"]="not_submitted"
                    journal_order=False
                elif mutation:
                    result["submission_outcome"]="unknown"
        if journal_order:
            if self.broker == 'bitget' and action == 'place_order':
                from bitget.feedback import command_time
                result['feedback_created_at'] = command_time(payload, kafka_timestamp_ms)
                result.setdefault('observed_at', iso_utc(datetime.now(timezone.utc)))
            self.journal.finish(self.scope, command["id"], result)
        return result

    def close(self):
        if self.journal:
            self.journal.close()
