"""Durable local duplicate suppression; ambiguous submissions are never replayed."""
import hashlib
import json
from pathlib import Path
import sqlite3
from .errors import CommandError, OutcomeUnknown


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)


class Journal:
    def __init__(self, path):
        path = Path(path).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS commands (scope TEXT, id TEXT, fingerprint TEXT NOT NULL, state TEXT NOT NULL, result TEXT, PRIMARY KEY(scope,id))")
        self.db.execute("CREATE TABLE IF NOT EXISTS settings (scope TEXT PRIMARY KEY, enabled INTEGER NOT NULL CHECK(enabled IN (0,1)))")
        if "command" not in {row[1] for row in self.db.execute("PRAGMA table_info(commands)")}:
            self.db.execute("ALTER TABLE commands ADD COLUMN command TEXT")
        self.db.commit()

    def trading_enabled(self, scope):
        row = self.db.execute("SELECT enabled FROM settings WHERE scope=?", (scope,)).fetchone()
        return True if row is None else bool(row[0])

    def set_trading_enabled(self, scope, command_id, enabled, result):
        if not isinstance(enabled, bool):
            raise CommandError("Trading state must be boolean")
        # The state change and command result become durable in one transaction;
        # replaying an old enable must never undo a subsequent disable.
        with self.db:
            self.db.execute("INSERT INTO settings VALUES (?,?) ON CONFLICT(scope) DO UPDATE SET enabled=excluded.enabled", (scope, int(enabled)))
            self.db.execute("UPDATE commands SET state=?,result=? WHERE scope=? AND id=?",
                            (result["status"], encoded(result), scope, command_id))

    def lookup(self, scope, command):
        fingerprint = hashlib.sha256(encoded(command).encode()).hexdigest()
        row = self.db.execute("SELECT fingerprint,state,result FROM commands WHERE scope=? AND id=?", (scope, command["id"])).fetchone()
        if row is None:
            return None
        if row[0] != fingerprint:
            raise CommandError("Command ID was already used with a different payload")
        if row[1] == "pending":
            raise OutcomeUnknown("Previous dispatch did not finish; reconcile broker orders before any new submission")
        return json.loads(row[2])

    def reserve(self, scope, command):
        fingerprint = hashlib.sha256(encoded(command).encode()).hexdigest()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("SELECT fingerprint,state,result FROM commands WHERE scope=? AND id=?", (scope, command["id"])).fetchone()
            if row:
                if row[0] != fingerprint:
                    raise CommandError("Command ID was already used with a different payload")
                if row[1] == "pending":
                    raise OutcomeUnknown("Previous dispatch did not finish; reconcile broker orders before any new submission")
                return json.loads(row[2])
            self.db.execute("INSERT INTO commands(scope,id,fingerprint,state,result,command) VALUES (?,?,?,'pending',NULL,?)", (scope, command["id"], fingerprint, encoded(command)))
            return None
        finally:
            self.db.commit()

    def finish(self, scope, command_id, result):
        self.db.execute("UPDATE commands SET state=?,result=? WHERE scope=? AND id=?",
                        ("unknown" if result.get("submission_outcome")=="unknown" else result["status"], encoded(result), scope, command_id))
        self.db.commit()

    def reject_unsubmitted(self, scope, command, result):
        """Atomically cache a pre-dispatch rejection without a pending window.

        Never replace an existing reservation or outcome, even on ID conflicts.
        """
        fingerprint = hashlib.sha256(encoded(command).encode()).hexdigest()
        with self.db:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO commands(scope,id,fingerprint,state,result,command) "
                "VALUES (?,?,?,'rejected',?,?)",
                (scope, command['id'], fingerprint, encoded(result), encoded(command)))
        return cursor.rowcount == 1

    def release_unaccepted_auth(self, scope, command_id):
        """Only after explicit transport evidence that no submission was accepted."""
        self.db.execute("DELETE FROM commands WHERE scope=? AND id=? AND state='pending'",(scope,command_id))
        self.db.commit()

    def unresolved(self, scope):
        rows=self.db.execute("SELECT id,state,command,result FROM commands WHERE scope=? AND state IN ('pending','unknown') ORDER BY id",(scope,))
        return [{"id":row[0],"state":row[1],"command":json.loads(row[2]) if row[2] else None,
                 "result":json.loads(row[3]) if row[3] else None} for row in rows]

    def reconcile(self, scope, command_id, outcome, evidence, broker_order_id=None):
        from datetime import datetime,timezone
        if outcome not in {"not_submitted","found","canceled","filled","rejected"}:
            raise CommandError("Invalid reconciliation outcome")
        if not isinstance(evidence,str) or not evidence.strip():
            raise CommandError("Record the broker evidence reviewed before clearing an uncertain action")
        if outcome in {"found","canceled","filled"} and (not isinstance(broker_order_id,str) or not broker_order_id.strip()):
            raise CommandError("A confirmed broker order ID is required for this outcome")
        with self.db:
            row=self.db.execute("SELECT state,result FROM commands WHERE scope=? AND id=?",(scope,command_id)).fetchone()
            if row is None or row[0] not in {"pending","unknown"}:
                raise CommandError("Only an existing pending/unknown command can be reconciled")
            result={"id":command_id,"status":"reconciled","outcome":outcome,"evidence":evidence,
                    "broker_order_id":broker_order_id,"reconciled_at":datetime.now(timezone.utc).isoformat(),
                    "previous_result":json.loads(row[1]) if row[1] else None,"orders_replayed":False}
            has_timed = self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='timed_cancellations'").fetchone()
            timed = self.db.execute("SELECT state,record FROM timed_cancellations WHERE scope=? AND command_id=?", (scope, command_id)).fetchone() if has_timed else None
            if timed:
                record = json.loads(timed[1])
                known_id = record.get("order_id")
                if known_id and broker_order_id and known_id != broker_order_id:
                    raise CommandError("Reconciliation cannot replace a persisted broker order identity")
                if known_id and outcome == "not_submitted":
                    raise CommandError("A confirmed broker reference cannot be reconciled as not_submitted")
                record["manual_reconciliation"] = dict(result)
                if outcome == "found":
                    record["order_id"] = broker_order_id
                    # Only a never-cancelled, previously unidentified submission
                    # may acquire a cancellation target. An ambiguous prior
                    # cancellation is never reset into a new write attempt.
                    state = "cancel_pending" if timed[0] in {"submission_pending", "submission_unknown"} else timed[0]
                else:
                    state = "complete"
                    record["result"] = {"status": "manual_reconciled", "command_id": command_id,
                        "terminal_confirmed": outcome != "not_submitted", "order_id": known_id or broker_order_id,
                        "orders_replayed": False, "manual_evidence": evidence, "outcome": outcome}
                record["state"] = state
                self.db.execute("UPDATE timed_cancellations SET state=?,record=? WHERE scope=? AND command_id=?", (state, encoded(record), scope, command_id))
                result["timed_cancellation_state"] = state
            self.db.execute("UPDATE commands SET state='reconciled',result=? WHERE scope=? AND id=?",(encoded(result),scope,command_id))
            return result

    def close(self):
        self.db.close()
