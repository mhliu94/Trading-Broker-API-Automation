"""Reference-runner account/order refreshes independent of financial commands."""
from datetime import datetime, timezone
from copy import copy
from contextlib import nullcontext, closing
import sqlite3
import time
from .holdings_timer import HoldingsTimer, PositionProjection, TERMINAL
from .errors import AuthenticationError, BrokerError, ConfigurationError
from .locking import operation_lock
from .snapshots import collect_account, collect_orders
from .utils import today_window, iso_utc, parse_time


class RuntimeSnapshots:
    def __init__(self, runner, publisher):
        self.runner, self.publisher = runner, publisher
        self.positions = PositionProjection(publisher.publish_account)
        self.timers = []
        self.prior_orders = {}
        self.discover_at = 0

    def _older_orders(self, runner, zone, start, errors):
        """Track GTC tickets by their creation window until a broker terminal event."""
        if time.monotonic() >= self.discover_at:
            self.discover_at = time.monotonic() + 30
            try:
                if runner.broker == "zhuorui":
                    open_ids = runner.adapter._bridge("working_order_dates")
                else:
                    from .cancellation import _discover_open_orders
                    open_ids = []
                    for raw in _discover_open_orders(runner.adapter, runner.broker, include_pending=True):
                        stamp = next((raw.get(k) for k in ("submittedAt", "submitted_at", "OpenedDateTime",
                                     "created_at", "place_time_at", "place_time") if raw.get(k)), None)
                        open_ids.append((raw["order_id"], stamp))
                for identity, stamp in open_ids:
                    created = parse_time(stamp)
                    if created < start:
                        self.prior_orders[str(identity)] = created
            except AuthenticationError:
                raise
            except Exception as error:
                errors.append({"component":"working_orders","error_type":type(error).__name__})
        for row in self.positions.working_orders():
            if row.get("created_at") and parse_time(row["created_at"]) < start:
                self.prior_orders[row["order_id"]] = parse_time(row["created_at"])
        days = {today_window(zone, created) for created in self.prior_orders.values()}
        found = []
        for lower, upper in sorted(days):
            try:
                observed = collect_orders(runner.adapter, runner.broker, lower, upper, errors=errors)
            except AuthenticationError:
                raise
            except Exception as error:
                errors.append({"component":"prior_order_history","error_type":type(error).__name__})
                continue
            for row in observed:
                if row["order_id"] in self.prior_orders:
                    found.append(row)
                    if row["status"] in TERMINAL:
                        self.prior_orders.pop(row["order_id"])
        return found

    def _guard(self):
        if self.runner.lock_path:
            return operation_lock(self.runner.lock_path, float(self.runner.config.get("lock_timeout_seconds", 30)))
        return nullcontext()

    def _report_runner(self, db):
        runner = copy(self.runner)
        if db is not None:
            runner.journal = copy(self.runner.journal)
            runner.journal.db = db
        return runner

    def _connection(self):
        if self.runner.journal_path and self.runner.journal:
            return closing(sqlite3.connect(self.runner.journal_path, timeout=5))
        return nullcontext(None)

    def start(self, interval, error):
        self.positions.observe(self.publisher.known_orders())
        self.publisher.allow_prior_orders = True
        def account_tick():
            token = self.positions.begin()
            with self._connection() as db, self._guard():
                runner = self._report_runner(db)
                account = collect_account(runner.adapter, runner.broker)
                routing = self._routing()
                for key, value in routing.items():
                    if key in account and account[key] != value:
                        raise BrokerError("Snapshot account routing mismatch")
                account.update(routing, trading_enabled=runner.trading_enabled(), ts=iso_utc(datetime.now(timezone.utc)))
            self.positions.checkpoint(token, account)

        def order_tick():
            with self._connection() as db:
                report = self._refresh("order_poll", runner=self._report_runner(db), account_read=False)
            for item in report.get("errors", []):
                if item.get("error_type") == "AuthenticationError":
                    raise AuthenticationError("Broker order poll requires authentication")

        self.timers = [HoldingsTimer(account_tick, interval, error), HoldingsTimer(order_tick, 1, error)]
        self.timers[0].start(wait_initial=True)
        self.timers[1].start()

    def close(self):
        for timer in self.timers:
            timer.stop.set()
        for timer in self.timers:
            timer.close()

    def _routing(self):
        routing = self.runner.config.get("routing", {})
        account = routing.get("account_id")
        if not isinstance(account,str) or not account.strip():
            raise ConfigurationError("Snapshot publication requires routing.account_id matching KTrader")
        result={"account_id":account}
        if routing.get("account_num_id") is not None:result["account_num_id"]=routing["account_num_id"]
        return result

    def _refresh(self, trigger, command_id=None, *, runner=None, account_read=True):
        runner=runner or self.runner; routing=self._routing()
        report={"event":"snapshot_refresh","broker":runner.broker,"trigger":trigger,"command_id":command_id,"errors":[]}
        if account_read:
            try:
                account=collect_account(runner.adapter,runner.broker)
                for key,value in routing.items():
                    if key in account and account[key]!=value:raise BrokerError("Account snapshot routing mismatch")
                account.update(routing)
                account["trading_enabled"]=runner.trading_enabled()
                account["ts"]=iso_utc(datetime.now(timezone.utc))
                self.publisher.publish_account(account)
                report["account_published"]=True
            except Exception as exc:
                report["account_published"]=False
                report["errors"].append({"component":"account","error_type":type(exc).__name__,"error":str(exc) if isinstance(exc,BrokerError) else "Account snapshot failed"})
        rows=[]
        try:
            zone=runner.config.get("timezone","America/New_York")
            start,end=today_window(zone)
            mapping_errors=[]
            with (self._guard() if not account_read else nullcontext()):
                orders=collect_orders(runner.adapter,runner.broker,start,end,errors=mapping_errors)
                if not account_read:
                    orders.extend(self._older_orders(runner,zone,start,mapping_errors))
            report["errors"].extend({"component":"order_mapping",**error} for error in mapping_errors)
            if today_window(zone)!=(start,end):raise BrokerError("Order query crossed local midnight; retrying on next refresh")
            for order in orders:
                for key,value in routing.items():
                    if key in order and order[key]!=value:raise BrokerError("Order snapshot routing mismatch")
                rows.append({**order,**routing,"schema_version":1})
        except Exception as exc:
            # No inferred cancellation for absent/malformed orders. Already
            # durable today's pending snapshots still retry independently below.
            rows=[]
            report["errors"].append({"component":"orders","error_type":type(exc).__name__,"error":str(exc) if isinstance(exc,BrokerError) else "Order snapshots failed"})
        try:
            if runner.broker == 'bitget':
                from bitget.feedback import CommandFeedback
                feedback = CommandFeedback(runner)
                feedback.refresh()
                rows = [feedback.correlate(row) for row in rows] + feedback.rejections()
                self.publisher.allow_prior_orders = True
            try:
                self.positions.observe(rows)
            except Exception as error:
                report["errors"].append({"component":"position_delta","error_type":type(error).__name__})
            report["order_publication"]=self.publisher.publish_orders(rows)
        except Exception as exc:
            report["errors"].append({"component":"order_delivery","error_type":type(exc).__name__,"error":str(exc) if isinstance(exc,BrokerError) else "Order publication failed"})
        return report

    def refresh(self, trigger, command_id=None):
        if self.runner.lock_path:
            with operation_lock(self.runner.lock_path,float(self.runner.config.get("lock_timeout_seconds",30))):
                return self._refresh(trigger,command_id)
        return self._refresh(trigger,command_id)
