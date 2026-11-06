"""Reference-runner account/order refreshes independent of financial commands."""
from datetime import datetime, timezone
from .errors import BrokerError, ConfigurationError
from .locking import operation_lock
from .snapshots import collect_account, collect_orders
from .utils import today_window, iso_utc


class RuntimeSnapshots:
    def __init__(self, runner, publisher):
        self.runner, self.publisher = runner, publisher

    def _routing(self):
        routing = self.runner.config.get("routing", {})
        account = routing.get("account_id")
        if not isinstance(account,str) or not account.strip():
            raise ConfigurationError("Snapshot publication requires routing.account_id matching KTrader")
        result={"account_id":account}
        if routing.get("account_num_id") is not None:result["account_num_id"]=routing["account_num_id"]
        return result

    def _refresh(self, trigger, command_id=None):
        runner=self.runner; routing=self._routing()
        report={"event":"snapshot_refresh","broker":runner.broker,"trigger":trigger,"command_id":command_id,"errors":[]}
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
            orders=collect_orders(runner.adapter,runner.broker,start,end,errors=mapping_errors)
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
            report["order_publication"]=self.publisher.publish_orders(rows)
        except Exception as exc:
            report["errors"].append({"component":"order_delivery","error_type":type(exc).__name__,"error":str(exc) if isinstance(exc,BrokerError) else "Order publication failed"})
        return report

    def refresh(self, trigger, command_id=None):
        if self.runner.lock_path:
            with operation_lock(self.runner.lock_path,float(self.runner.config.get("lock_timeout_seconds",30))):
                return self._refresh(trigger,command_id)
        return self._refresh(trigger,command_id)
