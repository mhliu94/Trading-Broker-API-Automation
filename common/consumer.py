import json
import math
import time
import signal
import threading
from pathlib import Path
from .commands import decode_command
from .config import resolve_env
from .errors import BrokerError, ConfigurationError
from .journal import Journal, encoded
from .locking import operation_lock


def consume(runner, config, max_messages=0):
    if isinstance(getattr(runner,"lock_path",None),Path):
        with operation_lock(runner.lock_path.with_suffix(".runner.lock"),timeout=0.1):
            return _consume(runner,config,max_messages)
    return _consume(runner,config,max_messages)


def _consume(runner, config, max_messages=0):
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.structs import OffsetAndMetadata
    settings = resolve_env(config.get("kafka", {}), strict=True)
    routing = runner.config.get("routing", {})
    if not routing.get("account_id") and not routing.get("account_num_id"):
        raise ConfigurationError("Configure broker routing.account_id/account_num_id before listening")
    account = str(routing.get("account_id") or routing["account_num_id"])
    mode = "live" if runner.execute else "preview"
    base_group = settings.get("group_prefix", "broker-api-scripts")
    server = str(routing.get("server_id") or "default")
    group = f"{base_group}.{runner.broker}.{account}.{server}.{mode}"
    numeric = routing.get("account_num_id")
    if numeric is not None:
        if isinstance(numeric, bool) or not str(numeric).isdigit() or int(numeric) <= 0:
            raise ConfigurationError("routing.account_num_id must be a positive integer")
        group += f".{int(numeric)}"
    connection = {key: settings[key] for key in ("security_protocol", "sasl_mechanism", "sasl_plain_username", "sasl_plain_password", "ssl_cafile", "ssl_certfile", "ssl_keyfile") if settings.get(key)}
    bootstrap = settings.get("bootstrap_servers", "45.32.121.19:9092")
    topic = settings.get("command_topic", "trading-commands")
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap, group_id=group,
        enable_auto_commit=False, auto_offset_reset=settings.get("auto_offset_reset", "latest"),
        max_poll_records=1, max_poll_interval_ms=900000, allow_auto_create_topics=False,
        client_id=group, **connection)
    producer = None
    publisher = None
    reporter = None
    monitor = None
    monitor_failed = False
    monitor_enabled = runner.broker == 'bitget' and runner.config.get('monitor', {}).get('enabled', False)
    snapshots_enabled = runner.execute and settings.get("publish_snapshots", True)
    if runner.execute and monitor_enabled and not snapshots_enabled:
        consumer.close()
        raise ConfigurationError('Bitget monitoring requires kafka.publish_snapshots=true')
    result_topic = settings.get("result_topic")
    account_topic = settings.get("account_topic", "account-details")
    order_topic = settings.get("order_topic", "order-status")
    interval = float(settings.get("snapshot_interval_seconds", 30))
    post_delay = float(settings.get("post_action_delay_seconds", 2))
    if not math.isfinite(interval) or interval <= 0 or not math.isfinite(post_delay) or not 0 <= post_delay <= 300:
        consumer.close()
        raise ConfigurationError("Snapshot interval must be positive; post-action delay must be 0..300 seconds")
    if runner.execute and (result_topic or snapshots_enabled):
        if result_topic in {topic, order_topic, account_topic}:
            consumer.close()
            raise ConfigurationError("Use a separate command-result topic; broker order/account topics use different schemas")
        if snapshots_enabled and (topic in {account_topic,order_topic} or account_topic==order_topic):
            consumer.close()
            raise ConfigurationError("Command/account/order topics must be distinct")
        required = ({account_topic,order_topic} if snapshots_enabled else set()) | ({result_topic} if result_topic else set())
        if required - consumer.topics():
            consumer.close()
            raise ConfigurationError("Provision the configured output topics before running; automatic creation is disabled")
        producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=0,
                                 client_id=group + ".results", **connection)
    if snapshots_enabled:
        from .publication import SnapshotPublisher
        from .reporting import RuntimeSnapshots
        if not runner.journal_path or not runner.config.get("journal_account"):
            consumer.close()
            if producer:producer.close()
            raise ConfigurationError("Publishing requires a persistent journal and stable journal_account")
        publication_path = settings.get("publication_journal") or str(runner.journal_path.with_suffix(".publication.sqlite3"))
        publisher = SnapshotPublisher(publication_path,{**routing,"timezone":runner.config.get("timezone","America/New_York")},producer,
                                      account_topic=account_topic,order_topic=order_topic)
        if runner.broker == 'bitget':
            publisher.republish_on_restart = False
        if monitor_enabled:
            from bitget.monitor import StockMonitor
            publisher.allow_prior_orders = True
            try:
                monitor = StockMonitor(runner, publisher)
            except Exception:
                publisher.close()
                producer.close()
                consumer.close()
                raise
        else:
            reporter = RuntimeSnapshots(runner,publisher)
    processed = 0
    next_refresh = time.monotonic()
    stopped = False
    authentication_required = False
    reconciliation_required = False
    automatic_reconciliation = (runner.execute and runner.broker == "bitget"
                                and isinstance(getattr(runner, "journal", None), Journal))
    next_reconciliation = math.inf
    reconciliation_attempts = 0
    health = None
    old_handlers = {}
    if isinstance(getattr(runner,"journal_path",None),Path):
        from .health import Health
        health_path=settings.get("status_file") or runner.journal_path.with_suffix(".status.json")
        stop_path=settings.get("stop_file") or runner.journal_path.with_suffix(".stop")
        health=Health(health_path,runner.broker,group,stop_path)
        health.stop_path.unlink(missing_ok=True)
        health.update(status="starting",processed=0)
    def stop(signum,frame):
        nonlocal stopped
        stopped=True
    if threading.current_thread() is threading.main_thread():
        for signum in (signal.SIGINT,signal.SIGTERM):
            old_handlers[signum]=signal.signal(signum,stop)
    def refresh(trigger, command_id=None):
        nonlocal next_refresh,authentication_required,stopped
        try:
            report=reporter.refresh(trigger,command_id)
            print(encoded(report),flush=True)
            if any(error.get("error_type")=="AuthenticationError" for error in report.get("errors",[])):
                authentication_required=stopped=True
            if health:health.update(last_refresh=report)
        except BrokerError as exc:
            print(encoded({"event":"snapshot_refresh","trigger":trigger,"error":str(exc)}),flush=True)
        finally:
            next_refresh = time.monotonic() + interval

    def reconcile():
        nonlocal reconciliation_required, next_reconciliation, reconciliation_attempts
        nonlocal authentication_required, stopped
        attempt = reconciliation_attempts + 1
        try:
            report = runner.reconcile_pending()
        except Exception as exc:
            if type(exc).__name__ == "AuthenticationError":
                authentication_required = stopped = True
            report = {"status": "pending", "error_type": type(exc).__name__,
                      "error": str(exc) if isinstance(exc, BrokerError) else "Order reconciliation failed"}
        reconciliation_required = report["status"] == "pending"
        if reconciliation_required:
            reconciliation_attempts = attempt
            delay = {1: 20, 2: 40}.get(attempt)
        else:
            reconciliation_attempts = 0
            delay = None
        next_reconciliation = time.monotonic() + delay if delay is not None else math.inf
        retry_exhausted = reconciliation_required and delay is None
        if reconciliation_required or report.get("resolved") or authentication_required:
            print(encoded({"event": "order_reconciliation", "attempt": attempt,
                           "next_check_seconds": delay, "retry_exhausted": retry_exhausted, **report}), flush=True)
            if report.get("resolved") and reporter:
                refresh("reconciliation")
        if health:
            health.update(status="authentication_required" if authentication_required else "running",
                          reconciliation={"attempt": attempt, "next_check_seconds": delay,
                                          "retry_exhausted": retry_exhausted, **report})

    def schedule_reconciliation():
        nonlocal reconciliation_required, reconciliation_attempts, next_reconciliation
        reconciliation_required = True
        reconciliation_attempts = 0
        next_reconciliation = time.monotonic() + 10
        if health:
            health.update(reconciliation={"status": "pending", "attempt": 0,
                "next_check_seconds": 10, "retry_exhausted": False})
    try:
        if runner.execute and isinstance(getattr(runner, "journal", None), Journal):
            try:
                recovery = runner.recover_pending()
                print(encoded({"event": "startup_recovery", **recovery}), flush=True)
                if recovery["status"] == "reconciliation_required":
                    reconciliation_required = True
                    stopped = stopped or not automatic_reconciliation
            except BrokerError as exc:
                print(encoded({"event": "startup_recovery", "error_type": type(exc).__name__, "error": str(exc)}), flush=True)
                if type(exc).__name__ == "AuthenticationError":
                    authentication_required = True
                else:
                    reconciliation_required = True
                stopped = stopped or authentication_required or not automatic_reconciliation
        if reporter:refresh("startup")
        if isinstance(getattr(runner,"journal",None),Journal) and runner.journal.unresolved(runner.scope):
            reconciliation_required = True
            stopped = stopped or not automatic_reconciliation
        if monitor and not stopped:
            monitor.start()
        if health:health.update(status="authentication_required" if authentication_required else "running")
        if automatic_reconciliation and reconciliation_required:
            schedule_reconciliation()
        next_monitor_health = 0.0
        while not stopped:
            if health and health.should_stop():break
            if automatic_reconciliation and time.monotonic() >= next_reconciliation:
                reconcile()
                if stopped:break
            if monitor:
                if monitor.failure is not None:
                    authentication_required = type(monitor.failure).__name__ == 'AuthenticationError'
                    monitor_failed = not authentication_required
                    stopped = True
                    break
                if health and time.monotonic() >= next_monitor_health:
                    health.update(monitor=monitor.snapshot())
                    next_monitor_health = time.monotonic() + 5
            if reporter and time.monotonic()>=next_refresh:refresh("periodic")
            if stopped:break
            batches=consumer.poll(timeout_ms=1000,max_records=1)
            for records in batches.values():
                for record in records:
                    identity = f"{record.topic}:{record.partition}:{record.offset}"
                    try:
                        result = runner.process(decode_command(record.value), identity, record.timestamp, require_account=True)
                    except BrokerError as exc:
                        result = {"status": "rejected", "broker": runner.broker, "message_id": identity,
                                  "error_type": type(exc).__name__, "error": str(exc)}
                    output = encoded(result).encode()
                    print(output.decode(), flush=True)
                    if result.get("status")=="authentication_required":
                        authentication_required=stopped=True
                        if health:health.update(status="authentication_required",last_result=result)
                        return
                    if producer and result_topic and result["status"] != "ignored":
                        producer.send(result_topic, key=identity.encode(), value=output).get(timeout=30)
                    # Commit only this record; a snapshot delivery failure below
                    # never replays a financial command or rolls back its journal.
                    tp = TopicPartition(record.topic, record.partition)
                    consumer.commit({tp: OffsetAndMetadata(record.offset + 1, "", -1)})
                    if monitor:
                        monitor.notify(result)
                    action=result.get("action")
                    if reporter and action in {"place_order","cancel_order","cancel_open_orders","set_trading_enabled","positions","orders_today"}:
                        if action in {"place_order","cancel_order","cancel_open_orders"} and post_delay:
                            time.sleep(post_delay)
                        refresh("post_action",result.get("id"))
                    if result.get("status")=="unknown":
                        if automatic_reconciliation:
                            if not result.get("duplicate") or not reconciliation_required:
                                schedule_reconciliation()
                        else:
                            reconciliation_required=stopped=True
                    processed += 1
                    if health:health.update(processed=processed,last_result={key:result.get(key) for key in
                        ("id","action","status","error_type","error","broker_error_code","http_status","submission_outcome")})
                    if max_messages and processed >= max_messages:return
    finally:
        consumer.close()
        if monitor:monitor.close()
        if publisher:publisher.close()
        if producer:
            producer.close()
        for signum,handler in old_handlers.items():signal.signal(signum,handler)
        if health:health.update(status="authentication_required" if authentication_required else ("reconciliation_required" if reconciliation_required else ("monitor_failed" if monitor_failed else "stopped")),processed=processed)
