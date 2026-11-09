from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from bitget.feedback import CommandFeedback
from common.commands import normalize
from common.errors import BrokerError, CommandError, ConfigurationError
from common.publication import SnapshotPublisher
from common.runner import Runner


@pytest.fixture
def runner(tmp_path):
    worker = Runner('bitget', {'api_key': 'test', 'api_secret': 'test', 'passphrase': 'test',
        'journal_account': 'feedback-test', 'trading_enabled': True,
        'routing': {'account_id': 'TEST', 'account_num_id': 1}},
        execute=True, journal_path=tmp_path / 'commands.sqlite3')
    worker.adapter.place_order = Mock(side_effect=AssertionError('Must not trade'))
    yield worker
    worker.close()


def payload(**changes):
    return {'command_id': 'origin-1', 'type': 'LIMIT_ORDER', 'account_id': 'TEST',
            'symbol': 'AAPL', 'side': 'BUY', 'qty_shares': 3, 'limit_price': 190,
            'ts': (datetime.now(timezone.utc) - timedelta(days=2)).isoformat(), **changes}


def reject(worker, raw):
    with pytest.raises(CommandError) as error:
        worker.process(raw, 'commands:0:5', require_account=True)
    return worker.record_preflight_rejection(raw, error.value, 'commands:0:5', None)


def feedback(worker):
    f = CommandFeedback(worker)
    f.refresh()
    return f


def test_stale_rejection_survives_restart_and_never_replays(runner):
    raw = payload()
    result = reject(runner, raw)
    assert result['requests_sent'] == 0 and result['id'] == raw['command_id']
    assert runner.process(raw)['duplicate']
    other = Runner('bitget', runner.config, True, runner.journal_path)
    try:
        assert other.process(raw)['duplicate']
        row, = feedback(other).rejections()
        assert row['command_id'] == raw['command_id']
        assert row['status'] == 'REJECTED' and row['filled_quantity'] == 0
        assert row['order_id'].startswith('rejected:')
        assert 'Local validation' in row['message']
    finally:
        other.close()
    runner.adapter.place_order.assert_not_called()


@pytest.mark.parametrize('order_type,tif', [('LIMIT_ORDER', 'day'), ('LIMIT_ORDER_FOK', 'fok')])
@pytest.mark.parametrize('timestamp_source', ['payload', 'kafka'])
def test_order_expiring_during_action_delay_publishes_rejection(runner, monkeypatch, order_type, tif, timestamp_source):
    clock = Mock(return_value=datetime.now(timezone.utc).timestamp())
    issued_at = clock.return_value - 119
    raw = payload(type=order_type, symbol='BDMD',
                  ts=datetime.fromtimestamp(issued_at, timezone.utc).isoformat())
    kafka_timestamp_ms = None
    if timestamp_source == 'kafka':
        raw.pop('ts')
        kafka_timestamp_ms = int(issued_at * 1000)
    runner.config.update(action_delay_seconds=5, command_max_age_seconds=120,
                         fok_policy='timed_cancel', cancel_after_seconds=1)
    runner.adapter.config.update(runner.config)

    def advance(seconds):
        clock.return_value += seconds

    sleep = Mock(side_effect=advance)
    request = Mock(side_effect=AssertionError('Must not contact the broker'))
    monkeypatch.setattr('common.commands.time.time', clock)
    monkeypatch.setattr('common.runner.time.sleep', sleep)
    monkeypatch.setattr('requests.request', request)
    result = runner.process(raw, kafka_timestamp_ms=kafka_timestamp_ms, require_account=True)
    assert result['status'] == 'rejected'
    assert result['submission_outcome'] == 'not_submitted' and result['requests_sent'] == 0
    assert result['error_type'] == 'CommandError' and 'stale' in result['error']
    assert runner.journal.unresolved(runner.scope) == [] and runner.unresolved_timed() == []

    # The monitor reads a separate journal connection, including after restart.
    other = Runner('bitget', runner.config, True, runner.journal_path)
    try:
        assert other.process(raw, kafka_timestamp_ms=kafka_timestamp_ms)['duplicate']
        row, = feedback(other).rejections()
        assert row['command_id'] == raw['command_id'] and row['symbol'] == 'BDMD'
        assert row['status'] == 'REJECTED' and row['filled_quantity'] == 0
        assert row['time_in_force'] == tif and 'Local validation' in row['message']
    finally:
        other.close()
    sleep.assert_called_once_with(5)
    runner.adapter.place_order.assert_not_called()
    request.assert_not_called()


def test_command_error_after_dispatch_is_not_proof_of_non_submission(runner):
    raw = payload(ts=datetime.now(timezone.utc).isoformat())
    runner.adapter.place_order.side_effect = CommandError('Failure after dispatch')
    result = runner.process(raw)
    assert result.get('submission_outcome') != 'not_submitted'
    assert result.get('requests_sent') != 0
    assert feedback(runner).rejections() == []
    assert runner.process(raw)['duplicate']
    runner.adapter.place_order.assert_called_once()


def test_rejection_never_closes_unknown_or_conflicting_command(runner):
    raw = payload()
    command = normalize(raw, 'bitget', runner.config)
    runner.journal.reserve(runner.scope, command)
    runner.journal.finish(runner.scope, command['id'], {'status': 'unknown', 'submission_outcome': 'unknown'})
    assert runner.record_preflight_rejection(raw, ConfigurationError('blocked'), 'commands:0:5', None) is None
    assert runner.record_preflight_rejection(payload(qty_shares=4), CommandError('ID conflict'), 'commands:0:5', None) is None
    assert feedback(runner).rejections() == []
    assert runner.journal.unresolved(runner.scope)[0]['id'] == command['id']


def test_foreign_or_malformed_commands_cannot_generate_rejection(runner):
    for raw in (payload(account_id='OTHER'), payload(side='invalid')):
        assert runner.record_preflight_rejection(raw, CommandError('invalid'), 'commands:0:5', None) is None
    assert feedback(runner).rejections() == []


def snapshot():
    return {'order_id': 'broker-1', 'symbol': 'AAPL', 'side': 'BUY', 'order_type': 'LIMIT',
            'quantity': 3, 'limit_price': 190}


def test_exact_correlation_rejects_conflicting_identity(runner):
    command = normalize(payload(), 'bitget', runner.config)
    runner.journal.reserve(runner.scope, command)
    runner.journal.finish(runner.scope, command['id'], {'status': 'submitted', 'data': {'orderId': 'broker-1'}})
    f = feedback(runner)
    assert f.correlate(snapshot())['command_id'] == command['id']
    unrelated = {**snapshot(), 'order_id': 'manual'}
    assert 'command_id' not in f.correlate(unrelated)
    with pytest.raises(BrokerError):
        f.correlate({**snapshot(), 'quantity': 4})
    with pytest.raises(BrokerError):
        f.correlate(unrelated, runner.adapter.correlation(command['id']))
    # Another account's command cannot establish ownership.
    runner.journal.db.execute('UPDATE commands SET scope=?', ('bitget:other',))
    runner.journal.db.commit()
    assert 'command_id' not in feedback(runner).correlate(snapshot())


def test_rejected_feedback_retries_after_outage_and_restart(runner, tmp_path):
    reject(runner, payload())
    row, = feedback(runner).rejections()
    producer = Mock()
    producer.send.return_value.get.side_effect = TimeoutError('Kafka unavailable')
    path = tmp_path / 'publications.sqlite3'
    pub = SnapshotPublisher(path, runner.config['routing'], producer, allow_prior_orders=True)
    assert pub.publish_orders([row])['failed'] == 1
    pending = pub.known_orders()[0]
    pub.close()
    producer.send.return_value.get.side_effect = None
    producer.send.return_value.get.return_value = SimpleNamespace(partition=0, offset=10)
    pub = SnapshotPublisher(path, runner.config['routing'], producer, allow_prior_orders=True)
    try:
        assert pub.publish_orders([])['delivered'] == 1
        sent = json.loads(producer.send.call_args.kwargs['value'])
        assert sent == pending
        assert pub.publish_orders([row])['delivered'] == 0
    finally:
        pub.close()


def test_explicit_broker_rejection_publishes_but_ambiguous_failure_does_not(runner):
    from common.errors import RequestRejected, OutcomeUnknown
    raw = payload(ts=datetime.now(timezone.utc).isoformat())
    runner.adapter.place_order.side_effect = RequestRejected('Not shortable', code='101108')
    assert runner.process(raw)['submission_outcome'] == 'not_submitted'
    assert len(feedback(runner).rejections()) == 1
    runner.adapter.place_order.side_effect = OutcomeUnknown('Timeout after dispatch')
    assert runner.process({**raw, 'command_id': 'unknown-2'})['status'] == 'unknown'
    assert len(feedback(runner).rejections()) == 1


def test_consumer_persists_rejection_before_committing_input(runner, monkeypatch):
    import kafka
    from common.consumer import consume
    consumer = Mock()
    record = SimpleNamespace(topic='trading-commands', partition=0, offset=5,
        timestamp=int(datetime.now(timezone.utc).timestamp() * 1000), value=json.dumps(payload()).encode())
    consumer.poll.return_value = {0: [record]}
    def commit(offsets):
        assert feedback(runner).rejections()[0]['command_id'] == 'origin-1'
        assert next(iter(offsets.values())).offset == 6
    consumer.commit.side_effect = commit
    monkeypatch.setattr(kafka, 'KafkaConsumer', Mock(return_value=consumer))
    consume(runner, {'kafka': {'publish_snapshots': False}}, max_messages=1)
    consumer.commit.assert_called_once()
    runner.adapter.place_order.assert_not_called()
