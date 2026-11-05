from datetime import datetime, timedelta, timezone
import json
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

import pytest

from bitget.adapter import ACTIVE, Adapter, PREFIX
from bitget.monitor import StockMonitor
from common.config import load_config
from common.errors import BrokerError
from common.publication import SnapshotPublisher
from test_bitget_monitor import monitor, raw, observe, Producer


@pytest.mark.parametrize('history', [False, True])
def test_active_filter_sends_all_statuses_but_signs_first_as_required_by_live_api(history):
    adapter = Adapter({'api_key': 'key', 'api_secret': 'secret', 'passphrase': 'pass'})
    response = Mock(status_code=200)
    response.json.return_value = {'code': '00000', 'data': {'orders': []}}
    with patch('requests.request', return_value=response) as request:
        assert (adapter._history(0, 1, statuses=ACTIVE) if history else adapter._today(statuses=ACTIVE)) == []
    args, kwargs = request.call_args
    url = urlsplit(args[1])
    query = parse_qs(url.query)
    assert set(query['status']) == ACTIVE
    assert query['market'] == ['US']
    from urllib.parse import urlencode
    signed = {k: v[0] for k, v in query.items()}
    assert kwargs['headers']['ACCESS-SIGN'] == adapter.signature(
        'secret', kwargs['headers']['ACCESS-TIMESTAMP'], 'GET', url.path + '?' + urlencode(signed))


def test_active_history_keeps_filter_on_every_subdivision():
    adapter = Adapter({})
    adapter._request = Mock(side_effect=[{'orders': [], 'hasMore': True},
        {'orders': [raw(orderId='older')], 'hasMore': False}, {'orders': [], 'hasMore': False}])
    assert len(adapter._history(0, 3, statuses=ACTIVE)) == 1
    assert all(set(call.args[2]['status']) == ACTIVE for call in adapter._request.call_args_list)
    assert [(call.args[2]['startAt'], call.args[2]['endAt'])
            for call in adapter._request.call_args_list] == [(0, 3), (0, 1), (2, 3)]


@pytest.mark.parametrize('configured,enabled', [(None, True), ({}, True), ({'enabled': False}, False)])
def test_live_config_defaults_to_1200ms_monitor(tmp_path, configured, enabled):
    selected = {'journal_account': 'test'}
    if configured is not None:
        selected['monitor'] = configured
    path = tmp_path / 'config.json'
    path.write_text(json.dumps({'brokers': {'bitget': selected}}))
    _, loaded = load_config(path, 'bitget', execute=True)
    assert loaded['monitor'] == {**(configured or {}), 'enabled': enabled, 'poll_seconds': 1.2}
    assert json.loads(path.read_text())['brokers']['bitget'] == selected


def test_example_preview_does_not_require_live_monitor_account():
    _, selected = load_config('config.example.json', 'bitget', execute=False)
    assert selected['monitor']['enabled'] is False
    Adapter(selected)


def run_polls(monitor, initial, details, *, turns=7):
    """Run the real order loop with a deterministic, nonblocking scheduler."""
    monitor.wake.wait = lambda _: False
    monitor.stop_event.wait = lambda _: False
    calls = []
    def today(*, statuses):
        assert statuses == ACTIVE
        calls.append(1)
        if len(calls) >= turns:
            monitor.stop_event.set()
        return initial if len(calls) == 1 else []
    monitor.adapter._today = Mock(side_effect=today)
    monitor.adapter.stock_order_detail = Mock(side_effect=details)
    monitor._orders()


def test_disappearing_active_order_publishes_terminal_once_then_stops_detail_polling(monitor):
    initial = raw()
    terminal = {**initial, 'status': 'Filled', 'executedQuantity': '3', 'executedPrice': '190'}
    run_polls(monitor, [initial], [terminal])
    assert [s['status'] for _, s in monitor.publisher.producer.sent] == ['NEW', 'FILLED']
    monitor.adapter.stock_order_detail.assert_called_once_with('001')


def test_failed_detail_or_still_active_order_is_retained_until_confirmed_terminal(monitor):
    initial = raw()
    terminal = {**initial, 'status': 'Canceled'}
    run_polls(monitor, [initial], [BrokerError('temporary failure'), initial, terminal], turns=9)
    assert [s['status'] for _, s in monitor.publisher.producer.sent] == ['NEW', 'CANCELED']
    assert monitor.adapter.stock_order_detail.call_count == 3


def test_restored_prior_day_gtc_is_checked_after_missing_from_active_list(monitor):
    initial = raw(submittedAt=(datetime.now(timezone.utc) - timedelta(days=3)).isoformat())
    observe(monitor, [initial])
    monitor.known = {row['order_id']: row for row in monitor.publisher.known_orders()}
    run_polls(monitor, [], [{**initial, 'status': 'Expired'}])
    assert [s['status'] for _, s in monitor.publisher.producer.sent] == ['NEW', 'EXPIRED']
    monitor.adapter.stock_order_detail.assert_called_once_with('001')


def test_periodic_sweep_discovers_order_that_completed_between_active_polls(monitor):
    completed = raw(status='Filled', executedQuantity='3', executedPrice='190')
    monitor.adapter._history = Mock(return_value=[])
    monitor.adapter._today = Mock(return_value=[completed])
    waits = []
    def wait(_):
        waits.append(1)
        if len(waits) == 2:
            monitor.stop_event.set()
        return False
    monitor.stop_event.wait = wait
    monitor._reconcile()
    first, second = monitor.adapter._history.call_args_list
    assert first.args[0] == 0 and first.kwargs['statuses'] == ACTIVE
    assert not second.kwargs
    while not monitor.inbox.empty():
        monitor._observe(*monitor.inbox.get_nowait())
    assert [s['status'] for _, s in monitor.publisher.producer.sent] == ['FILLED']


def test_reconciliation_keeps_previous_day_across_midnight(monitor, monkeypatch):
    before = datetime(2026, 11, 5, 5, tzinfo=timezone.utc)
    after = before + timedelta(days=1)
    windows = iter([(before, after), (after, after + timedelta(days=1))])
    monkeypatch.setattr('bitget.monitor.today_window', lambda _: next(windows))
    monitor.adapter._history = Mock(return_value=[])
    monitor.adapter._today = Mock(return_value=[])
    waits = []
    def wait(_):
        waits.append(1)
        if len(waits) == 2:
            monitor.stop_event.set()
        return False
    monitor.stop_event.wait = wait
    monitor._reconcile()
    assert monitor.adapter._history.call_args.args == (
        int(before.timestamp()), int((after + timedelta(days=1)).timestamp()) - 1)


def test_unchanged_order_not_republished_after_restart_but_pending_delivery_retries(tmp_path):
    routing = {'account_id': 'TEST'}
    producer = Producer()
    path = tmp_path / 'outbox.sqlite3'
    pub = SnapshotPublisher(path, routing, producer, allow_prior_orders=True, republish_on_restart=False)
    initial = {'account_id': 'TEST', 'schema_version': 1,
               **Adapter({}).snapshot_order(raw(), datetime.now(timezone.utc))}
    assert pub.publish_orders([initial])['delivered'] == 1
    pub.close()
    pub = SnapshotPublisher(path, routing, producer, allow_prior_orders=True, republish_on_restart=False)
    assert pub.publish_orders([initial])['delivered'] == 0
    changed = {**initial, 'status': 'CANCELED'}
    producer.fail = True
    assert pub.publish_orders([changed])['failed'] == 1
    pub.close()
    producer.fail = False
    pub = SnapshotPublisher(path, routing, producer, allow_prior_orders=True, republish_on_restart=False)
    assert pub.publish_orders([changed])['delivered'] == 1
    assert pub.publish_orders([changed])['delivered'] == 0
    assert [s['status'] for _, s in producer.sent] == ['NEW', 'CANCELED']
    pub.close()


def test_confirmed_terminal_reference_is_not_polled_again_after_command_wakeup(monitor):
    terminal = raw(status='Canceled')
    observe(monitor, [terminal])
    monitor.notify({'action': 'cancel_order', 'data': {'orderId': '001'}})
    run_polls(monitor, [], [])
    monitor.adapter.stock_order_detail.assert_not_called()
