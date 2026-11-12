"""Versioned account changes. Keep this dependency-free module identical across brokers.

Changed rows contain absolute values (not arithmetic increments). Acknowledged
checkpoints bound recovery time; a sequence gap must never construct partial holdings.
"""
from copy import deepcopy
from datetime import datetime, timezone
import math
import threading
import time
import uuid

VERSION = 1
MAPS = ('cash_by_currency', 'available_cash_by_currency')
EXTRA = ('cash_basis', 'buying_power')


def _number(value):
    if isinstance(value, bool):
        raise ValueError('Boolean account amount')
    result = float(value)
    if not math.isfinite(result):
        raise ValueError('Nonfinite account amount')
    return result


def _text(value):
    if not isinstance(value, str) or not value or len(value) > 256 or value != value.strip():
        raise ValueError('Invalid account identifier')
    return value


def _timestamp(value):
    result = datetime.fromisoformat(_text(value).replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Account timestamp needs a timezone')
    return result.astimezone(timezone.utc)


def _positions(rows):
    if not isinstance(rows, list):
        raise ValueError('Positions must be a complete list')
    result = {}
    for row in rows:
        symbol = _text(row['symbol'])
        if symbol in result:
            raise ValueError('Duplicate position symbol')
        value = {'symbol': symbol, 'qty': _number(row['qty']),
                 'avg_price': None if row.get('avg_price') is None else _number(row['avg_price'])}
        available = row.get('available_qty', row.get('salable_qty'))
        if available is not None:
            value['available_qty'] = _number(available)
        result[symbol] = value
    return result


def normalize(snapshot):
    """Validate a complete broker observation before diffing anything."""
    if not isinstance(snapshot, dict):
        raise ValueError('Account must be an object')
    result = {'account_id': _text(snapshot['account_id']), 'cash': _number(snapshot['cash']),
              'ts': snapshot['ts'], 'trading_enabled': snapshot['trading_enabled']}
    _timestamp(result['ts'])
    if type(result['trading_enabled']) is not bool:
        raise ValueError('Invalid trading state')
    numeric = snapshot.get('account_num_id')
    if numeric is not None and (type(numeric) is not int or numeric < 1):
        raise ValueError('Invalid numeric account ID')
    result['account_num_id'] = numeric
    if snapshot.get('cash_basis') is not None:
        result['cash_basis'] = _text(snapshot['cash_basis'])
    if snapshot.get('buying_power') is not None:
        result['buying_power'] = _number(snapshot['buying_power'])
    for field in MAPS:
        values = snapshot.get(field, {})
        if not isinstance(values, dict):
            raise ValueError('Invalid cash map')
        result[field] = {_text(k): _number(v) for k, v in values.items()}
    result['cash_by_currency'].setdefault('USD', result['cash'])
    if result['cash_by_currency']['USD'] != result['cash']:
        raise ValueError('USD cash disagrees with cash')
    positions = _positions(snapshot['positions'])
    result['positions'] = [positions[k] for k in sorted(positions)]
    return result


class AccountDeltaPublisher:
    """One ordered writer per account. Commit the baseline only after a Kafka ACK.

    An ambiguous delivery is retried byte-for-byte before any newer event. A
    restart creates a new stream and a full baseline; no disk state is trusted.
    """
    def __init__(self, checkpoint_seconds=30, clock=time.monotonic):
        self.clock = clock
        self.checkpoint_seconds = max(1, min(60, float(checkpoint_seconds)))
        self.stream_id = str(uuid.uuid4())
        self.sequence = 0
        self.baseline = None
        self.pending = None
        self.last_checkpoint = float('-inf')
        self.lock = threading.RLock()

    def _send_pending(self, send):
        event, state = self.pending
        receipt = send(deepcopy(event))
        self.baseline = state
        self.sequence = event['sequence']
        if event['message_type'] == 'account_snapshot':
            self.last_checkpoint = self.clock()
        self.pending = None
        return receipt

    def publish(self, snapshot, send, force=False):
        state = normalize(snapshot)
        with self.lock:
            receipt = None
            if self.pending is not None:
                receipt = self._send_pending(send)
            old = self.baseline
            if old is not None:
                if (state['account_id'], state['account_num_id']) != (old['account_id'], old['account_num_id']):
                    raise ValueError('Publisher account identity changed')
                if _timestamp(state['ts']) < _timestamp(old['ts']):
                    return receipt  # A slower concurrent read cannot undo a newer observation.
            full = old is None or force or self.clock() - self.last_checkpoint >= self.checkpoint_seconds
            if full:
                event = deepcopy(state)
            else:
                previous = {p['symbol']: p for p in old['positions']}
                current = {p['symbol']: p for p in state['positions']}
                event = {'account_id': state['account_id'], 'account_num_id': state['account_num_id'],
                         'ts': state['ts'], 'base_sequence': self.sequence,
                         'positions': [p for k,p in current.items() if previous.get(k) != p],
                         'removed_positions': sorted(previous.keys() - current.keys())}
                for field in MAPS:
                    changes = {k: v for k,v in state[field].items() if old[field].get(k) != v}
                    changes.update({k: None for k in old[field].keys() - state[field].keys()})
                    if changes:
                        event[field] = changes
                for field in ('cash', 'trading_enabled', *EXTRA):
                    if state.get(field) != old.get(field):
                        event[field] = state.get(field)
                if not event['positions'] and not event['removed_positions'] and not any(
                        field in event for field in (*MAPS, 'cash', 'trading_enabled', *EXTRA)):
                    return receipt
            event.update(account_update_version=VERSION, message_type='account_snapshot' if full else 'account_delta',
                         stream_id=self.stream_id, sequence=self.sequence + 1)
            self.pending = (event, state)
            return self._send_pending(send)


class AccountReplica:
    """Consumer reducer. Returns a complete state only for a valid new event."""
    def __init__(self):
        self.state = None
        self.stream_id = None
        self.sequence = 0
        self.retired_streams = set()
        self.needs_snapshot = False

    def apply(self, event):
        if not isinstance(event, dict) or event.get('account_update_version') != VERSION:
            raise ValueError('Unsupported account update version')
        stream = _text(event['stream_id'])
        sequence = event['sequence']
        if type(sequence) is not int or sequence < 1:
            raise ValueError('Invalid account sequence')
        kind = event['message_type']
        if kind not in {'account_snapshot', 'account_delta'}:
            raise ValueError('Unsupported account message type')
        stamp = _timestamp(event['ts'])
        if stream in self.retired_streams:
            return None
        if stream == self.stream_id and sequence <= self.sequence:
            return None
        if self.state is not None:
            if (event['account_id'], event.get('account_num_id')) != (self.state['account_id'], self.state['account_num_id']):
                raise ValueError('Account identity mismatch')
            if stamp < _timestamp(self.state['ts']):
                return None
        if kind == 'account_snapshot':
            state = normalize(event)
        else:
            if (self.needs_snapshot or self.state is None or stream != self.stream_id
                    or type(event.get('base_sequence')) is not int
                    or event['base_sequence'] != self.sequence or sequence != self.sequence + 1):
                self.needs_snapshot = True
                return None
            state = deepcopy(self.state)
            positions = {p['symbol']: p for p in state['positions']}
            changed = _positions(event['positions'])
            removed = event['removed_positions']
            if not isinstance(removed, list) or any(not isinstance(k, str) for k in removed):
                raise ValueError('Invalid removed positions')
            if len(set(removed)) != len(removed) or set(removed) & changed.keys():
                raise ValueError('Conflicting position operations')
            for symbol in removed:
                positions.pop(symbol, None)
            positions.update(changed)
            state['positions'] = list(positions.values())
            for field in MAPS:
                changes = event.get(field, {})
                if not isinstance(changes, dict):
                    raise ValueError('Invalid cash changes')
                for currency, amount in changes.items():
                    _text(currency)
                    if amount is None:
                        state[field].pop(currency, None)
                    else:
                        state[field][currency] = _number(amount)
            for field in ('cash', 'trading_enabled', *EXTRA):
                if field in event:
                    state[field] = event[field]
            state['ts'] = event['ts']
            state = normalize(state)
        if self.stream_id is not None and stream != self.stream_id:
            self.retired_streams.add(self.stream_id)
        self.state, self.stream_id, self.sequence = state, stream, sequence
        self.needs_snapshot = False
        return deepcopy(state)


class OrderChanges:
    """Detect partial fills and state changes independently of delivery success."""
    def __init__(self):
        self.orders = {}

    def observe(self, snapshots):
        changed = False
        for row in snapshots:
            key = str(row['order_id'])
            value = (str(row.get('status')), _number(row.get('filled_quantity', 0)))
            previous = self.orders.get(key)
            if previous is not None and value[1] < previous[1]:
                continue
            if value != previous:
                self.orders[key] = value
                changed = True
        return changed
