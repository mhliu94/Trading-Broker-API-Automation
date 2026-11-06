"""Exact command correlation and durable, zero-fill rejection feedback.

No broker writes occur here. A local rejection has a namespaced synthetic ID;
it must never be confused with a broker order or an uncertain submission.
"""
from datetime import datetime, timezone
from contextlib import closing
from decimal import Decimal
import hashlib
import json
import re
import sqlite3

from common.errors import BrokerError
from common.publication import validate_order_snapshot
from common.utils import iso_utc, parse_time


def command_time(payload, kafka_timestamp_ms=None):
    for key in ('ts', 'timestamp', 'timeStamp', 'created_at', 'createdAt', 'issued_at', 'issuedAt'):
        if payload.get(key) is not None:
            try:
                return iso_utc(parse_time(payload[key]))
            except (ValueError, TypeError, OverflowError, OSError):
                pass
    if kafka_timestamp_ms is not None:
        return iso_utc(datetime.fromtimestamp(kafka_timestamp_ms / 1000, timezone.utc))
    return iso_utc(datetime.now(timezone.utc))


class CommandFeedback:
    def __init__(self, runner):
        self.path, self.scope = runner.journal_path, runner.scope
        self.routing = {k: v for k, v in runner.config['routing'].items()
                        if k in ('account_id', 'account_num_id')}
        self.records = []

    def refresh(self):
        # Independent connection: monitoring must never use the command thread's
        # SQLite connection. Only this stable account scope is eligible.
        with closing(sqlite3.connect(self.path.resolve().as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
            rows = db.execute('SELECT command,result FROM commands WHERE scope=?', (self.scope,)).fetchall()
        self.records = [(json.loads(c), json.loads(r) if r else {}) for c, r in rows if c]

    @staticmethod
    def broker_id(result):
        data = result.get('data') or {}
        return result.get('broker_order_id') or data.get('orderId') or data.get('order_id')

    def correlate(self, snapshot, remark=None):
        matches = []
        for command, result in self.records:
            if command.get('action') != 'place_order':
                continue
            broker_id = self.broker_id(result)
            correlation = 'cmd:' + hashlib.sha256(command['id'].encode()).hexdigest()[:60]
            if broker_id == snapshot['order_id'] or remark and remark == correlation:
                if broker_id and broker_id != snapshot['order_id']:
                    raise BrokerError('Broker order ID conflicts with the exact command remark')
                matches.append(command)
        if not matches:
            return snapshot
        if len(matches) != 1:
            raise BrokerError('Multiple commands claim the same broker order')
        command = matches[0]
        if (command['symbol'].removesuffix('.US') != snapshot['symbol']
                or command['side'].upper() != snapshot['side']
                or Decimal(command['quantity']) != Decimal(str(snapshot['quantity']))
                or command['order_type'].upper() != snapshot['order_type']
                or command.get('limit_price') is not None
                and Decimal(command['limit_price']) != Decimal(str(snapshot.get('limit_price')))
                or snapshot.get('command_id', command['id']) != command['id']):
            raise BrokerError('Broker order metadata conflicts with its command')
        return {**snapshot, 'command_id': command['id']}

    def rejections(self):
        rows = []
        for command, result in self.records:
            if command.get('action') != 'place_order' or self.broker_id(result):
                continue
            proven = (result.get('status') == 'rejected'
                      and result.get('submission_outcome') == 'not_submitted')
            reviewed = (result.get('status') == 'reconciled'
                        and result.get('outcome') in ('not_submitted', 'rejected')
                        and bool(result.get('evidence')))
            if not (proven or reviewed):
                continue
            created = result.get('feedback_created_at')
            if not created:
                # Legacy KTrader IDs embed the original command timestamp.
                stamp = re.search(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z', command['id'])
                created = stamp.group() if stamp else result.get('observed_at')
            if not created:
                raise BrokerError('Rejected command lacks a durable source timestamp')
            updated = result.get('observed_at') or result.get('reconciled_at') or created
            updated = iso_utc(max(parse_time(created), parse_time(updated)))
            reason = result.get('error') or result.get('evidence') or 'Request was not submitted'
            origin = 'Local validation' if result.get('requests_sent') == 0 else 'Submission rejection'
            row = {**self.routing, 'schema_version': 1,
                   'order_id': 'rejected:' + hashlib.sha256(command['id'].encode()).hexdigest(),
                   'command_id': command['id'], 'created_at': created, 'updated_at': updated,
                   'symbol': command['symbol'].removesuffix('.US'), 'side': command['side'].upper(),
                   'order_type': command['order_type'].upper(), 'status': 'REJECTED',
                   'quantity': Decimal(command['quantity']), 'filled_quantity': 0,
                   'average_fill_price': None, 'currency': 'USD',
                   'time_in_force': command['time_in_force'],
                   'message': (origin + '; no broker order accepted. ' + reason)[:2000]}
            if command.get('limit_price') is not None:
                row['limit_price'] = Decimal(command['limit_price'])
            rows.append(validate_order_snapshot(row))
        return rows
