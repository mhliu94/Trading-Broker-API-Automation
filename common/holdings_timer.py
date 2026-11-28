"""Periodic broker checkpoints and account-local cumulative-fill accounting."""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import logging
import math
import threading
import time

if __package__:
    from .account_delta import normalize
else:  # Direct Tiger script launcher.
    from account_delta import normalize

LOG = logging.getLogger("broker.holdings")
TERMINAL = {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}


def _amount(value):
    if value is None or value == "":
        return None
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("Nonfinite order amount")
    return result


def _instant(value):
    if not value:
        return None
    stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("Order timestamp requires a timezone")
    return stamp


class PositionProjection:
    """One state owner for broker checkpoints and observed working/terminal orders.

    publish(state, full) must acknowledge or raise. A broker read runs outside
    this lock. A symbol changed during that read keeps its newer local quantity
    until a non-overlapping checkpoint can reconcile it.
    """

    def __init__(self, publish, *, now=None):
        self.publish = publish
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.lock = threading.RLock()
        self.account = None
        self.orders = {}
        self.open_orders = {}
        self.positions = {}
        self.revision = 0
        self.read_generation = 0
        self.applied_read = 0
        self.changed = {}
        self.cutoff = None
        self.dirty = False
        self.full = False

    def begin(self):
        with self.lock:
            self.read_generation += 1
            return self.read_generation, self.revision, self.now()

    def working_orders(self):
        with self.lock:
            return [dict(row) for row in self.open_orders.values()]

    def _send(self):
        if self.account is None or not self.dirty:
            return
        snapshot = dict(self.account, positions=[dict(self.positions[s]) for s in sorted(self.positions)])
        self.publish(snapshot, self.full)
        self.full = self.dirty = False

    def checkpoint(self, token, snapshot):
        generation, revision, started = token
        with self.lock:
            if generation <= self.applied_read:
                return
            snapshot = normalize(snapshot)
            if self.account is not None and (snapshot["account_id"],snapshot["account_num_id"]) != (
                    self.account["account_id"],self.account["account_num_id"]):
                raise ValueError("Checkpoint account changed")
            positions = {r["symbol"]: dict(r) for r in snapshot["positions"]}
            if len(positions) != len(snapshot["positions"]):
                raise ValueError("Duplicate checkpoint position")
            for symbol, observed in self.changed.items():
                if observed > revision and self.account is not None:
                    if symbol in positions and symbol in self.positions and positions[symbol]["qty"] == self.positions[symbol]["qty"]:
                        continue  # Broker has caught up; accept its cost and sellable basis.
                    if symbol in self.positions:
                        positions[symbol] = self.positions[symbol]
                    else:
                        positions.pop(symbol, None)
            self.changed = {s: v for s, v in self.changed.items() if v > revision}
            self.account = {k: deepcopy(v) for k, v in snapshot.items() if k != "positions"}
            self.account["ts"] = self.now().isoformat()
            self.positions = positions
            self.cutoff = started
            self.applied_read = generation
            self.full = self.dirty = True
            # Old terminal tickets can be seeded again without applying their
            # historical fills. Working/GTC orders survive date boundaries.
            self.orders = {k: v for k, v in self.orders.items() if k in self.open_orders
                           or v[3] is None or (started-v[3]).days < 3}
            self._send()

    def observe(self, rows):
        with self.lock:
            for row in rows:
                try:
                    self._observe(row)
                except (KeyError, ValueError, InvalidOperation, TypeError):
                    LOG.warning("Incomplete order observation; retaining its previous position")
            self._send()

    def _observe(self, row):
        if self.account and row.get("account_id", self.account["account_id"]) != self.account["account_id"]:
            raise ValueError("Order belongs to another account")
        identity = row["order_id"]
        symbol = row["symbol"]
        side = str(row["side"]).upper()
        if (not isinstance(identity, str) or not identity or not isinstance(symbol, str)
                or not symbol or side not in ("BUY", "SELL")):
            raise ValueError("Missing order identity, symbol or side")
        created = _instant(row.get("created_at"))
        updated = _instant(row.get("updated_at"))
        key = (identity, created)
        filled = _amount(row.get("filled_quantity", 0))
        if filled is None or filled < 0:
            raise ValueError("Invalid cumulative fill")
        price = _amount(row.get("average_fill_price"))
        gross = filled * price if price is not None and price > 0 else None
        terminal = str(row["status"]).upper() in TERMINAL
        previous = self.orders.get(key)
        old_fill, old_gross = (previous[0], previous[1]) if previous else (Decimal(0), Decimal(0))
        if previous and (filled < old_fill or (previous[2] and not terminal)
                         or (updated and previous[3] and updated < previous[3])):
            return
        self.orders[key] = (filled, gross, terminal, updated or created)
        if terminal:
            self.open_orders.pop(key, None)
        else:
            self.open_orders[key] = dict(row)
        increase = filled-old_fill
        if not increase or self.account is None:
            return
        # A newly discovered pre-checkpoint order can already be in the broker
        # position. Seed its cumulative quantity; project future increments.
        if previous is None and (created is None or created < self.cutoff):
            return
        if updated and updated < self.cutoff:
            return
        signed = increase if side == "BUY" else -increase
        last_price = None
        if gross is not None and (old_gross is not None or not old_fill):
            last_price = (gross - (old_gross or 0)) / increase
            if last_price <= 0:
                last_price = None
        old = self.positions.get(symbol, {"symbol": symbol, "qty": 0, "avg_price": None, "available_qty": 0})
        quantity = _amount(old["qty"])
        new_quantity = quantity + signed
        if not new_quantity:
            self.positions.pop(symbol, None)
        else:
            pos = dict(old, qty=float(new_quantity))
            avg = _amount(old.get("avg_price"))
            if not quantity or quantity * new_quantity < 0:
                avg = last_price
            elif quantity * signed > 0:
                avg = ((abs(quantity)*avg + increase*last_price)/abs(new_quantity)
                       if avg is not None and last_price is not None else None)
            pos["avg_price"] = float(avg) if avg is not None else None
            # Settlement and reserved sells are broker-specific. Do not credit
            # new selling power before the next authoritative balance.
            for field in ("available_qty", "salable_qty"):
                if pos.get(field) is not None and quantity >= 0:
                    pos[field] = min(pos[field], max(0, float(new_quantity)))
            self.positions[symbol] = pos
        self.revision += 1
        self.changed[symbol] = self.revision
        self.account["ts"] = self.now().isoformat()
        self.dirty = True


class HoldingsTimer:
    """Run a startup checkpoint and then one read on each monotonic interval."""

    def __init__(self, read, interval=30, on_error=None):
        self.read = read
        self.interval = float(interval)
        if not math.isfinite(self.interval) or self.interval <= 0:
            raise ValueError("Holdings interval must be positive")
        self.on_error = on_error
        self.stop = threading.Event()
        self.initial_attempt = threading.Event()
        self.thread = threading.Thread(target=self._run, name="holdings-timer", daemon=True)

    def start(self, *, wait_initial=False):
        self.thread.start()
        if wait_initial:
            self.initial_attempt.wait()

    def _run(self):
        due = time.monotonic()
        while not self.stop.wait(max(0, due-time.monotonic())):
            try:
                self.read()
            except Exception as error:
                if self.on_error:
                    self.on_error(error)
                else:
                    LOG.error("Holdings checkpoint failed (%s)", type(error).__name__)
            finally:
                self.initial_attempt.set()
            # Missed ticks coalesce. Broker slowness never creates catch-up reads.
            due += self.interval * max(1, math.floor((time.monotonic()-due)/self.interval)+1)

    def close(self):
        self.stop.set()
        self.initial_attempt.set()
        if self.thread.ident is not None and self.thread is not threading.current_thread():
            self.thread.join()
