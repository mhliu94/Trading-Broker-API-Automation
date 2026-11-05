"""Stock+ trade quota shared by the monitor and local command processes.

Conservatively share the published 30 requests / 30 seconds across trade
endpoints. Background reads cannot consume the final five request slots.
"""
from contextlib import contextmanager
import json
import math
import os
from pathlib import Path
import threading
import time

from common.errors import BrokerError, ConfigurationError
from common.locking import operation_lock


class MonitorStopped(BrokerError):
    pass


class TradeRateLimiter:
    limit = 30
    window = 30.0
    spacing = 0.02

    def __init__(self, path, reserve=5):
        if type(reserve) is not int or not 5 <= reserve < self.limit:
            raise ConfigurationError("monitor.command_reserve must be an integer from 5 to 29")
        self.path = Path(path)
        self.reserve = reserve
        self.lock = threading.Lock()
        # Monotonic timestamps are shared across local processes, but not boots.
        self.boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()

    @contextmanager
    def _state(self):
        with self.lock, operation_lock(self.path.with_suffix('.lock'), timeout=5):
            if self.path.exists():
                try:
                    data = json.loads(self.path.read_text())
                    if (not isinstance(data.get('boot'), str)
                            or not isinstance(data.get('requests'), list)
                            or not all(type(t) in (int, float) and math.isfinite(t) for t in data['requests'])
                            or type(data.get('cooldown')) not in (int, float)
                            or not math.isfinite(data['cooldown'])):
                        raise ValueError
                except (ValueError, TypeError, AttributeError):
                    raise ConfigurationError("Invalid Stock+ quota state; inspect before resetting") from None
            else:
                data = {}
            if data.get('boot') != self.boot:
                data = {'boot': self.boot, 'requests': [], 'cooldown': 0.0}
            yield data
            temporary = self.path.with_name(self.path.name + f'.{os.getpid()}.tmp')
            try:
                temporary.write_text(json.dumps(data))
                os.chmod(temporary, 0o600)
                os.replace(temporary, self.path)
            finally:
                temporary.unlink(missing_ok=True)

    def acquire(self, *, background=False, low_priority=False, stop=None):
        started = time.monotonic()
        while True:
            if stop is not None and stop.is_set():
                raise MonitorStopped("Stock+ monitor stopped")
            with self._state() as state:
                now = time.monotonic()
                recent = sorted(t for t in state['requests'] if now - t < self.window)
                state['requests'] = recent
                allowance = self.limit - self.reserve if background else self.limit
                # Give live polls a brief preference, but never indefinitely
                # starve reconciliation when the steady query budget is full.
                if low_priority and now - started < 1.2:
                    allowance = max(1, allowance - 6)
                delay = max(0, state['cooldown'] - now,
                            recent[-1] + self.spacing - now if recent else 0,
                            recent[len(recent) - allowance] + self.window - now
                            if len(recent) >= allowance else 0)
                if delay <= 0:
                    recent.append(now)
                    return (now - started) * 1000
            # Release both locks while waiting so a command can take its reserve.
            if stop is not None:
                stop.wait(min(delay, 0.25))
            else:
                time.sleep(min(delay, 0.25))

    def throttle(self, retry_after=None):
        try:
            seconds = float(retry_after)
            if not math.isfinite(seconds):
                raise ValueError
        except (ValueError, TypeError):
            seconds = self.window
        with self._state() as state:
            state['cooldown'] = max(state['cooldown'], time.monotonic() + max(1, min(seconds, 300)))

    def wait_cooldown(self, stop=None):
        """Asset/quote reads respect a 429 without spending the trade quota."""
        started = time.monotonic()
        while True:
            if stop is not None and stop.is_set():
                raise MonitorStopped("Stock+ monitor stopped")
            with self._state() as state:
                delay = state['cooldown'] - time.monotonic()
            if delay <= 0:
                return (time.monotonic() - started) * 1000
            if stop is not None:
                stop.wait(min(delay, .25))
            else:
                time.sleep(min(delay, .25))
