class BrokerError(Exception):
    """Broker or transport failure; never blindly retry a write."""


class AuthenticationError(BrokerError):
    """Credentials/access failed; stop intake until access is repaired.

    ``no_submission`` is evidence that this operation did not accept a trading
    write. It must never be inferred from a timeout, an ambiguous response, or
    an authentication failure after an earlier successful write.
    """
    def __init__(self, message, *, no_submission=False):
        super().__init__(message)
        self.no_submission = no_submission is True

    @property
    def safe_to_retry_submission(self):
        return self.no_submission


class UnsupportedOrder(BrokerError):
    """This broker/asset cannot preserve the requested order semantics."""


class ConfigurationError(BrokerError):
    """Missing or invalid local configuration."""


class CommandError(BrokerError):
    """Malformed, conflicting, stale or incorrectly routed command."""


class OutcomeUnknown(BrokerError):
    """An order may have reached the broker; reconcile before taking further action."""


class RequestRejected(BrokerError):
    """Positive evidence that this request did not accept a trading operation.

    Only adapters that can establish rejection may raise this. Timeouts,
    internal errors, contradictory responses and decoding failures do not qualify.
    The failed command stays cached and must never be automatically replayed.
    """
    def __init__(self, message, *, code=None, http_status=None, requests_sent=1):
        super().__init__(message)
        self.code = code
        self.http_status = http_status
        self.requests_sent = requests_sent


class CancellationDiscoveryError(BrokerError):
    """Open-order discovery failed before any cancellation write was attempted."""
