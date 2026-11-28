from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

from common.reporting import RuntimeSnapshots


def test_external_gtc_order_is_followed_after_it_disappears_from_open_discovery(monkeypatch):
    runner=SimpleNamespace(adapter=Mock(), broker="alpaca")
    reporter=RuntimeSnapshots(runner,Mock())
    prior=datetime(2026,11,20,15,tzinfo=timezone.utc)
    today=datetime(2026,11,28,5,tzinfo=timezone.utc)
    discovery=Mock(side_effect=[[{"order_id":"gtc","submitted_at":prior.isoformat()}],[]])
    monkeypatch.setattr("common.cancellation._discover_open_orders",discovery)
    observed={"account_id":"A","order_id":"gtc","created_at":prior.isoformat(),"updated_at":today.isoformat(),
              "symbol":"ABC","side":"BUY","status":"NEW","quantity":10,"filled_quantity":0}
    history=Mock(side_effect=[[observed],[dict(observed,status="FILLED",filled_quantity=10)]])
    monkeypatch.setattr("common.reporting.collect_orders",history)
    first=reporter._older_orders(runner,"America/New_York",today,[])
    reporter.positions.observe(first)
    reporter.discover_at=0
    final=reporter._older_orders(runner,"America/New_York",today,[])
    reporter.positions.observe(final)
    assert not reporter.positions.working_orders()
    assert not reporter.prior_orders
    assert history.call_count==2
    assert final[0]["filled_quantity"]==10
