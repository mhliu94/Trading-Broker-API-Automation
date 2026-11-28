from datetime import datetime, timedelta, timezone
from threading import Event
import unittest
from unittest.mock import patch

from common.account_delta import AccountDeltaPublisher, AccountReplica
from common.holdings_timer import HoldingsTimer, PositionProjection


class PositionTests(unittest.TestCase):
    def setUp(self):
        self.clock = datetime(2026, 11, 28, tzinfo=timezone.utc)
        self.messages = []
        self.publisher = AccountDeltaPublisher()
        self.tracker = PositionProjection(
            lambda state, full: self.publisher.publish(state, self.messages.append,
                                                        force=full, automatic_checkpoint=False),
            now=lambda: self.clock)
        self.checkpoint(10)

    def snapshot(self, shares):
        return {"account_id":"A", "account_num_id":1, "cash":100, "ts":self.clock.isoformat(),
                "trading_enabled":True, "positions":[{"symbol":"XYZ","qty":shares,"avg_price":10,
                                                       "available_qty":shares}] if shares else []}

    def checkpoint(self, shares):
        self.tracker.checkpoint(self.tracker.begin(), self.snapshot(shares))
        self.clock += timedelta(seconds=1)

    def order(self, qty, *, side="BUY", status="PARTIALLY_FILLED", identity="first", avg=12):
        return {"account_id":"A","order_id":identity,"symbol":"XYZ","side":side,"status":status,
                "created_at":"2026-11-28T00:00:01Z","updated_at":self.clock.isoformat(),
                "quantity":10,"filled_quantity":qty,"average_fill_price":avg}

    def test_cumulative_fill_duplicates_partial_cancel_and_weighted_price(self):
        one=self.order(2);self.tracker.observe([one,one])
        self.clock+=timedelta(seconds=1)
        final=self.order(5,status="CANCELED",avg=13)
        self.tracker.observe([final,one,final])
        self.assertFalse(self.tracker.open_orders)
        self.assertEqual([m["message_type"] for m in self.messages],["account_snapshot","account_delta","account_delta"])
        self.assertEqual(self.messages[-1]["positions"][0]["qty"],15)
        self.assertAlmostEqual(self.messages[-1]["positions"][0]["avg_price"],(100+65)/15)
        self.assertNotIn("cash",self.messages[-1])
        replica=AccountReplica()
        for m in self.messages: replica.apply(m)
        self.assertEqual(replica.state["positions"][0]["qty"],15)

    def test_snapshot_cannot_double_apply_or_erase_an_overlapping_fill(self):
        token=self.tracker.begin()
        self.clock+=timedelta(seconds=1)
        self.tracker.observe([self.order(4)])
        self.tracker.checkpoint(token,self.snapshot(10))
        self.assertEqual(self.messages[-1]["positions"][0]["qty"],14)
        self.checkpoint(14)
        self.tracker.observe([self.order(4)])
        self.assertEqual(self.messages[-1]["positions"][0]["qty"],14)
        self.assertEqual(len(self.messages),4)

    def test_pre_checkpoint_terminal_and_gtc_high_water_marks(self):
        old=self.order(4);old["created_at"]="2026-11-26T10:00:00Z"
        self.tracker.observe([old])
        self.assertEqual(len(self.messages),1)
        self.assertEqual(len(self.tracker.open_orders),1)
        self.clock+=timedelta(seconds=1)
        self.tracker.observe([dict(old,filled_quantity=6,status="FILLED",
                                   updated_at=self.clock.isoformat())])
        self.assertEqual(self.messages[-1]["positions"][0]["qty"],12)
        self.assertFalse(self.tracker.open_orders)

    def test_sale_to_zero_removes_the_position_and_retains_cash(self):
        self.tracker.observe([self.order(10,side="SELL",status="FILLED")])
        self.assertEqual(self.messages[-1]["removed_positions"],["XYZ"])
        self.assertNotIn("cash",self.messages[-1])
        self.checkpoint(0)
        self.assertEqual(self.messages[-1]["positions"],[])
        self.assertEqual(self.messages[-1]["cash"],100)

    def test_old_snapshot_completion_and_duplicate_delivery_cannot_roll_back_fills(self):
        slow=self.tracker.begin()
        fast=self.tracker.begin()
        self.tracker.checkpoint(fast,self.snapshot(12))
        self.tracker.checkpoint(slow,self.snapshot(1))
        self.assertEqual(self.messages[-1]["positions"][0]["qty"],12)
        send=self.tracker.publish
        failures=[]
        def fail_once(state,full):
            if not failures:
                failures.append(True)
                raise OSError("temporary delivery failure")
            return send(state,full)
        self.tracker.publish=fail_once
        with self.assertRaises(OSError):
            self.tracker.observe([self.order(2)])
        self.tracker.observe([self.order(2)])
        self.assertEqual(self.messages[-1]["positions"][0]["qty"],14)
        self.assertEqual(self.messages[-1]["base_sequence"],2)

    def test_slow_timer_does_not_block_order_updates(self):
        entered,release=Event(),Event()
        def read():
            token=self.tracker.begin()
            entered.set()
            release.wait(5)
            self.tracker.checkpoint(token,self.snapshot(10))
        timer=HoldingsTimer(read,30)
        timer.start()
        try:
            self.assertTrue(entered.wait(2))
            self.tracker.observe([self.order(1)])
            self.assertEqual(self.messages[-1]["positions"][0]["qty"],11)
        finally:
            release.set()
            timer.close()
        self.assertEqual(self.messages[-1]["positions"][0]["qty"],11)

    def test_timer_skips_missed_ticks_and_uses_fixed_start_times(self):
        now=[0];calls=[]
        def read():
            calls.append(now[0])
            now[0]+=38 if len(calls)==2 else 2
        timer=HoldingsTimer(read,30)
        def wait(delay):
            now[0]+=delay
            return len(calls)==3
        with patch.object(timer.stop,"wait",side_effect=wait), patch("time.monotonic",side_effect=lambda:now[0]):
            timer._run()
        self.assertEqual(calls,[0,30,90])
