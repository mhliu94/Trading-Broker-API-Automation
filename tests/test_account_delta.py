from copy import deepcopy
import random
import unittest
from common.account_delta import AccountDeltaPublisher, AccountReplica, OrderChanges, normalize

def snapshot():
    return {'account_id':'TEST','account_num_id':29,'cash':100.0,'cash_by_currency':{'USD':100,'HKD':20},
        'available_cash_by_currency':{'USD':80},'ts':'2026-11-11T12:00:00+00:00','trading_enabled':True,
        'positions':[{'symbol':'AAA','qty':10,'avg_price':2}, {'symbol':'BBB','qty':4,'avg_price':3}]}

class DeltaTests(unittest.TestCase):
    def setUp(self):
        self.clock=[0]
        self.publisher=AccountDeltaPublisher(clock=lambda:self.clock[0])
        self.events=[]
        self.state=snapshot()
        self.publisher.publish(self.state,self.events.append)
        self.replica=AccountReplica()
        self.replica.apply(self.events[-1])

    def publish(self):
        self.publisher.publish(self.state,self.events.append)
        return self.events[-1]

    def test_partial_fill_and_full_close_preserve_unrelated_position(self):
        self.state['positions'][0]['qty']=7
        self.state['cash']=self.state['cash_by_currency']['USD']=106
        event=self.publish()
        self.assertEqual(event['message_type'],'account_delta')
        self.assertEqual([x['symbol'] for x in event['positions']],['AAA'])
        self.assertEqual(self.replica.apply(event),normalize(self.state))
        self.state['positions'].pop(0)
        event=self.publish()
        self.assertEqual(event['removed_positions'],['AAA'])
        self.assertEqual(self.replica.apply(event),normalize(self.state))

    def test_cash_zero_currency_removal_and_short_position(self):
        self.state['cash']=self.state['cash_by_currency']['USD']=0
        del self.state['cash_by_currency']['HKD']
        self.state['available_cash_by_currency']={}
        self.state['positions'][0]['qty']=-2
        event=self.publish()
        self.assertIsNone(event['cash_by_currency']['HKD'])
        self.assertEqual(event['cash'],0)
        self.assertEqual(self.replica.apply(event),normalize(self.state))

    def test_unchanged_and_row_reordering_suppressed_until_checkpoint(self):
        self.state['positions'].reverse()
        self.state['ts']='2026-11-11T12:00:01+00:00'
        self.publish()
        self.assertEqual(len(self.events),1)
        self.clock[0]=30
        self.assertEqual(self.publish()['message_type'],'account_snapshot')

    def test_duplicates_and_out_of_order_are_ignored(self):
        baseline=self.events[0]
        self.state['cash']=self.state['cash_by_currency']['USD']=98
        event=self.publish()
        expected=self.replica.apply(event)
        self.assertIsNone(self.replica.apply(event))
        self.assertIsNone(self.replica.apply(baseline))
        self.assertEqual(self.replica.state,expected)

    def test_gap_requires_checkpoint(self):
        self.state['positions'][0]['qty']=8
        missed=self.publish()
        self.state['positions'][0]['qty']=6
        latest=self.publish()
        self.assertIsNone(self.replica.apply(latest))
        self.assertTrue(self.replica.needs_snapshot)
        self.assertIsNone(self.replica.apply(missed))
        self.clock[0]=30
        self.assertEqual(self.replica.apply(self.publish()),normalize(self.state))
        self.assertFalse(self.replica.needs_snapshot)

    def test_cold_consumer_waits_for_full(self):
        self.state['positions']=[]
        cold=AccountReplica()
        self.assertIsNone(cold.apply(self.publish()))
        self.clock[0]=30
        self.assertEqual(cold.apply(self.publish())['positions'],[])

    def test_restart_retires_old_stream(self):
        old=self.events[0]
        new=AccountDeltaPublisher()
        state=deepcopy(self.state); state['ts']='2026-11-11T12:01:00Z'
        new.publish(state,self.events.append)
        self.replica.apply(self.events[-1])
        late=dict(old,sequence=99,ts='2026-11-11T12:02:00Z')
        self.assertIsNone(self.replica.apply(late))

    def test_uncertain_ack_retries_exact_event_before_new_state(self):
        attempts=[]
        def failed(event):
            attempts.append(event)
            raise TimeoutError()
        self.state['positions'][0]['qty']=9
        with self.assertRaises(TimeoutError): self.publisher.publish(self.state,failed)
        self.assertEqual(self.publisher.sequence,1)
        self.state['positions'][0]['qty']=8
        self.publish()
        self.assertEqual(self.events[1],attempts[0])
        self.assertEqual([e['sequence'] for e in self.events],[1,2,3])
        for event in self.events[1:]: self.replica.apply(event)
        self.assertEqual(self.replica.state,normalize(self.state))

    def test_malformed_and_nonfinite_never_advance_baseline(self):
        for bad in [float('nan'),float('inf'),True]:
            self.state['cash']=bad
            with self.assertRaises((ValueError,TypeError)): self.publish()
        self.assertEqual(self.publisher.sequence,1)
        malformed=dict(self.events[0],sequence=2,positions=[{'symbol':'AAA'}])
        with self.assertRaises((ValueError,KeyError)): self.replica.apply(malformed)
        self.assertEqual(self.replica.sequence,1)

    def test_trading_state_and_available_quantity(self):
        self.state['trading_enabled']=False
        self.state['positions'][0]['available_qty']=0
        self.assertEqual(self.replica.apply(self.publish()),normalize(self.state))

    def test_order_change_detection_includes_same_status_partial_fills(self):
        tracker=OrderChanges()
        row={'order_id':'1','status':'PARTIALLY_FILLED','filled_quantity':1}
        self.assertTrue(tracker.observe([row]))
        self.assertFalse(tracker.observe([row]))
        self.assertTrue(tracker.observe([dict(row,filled_quantity=2)]))
        self.assertFalse(tracker.observe([row]))
        self.assertTrue(tracker.observe([dict(row,status='CANCELED',filled_quantity=2)]))

    def test_random_state_sequences_reconstruct_exactly(self):
        rng=random.Random(42)
        for index in range(150):
            self.state['cash']=self.state['cash_by_currency']['USD']=rng.randint(-20,200)
            self.state['positions']=[{'symbol':symbol,'qty':rng.randint(-5,20),'avg_price':1.5}
                for symbol in ('AAA','BBB','CCC') if rng.choice([True,False])]
            self.clock[0]=index
            self.replica.apply(self.publish())
            self.assertEqual(self.replica.state,normalize(self.state))

if __name__=='__main__': unittest.main()
