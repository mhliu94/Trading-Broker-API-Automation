# Kafka runtime and recovery

The worker uses the GigaMoney/Zhuorui command and snapshot workflows for US stocks only while retaining each broker's actual API capabilities. It does not start an emulator, install a service, or place a verification trade during setup.

## Run and control

Start one logical worker per configured broker account:

```bash
.venv/bin/python alpaca/consume.py --config config.json --execute
```

The local account operation lock serializes CLI actions, worker actions and snapshot reads. A separate runner lock prevents two local workers from processing the same account concurrently. Preserve the command and publication databases. Independent machines with separate databases do not provide account-wide duplicate protection.

The example configuration uses a five-second delay before each direct API order/cancellation, under the account operation lock. Queued delays accumulate. Zhuorui keeps its upstream five-second delay and does not add another. Set `action_delay_seconds` explicitly to adjust this. A fresh timestamp check runs after waiting, before dispatch.

The worker handles `MARKET_ORDER`, `LIMIT_ORDER`, `LIMIT_ORDER_FOK`, `CANCEL_ORDER`, `CANCEL_OPEN_ORDERS`, `SET_TRADING_ENABLED`, positions, today's orders, capabilities, tradability, and borrow availability. The legacy `ts` timestamp and boolean `cancel_unfilled` flag are recognized. Conflicting fields and aliases reject instead of silently changing an instruction.

`SET_TRADING_ENABLED` changes durable runtime state. Disabling blocks new submissions but permits reads and cancellations. Remote enable cannot override a locally disabled `trading_enabled` setting. Use a new command ID for each intended state transition; replaying an older enable cannot undo a later disable.

```bash
.venv/bin/python alpaca/set_trading_enabled.py --id disable-001 --enabled false --execute
.venv/bin/python alpaca/cancel_order.py --id cancel-001 --order-id EXACT_BROKER_ID --execute
.venv/bin/python alpaca/cancel_open_orders.py --id cancel-all-001 --execute
.venv/bin/python tools/runner_control.py status --broker alpaca
.venv/bin/python tools/runner_control.py stop --broker alpaca
```

SIGINT, SIGTERM and the stop-file control request shutdown after the current operation. A stop request does not cancel broker orders. Health records show the last result/refresh and whether authentication or reconciliation is required. Linux status checks compare process-start identity, avoiding a recycled PID; other platforms explicitly report when that live-process check is unavailable. Custom status/stop paths can be set in Kafka configuration; the helper's default paths use `.state/<broker>.*`.

For direct broker adapters, cancel-all discovery finishes before any cancellation writes. Discovery failures reject only that command with `requests_sent:0` and `submission_outcome:not_submitted`; they do not create an unknown journal entry or stop the listener. Authentication failures still stop intake, and uncertain writes still require reconciliation. Zhuorui's upstream combined cancellation operation retains its own outcome handling. Unknown entries from older versions require evidence-based review; a code upgrade alone does not resolve them.

## Account and order reporting

With `kafka.publish_snapshots:true` (the live default), startup and periodic refreshes publish account snapshots to `account-details` and full order snapshots to `order-status`. The example interval is 30 seconds, scheduled from completion of the previous refresh attempt. After an order/cancellation, the worker commits the processed input and waits two seconds before refreshing; reporting failure never resubmits that financial command. Persistent trading controls and explicit account/order queries also refresh reporting.

Bitget can opt into independent monitoring with `brokers.bitget.monitor.enabled`.
This replaces its periodic/post-action snapshot loop with order, account, and
reconciliation workers, a shared Stock+ request budget, and JSON latency logs.
Order updates are published promptly without waiting for account queries. See
[Bitget monitoring](../bitget/README.md#order-monitoring-and-latency-statistics)
for defaults, quota limits, latency definitions, and the statistics command.

Output topics must already exist. The worker verifies them before sending; it does not provision topics. `result_topic`, when configured, must be separate from the command/account/order topics. Live and preview consumer groups remain distinct. Preview mode does not publish account/order data or call brokers.

Order snapshots follow KTrader v1: exact string broker IDs, immutable creation times, cumulative fills, distinct lifecycle states, and actual average execution prices. Broker acknowledgement is never treated as a fill. External/manual orders and today's completed orders are included. One record's mapping failure does not discard valid siblings; broker-wide read failures remain explicit. Multi-leg orders that cannot be represented by the single-symbol schema are reported as mapping errors while the raw `orders_today.py` query remains available.

Order delivery uses a separate SQLite outbox. It saves the full payload and sequence before sending, acknowledges only confirmed Kafka deliveries, and retries an identical pending payload after failure/restart. Unchanged observations do not increment sequence. Unchanged orders are republished once after process restart when seen again. Today's pending messages retry even if absent from a new broker response. This follows GigaMoney's current-day policy: prior/future-date pending records are retained for inspection but not replayed. It does not infer that disappearing orders were canceled.

The reference account consumer requires numeric cash. Account mappings preserve broker meaning:

- Alpaca and TradeStation use reported cash balances.
- Longbridge reports available native-currency cash with an explicit basis, preserving the broker's reported consolidated totals separately.
- Webull uses per-currency cash balances.
- Bitget Stock+ uses reported USD totalCash and separately reported available cash, with stock share positions.
- Robinhood Trading MCP uses reported portfolio cash (including any early access) and retains buying power separately. It no longer uses the crypto buying-power proxy.
- Zhuorui reuses the verified snapshot mappings from the pinned external runtime and verifies account bindings before publication.

Unknown cost basis remains null; a broker's negative diluted position cost remains signed. Available position quantities use the consumer's `available_qty` field. Money/fills are validated, not filled in with invented prices or quantities.

## Native FOK and optional timed cancellation

Native FOK is the default and uses only combinations supported by the broker. For the six direct API adapters, setting `fok_policy:"timed_cancel"` explicitly enables the reference-style limit-then-cancel workflow for FOK commands. An offline preview shows the actual submitted DAY/GTC request, `native_fok:false`, and `partial_fills_possible:true`. Zhuorui continues to use its own `allow_timed_cancel_fok` option and upstream journal.

The timer targets `cancel_after_seconds` from submission dispatch (default one second). Network/durable-state latency can miss the target. The workflow persists intent before submission, saves only an unambiguous accepted broker reference, then validates that exact account/ticket before cancellation. It never selects an order using only symbol, quantity or timing. It queries terminal state after a cancellation acknowledgement. An acknowledged but still-open order remains unresolved.

A crash/timeout after possible cancellation dispatch causes read-only recovery, not another cancellation write. A confirmed order with a never-attempted cancellation may be canceled on recovery. Unknown submissions without a confirmed reference are never resubmitted or guessed. LULD reopening intent disallows timed cancellation because that policy cannot wait for the intended auction.

```bash
# Preview recovery; no broker operation:
.venv/bin/python alpaca/recover_cancellations.py --config config.json
# Recover existing known work; never creates a new order:
.venv/bin/python alpaca/recover_cancellations.py --config config.json --execute
```

Live worker startup performs this recovery before intake. Durable terminal evidence also resolves an outer command left pending by a crash between the two journal commits. Unresolved account work blocks new submissions and stops the worker for reconciliation; reads and risk-reducing maintenance remain available through the CLI.

## Uncertain outcomes and credentials

`tools/journal.py --broker alpaca` lists uncertain command records and pending timed cancellations. Verify the exact account, broker/client order ID and outcome using broker evidence before recording reconciliation:

```bash
.venv/bin/python tools/journal.py --broker alpaca --command-id original-command \
  --outcome canceled --broker-order-id EXACT_BROKER_ID \
  --evidence 'Reviewed broker order detail showing this exact ID canceled' --execute
```

This records your reviewed evidence; it does not independently prove it and never sends a trade. `found` on a previously unidentified timed submission retains cancellation responsibility and requires exact-ticket verification during recovery. An uncertain cancellation is never reset into a new cancellation attempt. Contradictory replacement of a persisted broker ID is rejected. Ordinary native-order uncertainty likewise requires reconciliation before a new command ID is used.

A transport-authentication refusal that proves no submission was accepted releases only that safe submission reservation. The worker stops without committing the input so credentials can be repaired and the worker explicitly restarted; normal staleness checks still apply. Timeouts, server failures, or authentication problems after an accepted submission never receive this retry permission. They remain unresolved. Snapshots also stop intake on authentication failures rather than presenting stale account data as current.

## Deployment boundary

No worker is launched by this deliverable. Cancellation discovers and verifies stock records before sending individual requests; an account-wide delete that could touch other assets is not used. Instrument metadata, when unavailable, prevents claiming complete classification.

A service can execute the same `consume.py --execute` command with a private config/environment and preserved state directory. Use graceful shutdown and restart only after checking authentication/reconciliation status. Source-backed tests cover these control, transport and recovery contracts, but authenticated broker permissions, fills and auction participation still require the actual account setup.
