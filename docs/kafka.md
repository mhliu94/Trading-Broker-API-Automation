# Kafka command protocol

Default bootstrap: `45.32.121.19:9092`; input topic: `trading-commands`. Configuration accepts standard Kafka security protocol, SASL and TLS fields. No credentials or TLS mode were inferred from the reachable plaintext metadata service.

The inspection tool assigns partitions directly, uses no consumer group, disables automatic commits, and reads bounded historical samples without creating topics. The server reported these topics: `account-details`, `actions`, `aggregate-status`, `client-requests`, `order-status`, `price-books`, `server-status`, `stock-quotes`, `trading-commands`, `trading-volume`. The three inspected command-related topics had no retained messages. Therefore no claim is made that live producer payloads were observed or end-to-end executions were verified.

Command field mapping is based on the supplied [Zhuorui KTrader contract](https://github.com/mhliu94/Zhuorui-Securities-Automation/blob/002aa6866b246dbc4c1f5e1f647019abc170080d/docs/api.md#kafka-command-contract) and parser interfaces. We did not copy its implementation. The query actions below extend that contract for these new scripts.

| Action | Accepted `type` / `action` examples | Required action fields |
| --- | --- | --- |
| Market order | `MARKET_ORDER`, `market` | `symbol`, `side`, `qty_shares` or `quantity` |
| Limit order | `LIMIT_ORDER`, `limit` | Above plus `limit_price` |
| FOK limit | `LIMIT_ORDER_FOK`, `fok` | Above plus `limit_price` |
| Cancel one | `CANCEL_ORDER`, `cancel_order` | Exact broker `order_id`; some brokers require `symbol` |
| Cancel open orders | `CANCEL_OPEN_ORDERS`, `cancel_open_orders` | None; configured-account scope |
| Trading state | `SET_TRADING_ENABLED`, `set_trading_enabled` | Boolean `trading_enabled` |
| Positions | `QUERY_POSITIONS`, `positions`, `holdings` | None; queries all positions |
| Today's orders | `QUERY_ORDERS`, `orders_today`, `GET_TODAY_ORDERS` | None; queries all today's orders |
| Capabilities | `GET_CAPABILITIES`, `capabilities` | None |
| Tradability | `TEST_TRADABLE`, `tradability`, `CHECK_SYMBOL` | `symbol` |
| Borrow availability | `QUERY_BORROW_AVAILABILITY`, `borrow_availability`, `SHORT_AVAILABILITY` | `symbol` |

Bitget also accepts `symbol` on `CANCEL_OPEN_ORDERS`, as sent by Smart buying/selling.
It cancels only matching US-stock orders in the configured account, after complete
open-order discovery. Omitting `symbol` keeps the account-wide scope. An `order_id`
requires `CANCEL_ORDER`. A cancellation acknowledgement is not a terminal result;
the order-status feed confirms the eventual fill or cancellation.

Kafka commands require `account_id` or `account_num_id`. `broker` and `server_id` may also restrict routing. All supplied selectors must match the selected config. Another target is ignored. A numeric routing ID in `account_id` is accepted, as in KTrader. Query controls can send the same account-addressed envelopes as trading controls.

```json
{"id":"order-101","account_num_id":3,"type":"LIMIT_ORDER_FOK","symbol":"AAPL","side":"buy","qty_shares":1,"limit_price":"100.00"}
```

```json
{"id":"borrow-101","account_num_id":3,"action":"QUERY_BORROW_AVAILABILITY","symbol":"AAPL"}
```

```json
{"id":"halt-limit-101","account_num_id":3,"type":"LIMIT_ORDER","symbol":"AAPL","side":"buy","qty_shares":1,"limit_price":"100.00","time_in_force":"day","asset_class":"us_equity","auction_intent":"luld_reopening"}
```

Aliases include `command_id`/`commandId`, `ticker`/`code`, `qty`/`shares`, `price`/`limitPrice`, `timeInForce`/`tif`, `accountId`/`accountNumId`, and `serverId`. Conflicting aliases, nonfinite numbers, duplicate JSON keys, and ambiguous query/order mixtures reject. Quantity is explicit; common notional sizing is not implemented. Cancellation is account-scoped and does not infer broker identity from a symbol/quantity match. `cancel_unfilled:true` is a strict FOK-intent alias; its actual native or timed behavior follows the configured FOK policy. Unknown actions reject instead of becoming market orders. `options` holds broker-specific typed settings; each adapter rejects unsupported options.

Producer IDs are preferred. If missing, the worker uses `topic:partition:offset`. New submissions must pass the configurable age limit (default 120 seconds); the Kafka record timestamp and any supplied `ts`/`issued_at`/`timestamp` are checked. `expires_at` is enforced. Cached duplicate results are returned even after the original command becomes stale; pending/unknown submissions never repeat automatically.

Every account/broker/server has a separate consumer group so filtering does not cause another broker to lose its messages. The group is `<group_prefix>.<broker>.<account_id-or-account_num_id>.<server_id-or-default>.<live-or-preview>`, with `.<account_num_id>` appended whenever a numeric routing ID is configured. For example, account `BITGET_DENGLEI` (`2`) uses `broker-api-scripts.bitget.BITGET_DENGLEI.default.live.2`. Each of the five Bitget accounts must have unique string and numeric routing IDs and its own group; separate server IPs alone do not separate subscriptions. Each group receives the full stream and filters for its configured account. To target multiple accounts, send one addressed command per account.

Live and preview groups are distinct. Default offset reset is `latest`. Adding the numeric suffix creates a new group for existing numeric-account deployments; its offsets are not inherited. Stop the old worker and transfer its committed offsets before starting the replacement when preserving queued commands is required, and keep its journals. Workers without numeric IDs retain their existing group names. One account addressed without `server_id` may reach multiple server groups: use one logical executor or a shared SQLite journal, and do not run independent host journals for the same account. The broker's client order ID is deterministic where supported, but these scripts do not promise distributed exactly-once trading.

For order executions, SQLite reserves the ID before dispatch and stores the result before the input offset is committed. Unexpected exit during dispatch leaves a pending reservation requiring reconciliation. Read-only queries always read fresh data and may repeat after delivery failure. The consumer commits only the individual processed partition offset. Results are printed as JSON. Optional `kafka.result_topic` publishes results to a separate existing topic and waits for acknowledgement before committing the input. Duplicate result messages are possible; deduplicate using command ID/message identity. Publication failure stops consumption without committing the record, permitting safe cached-result redelivery for orders.

`result_topic` is unset by default. Do not send command results into `order-status` or `account-details`: those KTrader topics have broker-snapshot schemas. The live worker publishes these snapshot formats on startup, periodically and after relevant actions. Query responses still return broker data in a separate command-result envelope; they are not placed into the snapshot topics. A result such as `submitted` is an acknowledgement, not a filled order.


Live workers perform known pending-cancellation recovery before intake. A new order is blocked while any account command or timed cancellation remains unresolved. `tools/journal.py` inspects that state and can record explicitly reviewed broker evidence; it never submits an order. `recover_cancellations.py --execute` may send a never-attempted cancellation for a confirmed exact reference, but never repeats an ambiguous cancellation or submission.

See [runtime operations](runtime.md) for the account/order outbox, current-day retry policy, graceful shutdown and authentication boundaries. The reference implementations have broker-specific extended-hours conversions; this suite retains native market behavior and explicit extended-hours limit settings rather than silently replacing market orders with a global price-book strategy.


## US-stock-only deployment

All groups now enforce asset_class us_equity and market US. Plain US tickers are accepted; Bitget Stock+ and Longbridge normalize them to .US. Crypto, derivatives and non-US records cannot be selected for submission or cancellation. Position/history queries report the US-stock subset with explicit errors for unresolvable classification. Bitget uses Stock+ API credentials; Robinhood uses an OAuth Agentic equity account, not Crypto API keys. Native FOK support is stock-specific; the explicit timed policy remains distinct.
