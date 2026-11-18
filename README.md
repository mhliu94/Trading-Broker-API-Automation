# US stock broker scripts

The current scope is **US-listed stocks only**, including instruments the broker classifies as US equities. Crypto, tokenized stocks, options, futures, event contracts and non-US listings are not trading paths in this suite. Explicit non-stock commands reject before submission; positions, order history and cancellation are scoped to US stocks. Unknown instrument classification is resolved through metadata where available or reported as an error.

Seven separate groups: [Bitget Stock+](bitget/README.md), [Robinhood Trading MCP](robinhood/README.md), [Alpaca](alpaca/README.md), [TradeStation](tradestation/README.md), [Longbridge](longbridge/README.md), [Webull](webull/README.md), and [Zhuorui](zhuorui/README.md).

| Broker | US-stock connection | Native FOK | Setup distinction |
| --- | --- | --- | --- |
| Bitget | Stock+ `/api/v3/stockplus` | Not documented | Stock+ access; API key, secret and passphrase |
| Robinhood | Official Trading MCP | Not documented | OAuth bearer and an Agentic trading account; crypto keys do not apply |
| Alpaca | Trading API v2 | Eligible whole-share stocks | Paper by default; FOK entitlement/venue restrictions, no OTC FOK |
| TradeStation | Brokerage API v3 | Native duration, subject to route/account | SIM by default; OAuth token/refresh setup |
| Longbridge | AP HK/SG account API, trading US stocks | Not documented | App key/secret/access token; US-domiciled account APIs differ |
| Webull | Official US Trading API SDK | Not supported for stocks | US stock account and SDK authorization |
| Zhuorui | Pinned reference runtime | No verified native FOK | Windows DPAPI session and external checkout |

Every group has `market_order.py`, `limit_order.py`, `fok_order.py`, `positions.py`, `orders_today.py`, `capabilities.py`, `tradability.py`, `borrow_availability.py`, `cancel_order.py`, `cancel_open_orders.py`, `set_trading_enabled.py`, `recover_cancellations.py`, `command.py`, and `consume.py`. Robinhood also provides read-only `discover_tools.py` for its authenticated live tool schemas.

## Setup and examples

Use Python 3.11 or newer; validation used Python 3.14.

```bash
cd ~/broker-api-scripts
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
# Only for Webull:
.venv/bin/python -m pip install -r webull/requirements.txt
cp config.example.json config.json
```

Fill only the broker entries you use. `${ENV_VARIABLE}` values resolve from the environment, and only the selected broker's placeholders are required. Read each broker's README for its authentication format. Use a separate config for each account when running multiple accounts at one broker.

Set application `routing.account_id`/`account_num_id` to your Kafka producer's account labels. Broker account IDs remain separate broker-specific fields. Set `journal_account` to a permanent label identifying the exact account, product and paper/live environment. Preserve that label and `.state/` across restarts and credential rotation. The old crypto products must not reuse the same journal identity as the new Stock+/Robinhood stock accounts.

For the five Bitget accounts on separate servers, use each server's own
`~/bitget-configs/config.properties`: `string_id` and `numerical_id` must match
the local routing, and `kafka_ip` must match the Kafka bootstrap address.
Each account receives the command topic through its own group, ending in its
numeric account ID; different IP addresses alone do not provide this isolation.
This server runs `BITGET_DENGLEI` (`2`), while `BITGET_LYJ` (`1`) is a different
account. See [Bitget deployment](bitget/README.md#five-accounts-on-separate-servers)
for all five servers' setup, group naming, and upgrade instructions.

All examples default to offline preview. `--execute` performs broker reads or mutations; submissions also require local `trading_enabled:true`. The delivered config disables submissions.

```bash
.venv/bin/python alpaca/capabilities.py --config config.example.json
.venv/bin/python alpaca/limit_order.py --config config.example.json \
  --id limit-001 --symbol AAPL --side buy --quantity 1 --limit-price 100

# Read-only broker queries after configuration:
.venv/bin/python alpaca/positions.py --execute
.venv/bin/python alpaca/orders_today.py --execute
.venv/bin/python alpaca/tradability.py --symbol AAPL --execute
.venv/bin/python alpaca/borrow_availability.py --symbol AAPL --execute

# Running the configured Kafka worker performs its enabled account operations:
.venv/bin/python alpaca/consume.py --config config.json --execute
```

Use plain US tickers such as `AAPL`; Bitget and Longbridge normalize them to `.US` internally. Quantities are shares. A stable command ID is required for mutations. Reusing an ID returns its recorded result; changing the payload under that ID rejects. An uncertain operation is never blindly resubmitted, and it blocks new orders until recovered or reconciled against broker evidence.

Today's orders are orders created/submitted in `[New York midnight, next midnight)` by default, including open, completed, canceled and rejected records exposed by the broker. This does not include an older order solely because it filled today. Native query data is retained, and pagination or classification failures are explicit rather than silently returning a partial portfolio.

## FOK, borrowing and LULD

Native FOK remains the default. Where native stock FOK is unavailable, `fok_order.py` rejects explicitly. Setting `fok_policy:"timed_cancel"` enables the reference-style limit-then-cancel workflow for the six direct adapters. It targets cancellation after one second from dispatch and **can partially fill**; it is not exchange-atomic FOK. Zhuorui uses its separate `allow_timed_cancel_fok` option and upstream journal. See [runtime recovery](docs/runtime.md).

[Borrow API coverage](docs/borrow-availability.md) distinguishes short eligibility, ETB flags, available inventory, locate cost and recurring borrow rates. A missing API or field returns an explicit unavailable/unknown result, not zero. No query acquires a locate or opens a short. Robinhood's current Agentic workflow supports long equities; a shortability flag does not authorize a short order through that interface.

[Tradability and LULD notes](docs/luld.md) distinguish US listing eligibility from live halt state and auction routing. `auction_intent:"luld_reopening"` accepts only a regular-session limit DAY order, with no timed FOK/IOC substitution. Ordinary-order acknowledgement does not prove auction participation.

## Kafka and operations

The worker uses `trading-commands` on `45.32.121.19:9092` by default. [The command protocol](docs/kafka.md) documents legacy `MARKET_ORDER`, `LIMIT_ORDER`, `LIMIT_ORDER_FOK`, cancel controls, persistent trading enable/disable, and the position/order/tradability/borrow query controls.

Live workers publish `account-details` and KTrader v1 `order-status` snapshots at startup, periodically, and after financial actions. They include manual and completed US stock orders. Durable sequencing, pending-delivery recovery, local operation locks, graceful stop controls and exact-reference timed-cancellation recovery preserve the reference runner workflows. Publication failure never replays a completed trade.

```bash
.venv/bin/python alpaca/cancel_open_orders.py --id cancel-all-001
.venv/bin/python alpaca/set_trading_enabled.py --id disable-001 --enabled false
.venv/bin/python alpaca/recover_cancellations.py
.venv/bin/python tools/runner_control.py status --broker alpaca
.venv/bin/python tools/journal.py --broker alpaca
```

Add `--execute` deliberately to perform mutation/recovery commands. [Runtime operations](docs/runtime.md) explain process control, persistent state, startup recovery and authentication boundaries. Installing the repository alone does not start a worker; the Bitget deployment instructions describe the explicitly started service.

[Verification evidence](docs/verification.md) records the tests and their limits. Public API documentation and mocked contract tests were checked; authenticated order acceptance, account permissions, fills and auction routing remain untested without your actual account connection. No trades were placed. The last read-only Kafka inspection found both snapshot topics present and `trading-commands` empty at partition 0 (`begin=end=9238`); the legacy command shape follows the supplied reference repositories.

### Worker latency

The listener emits JSON events named `broker_latency` to its service log.
`command_received` records the Kafka timestamp, worker pickup timestamp and
their difference in milliseconds. `command_execution`, `broker_request` and
`report_delivery` record UTC Unix start/end milliseconds and monotonic
`elapsed_ms`. A short hash of the command ID links related events. Broker
timestamps retain their original precision. Pickup comparisons across hosts
include any difference between their clocks.
