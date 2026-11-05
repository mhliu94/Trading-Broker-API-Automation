# Bitget Stock+ US stocks

This group uses the official **Stock+ securities API** at `/api/v3/stockplus`. It supports US stock market and limit orders, position queries, today's orders, cancellation, account/order snapshots and instrument checks. Crypto, Reality/rToken, CFDs, options and foreign-listed securities are excluded. Older `account_type:classic_spot` and crypto-borrow settings fail configuration validation.

Use an API key, secret and passphrase with Stock+ trading/account read permissions, trade-write permission for orders, and Stock+ market-data access for instrument checks. Complete the Stock+ account onboarding and any market-data entitlement requirements with Bitget. `config.example.json` shows the settings; `base_url` defaults to `https://api.bitget.com` and request timeout to 20 seconds.

Supply `AAPL` or `AAPL.US`; the broker payload always uses `AAPL.US`. Quantities are shares for buys and sells, including market buys. Fractional quantities remain decimal strings. A live placement first verifies the requested instrument using Stock+ basic information. `build_order` itself is offline. Quotes/instrument metadata do not prove account-specific trading permission or order acceptance.

Market and limit orders support `day`, `gtc` and `gtd`. GTD requires `options.expire_date` in `YYYY-MM-DD` format. **Native FOK and IOC are not documented and are rejected**; timed cancellation, when separately selected through the shared runtime, allows partial fills and is not true FOK. Other broker-documented conditional order types are reported in capabilities but not submitted by these scripts.

Set `default_outside_rth:"Auto"` in the Bitget configuration to select the session for new market and limit orders automatically. Between 20:00 (inclusive) and 04:00 (exclusive) in `America/New_York`, new orders use `outsideRth:Overnight`; during the remaining hours they use `AnyTime` for regular, pre-market and after-hours trading. New York daylight-saving time is handled automatically. The example configuration enables this policy. Configurations without this setting retain the previous `AnyTime` default. Session selection is a routing instruction, not a market-open or stock-eligibility guarantee; Bitget still applies its symbol, order-type, holiday and venue rules. Previously submitted orders keep their existing session setting.

Overnight orders require an explicit limit price in this adapter. A live overnight market request returned HTTP 400, so `MO` with `Overnight` is now rejected locally before any broker write; use `LIMIT_ORDER` with `limit_price`. Market orders in regular/pre/post sessions remain `MO`, and limit orders remain `LO` with the supplied price. An order is never silently converted to another type. This is an adapter restriction based on the failed live request, not a claim that the Stock+ API documents a universal market-order prohibition.

To restrict an order to regular hours, use `options:{"extended_hours":false}` or `options:{"outside_rth":"RTHOnly"}`. An explicit `outside_rth` accepts `Auto`, `AnyTime` and `Overnight` (or `AUTO`, `ANY_TIME`, `OVERNIGHT`) and overrides the configured default. With the automatic policy enabled, `extended_hours:true` also selects the current session; otherwise it retains `AnyTime`. An explicit overnight session is compatible with `extended_hours:true`; regular-only and extended-hours flags that disagree are rejected. LULD intent defaults to regular hours and rejects an extended-session override, using the shared halt controls. No dedicated Stock+ LULD reopening auction route or guaranteed participation is documented; ordinary eligible limits must not be described as an auction guarantee.

An explicit overnight limit command can use `"options":{"outside_rth":"Overnight"}`. With `default_outside_rth:"Auto"`, the existing KTrader commands need no new field during the overnight window.

A deterministic `remark` (`cmd:` plus a command-ID hash) correlates submissions. It is **not broker idempotency**. The docs mention `clientOid` in responses/cancellation but omit it from the placement request, so this adapter does not invent one. The local journal provides duplicate protection. Unknown write outcomes require reconciliation; writes are never automatically retried.

Orders for a requested day combine `today-orders` and `history-orders`, deduplicate by exact ID and filter `submittedAt` to the configured day's UTC bounds. Historical `hasMore` splits the inclusive integer-second interval recursively. Live history responses may omit `hasMore` on empty or nonempty pages. With the flag absent, fewer than the documented 1000-order limit establishes a complete page; a full page triggers interval subdivision. An explicitly supplied flag must be boolean. A single second that cannot establish completeness, malformed pagination metadata, excessive windows, or rejected history range fails explicitly. Open-order cancellation queries history from Unix epoch through now as well as today's endpoint, preserving older GTC orders. Broker-side retention cannot be enlarged by this script; a server rejection is not silently replaced with a shorter range.

Cancel-all discovers and validates the entire order list before sending cancellations. A failure during those reads rejects that command with `requests_sent:0` and `submission_outcome:not_submitted`, allowing the listener to continue. Authentication failures still stop intake. Once a cancellation write has an uncertain outcome, the listener still requires reconciliation and never retries it automatically. Existing journal entries marked unknown by older code remain blocked until reviewed; upgrading does not clear them or replay the original cancel-all command.

`positions` reads every account channel from `stock-position` and includes only US stock symbols; snapshots retain the channel, signed share quantity and reported cost. Cancellation resolves an exact order detail, checks the requested symbol and stock instrument, and only then sends a cancel. The acknowledgement alone does not prove cancellation or fills.

If a cancellation acknowledgement is incomplete or mismatched, the adapter reads the exact target order once. A verified terminal result resolves that target; an open order, failed read, or wrong identity remains unknown and stops intake for reconciliation. The cancellation write is never retried by this check.

The borrow control returns `supported:false`, `status:unavailable` and unknown quantity/rate/shortability. Stock+ supports ETB short selling, but its published API has no stock borrow-availability endpoint. Crypto loan APIs do not answer this query. Tradability reads security metadata and quote status; unknown statuses remain unknown.

Transport uses the documented Bitget HMAC headers and signed JSON POST bodies. The generated Stock+ cURL examples inconsistently put POST parameters in the query string; JSON follows Bitget's general REST contract. Mocked contract tests cover the transmitted bytes, but authenticated live Stock+ acceptance remains to be verified with the user's configured account. No test trades have been placed.

Definite request rejections are recorded as `rejected` with `submission_outcome:"not_submitted"`, the HTTP status, Bitget error code and a credential-redacted error message. They remain cached by command ID and do not disable trading or stop intake; the worker can process the next command. Instrument-read failures before order dispatch also record zero writes. A rejected command is never automatically replayed.

Timeouts, server failures, malformed/contradictory responses, unknown response codes, duplicate client references and Bitget uncertainty codes `40010`, `40725`, `45001`, `25000`, and `25001` remain unresolved. An HTTP 400 alone is not enough to classify a new response as a rejection: a valid error response or recognized rejection code is required. Authentication failures still stop intake until access is repaired. Timed-cancellation submissions apply the same rejection rules and recover a durable rejected result after a crash. Existing unexplained journal entries are never silently cleared by a source upgrade; they need evidence-backed review. The runner health record now includes the last error code and message.

Response semantics: [Bitget REST conventions](https://www.bitget.com/docs/uta/rest-api), [error codes](https://www.bitget.com/docs/uta/error-code/restapi), and [documented uncertain outcomes](https://www.bitget.com/docs/catalog/trading/order-management). Stock+ order and instrument semantics continue to follow the Stock+ documentation.

Official sources checked 2026-10-25: [Stock+ orders](https://www.bitget.com/docs/catalog/stock-plus/orders), [Stock+ assets](https://www.bitget.com/docs/catalog/stock-plus/assets), [Stock+ instrument/quotes](https://www.bitget.com/docs/catalog/stock-plus/stock-quotes), [REST authentication](https://www.bitget.com/docs/uta/rest-api), [stock short-selling announcement](https://www.bitget.com/support/articles/12560603892050).

## Order monitoring and latency statistics

Set `brokers.bitget.monitor.enabled:true` with `kafka.publish_snapshots:true` to
run independent order, account, and reconciliation workers. Configure the stable
`journal_account` first. This replaces the shared 30-second snapshot loop for
Bitget. The post-action snapshot sleep does not run in this mode. Existing order
execution policy, including `action_delay_seconds`, remains in force; monitoring
continues during command execution.

The default REST schedule is 1.2 seconds between order-query starts. After a
command completes, the monitor wakes immediately and requests a 250 ms cadence
for up to two seconds, subject to the quota and request duration. Cash and
positions refresh independently every five seconds, immediately after an order
change subject to a one-second minimum spacing, then once per second for three
seconds. History reconciliation runs every 60 seconds; discovery of older active
orders runs every 300 seconds. Missing active orders are queried individually,
including GTC orders created on earlier days. Only broker observations establish
fills/cancellations; disappearance or an acknowledgement never establishes them.

Stock+ order-query endpoints advertise 30 calls/30 seconds/UID and a 20 ms
minimum spacing (checked 2026-11-05). All trade calls in this implementation share
that conservative budget. Monitoring leaves five slots for command requests;
history queries briefly defer to live polls without indefinite starvation. The quota file coordinates local processes
using the same account label and survives process restarts. Other hosts or clients
can still consume the broker's UID quota. HTTP 429 responses establish a shared
cooldown; no financial write is automatically retried. Workers use separate
persistent HTTP sessions and finite request timeouts.

There is no documented Stock+ order WebSocket in the sources checked. The UTA
order channel documents spot/margin/futures, and is not used for Stock+ here.
**Sustained sub-second detection is not guaranteed by this REST implementation.**
Bursts can improve notification time immediately after a command. Steady-state
latency includes the poll gap, broker visibility delay, network time, and Kafka
acknowledgement. Multiple fills between polls may appear as one cumulative fill
update. A supported Stock+ push feed or a higher confirmed query allowance is
needed to target sustained sub-second detection.

Raw JSONL telemetry is written to `.state/bitget.monitor.jsonl` by default, with
five rotated files of up to 10 MiB each plus the current file. `monitor.log_path`
overrides this location. A JSON summary is also printed to the service log every
60 seconds (`monitor.summary_seconds`). No credentials, signed headers, request
bodies, or balances are logged. Health includes each worker's last successful
refresh and latest error type.

```bash
.venv/bin/python tools/bitget_latency.py
.venv/bin/python tools/bitget_latency.py --since 2026-11-05T00:00:00Z
```

The report includes counts, mean, p50/p95/p99, and maximum milliseconds for HTTP
requests, quota waits, poll gaps, order observation-to-Kafka acknowledgement, and
account refreshes. Order delivery statistics are separated by status, including
`PARTIALLY_FILLED`, `FILLED`, and `CANCELED`. Broker-to-observation and
broker-to-Kafka latency require the actual `updatedAt`; missing timestamps remain
null and are counted separately. Initial and recovery snapshots are excluded
from live latency percentiles. Broker timestamps may have only second precision
and depend on clock alignment. Negative values are flagged as clock skew and
excluded. `updatedAt` is an order update time, not necessarily an individual
execution timestamp. Kafka acknowledgement does not measure downstream UI or
consumer latency. `since_order_observed_ms` for an account refresh measures the
local follow-up, not proof that the broker's balances have incorporated the fill.

The durable publication journal is reused for retries and sequences. Monitor
mode explicitly permits publication of prior-day order updates; the default
today-only publication behavior remains for other workflows. Stop and join all
workers before closing the publisher. Authentication or unexpected worker failure
halts command intake; temporary read/delivery errors are logged and retried with
backoff. To revert the scheduling mode, disable `monitor.enabled` and restart the
existing service, preserving all journals and quota files.

## Five accounts on separate servers

Run one Bitget worker per account, with that account's credentials and routing on
its own server. All five workers subscribe to `trading-commands` on
`45.32.121.19:9092`, each using an independent consumer group. Separate IP
addresses do not create separate Kafka subscriptions: consumers in the same
group share partitions, so one account can otherwise miss its commands.

Each server has its own `/root/bitget-configs/config.properties` and private
`config.json`. Start from the repository's `config.example.json`, then set the
local configuration from these properties:

| Properties entry | Local setting |
| --- | --- |
| `client_id` | Loaded into `BITGET_API_KEY` at startup |
| `client_secret` | Loaded into `BITGET_API_SECRET` at startup |
| `passphrase` | Loaded into `BITGET_PASSPHRASE` at startup |
| `string_id` | `brokers.bitget.routing.account_id` |
| `numerical_id` | `brokers.bitget.routing.account_num_id`, as a positive JSON integer |
| `kafka_ip` | `kafka.bootstrap_servers` |

Keep the credential environment references from the example; credentials stay
out of `config.json`, command-line arguments, and Git. `bitget/start.py` checks
any supplied routing/Kafka properties against `config.json` before connecting,
and rejects mismatches. Older files containing only credentials remain supported.
Give every account a distinct `string_id` and `numerical_id`; command producers
must use those same account selectors.

This server's account is `BITGET_DENGLEI`, numeric ID `2`. Its local configuration
uses `trading_enabled:true` and `journal_account:"BITGET_DENGLEI-stock-plus-live"`.
`BITGET_LYJ`, numeric ID `1`, belongs to a different server and account. Configure
the other three servers from their own properties rather than copying either
account's identity. Give each real account its own permanent `journal_account`
and preserve that label and `.state/` across restarts and credential rotation.

The group format for numeric accounts is:

```text
<group_prefix>.bitget.<account_id>.<server_id-or-default>.<live-or-preview>.<account_num_id>
broker-api-scripts.bitget.BITGET_LYJ.default.live.1
broker-api-scripts.bitget.BITGET_DENGLEI.default.live.2
```

Each group receives the complete command stream and executes only commands
matching its account selectors. For this server, target `account_num_id:2` or
`account_id:"BITGET_DENGLEI"`; if both are supplied, both must match. To trade on
all five accounts, publish one account-addressed command per account. Account
and order snapshots use `account-details` and `order-status` with the same
account identifiers.

Appending the numeric ID changes the Kafka group for existing deployments.
A new group starts at `latest` by default and does not inherit the old group's
offsets. When upgrading an existing worker, stop it first and migrate its
committed offsets if queued commands must be preserved. Keep its original
journals. An older worker on another account can keep its existing group;
it remains independent of this server's new group. Accounts without numeric IDs
retain the previous group format.

After creating the Python environment and installing `requirements.txt`, use
the same generic service on each server:

```bash
cd /root/broker-api-scripts
.venv/bin/python bitget/start.py --check
install -m 0644 deploy/bitget.service /etc/systemd/system/bitget.service
systemctl daemon-reload
systemctl start bitget
systemctl status bitget --no-pager
journalctl -u bitget -n 50 --no-pager
# To stop after the current operation:
systemctl stop bitget
```

The service runs read-only Kafka, account, order, and instrument checks before
starting the consumer. It does not automatically restart after an authentication
or reconciliation failure and is not enabled at boot. The older
`deploy/bitget-lyj.service` is a legacy unit name; stop that unit before replacing
it with `bitget.service` on the same server. Use only one unit per local account.

Bitget error `40084` means the supplied account is in Classic Account mode and
the requested API requires Unified Account access. Resolve account eligibility
and Stock+ permissions in Bitget, then run the check and start commands above.
Changing the local `account_type` cannot resolve this server-side rejection.

## Start manually with nohup

Install `requirements.txt` in the repository's `.venv` and prepare
`~/bitget-configs/config.json` with the account routing, Kafka settings, permanent
`journal_account`, and credential environment references described above. Keep
the credentials in `~/bitget-configs/config.properties`.

Run the launcher from any working directory:

```bash
~/broker-api-scripts/bitget/start-live.sh
```

The launcher locates the repository relative to itself and checks for an existing
worker. It runs the read-only startup checks with a 180-second timeout; a failed
check stops the launch. Once those checks pass, it sets
`brokers.bitget.trading_enabled` to `true` in the private configuration and starts
`bitget/start.py --execute` in the background using `nohup`, with stdin detached.
Output is appended to `~/bitget-configs/bitget.log`, and the process ID is saved
in `~/bitget-configs/bitget.pid`. The initial process check confirms that the
worker has survived three seconds; inspect its health record for runtime status:

```bash
~/broker-api-scripts/.venv/bin/python ~/broker-api-scripts/tools/runner_control.py status --broker bitget
tail -n 50 ~/bitget-configs/bitget.log
# To stop after the current operation:
~/broker-api-scripts/.venv/bin/python ~/broker-api-scripts/tools/runner_control.py stop --broker bitget
```
