# Account changes and Linux services

A background timer reads broker positions and cash at startup and every 30 seconds.
Order submission, cancellation, and order-status changes leave this schedule alone.
Order observers retain working tickets and cumulative fills. Each new filled share
increments the position on a buy or decrements it on a sell. The account report carries
the resulting absolute position row, or a removal when its quantity reaches zero.
Cash and newly available selling power come from the broker checkpoint.

A single account writer orders checkpoints and deltas on the existing sequence.
A checkpoint that overlaps an observed fill preserves the newer local position for
that symbol. A later quiet checkpoint reconciles it. A broker that reports assets
and executions at different times can still cause a temporary discrepancy.

`account-details` uses the UTF-8 account ID as the Kafka key. Version 1 messages
have `account_update_version`, `message_type`, `stream_id`, `sequence`, account IDs
and a UTC `ts`. Each process starts with `account_snapshot`; each successful periodic asset read publishes a full checkpoint.

`account_delta` has `base_sequence`, changed absolute position rows in `positions`,
explicit `removed_positions`, and only changed cash/trading fields. Currency maps
contain changed absolute balances; null removes a currency. An absent field means
unchanged. These are replacement values, not arithmetic increments. Unchanged reads
produce no message. Only broker-reported balances are published; fills never synthesize cash.

The producer advances its baseline only after a Kafka acknowledgement. An uncertain
delivery retries the same stream/sequence before publishing newer changes. The control
consumer reconstructs complete accounts, ignores duplicates and retired streams, and
disables algorithms for a gapped account until a full checkpoint restores its state.
Legacy snapshots remain supported for rolling upgrades. Upgrade consumers first.

## systemd

Use system services so workers survive logout and reboot:

```sh
sudo systemctl status SERVICE
sudo systemctl restart SERVICE
sudo journalctl -u SERVICE --since '10 minutes ago'
sudo systemctl is-enabled SERVICE
```

Existing service names are preserved. New workers use `bosco-runner.service`,
`starwealth.service`, and `tiger-trading.service`. Control services are
`ktrader.service`, `ktrader-http.service`, and (where already installed)
`ktrader-orders.service`. Broker configuration and authentication material remain
outside version control. Services run as the existing account owner with the same
interpreter, working directory, arguments and environment. Private launch captures
and rollback copies are kept in `/var/lib/ktrader-delta-rollout` (root only).

KTrader's `ops/systemd_adopt.py` migrates a captured worker without running a duplicate.
It stops the old user service/process gracefully, disables user-service autostart,
installs/enables the system unit, and starts it. It refuses to start a replacement
if the old process has not exited. Existing system units retain their configuration.
`ops/capture_runtime.py` captures the matching launch settings; review the selected
repository and service before migration. Do not commit runtime captures or env files.
