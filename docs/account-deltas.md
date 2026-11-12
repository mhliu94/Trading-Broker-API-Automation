# Account changes and Linux services

Workers poll broker order status at a target interval of one second or less.
Changes to status or cumulative fill quantity trigger a fresh positions/cash query.
Asset reads repeat for ten seconds after a change to cover delayed broker settlement.
Network latency, broker throttling and serial API requests can extend the observed interval.

`account-details` uses the UTF-8 account ID as the Kafka key. Version 1 messages
have `account_update_version`, `message_type`, `stream_id`, `sequence`, account IDs
and a UTC `ts`. Each process starts with `account_snapshot`; full checkpoints recur
on the first successful asset query at least 30 seconds after the previous checkpoint.

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
