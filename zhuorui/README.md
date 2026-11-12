# Zhuorui Securities

This group bridges the [requested Zhuorui Securities Automation project](https://github.com/mhliu94/Zhuorui-Securities-Automation) at commit `002aa6866b246dbc4c1f5e1f647019abc170080d`. It reuses its signed API client and configured imported session in a separate Python process. The source project is not copied into this deliverable. Only the common broker-api-scripts Kafka listener is needed; the upstream listener must remain stopped because both share its command journal and process lock.

Implemented operations: native US market orders in regular hours; DAY limit orders; all holdings; today's open and historical orders; static capabilities; and a **new read-only symbol diagnostic** that combines quote identity, quote freshness, suspension status and US market-session status. Symbol visibility cannot prove account permission or LULD reopening-auction eligibility. Unknowns are returned as JSON `null`. No order is submitted to test tradability.

The upstream session is encrypted with Windows DPAPI. Live calls must run on Windows under the Windows user who imported the session. This Linux-hosted deliverable can validate/dry-run orders and run mocked tests, but it cannot decrypt that Windows session. No account login, session import, APK capture, password reset or live trade is performed during setup here.

Create an external pinned checkout on that Windows machine, then follow its [session setup guide](https://github.com/mhliu94/Zhuorui-Securities-Automation/blob/002aa6866b246dbc4c1f5e1f647019abc170080d/docs/api.md). For example, in PowerShell:

```powershell
git clone https://github.com/mhliu94/Zhuorui-Securities-Automation.git C:\Trading\Zhuorui-Securities-Automation
git -C C:\Trading\Zhuorui-Securities-Automation checkout 002aa6866b246dbc4c1f5e1f647019abc170080d
py -m venv C:\Trading\Zhuorui-Securities-Automation\.venv-api
& C:\Trading\Zhuorui-Securities-Automation\.venv-api\Scripts\python.exe -m pip install -r C:\Trading\Zhuorui-Securities-Automation\requirements-api.txt
```

Set `ZHUORUI_UPSTREAM_ROOT` to that checkout, `ZHUORUI_UPSTREAM_CONFIG` to its private `zhuorui_config.json`, and `ZHUORUI_PYTHON` to its `.venv-api\Scripts\python.exe`. Copy this folder's `config.example.json` as the shared `brokers.zhuorui` config. Account/server selectors, if set in both configs, must match. The worker verifies the commit and refuses tracked changes to the upstream runtime. Its requirements pin `cryptography`, `gmalg`, `kafka-python`, and `tzdata`; the bridge itself starts no Kafka client.

Native FOK has not been established by this broker's captured API. By default, FOK is rejected. Setting **`allow_timed_cancel_fok: true`** explicitly enables the upstream limit-then-cancel workflow for FOK commands: cancellation targets the configured `api.cancel_after_seconds` after dispatch, default 1 second. **Partial fills remain possible, cancellation may fail or arrive late, and this is not all-or-none FOK.** Results always say `native_fok:false`. The upstream durable journal blocks unconfirmed submissions; pending timed cancellations block new bridge orders until recovered. Run:

```powershell
python zhuorui/recover_cancellations.py --config config.json --execute
```

Without `--execute`, the recovery script only prints a preview. Executed recovery resumes only journaled pending cancellations and never replays order submissions. Interrupted cancellation requests with unknown outcomes require account reconciliation. A successful cancellation acknowledgement does not prove the final state; query today's orders.

Market orders default to native regular-hours execution. Optional `allow_synthetic_extended_hours_market:true` permits the upstream explicit 1% quote-offset DAY-limit conversion during verified pre/post sessions; the returned execution record shows the actual submitted type and price. Limit orders default to regular hours; set `options.allow_pre_post:true` intentionally for pre/post trading. Quantity must be whole shares. Prices must already align to cents to prevent upstream rounding from changing your requested limit.

For LULD reopening intent, use a regular-hours DAY limit and inspect venue/broker acceptance separately. The bridge does not invent an auction order type, guarantee auction routing, infer a halt reason from an unknown suspension code, or replace DAY with FOK. It does not add a market-halt rejection to explicit limit orders. Upstream execution defaults to zero pre-action delay and retains its trade-authorization checks. Both upstream `trading_enabled` and `api.live_orders_enabled` switches remain effective.

Today's orders combines the upstream current-order and date-filtered historical endpoints, deduplicates transaction references, and applies exact `[start,end)` creation-time boundaries. Date range queries span every overlapping New York date. Upstream returns complete lists without a documented cursor; an unexpected shape raises an error instead of claiming an empty portfolio.

`borrow_availability(symbol)` returns `unverified_api` and null shortability, easy-to-borrow, available quantity, fee and locate fields. The pinned upstream request allowlist and typed client provide no verified stock-borrow/locate endpoint. Neither `holdList` available shares nor a visible quote is borrowing inventory. This limitation is surfaced without making a test order or claiming zero availability.

Reference interfaces: [client.py](https://github.com/mhliu94/Zhuorui-Securities-Automation/blob/002aa6866b246dbc4c1f5e1f647019abc170080d/zhuorui/api/client.py), [market_data.py](https://github.com/mhliu94/Zhuorui-Securities-Automation/blob/002aa6866b246dbc4c1f5e1f647019abc170080d/zhuorui/api/market_data.py), [execution.py](https://github.com/mhliu94/Zhuorui-Securities-Automation/blob/002aa6866b246dbc4c1f5e1f647019abc170080d/zhuorui/api/execution.py). These are user-provided integration references, not an official broker SDK or a claim of official API support.


## US-stock scope

The shared command layer and bridge require US stocks only. Broker position/order records must identify market `ts: US` and a stock ticker; other markets and derivative symbols are excluded. Missing market identity is reported rather than assumed. The guard also applies to upstream cancellation reads and exact-order details, so an all-cancel operation cannot cross into another asset market. Existing external session/account binding checks remain in place.
