# Borrow availability API coverage

All seven groups expose `borrow_availability.py --symbol SYMBOL --execute` and the Kafka action `QUERY_BORROW_AVAILABILITY`. These are read-only checks. `available_quantity`, `shortable`, `easy_to_borrow`, `fee_rate`, and `locate_required` are common response fields. Unexposed or unverified values are JSON `null`. Native evidence and limitations are retained alongside those fields. All quantities are shares. No query buys a locate, opens a short, or reserves inventory.

| Broker | Verified interface used | Returned information | Not established |
| --- | --- | --- | --- |
| Bitget | Official Stock+ API catalog | Explicit unavailable stock-borrow API result; broker supports ETB short selling | No documented stock inventory/rate/shortability endpoint |
| Robinhood | Official equity tradability MCP tool | Returned short-selling eligibility flag | No published borrow inventory/rate/locate tool; current Agentic order workflow is long-only |
| Alpaca | Asset record, live HTB `GET /v1/locates/quotes` | Shortable/borrow classification; available HTB quote shares, per-share locate price and quote timestamp when authorized | Daily stock-borrow rate, account approval or reservation; paper has no locates API |
| TradeStation | Quote `MarketFlags`, selected account `AccountDetail` | Nondelayed HTB flag and account stock-locate eligibility | Available shares, confirmed symbol shortability, borrow fee or locate approval |
| Longbridge | Margin ratio and US account sell-capacity estimate | Raw margin ratios and sell-capacity estimates | Actual borrow inventory, fees, ETB/shortable flag or locate approval; sell capacity is not lendable inventory |
| Webull | US retail stock profiles | `shortable` and `easy_to_borrow` flags | Available shares, fee rate or locate confirmation; institutional/HK APIs are different products |
| Zhuorui | Pinned reference client reviewed | Explicit unverified-API result | No verified borrow/locate endpoint in that reference; owned holdings are not borrowed inventory |

Alpaca's implementation uses its current `borrow_status` field, with the old ETB boolean only as a compatibility fallback. An HTB quote's `price` is returned as `locate_fee_per_share`, never as a daily `fee_rate`. Advisory quote quantities are not reservations. Failed/unauthorized quote retrieval leaves quantity/price unknown while retaining classification and the error. See [Alpaca's current short-selling workflow](https://docs.alpaca.markets/us/docs/margin-and-short-selling), [locate quotes](https://docs.alpaca.markets/us/reference/listlocatequotes), and [paper-trading limitation](https://docs.alpaca.markets/us/changelog/2026-09-10-locates-paper-trading-5658e38).

Bitget uses Stock+ US stocks, not crypto margin borrowing. Its [stock short-selling announcement](https://www.bitget.com/support/articles/12560603892050) establishes ETB availability as a product feature, but the [Stock+ API catalog](https://www.bitget.com/docs/catalog/stock-plus/orders) supplies no verified borrow-inventory endpoint. Robinhood's [agentic tool catalog](https://robinhood.com/us/en/support/articles/trading-with-your-agent/) exposes stock eligibility but does not publish a locate/inventory tool. Neither is replaced with a crypto borrowing query.

Other references: [TradeStation v3 specification](https://api.tradestation.com/docs/specification/), [Longbridge margin ratios](https://open.longbridge.com/docs/trade/asset/margin_ratio), [Longbridge capacity estimates](https://open.longbridge.com/docs/trade/order/estimate_available_buy_limit), [Webull US profiles](https://developer.webull.com/apis/docs/reference/instrument-list/), [Robinhood equity tools](https://robinhood.com/us/en/support/articles/trading-with-your-agent/), and the [pinned Zhuorui client](https://github.com/mhliu94/Zhuorui-Securities-Automation/blob/52149ec52810a0872875f3860bbc0b3ed0cdaca9/zhuorui/api/client.py). Broker-specific READMEs contain further setup and scope details.

Example control message:

```json
{"id":"borrow-check-001","account_num_id":3,"action":"QUERY_BORROW_AVAILABILITY","symbol":"AAPL"}
```

The response envelope contains `action: "borrow_availability"`, `status: "ok"` for a completed query, and a `data` object. A successful query with null quantity is not a failed query and is not zero availability. A broker access/API failure yields an error, except for explicitly documented partial results such as a classification obtained before a locate quote fails. Controls should show the explanation and observation timestamp rather than collapsing unknown, unsupported, false and zero into the same state.
