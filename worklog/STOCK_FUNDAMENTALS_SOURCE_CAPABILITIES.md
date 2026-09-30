# Stock fundamentals source capability inventory

Research date: 2026-09-29. Scope: public documentation and project repository pages for the seven requested sources, with emphasis on historical and reported stock fundamentals. This is a documentation inventory, not a live data-quality test or a grant of redistribution rights. Real-time and WebSocket features are out of scope.

| Source | Role in a fundamentals API | Access / reuse position |
|---|---|---|
| OpenFIGI | Security identification and symbol matching | Free public API; optional key raises rate limits |
| GLEIF | Legal-entity identification and relationships | Public API; GLEIF states its website data is CC0 |
| yfinance / Yahoo Finance | Broad market and fundamental data for research | Library is open source; Yahoo data is described by the project as for personal use |
| SEC ownership and fund datasets | Official US insider, institutional, and fund disclosures | Public bulk datasets; filings are delayed/as filed |
| SEC EDGAR | Official US filings and reported financial facts | Public, no API key; SEC automated-access rules apply |
| Alpha Vantage | Aggregated fundamentals, actions, estimates, and prices | Free key is low-volume; commercial use requires agreement |
| Parsee Core | Extraction of structured fields and tables from documents | MIT-licensed software; it does not supply financial data |

## OpenFIGI

**Capabilities**

- `POST /v3/mapping` maps third-party identifiers, including tickers with exchange filters, to a FIGI. Results can contain instrument name, ticker, exchange code, market sector, security type, composite FIGI, and share-class FIGI.
- `POST /v3/search` and `/v3/filter` discover instruments by text and filters; results are paginated. Filters include exchange/MIC, currency, security type, and other instrument attributes. The API also exposes enumerated filter values and an OpenAPI schema.
- Covers identification across asset classes, not just equities. It can help resolve ambiguous tickers before joining filings and price data.

**URL:** [API documentation](https://www.openfigi.com/api/documentation) · [API base](https://api.openfigi.com/v3) · [OpenAPI schema](https://api.openfigi.com/schema).

**Pros**

- Free API without a key; a free key increases throughput.
- Stable instrument identifiers and exchange filters improve cross-source matching.

**Cons**

- No financial statements, ownership disclosures, prices, or company ratios.
- Mapping a ticker may return multiple instruments; exchange and security-type disambiguation are needed.
- Published request limits apply. The documentation currently lists 25 mapping requests/minute without a key and 25/6 seconds with a key; search/filter limits are lower.

## GLEIF

**Capabilities**

- Search legal entities by LEI, name, address, and other fields, including fuzzy matching.
- Retrieve legal-entity reference data and reported direct/ultimate parent and child relationships, subject to reporting exceptions.
- Access mapped identifiers such as BIC and ISIN where available, plus LEI issuer and code-list reference data.
- Golden Copy, delta, and mapping downloads are available for bulk identity matching.

**URL:** [GLEIF API overview](https://www.gleif.org/en/lei-data/gleif-api/) · [API documentation](https://api.gleif.org/docs) · [LEI data access](https://www.gleif.org/en/lei-data/access-and-use-lei-data) · [open-data statement](https://www.gleif.org/en/about/open-data).

**Pros**

- Global, standardized legal-entity identifiers and relationship data.
- GLEIF states that data on its website is provided under CC0.

**Cons**

- LEI identifies a legal entity, not a particular listed security; ticker-to-LEI joins require other identifiers.
- Parent relationships are reported with exceptions and should not be read as complete beneficial ownership.
- No company financial statements, market prices, or analyst estimates.

## yfinance / Yahoo Finance

**Capabilities**

- Ticker profile and information; annual, quarterly, and trailing income statements, balance sheets, and cash-flow statements.
- Historical prices; splits, dividends, other actions, and share counts.
- Earnings dates/history, EPS and revenue estimates, revisions, growth estimates, recommendations, analyst price targets, and upgrades/downgrades.
- Institutional, mutual-fund, major, and insider holders/transactions; SEC-filing links; fund data, options, news, and sustainability fields where Yahoo provides them.

**URL:** [project and usage notice](https://github.com/ranaroussi/yfinance) · [Ticker API reference](https://ranaroussi.github.io/yfinance/reference/api/yfinance.Ticker.html).

**Pros**

- Broad, convenient Python interface that is useful for coverage comparisons and prototypes.
- Includes several fields absent from SEC XBRL, notably analyst data and actions.

**Cons**

- The project says Yahoo's API is intended for personal use and directs users to Yahoo's terms for rights in downloaded data. Its Apache software license does not license Yahoo's data for a public fundamentals API.
- It is an unofficial Yahoo interface; availability and schemas can change, and fields can be missing by ticker or market.
- It does not supply an authoritative filing provenance trail for every normalized value.

## SEC ownership and fund datasets

| Dataset | Contents | Publication / main limit |
|---|---|---|
| [Insider Transactions](https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets) | Flattened Forms 3, 4, and 5: reporting persons, ownership, and transactions. | Bulk datasets updated quarterly; individual filings can be found sooner through EDGAR. |
| [Form 13F](https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets) | Holdings disclosed by larger institutional investment managers. | Quarterly positions, filed after quarter end; incomplete view of all ownership. |
| [Form N-PORT](https://www.sec.gov/data-research/sec-markets-data/form-n-port-data-sets) | Public portfolio holdings of registered funds and applicable ETFs. | Monthly portfolio reports; downloadable datasets updated quarterly. |
| [Form N-MFP](https://www.sec.gov/data-research/sec-markets-data/dera-form-n-mfp-data-sets) | Money-market fund and portfolio-holding information. | Monthly dataset; specific to money-market funds. |
| [Form N-CEN](https://www.sec.gov/data-research/sec-markets-data/form-n-cen-data-sets) | Annual registered investment-company census/reporting information. | Fund metadata rather than a full current holdings feed. |

**Pros**

- Official structured disclosures with filing provenance and downloadable historical ZIPs.
- Supports insider-transactions, institutional-holdings, and fund-holdings API features.

**Cons**

- As-filed, not a reconciled ownership graph; amendments and identifier matching need handling.
- Holdings are delayed and disclosure rules differ across forms. A 13F portfolio is not a complete investor portfolio or current ownership percentage.
- Fund, manager, issuer, and instrument identifiers must be joined across datasets.

## SEC EDGAR

**Capabilities and entry points**

- [Company ticker/exchange/CIK mapping](https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data), including `company_tickers_exchange.json`.
- `GET https://data.sec.gov/submissions/CIK##########.json`: company name, former names, ticker/exchange metadata, recent filing history, and pointers to older submission files.
- `GET https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json`: reported standard-taxonomy XBRL facts for a company across filings.
- `GET https://data.sec.gov/api/xbrl/companyconcept/CIK##########/{taxonomy}/{tag}.json`: history for one concept and units.
- `GET https://data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json`: a concept compared across issuers for a requested calendar period.
- [Original EDGAR archives](https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data): 10-K, 10-Q, 8-K, 20-F, 6-K, proxy, ownership, fund, and other submissions with their original HTML, inline XBRL, XML, text, and exhibit files as filed.
- [Financial Statement and Notes datasets](https://www.sec.gov/data-research/sec-markets-data/financial-statement-notes-data-sets): flattened numeric and text facts from statements and notes, updated monthly. SEC also offers nightly `companyfacts.zip` and `submissions.zip` bulk files.

**URL:** [EDGAR API documentation](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) · [archive access](https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data) · [automated-access guidance](https://www.sec.gov/about/webmaster-frequently-asked-questions).

**Pros**

- Official filing source; API requires no key; strong provenance through CIK, accession number, form, filing date, taxonomy, units, and period.
- Core source for reported US financial statements, shares, filing history, and disclosed segments/notes.

**Cons**

- `companyfacts`/`companyconcept`/`frames` contain only non-custom taxonomy facts applying to the whole entity. Company-specific tags and dimensioned segment facts require the full filing or notes datasets.
- Reported facts require period, amendment, unit, fiscal-year, and taxonomy normalization before they resemble a vendor's standardized statements or ratios.
- SEC guidance sets a maximum of 10 requests/second and requires a declared User-Agent; bulk ZIPs are preferable for large backfills.
- EDGAR does not provide analyst consensus, price targets, or a complete historical market-price feed.

## Alpha Vantage

**Documented stock-relevant capabilities**

- **Fundamentals:** `OVERVIEW` company profile, ratios, and metrics; `INCOME_STATEMENT`, `BALANCE_SHEET`, `CASH_FLOW`; `SHARES_OUTSTANDING`; `EARNINGS` actual EPS, estimates, and surprise; `EARNINGS_ESTIMATES` annual/quarterly EPS and revenue estimates with analyst counts/revisions.
- **Corporate and fund data:** `DIVIDENDS`, `SPLITS`, `ETF_PROFILE` holdings and fund metrics, listing/delisting status, earnings calendar, and IPO calendar.
- **Ownership and research:** insider transactions, institutional holdings, earnings-call transcripts, news/sentiment, and analyst-style analytics where offered.
- **Price context:** daily, weekly, and monthly historical OHLCV; some adjusted/history depth and faster quote features are marked premium. Economic and technical-indicator APIs exist but are outside reported stock fundamentals.

**URL:** [full API documentation](https://www.alphavantage.co/documentation/) · [free-tier FAQ](https://www.alphavantage.co/support/) · [terms](https://www.alphavantage.co/terms_of_service/).

**Pros**

- One normalized API spans filings-derived fields, market prices, actions, earnings, estimates, transcripts, and funds.
- Useful benchmark for fields and formats that would need multiple primary sources to reproduce.

**Cons**

- The standard free service is 25 API requests/day for most datasets; particular endpoints and features are premium. Confirm entitlement per endpoint.
- Its terms grant personal, noncommercial use unless otherwise agreed in writing and classify providing the information to other users as commercial use. A public API needs an appropriate agreement.
- Aggregated fields and vendor calculations can differ from as-filed values; keep provenance and formula definitions if comparing to SEC facts.

## Parsee Core

**Capabilities**

- Open-source Python framework to turn unstructured documents, especially PDFs, HTML, and images, into structured outputs; it is built for financial tables and numeric fields.
- Converts input into a `StandardDocumentFormat`; extraction templates define typed questions (`StructuringItem`), metadata axes (`MetaItem`), and target tables (`TableItem`).
- Table workflow detects relevant tables, extracts column metadata, maps rows to standardized buckets, and can export the result through pandas/CSV.
- Supports local model use (README example: Ollama) and configured external/multimodal LLMs; tutorials cover custom prompts, document chat, datasets, evaluations, and LangChain integration.
- Could extract SEC filing tables, issuer presentations, or reports that are not available as normalized XBRL facts.

**URL:** [repository and README](https://github.com/parsee-ai/parsee-core/tree/master) · [table extraction tutorial](https://github.com/parsee-ai/parsee-core/blob/master/tutorials/2_table_extraction.py) · [MIT license](https://github.com/parsee-ai/parsee-core/blob/master/LICENSE).

**Pros**

- Extraction code can run locally; templates make target fields explicit; MIT software license.
- Useful for segment tables and other disclosures omitted by SEC's entity-wide Company Facts API.

**Cons**

- It is a parser, not a filings feed, price feed, or prebuilt fundamentals database.
- Extracted values need validation against original documents, units, period labels, and issuer-specific table layouts. Model services may have separate costs and terms.

## Coverage implication

The strongest source-backed path for **US reported financials** is SEC EDGAR plus normalization; SEC ownership/fund datasets cover several ownership endpoints. OpenFIGI and GLEIF address instrument/entity identity. Parsee Core can recover difficult document fields. yfinance and Alpha Vantage expose useful additional capabilities for research, especially prices and analyst estimates, but their free access should not be treated as permission to republish their data in a public API.

---

# API and database design: Finnhub stock fundamentals scope

This design targets the **stock fundamentals** portion of [Finnhub's API documentation](https://finnhub.io/docs/api/) and its [published Python client paths](https://github.com/Finnhub-Stock-API/finnhub-python/blob/master/finnhub/client.py). It specifies a US-first API using the seven sources above. The schema allows later international issuers, but US coverage is the only coverage supported by a strong public filing source in this inventory. Finnhub's exact metric definitions, international normalization, and proprietary calculations are not assumed to be reproducible.

## Target features and collection plan

`Available` means a source-backed equivalent can be designed now, not that the data has been ingested or verified for every issuer. `Partial` means coverage, history, normalization, or field definitions differ. `Rights-dependent` means a free research feed does not establish public API reuse rights.

| Finnhub feature / path | Collect from where | What to store or calculate | Expected coverage |
|---|---|---|---|
| Symbol and profile: `/stock/symbol`, `/stock/profile2`, `/stock/profile` | SEC ticker/CIK file and submissions; OpenFIGI for FIGI, exchange, security class; GLEIF for LEI and legal-entity relationships | Issuer legal identity; security and listing history; CIK, LEI, FIGI, ISIN/CUSIP where matched; name, SIC, address, fiscal year end, exchanges, currencies | **Partial**: logo, phone, business description, IPO date, and full global profile need other sources or filing extraction. Do not equate legal-entity LEI with a listed share class. |
| SEC filings: `/stock/filings` | SEC submissions history and accession archives | Form, accession, accepted/filed/report dates, primary document, exhibit URLs, amendment links and content hashes | **Available for SEC filers**. |
| Financials as reported: `/stock/financials-reported` | SEC Company Facts and original XBRL/inline XBRL; SEC Financial Statement and Notes datasets for broader tagged disclosures | Every reported numeric fact with taxonomy/tag, context, period, unit, dimensions, value, filing accession, and availability time | **Strong for SEC filers**, but `companyfacts` alone misses custom/dimensioned disclosures. |
| Standardized statements: `/stock/financials` (`bs`, `ic`, `cf`; annual/quarterly/TTM) | Normalize SEC facts to a versioned metric dictionary; use filing notes and Parsee only for missing or dimensioned facts | Revenue, costs, income, assets, liabilities, equity, operating/investing/financing cash flow, capex, shares, EPS, and derived TTM | **Partial**: taxonomy and fiscal-period mapping, bank/insurer models, non-GAAP choices, and 30-year/global parity remain gaps. |
| Basic financials: `/stock/metric` | Standardized statement facts plus licensed daily close and point-in-time share counts | Margins, ROE/ROA, liquidity, leverage, growth, cash-flow ratios; P/E, P/S, P/B, yields and 52-week measures only when lawful price/action inputs exist | **Partial**: price-based metrics and parity with Finnhub formulas are unavailable from SEC alone. |
| Revenue breakdown/KPI: `/stock/revenue-breakdown`, `/stock/revenue-breakdown2` | SEC dimensioned XBRL, notes datasets and filing exhibits; Parsee extraction with review for prose/tables | Product/geography/business-segment revenue and issuer-specific KPI, keeping the original dimension/member label | **Partial and issuer-dependent**; no reliable universal KPI taxonomy. |
| Executives and peers: `/stock/executive`, `/stock/peers` | SEC proxy/10-K and profile fields; SIC/industry classifications; Parsee for executive tables | Person, role, compensation where disclosed; transparent industry-based peer candidates | **Partial**: officer data is not uniformly structured and peer rules will differ from Finnhub. |
| Splits/dividends: `/stock/split`, `/stock/dividend`, `/stock/dividend2` | Issuer/SEC disclosures where present; Alpha Vantage or Yahoo only if a distribution license is obtained | Ex/record/pay dates, cash amount/currency, split factors and evidence | **Rights-dependent** for a complete historical event feed; filing extraction alone is incomplete. |
| Earnings actuals/calendar: `/stock/earnings`, `/calendar/earnings` | Reported GAAP EPS/revenue from SEC; 8-K earnings releases and issuer disclosures for announcement date/adjusted figures; Alpha Vantage only with rights | Fiscal period, announcement date/time if known, GAAP actual EPS/revenue, separately labeled adjusted actuals, estimate and surprise only with a licensed estimate source | **Partial**: SEC does not supply consensus or a complete forward calendar. GAAP EPS must not be substituted for adjusted EPS. |
| Insider transactions/sentiment: `/stock/insider-transactions`, `/stock/insider-sentiment` | SEC Forms 3/4/5 XML and bulk dataset | Reporter, transaction code/date, acquired/disposed quantity, price, ownership after trade; optionally a separately defined own net-buying metric | **Transactions available for US filers**; Finnhub's proprietary MSPR sentiment is not reproducible from the disclosed transactions alone. |
| Ownership: `/stock/ownership`, `/stock/fund-ownership`, `/institutional/portfolio`, `/institutional/ownership` | SEC 13F filings/bulk data; N-PORT for funds; [13D/13G beneficial-ownership filings](https://www.sec.gov/rules-regulations/staff-guidance/corporation-finance-interpretations/exchange-act-sections-13d-13g-regulation-13d-g-beneficial-ownership-reporting) separately | Manager/fund position, report date, filing date, security identifier, shares, value, put/call, voting/discretion; beneficial-owner stakes as a distinct type | **Partial**: 13F is delayed and does not equal all shareholders, beneficial ownership, or fund-level ownership. |
| Estimates/research: `/stock/eps-estimate`, `/stock/revenue-estimate`, other estimate paths, `/stock/recommendation`, `/stock/price-target`, `/stock/upgrade-downgrade` | Alpha Vantage or Yahoo only after the required public-API rights are secured; otherwise no production ingest | Timestamped consensus by fiscal period, metric, basis, statistic, contributor count, recommendation or rating event | **Rights-dependent/unavailable** from the public filing sources. Never infer analyst consensus from company guidance. |

Standalone ETF/mutual-fund products, intraday prices, WebSockets, forex/crypto, technical indicators, news/social sentiment, ESG scores, supply-chain scores, transcripts, and Finnhub's proprietary earnings-quality score are outside this initial stock-fundamentals contract. N-PORT and N-MFP are included only where they help answer stock ownership questions. Finnhub lists several of these adjacent capabilities separately in its [commercial coverage table](https://api.finnhub.io/pricing-startups-and-enterprise).

## What must be joined or correlated

1. **Issuer to security to listing.** Use SEC CIK as the US filing-entity key, FIGI/ISIN/CUSIP as security identifiers, and `(MIC, ticker, effective date)` as a listing alias. A ticker alone is not a durable join key. Match through an observed SEC ticker/CIK association and OpenFIGI security result; attach a GLEIF LEI only after entity-level validation. Preserve candidate matches and their evidence instead of silently choosing a name match.
2. **Filing to reported facts.** Join XBRL facts and extracted tables by CIK plus SEC accession. Preserve form, accepted time, fiscal period, taxonomy, unit, XBRL dimensions, and source document. A later amended filing is a new version; it does not overwrite the earlier as-filed values.
3. **Facts to fiscal periods.** Use issuer fiscal-year start/end and the fact's actual start/end dates. Distinguish instant balance-sheet facts from duration income/cash-flow facts. Map annual, quarter, year-to-date, and derived TTM explicitly; never sum overlapping year-to-date facts as if they were discrete quarters.
4. **Standardized metric to evidence.** Map each SEC taxonomy/tag or reviewed Parsee extraction to a versioned internal metric. Keep multiple candidate facts, the chosen value, conversion/formula, and every contributing raw fact. Separate GAAP, IFRS, company-adjusted/non-GAAP, estimated, and calculated values.
5. **Security to prices and actions.** Join prices/actions to a share class and listing, not just CIK. Calculate market cap and valuation ratios only when close price, share count, currency, and split basis align on a known date. Record FX conversion separately if required; no FX source is included in this inventory.
6. **Holder/fund to stock.** Match 13F and N-PORT reported CUSIP/ISIN/security descriptions to a security with effective dates. Keep 13F manager holdings, N-PORT fund holdings, and 13D/G beneficial stakes separate. For a portfolio percentage, use the relevant fund/manager portfolio denominator at that report date; for issuer ownership percentage, use the matching share-class denominator at that report date.
7. **Earnings to consensus.** Join reported actuals and earnings announcements to the same issuer, fiscal period, currency, per-share basis, and GAAP/adjusted basis. Choose the consensus snapshot recorded before the announcement. Store surprise only when that comparable pre-event estimate exists.
8. **Point-in-time visibility.** Every fact or position needs `reported_for` and `available_at` (filing acceptance or provider publication). An `as_of` API request must exclude records first published later, even if their financial period is earlier.

## Missing data and its impact

| Missing or weak input | Why these sources do not fully supply it | Impact on the API |
|---|---|---|
| Licensed historical prices and complete corporate actions | SEC is a filings service; Alpha Vantage/Yahoo free access does not authorize a public redistribution feed | P/E, P/S, P/B, market cap, yield and 52-week metrics remain absent or restricted; do not emit misleading zeroes. |
| Analyst consensus, price targets, recommendations, revisions | Not filed with the SEC; aggregated commercial data has separate rights | Estimate, surprise and analyst endpoints remain unavailable. Actual EPS/revenue can still be served with an explicit `basis=gaap` label. |
| Global standardized statements | EDGAR covers SEC filers, including some foreign issuers, not all global companies | Coverage outside SEC filers is sparse; keep `unsupported_jurisdiction` rather than returning an empty statement as if the issuer had no revenue. |
| Custom tags, segment dimensions and issuer KPIs | SEC Company Facts exposes standard entity-wide facts only | Product/geography revenue and industry KPIs are incomplete unless original XBRL/notes and reviewed document extraction are processed. |
| Exact Finnhub normalization and scores | Vendor field definitions, adjustments, peer sets and proprietary models are not all public | Ratios, peers, insider sentiment and quality scores may disagree; publish our formula/method version and avoid parity claims. |
| Current/full institutional and fund ownership | 13F, N-PORT, 13D/G cover different reporters and are delayed; positions can be confidential or absent | Holder rankings and ownership percentages are incomplete and stale; label report date and form, and do not call the result a complete cap table. |
| Precise future earnings times and adjusted earnings | SEC filings often occur after announcement and do not standardize a forward schedule or media-style adjusted EPS | Calendar can be delayed/incomplete; historical GAAP actuals are not equivalent to Finnhub's adjusted calendar figures. |
| Complete business profiles and executive history | SEC filings disclose many details in prose and proxy tables, not a clean uniform feed | Missing description, logo, role dates or compensation for some issuers; Parsee output requires validation. |
| Reliable crosswalk for changed tickers and classes | Symbols are reused, companies can have multiple classes, and LEIs refer to entities | Bad joins would corrupt prices, ownership and ratios. Keep unresolved matches out of published numerical aggregates. |

For every API field, distinguish `not_disclosed`, `not_collected`, `not_yet_processed`, `unresolved_identity`, `restricted_source`, and `not_applicable`. Use `null` plus a status code and provenance; never turn missing values into zero.

## Database structure (PostgreSQL proposal)

Keep source observations immutable and build normalized/query tables from them. UUIDs below are internal IDs; CIK, LEI, FIGI, ISIN, CUSIP, and ticker remain typed external identifiers. Timestamps use UTC; money and share values use `numeric`, not floating point.

| Table | Main columns and keys | Purpose |
|---|---|---|
| `data_source` | `source_id PK`, name, source URL, access class, license/reuse status, `serve_allowed`, checked-at | Explicit rights gate; `serve_allowed` defaults false until verified. |
| `ingest_run` | `run_id PK`, source FK, started/finished, cursor, status, error | Refresh and backfill audit. |
| `source_record` | `record_id PK`, source/run FK, external key, URL, retrieved/available timestamps, SHA-256, content type, raw object location, public-output status | Immutable payload/evidence, with unique `(source_id, external_key, sha256)`. |
| `party` | `party_id PK`, kind (`organization`, `person`, `fund`), canonical name | Shared identity for issuers, managers, funds, and insiders. |
| `party_identifier` | party FK, scheme (`CIK`, `LEI`, registry ID, etc.), value, valid dates, source FK | Multiple dated legal-entity/person identifiers. |
| `issuer` | `issuer_id PK`, party FK, `cik UNIQUE NULL`, domicile, SIC, fiscal year end | Filing company; CIK is its US filing anchor. |
| `security` | `security_id PK`, issuer FK, instrument type, FIGI/ISIN/CUSIP, share class | A stock class or other identifiable instrument; do not store ticker as its identity. |
| `listing_symbol` | security FK, MIC, ticker, currency, valid-from/to, source FK | Historical venue-symbol aliases; constrain overlapping `(MIC,ticker)` intervals after review. |
| `identity_match` | source record FK, candidate party/issuer/security FK, method, confidence, decision, reviewed-at | Keep ambiguous OpenFIGI/GLEIF/SEC matches visible. |
| `filing` | `filing_id PK`, issuer FK, accession `UNIQUE`, form, report/filing/accepted dates, amendment link, source FK | SEC event/provenance. |
| `filing_document` | filing FK, filename, URL, MIME type, hash, source record FK | Original report and exhibits; unique `(filing_id, filename)`. |
| `fiscal_period` | `period_id PK`, issuer FK, fiscal year/quarter, start/end, duration type | Calendar mapping and TTM construction. |
| `raw_fact` | `raw_fact_id PK`, filing/document FK, taxonomy, tag, context, dimensions JSONB, start/end, unit, reported value, source FK | Lossless XBRL or reviewed extracted observation; dedupe by a source-specific fingerprint, retain duplicate revisions. |
| `metric_definition` | `metric_code PK`, statement group, default unit, definition, formula, method version | Public semantic contract for normalized/derived fields. |
| `financial_fact` | `fact_id PK`, issuer/security/period FKs, metric FK, value, unit/currency, `basis`, `value_kind`, available-at, source FK, method version, quality | Comparable candidate fact; `basis` separates GAAP/IFRS/adjusted, `value_kind` separates reported/normalized/derived. |
| `fact_lineage` | output fact FK, nullable input raw-fact FK, nullable input financial-fact FK, contribution role | Trace each standardized/derived value; require exactly one input FK per row. |
| `fact_selection` | issuer/security/period/metric/basis, selected fact FK, effective-at, decision rule | Versioned preferred-value selection without deleting alternatives. |
| `segment_fact` | issuer/period/filing FK, axis (`product`, `geography`, `business`), original member, mapped member, metric, value, unit, raw fact FK | Segment and KPI data without forcing incompatible company dimensions together. |
| `daily_price` | security FK, trade date, close, currency, adjustment basis, source FK, available-at | Licensed end-of-day input; unique `(security_id, trade_date, source_id, adjustment_basis)`. |
| `corporate_action` | security FK, action type, ex/record/pay dates, amount/factors, currency, source FK | Dividends and splits; retain conflicting source observations. |
| `earnings_event` | issuer/period FK, scheduled/announced timestamps, GAAP or adjusted basis, source FK | Calendar and actual-release metadata. |
| `estimate_snapshot` | issuer/period/metric FK, observed-at, mean/median/low/high, analyst count, basis, currency, source FK | Licensed consensus history; immutable so pre-announcement values remain available. |
| `analyst_opinion` | security FK, observed-at, firm, recommendation/target/action, source FK | Licensed rating and target events. |
| `person_role` | issuer/person-party FKs, title, role start/end, compensation, filing FK | Executives/directors with filing provenance. |
| `insider_transaction` | filing/security/reporter-party FKs, transaction sequence/date/code, acquired/disposed, shares, price, post-trade shares, ownership mode | Form 3/4/5 transactions; sequence prevents same-day trades from collapsing. |
| `institutional_position` | manager-party/filing/security FKs, report date, filed-at, reported CUSIP, shares/value, put-call, voting/discretion, match status | 13F manager positions, separate from beneficial ownership. |
| `fund_position` | fund-party/filing/security FKs, report date, shares/value, reported identifier, match status | N-PORT fund holdings. |
| `beneficial_stake` | owner-party/issuer/security/filing FKs, report date, shares, percentage, ownership basis | 13D/G disclosure; distinct from 13F positions. |
| `coverage_status` | issuer/security/feature, as-of, status/reason, last source/run FK | Explicit gaps and freshness for API responses. |

**Key constraints and query rules**

- Index `filing(issuer_id, accepted_at DESC)`, `raw_fact(filing_id, taxonomy, tag, end_date)`, `financial_fact(issuer_id, metric_code, period_id, available_at DESC)`, `institutional_position(security_id, report_date)`, and `listing_symbol(mic, ticker, valid_from, valid_to)`.
- Foreign keys from all published values to source evidence are required. A `source_record` may be stored for evaluation even when `data_source.serve_allowed=false`, but public queries may use only records whose source and record-level output rights have been verified for that field and use.
- `fact_selection` should select an eligible fact available at or before the request's `as_of` timestamp. A restatement gets a new `financial_fact` and a later selection; historical `as_of` queries retain the earlier answer.
- A null `security_id` on `financial_fact` means issuer-wide; security-specific share counts or per-share metrics must carry the security ID or an explicit share-class basis.
- Reject ambiguous identity joins, incompatible currency/unit/basis joins, and price metrics with a missing share denominator. Return a coverage reason rather than a calculated number.

**Illustrative query contract**

```text
GET /fundamentals/{mic}/{ticker}?as_of=2026-09-29&basis=gaap
→ issuer_id, security_id, fiscal_period, metrics[]
→ each metric: value | null, unit, status, method_version, available_at,
               source_url, accession (when filed), report_period
```

**Collection order**

1. Build the dated identity crosswalk from SEC CIK/ticker, OpenFIGI, and GLEIF; quarantine unresolved matches.
2. Backfill SEC submissions, Company Facts, original filing documents, and notes datasets. Parse reported facts before calculating metrics.
3. Add SEC insider, 13F, N-PORT and beneficial-ownership filings with distinct disclosure semantics.
4. Normalize statements and compute source-backed ratios; run Parsee only against identified gaps and require evidence/review for published extracted values.
5. Add price, corporate-action, estimate and analyst tables to public responses only when source rights and quality are verified.
