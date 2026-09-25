# Future Work: Opportunity Tracking and RFP Extraction

Track this as a future extraction and enrichment capability for identifying commercial opportunities and requests for proposals (RFPs). The extractor should produce one normalized opportunity record per finding and preserve enough source evidence for review, qualification, and outreach.

## Target record

```json
{
  "id": "",
  "company": "",
  "experienceLane": "",
  "trigger": "",
  "recommendedOffer": "",
  "estimatedValue": null,
  "evidenceLabel": "",
  "evidenceUrl": "",
  "sourceTitle": "",
  "sourcePublisher": "",
  "sourcePageText": "",
  "sourceEnrichment": {
    "status": "",
    "finalUrl": "",
    "dateEvidence": "",
    "countryEvidence": ""
  },
  "country": "",
  "location": "",
  "publishedAt": null,
  "opportunityType": "",
  "solicitationNumber": "",
  "responseDeadline": null,
  "problem": "",
  "evidence": "",
  "summary": "",
  "score": null,
  "scoreBreakdown": {},
  "nextAction": "",
  "outreachPlan": "",
  "outreachDraft": "",
  "foundAt": null,
  "qualificationStatus": "",
  "qualificationReason": ""
}
```

## Field groups and extraction intent

| Group | Fields | Intent |
|---|---|---|
| Identity and fit | `id`, `company`, `experienceLane`, `trigger` | Stable identity, relevant capability lane, and the event or signal that created the opportunity. |
| Offer and value | `recommendedOffer`, `estimatedValue` | Suggested service/product response and an explicitly estimated value, with units and currency defined during implementation. |
| Evidence | `evidenceLabel`, `evidenceUrl`, `evidence`, `sourcePageText` | Human-reviewable proof tied to the source page; retain quoted or selected page text rather than only an LLM-generated summary. |
| Source and provenance | `sourceTitle`, `sourcePublisher`, `sourceEnrichment`, `publishedAt`, `foundAt` | Original publication metadata plus enrichment status, resolved URL, date evidence, and country evidence. |
| Geography | `country`, `location` | Normalized country and more specific place or service location, while preserving the evidence used to infer them. |
| RFP details | `opportunityType`, `solicitationNumber`, `responseDeadline` | Classify the opportunity and capture procurement identifiers and deadlines when present. |
| Qualification | `problem`, `summary`, `score`, `scoreBreakdown`, `qualificationStatus`, `qualificationReason` | Explain the opportunity, rank it consistently, and record why it is qualified, rejected, or pending review. |
| Action | `nextAction`, `outreachPlan`, `outreachDraft` | Make the recommended follow-up executable and preserve a draft message for human approval. |

## Future implementation notes

- Define controlled vocabularies for `opportunityType`, `qualificationStatus`, `experienceLane`, and `sourceEnrichment.status` before production extraction.
- Store dates as timezone-aware timestamps; retain the original text used to infer `publishedAt` and `responseDeadline` when normalization is uncertain.
- Define `estimatedValue` as a structured amount with currency and confidence, even if the first JSON export keeps the field scalar for compatibility.
- Make `scoreBreakdown` explainable and additive (for example: fit, urgency, value, evidence quality, geography, and source confidence).
- Treat `evidenceUrl` and `finalUrl` as separate: the former identifies the supporting passage or source reference, while the latter records redirect resolution.
- Require human review before sending `outreachDraft` or changing a record to a final qualified state.
- Add deduplication using canonical URL, solicitation number, company, and normalized deadline; preserve source provenance when merging records.

## Example extraction contract

```python
def extract_opportunity(source_page: str) -> dict:
    """Return a normalized opportunity record with evidence and provenance."""
    ...
```

The eventual worker should follow the existing Coffeemaker state-machine pattern, persist raw source evidence alongside normalized fields, and make extraction idempotent so repeated collection does not create duplicate opportunities.


## Normalized career job opportunities

Career opportunities should use a separate normalized shape from commercial opportunities and RFPs:

```json
{
  "title": "",
  "company": "",
  "location": "",
  "remote": null,
  "employmentType": "",
  "c2cEligible": null,
  "c2cStatus": "",
  "compensation": "",
  "source": "",
  "sourceUrl": "",
  "description": "",
  "importedAt": null,
  "status": "",
  "discoveryQuery": "",
  "discoveryNote": ""
}
```

| Group | Fields | Intent |
|---|---|---|
| Role identity | `title`, `company`, `location`, `remote` | Normalize the position, hiring organization, work location, and remote-work arrangement. |
| Engagement | `employmentType`, `c2cEligible`, `c2cStatus`, `compensation` | Capture employment model, contract-to-contract eligibility and confidence/status, plus the advertised pay details. |
| Provenance | `source`, `sourceUrl`, `importedAt` | Preserve where and when the job was discovered or imported. |
| Workflow | `status` | Track the job through a controlled lifecycle such as `new`, `reviewing`, `applied`, `rejected`, or `closed`. |
| Discovery context | `discoveryQuery`, `discoveryNote` | Record the search/query that found the job and any human or system context needed to interpret it. |
| Content | `description` | Retain the normalized job description used for review and matching. |

Future implementation should define controlled vocabularies for `employmentType`, `c2cStatus`, and `status`; represent `remote` as a tri-state value when the source is ambiguous; and preserve the original compensation wording before parsing it into amount, currency, and interval fields.


## Required extraction inventory from referenced opportunity and career flows

The future extractor should retain the following fields for jobs and/or RFPs. Each field must identify whether it came from an internet response, a fetched source page, a supplied local record, or local derivation.

### RFP and public-opportunity fields

| Field | Required source/extraction path | Classification |
|---|---|---|
| `company` | Search-result title/description; for procurement notices, buyer extraction from the same fields | Extracted from search response, then normalized |
| `experienceLane` | Matching hard-coded query-taxonomy definition, with keyword fallback | Assigned/derived locally |
| `trigger` | Taxonomy definition; procurement records prefix the lane with `RFP` or `RFI` | Assigned/derived locally |
| `recommendedOffer` | Matching taxonomy definition | Hard-coded recommendation |
| `estimatedValue` | Matching taxonomy definition unless a future source explicitly provides a budget | Hard-coded/defaulted today; source-derived later |
| `evidenceLabel` | Taxonomy or procurement label | Assigned locally |
| `evidenceUrl` | Search provider result URL | Extracted from search response |
| `sourceTitle` | Fetched page `og:title`, `twitter:title`, or HTML `<title>` | Extracted from source page |
| `sourcePublisher` | Fetched page `og:site_name`, `publisher`, or `author` metadata | Extracted from source page |
| `sourcePageText` | Fetched HTML title, descriptions, and stripped page text | Extracted from source page |
| `sourceEnrichment.status` | Fetch outcome and parsing branch | Derived locally |
| `sourceEnrichment.finalUrl` | HTTP response URL after redirects | Extracted from HTTP response |
| `sourceEnrichment.dateEvidence` | Date metadata selected from the source page | Extracted/derived locally |
| `sourceEnrichment.countryEvidence` | Text matched by country/location detection | Extracted from source page |
| `country`, `location` | Search fields when available; otherwise page metadata (`geo.placename`, `location`) or page-text regexes | Extracted and normalized |
| `publishedAt` | Search-provider date (`publishedDate`, `published_date`, Google article/date metadata, or `page_age`); page metadata fallback | Extracted and normalized |
| `opportunityType` | RFP/RFI language in normalized result title plus description | Extracted by rules |
| `solicitationNumber` | Notice/solicitation/RFP/RFI identifier in normalized result title plus description | Extracted by rules |
| `responseDeadline` | Due/deadline phrases in procurement result title plus description | Extracted by rules; do not assume source-page extraction |
| `problem`, `summary`, `nextAction` | Taxonomy and procurement templates, using search evidence as context | Generated/assigned locally |
| `evidence` | Search result description/content or title | Extracted from search response |
| `scoreBreakdown`, `score` | Taxonomy weights, local scoring, and capped total | Derived locally |
| `outreachPlan`, `outreachDraft` | Static outreach plays interpolated with opportunity fields | Generated locally |
| `foundAt` | Scan execution timestamp | Derived locally |
| `qualificationStatus`, `qualificationReason` | Fetch status, geography, recency, deadline, and buyer/editorial rules | Derived locally |

Search adapters should normalize provider payloads before extraction:

```js
{
  title,
  description,
  url,
  publishedAt
}
```

The supported internet search response mappings are Exa (`results[].title`, `text`/`summary`, `url`, `publishedDate`), Tavily (`results[].title`, `content`, `url`, `published_date`), Google (`items[].title`, `snippet`, `link`, article/date metadata), and Brave (`web.results[].title`, `description`, `url`, `page_age`).

### Career-job fields

| Field | Required source/extraction path | Classification |
|---|---|---|
| `title` | Live search result title; generic JSON `title`/`name`; RSS `<title>`; or manual job record | Extracted or manually supplied |
| `company` | Live result title/URL hostname; JSON `company`/`organization`/`companyName`; RSS `<company>`/`<source>`; or manual record | Extracted or manually supplied |
| `location` | Live result snippet regex or remote marker; JSON `location`/`city`; RSS `<location>`; or manual record | Extracted or manually supplied |
| `remote` | Remote terms in title, location, and description; or supplied boolean | Extracted/derived or manually supplied |
| `employmentType` | Live job-language inference; JSON `employmentType`/`type`; RSS contract-language inference; or manual record | Extracted/derived or manually supplied |
| `c2cEligible`, `c2cStatus` | C2C terms and exclusion terms in live title/description; supplied JSON/manual values; otherwise explicit unverified default | Extracted/derived or defaulted |
| `compensation` | Dollar/pay text in live search description; JSON `compensation`/`salary`/`pay`; or manual record | Extracted or manually supplied |
| `source` | Search provider name, JSON source, RSS marker, or manual source | Assigned from ingestion path |
| `sourceUrl` | Live result URL; JSON `sourceUrl`/`url`/`applyUrl`; RSS `<link>`; or manual record | Extracted or manually supplied |
| `description` | Live result title plus description; JSON `description`/`summary`; RSS `<description>` with tags removed; or manual record | Extracted or manually supplied |
| `importedAt` | Import/scan timestamp or supplied source timestamp | Derived or manually supplied |
| `status` | Supplied job status or ingestion default | Assigned/defaulted locally |
| `discoveryQuery` | Search query that returned the job | Derived from search process |
| `discoveryNote` | Human or system note about why the job was retained | Assigned/generated locally |

Career ingestion paths are intentionally explicit: local manual JSON, a caller-supplied RSS URL, a caller-supplied JSON endpoint, or live search results. There is no implicit job-API catalog and no automatic page scrape after a live job-search result. Any future implementation that adds page fetching should preserve the raw job-page URL and distinguish page-extracted fields from search-result fields.

### Provenance requirement

For every extracted field, retain a provenance label such as `search`, `sourcePage`, `rss`, `jsonApi`, `manual`, `hardcoded`, or `derived`, plus the contributing source URL where one exists. Do not present taxonomy defaults, local templates, or fallback values as if they were extracted from the internet.
