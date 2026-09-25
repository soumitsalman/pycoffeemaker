# Feedly Content Collection & AI Capability Inventory
Evidence-based from public product/docs/marketing pages (WebFetch). Browser screenshots not captured (Browser-use MCP CDP unavailable). No signup/login attempted; public pages loaded without login walls.

Research date: Fri Sep 18, 2026 (America/New_York)

---

## 1. Source types Feedly supports

Evidence primarily from:
- https://feedly.com/new-features/posts/the-10-types-of-sources-you-can-add-on-feedly (Aug 11, 2020; lists “11 and counting”)
- https://docs.feedly.com/article/768-follow-sources-in-feedly (updated Jul 31, 2025)
- https://docs.feedly.com/article/844-import-files-to-feedly
- https://docs.feedly.com/article/627-new-patents

| Source / ingest path | Notes (as stated publicly) |
|---|---|
| **Websites / blogs / trade pubs / news / research journals** | Search by name or paste URL; Feedly auto-detects RSS; includes Medium with extra steps |
| **RSS feeds** | Classic ingest; paste feed URL |
| **RSS builder** | If no RSS, analyzes structured list pages (blog archives/news) and builds a feed |
| **Newsletters (email)** | Custom Feedly email address; subscribe or forward; Pro+/Enterprise historically; converted to article format |
| **Keyword alerts** | Boolean AND/OR keyword monitoring across web |
| **YouTube channels** | Search, channel URL, or OPML import of YouTube subscriptions |
| **Podcasts** | Via podcast RSS URL |
| **X / Twitter** | Accounts, hashtags, searches, Lists (older marketing); docs: official API + user API key (~$100–200/mo) |
| **Reddit** | Subreddit URL or `r/name`; appears as article stream |
| **Curated bundles** | Themes: cybersecurity, vuln intel, national newspapers, foreign affairs, threat intel, tech/innovation; Market Intel bundle ~26k sources (docs) |
| **PDF / file upload** | Team feature: PDF ≤30 MB (bulk ≤10); not a continuous “source feed” but ingest into Boards / Research / Files |
| **Patents** | Not a raw source connector; **Enterprise AI Model “New Patents”** flags new/provisional/assigned/filed patents (excludes lawsuits/expirations/settlements). Historically marketed as a “Leo Model” |
| **OPML** | YouTube OPML import explicitly; Feedly generally known for OPML (not re-verified beyond YouTube mention on sources post) |

Homepage (https://feedly.com/) claims **10,000+ trusted TI sources** and continuous analysis of large article/entity graphs (marketing scale claims).

Market Intel AI Feed docs claim scanning **~100M articles / ~140M sources daily** (marketing scale).

---

## 2. Content / artifact types

What Feedly surfaces as first-class content objects (from docs + marketing):

- **Articles** (from RSS, newsletters, Reddit, keyword alerts, AI Feeds, search)
- **Tweets** (X integration)
- **Podcast episodes** (via RSS)
- **YouTube videos** (channel subscriptions as feed items)
- **Uploaded PDFs** → processed into “article versions” (extracted text + metadata) for Ask AI / Report Builder / newsletters (team-only link)
- **Insights Cards** (e.g. Cyberattack Insights Cards) — structured attack nodes
- **Threat Graph nodes** — relationships among victims, actors, malware, CVEs, IOCs, TTPs
- **AI Feed result streams** — filtered/tagged articles matching AI Models + Boolean query
- **Top Stories** — stories appearing in ≥5 publications
- **Newsletters / issues** — automated digests from Boards / AI Feeds / Folders
- **Ask AI / AI Actions outputs** — summaries, reports, tables (IoCs, TTPs, tech monitoring, sentiment)
- **Reports** (Report Builder mentioned on homepage + file-upload docs)
- **STIX-formatted threat intel** for security-stack integrations (homepage claim)

---

## 3. Enrichment / NLP / AI features

### A. Classifier / concept tagging (“Feedly AI” / historically “Leo”)
- Large library of **pre-trained AI Models** (docs: **30,000+** for cyber/intel concepts; Market Intel marketing: **1,000+ market concepts**)
- Concept understanding beyond keyword match (context/nuance)
- **Smart tagging** of articles with concepts (e.g. TTP recognition even when wording varies)
- Threat-intel entity extraction (docs + Insights Cards): victims, industries, threat actors, malware families, CVEs, IOCs, TTPs
- Homepage: **1,000+ AI models** extract entities, identify incidents, tag TTPs; relationships across articles, threat actors, CVEs, malware, TTPs, IoCs
- Market features: prioritize topics/trends/keywords; **deduplicate**; **mute**; **summarize**
- Patent detection model (Enterprise)
- Source quality: **three-tier ranking**; filters **Top sources only**, **Top stories only**

### B. AI Feeds (core collection+filter layer)
- Boolean composition of AI Models + keywords (AND / OR / NOT)
- Scope: title+content vs title-only (or “first 2–3 sentences” when narrowing high-volume feeds — Market Intel guide)
- Source scope options: Market Intel Bundle, All Feedly Sources (~140M), All Team Feeds, industry/press-release/research-journal bundles
- Continuous web scan → curated stream (not raw RSS dump)

### C. Generative AI on selected content
- **Ask AI** (Threat Intelligence): Analyze button; up to **50** articles on Boards/Folders/Feeds (TI guide) / marketing also cites 25 for synthesis; **inline citations**; not multi-turn chatbot
  - Suggested: executive summary, report, vuln advisory, threat actors+TTPs table, IoCs table
  - Expert prompt library; saved prompts; summary/translation at single-article level
- **AI Actions** (Market Intelligence; related/earlier naming): up to **25** articles
  - Summarization, translation→EN, executive summary, report, technology monitoring table, market trends, **sentiment and brand monitoring**
- **Research / Analyze** on uploaded PDFs (LLM insights + Ask AI)
- **Report Builder** (homepage + file docs): tailored briefings with citations
- **AI Agents** (Ask AI access points listed)
- Homepage: Ask AI + Report Builder; deep citations; STIX delivery

### Naming note (Lexi / Leo)
- Current public branding: **Feedly AI**, **AI Models**, **Ask AI**, **AI Actions**, **AI Feeds**
- **Leo** appears historically (e.g. “New Patents Leo Model”, 2022 Reddit/Feedly post)
- **Lexi** not found on the public pages fetched in this pass

---

## 4. Organization features (data-model relevant)

From https://docs.feedly.com/article/805-feeds-and-boards and related docs:

| Construct | Role |
|---|---|
| **Folders** | Group Team Feeds (AI Feeds + RSS); best practice: separate `sources - *` vs `ai - *` |
| **Source Feeds / RSS Feeds** | Unfiltered ingest streams |
| **AI Feeds** | Filtered/enriched streams |
| **Personal vs Team Feeds/Boards** | Privacy / collaboration boundary |
| **Boards / Team Boards** | Manual curation; notes/comments; newsletter/Slack/email handoff; PDF upload target |
| **Files page** | Team hub for uploaded PDFs |
| **Automated Newsletters** | Pull from Boards, AI Feeds, Folders on schedule; branding + analytics |
| **Integrations** | Slack, Teams, email, STIX/security stack (homepage); board-triggered automations |
| **Power Search / Search / Top Stories** | Discovery + Ask AI entry points |
| **Workspace settings** | e.g. File Upload admin toggle |

---

## 5. Gaps vs a typical RSS + scraper pipeline

What Feedly adds beyond “poll RSS + scrape HTML”:

1. **RSS builder** for sites without feeds (list-page → synthetic feed)
2. **Non-RSS connectors**: newsletters (email→article), Reddit, X (API-keyed), YouTube, podcasts
3. **Cross-web keyword alerts** and **huge source index** (bundles / “all sources”) — not just user-subscribed RSS
4. **Concept/entity NLP layer** (thousands of models) + Boolean AI Feeds vs keyword-only filters
5. **Dedup / mute / top-sources / top-stories** ranking and clustering signals
6. **Threat graph + Insights Cards** with continuous enrichment (incident as evolving object)
7. **GenAI synthesis** with grounded citations (Ask AI / AI Actions / Report Builder)
8. **PDF proprietary ingest** into same enrichment/Ask AI pipeline
9. **Team curation model** (Boards, notes, newsletters, STIX/export integrations)
10. **Patent / market-concept / TI-specific taxonomies** as first-class models

What a DIY RSS+scraper may still need that Feedly markets but is hard:
- Maintaining 10k–140M source coverage and freshness
- Training/maintaining concept models at Feedly’s claimed scale
- Entity linking into a live threat/market graph
- Trusted source ranking and multi-publisher “top stories”
- Citation-grounded multi-doc report generation productized for teams

Not observed as public continuous sources (in pages visited): generic “all social networks,” arbitrary APIs, or non-PDF file types for upload (PDF only stated).

---

## 6. URLs visited (this agent)

- https://feedly.com/
- https://feedly.com/ai
- https://feedly.com/new-features/posts/the-10-types-of-sources-you-can-add-on-feedly
- https://feedly.com/new-features/posts/meet-feedly-ai-for-market-intelligence
- https://docs.feedly.com/article/768-follow-sources-in-feedly
- https://docs.feedly.com/article/764-what-is-an-ai-feed-feedly
- https://docs.feedly.com/article/723-guide-to-ai-actions-ti
- https://docs.feedly.com/article/741-guide-to-ai-actions-for-feedly-market-intelligence
- https://docs.feedly.com/article/828-how-cyberattack-insights-cards-are-generated-feedly
- https://docs.feedly.com/article/844-import-files-to-feedly
- https://docs.feedly.com/article/627-new-patents

Also referenced via search snippets (not all fully re-fetched): AI Feeds market intel guide (699), Team Feeds/Boards (805), Automated Newsletters (692), Ask AI marketing posts.

---

## 7. Screenshots

**None saved.** Browser-use MCP could not attach to Chrome CDP (`chrome-not-running` / daemon failure). Parent noted same. All evidence from WebFetch/WebSearch of public pages.

Report file: `/workspace/feedly-research/feedly-capability-inventory.md`
