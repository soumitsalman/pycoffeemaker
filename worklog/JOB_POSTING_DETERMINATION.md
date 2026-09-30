The job discovery code performs **web searches, filters the returned snippets, and turns accepted snippets into job records**. It never follows the result link to read or verify the job posting. Here is the complete mechanism, including the details that affect what you actually get.

**1. The search requests**

In [search-discovery.js](/home/soumitsr/codes/hcdx-intelligence-platform/src/sources/jobs/search-discovery.js:58), each search uses one of these endpoints:

| Provider | Hard-coded endpoint | Content returned to the job filter |
|---|---|---|
| Exa | `https://api.exa.ai/search` | Title, URL, and requested page text capped at 1,200 characters |
| Tavily | `https://api.tavily.com/search` | Title, URL, and result content; basic search, no raw content |
| Brave | `https://api.search.brave.com/res/v1/web/search` | Title, URL, description |
| Google | `https://www.googleapis.com/customsearch/v1` | Title, link, snippet |

Default: **30 queries, 5 results per query**, so up to 150 candidate results before filtering and deduplication. There is no pagination.

With `SEARCH_PROVIDER=auto`, it tries configured providers in this order: **Exa → Tavily → Brave → Google**. For each query, the first nonempty response wins.

A consequential detail: **if that response contains five results and the job filter rejects all five, the code does not try the next provider.** Provider fallback happens before job filtering.

**2. Exactly what it searches for**

[buildCareerSearchQueries()](/home/soumitsr/codes/hcdx-intelligence-platform/src/sources/jobs/search-discovery.js:87) applies these four templates, in order, to each configured role:

```text
"<role>" "remote contract"
"<role>" ("C2C" OR "corp to corp" OR "corp-to-corp") remote
"<role>" "fractional consultant"
"<role>" "digital transformation"
```

The [current profile](/home/soumitsr/codes/hcdx-intelligence-platform/config/career-profile.json:5) supplies the following roles. With the default 30-query limit, this is the actual coverage:

| Role | Searches executed |
|---|---|
| Digital Transformation Consultant | All four |
| Product Strategy Consultant | All four |
| Digital Product Consultant | All four |
| Experience Strategy Consultant | All four |
| Principal UX Consultant | All four |
| Service Design Lead | All four |
| Accessibility Program Manager | All four |
| AI Product Consultant | First two only |
| Fractional Product Advisor | None |
| Fractional Chief Experience Officer | None |
| Fractional Chief Digital Officer | None |
| Director of Product | None |
| Director of Digital Experience | None |
| Director of CX | None |

For example, the first four actual requests use:

```text
"Digital Transformation Consultant" "remote contract"
"Digital Transformation Consultant" ("C2C" OR "corp to corp" OR "corp-to-corp") remote
"Digital Transformation Consultant" "fractional consultant"
"Digital Transformation Consultant" "digital transformation"
```

After the 56 role searches, the code appends these nine literal queries:

```text
"UX strategy" "contract" "remote" "apply"
"accessibility program manager" "WCAG" "remote" "apply"
"service design lead" "contract" "remote" "apply"
"AI product consultant" "workflow" "remote" "apply"
"fractional chief experience officer" "apply"
"director digital experience" "remote" "apply"
("C2C" OR "corp to corp") ("UX strategy" OR "service design" OR accessibility) remote
("C2C" OR "corp-to-corp") ("digital transformation consultant" OR "product strategy consultant") remote
("C2C" OR "corp to corp") ("AI product consultant" OR "AI workflow") remote
```

**None of those nine runs under the default limit.** There is no rotation between runs: each run starts at query one again.

These searches contain no `site:` restriction, publication-date restriction, or geographic restriction. The source sites are whatever the search provider returns.

**3. The exact decision that a result looks like a job**

[looksLikeJobResult()](/home/soumitsr/codes/hcdx-intelligence-platform/src/sources/jobs/search-discovery.js:289) constructs:

```js
text = title + description + url + query
```

It lowercases those values and applies the following checks.

The URL must contain at least one of these literal substrings:

```text
/job
/jobs
/careers
/career
/positions
/opening
/apply
greenhouse.io
lever.co
workdayjobs.com
ashbyhq.com
smartrecruiters.com
icims.com
builtin.com/job
remoteotter.com/company
```

These are **recognition patterns, not sites the code visits to collect listings**. An arbitrary website with `/jobs` in its URL qualifies for this part of the test.

The result **title** must contain at least one of:

```text
consultant, product, strategy, strategist, experience, ux,
service design, accessibility, program manager, director,
fractional, chief, advisor, transformation, ai
```

The combined `text` must contain at least one of:

```text
job, jobs, careers, hiring, opening, role, position, apply
```

It rejects results matching:

```text
LinkedIn profile URL: linkedin.com/in/
Career advice URL: /career-advice/
Salary content: /salary, salaries, with salaries,
                salary benchmark, people also searched
Bare homepage URL
Other content: template, course, certification, definition,
               examples, interview questions, what is
```

Finally, the combined text must match:

```regex
job|career|greenhouse|lever|workday|ashbyhq|smartrecruiters|icims|apply|remote|contract|fractional|consultant|director|manager|lead
```

There are three practical weaknesses in those checks:

- **Substring matching:** `ai` can match inside an unrelated word. Domain strings are checked anywhere in the URL, rather than checking its actual hostname.
- **The search query counts as evidence:** words supplied by the search itself can satisfy parts of the predicate.
- **A listing page is not required:** a careers directory or job-search results page can pass. There is no check for `JobPosting` structured data, an individual requisition ID, an application form, or whether the opening is still active.

**4. What “follow up from search” actually does**

Once a result passes, [jobFromSearchResult()](/home/soumitsr/codes/hcdx-intelligence-platform/src/sources/jobs/search-discovery.js:252) immediately builds a record from the returned title and snippet.

| Field | How it is obtained |
|---|---|
| Job title | Parses patterns such as `Title at Company` or `Title - Company`; otherwise uses the cleaned result title |
| Company | Parses the title; otherwise derives a name from the URL hostname |
| Description | Concatenates search-result title and snippet |
| Posting URL | Copies the search-result URL |
| Remote | Checks for `remote` in title/snippet |
| Location | Checks snippet for `remote`, otherwise a `City, ST` pattern |
| Compensation | Extracts the first matching dollar amount/range from the snippet |
| C2C | Checks title/snippet for acceptance or rejection terms |
| Employment type | Checks **title + snippet + search query** for fractional, contract/consultant/consulting, or part-time; otherwise defaults to full-time |

That employment-type rule means a result from the `"remote contract"` query can be labeled **Contract even when its own text never says contract**. Likewise, the fractional query can make it **Fractional**.

If company parsing fails for a Greenhouse result, the hostname fallback can produce **“Greenhouse” as the employer**.

**There is no second request to the posting URL, no search to verify the employer, and no additional search to fill missing information.** The source URL is saved for a person to open.

**5. What controls whether the result survives and what happens next**

[run-career-scan.js](/home/soumitsr/codes/hcdx-intelligence-platform/src/career/run-career-scan.js:36) then scores each record using [job-fit-scorer.js](/home/soumitsr/codes/hcdx-intelligence-platform/src/scoring/job-fit-scorer.js:1).

Most scoring categories award their full weight when **any one** category keyword appears:

| Category | Points |
|---|---:|
| HCD leadership | 12 |
| Product strategy / configured target-role match | 12 |
| Accessibility | 10 |
| Enterprise software | 8 |
| Federal / regulated work | 8 |
| Research | 9 |
| Service design | 8 |
| HTML / prototyping | 6 |
| AI workflow | 9 |
| Agile delivery | 7 |
| Work arrangement | Up to 7 |
| Compensation | Up to 4 |

Live scans default to dropping anything below **25**. The score is based on available record text, which for search results is largely the title and snippet—not the full posting.

Survivors are deduplicated using company, title, and exact source URL. Every survivor gets a template-generated application package and is written to `out/latest-career-scan.json`.

The `nextAction` field is only an instruction string:

```text
score >= 85  → review package and seek Evan's approval
score >= 70  → review gaps and decide whether to tailor
otherwise, compensation missing → research pay
otherwise → watch list
```

Those instructions **do not trigger further searches or actions**. Even “research pay” is just displayed text. Application packages are produced for every retained job regardless of that recommendation.

A further detail: discovery initially records `discoveryQuery` and `discoveryNote`, but `normalizeJob()` does not preserve them in the final job object, so the saved queue loses which query found the posting.

**6. The other three ingestion mechanisms**

These bypass the search-result job predicate entirely:

- [Manual JSON](/home/soumitsr/codes/hcdx-intelligence-platform/src/sources/jobs/manual-import.js:3): reads an array or `{ jobs: [...] }` from `data/career-jobs.json`, or a supplied `--input` path.
- [RSS](/home/soumitsr/codes/hcdx-intelligence-platform/src/sources/jobs/rss-feed.js:1): fetches the supplied `--rss` URL once, parses `<item>` elements, and treats items with a title or description as jobs.
- [JSON API](/home/soumitsr/codes/hcdx-intelligence-platform/src/sources/jobs/api-feed.js:1): fetches the supplied `--api` URL once and treats its array, `jobs`, or `results` entries as jobs.

There are no built-in feed URLs, pagination, or follow-up fetches in those adapters. They feed into the same scoring and package-generation process.