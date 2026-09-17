# Content-specific extraction requirements

Scope: all 28 canonical `kind` values in `utils/kinds.py`. This document proposes information to extract, not implemented models or determinations of legal or financial effect. Field names are proposed schema names; all source-dependent facts are optional unless supported by the supplied content.

## Current behavior

`workers/analyzerorch.py::Digestor` always selects `output_model=Digest`. `Digest` in `nlp/models.py` is event-oriented: `key_points`, `drivers`, `event_type`, `impacts`, `impacted_domains`, `impact_level`, `macro_context`, `future_outlook`, and `briefing`. This is useful for reading summaries but cannot preserve clauses, financial periods, legal provisions, research methods, or API signatures as structured records. Its required `impact_level` string also encourages an assessment where none may be stated.

`Extractor` separately produces entity tags, not relationships such as plaintiff/defendant or buyer/supplier. Existing financial and topic-specific `Digest` subclasses provide useful field ideas but are not selected by this worker and inherit its event requirements. The financial core assumes USD millions; general extraction must preserve original currency and scale.

The examples “SEC report” and “legislation” map respectively to `sec_filing` and the distinct legislative kinds below. `sec_report` and `legislation` are not canonical values. Choose a profile for the document itself: a news story about a complaint remains `news`; the complaint is `lawsuit`.

## Shared contract

Use a small common envelope plus kind-specific structured records. The pipeline supplies identity/version/coverage where possible; the model extracts supported content.

| Shared field | Information |
|---|---|
| `kind`, `schema_version` | Canonical kind and extraction schema revision, assigned by the pipeline. |
| `document` | Source URL, title, issuer/publisher, author, language, document identifiers and version. Distinguish input metadata from text-derived facts. |
| `dates` | Labeled publication, filing, signing, effective, reporting-period, amendment and deadline dates. Preserve timezone and date precision; never invent missing days. |
| `briefing` | Short, kind-appropriate summary of what the document is and its supported substance; omit for unusable input. |
| `key_points` | Optional reading aid derived from the structured records, not a replacement for them. |
| `actors` | People/organizations with document roles and stated identifiers. Existing entity tags can be a search projection. |
| `evidence` | Field/record path linked to source URL and available section, page, paragraph, table/cell, offsets or timestamps; optionally a short exact supporting span. Do not invent locators. |
| `coverage` | Pipeline-recorded full text/excerpt/metadata-only/truncated status, sections processed, missing tables/attachments/transcript and extraction failures. |

Reusable record shapes:

- **Claim:** statement, attributed source/speaker, status (allegation, opinion, finding, forecast, commitment), qualifiers and evidence. Status describes how the source presents the statement, not independent truth verification.
- **Quantity:** metric, exact value/range, unit, currency, scale, period/as-of date, reporting basis, scope/segment, comparator and evidence. Preserve percentages versus percentage points and adjusted versus unadjusted values.
- **Obligation:** responsible actor, required/permitted/prohibited action, beneficiary, trigger, deadline/recurrence, conditions, exceptions, consequences and clause reference. Preserve “may” versus “must.”
- **Provision/change:** section, subject, rule summary, affected actors, applicability, exceptions, dates, cited authority and explicit amendment/repeal target.
- **Deadline:** action, responsible actor, absolute date or original relative expression, trigger, timezone and evidence. Computed dates belong in separately labeled derived fields.

Missing means omitted, not zero, false, “none,” or an invented value. Empty lists do not establish absence. Explicit negatives require evidence. Keep conflicting statements attributed. Status is **as of the source**, not necessarily current. Do not compute conversions, growth rates, consensus surprises or projected consequences as extracted facts. Do not require generic impact, severity, sentiment, or causal judgments across kinds.

## Profiles

Each profile lists the target information and the key distinction the extraction must preserve. Fields are optional when unsupported. Repeated entities, clauses, metrics, provisions and claims should be records rather than flattened prose.

### `post`

Purpose: preserve a short authored assertion, reaction, question or announcement in context.

| Fields | Information to extract |
|---|---|
| `author`, `platform`, `post_id`, `posted_at` | Post identity and authorship. |
| `post_type`, `main_message`, `claims` | Announcement/opinion/question/reply/repost; central message, attributed assertions, uncertainty and stated metrics. |
| `reply_to`, `quoted_content`, `linked_resources` | Available parent/quoted context and URLs; distinguish quoted authors from the poster. |
| `requests`, `commitments`, `disclosures` | Calls to action, promises, sponsorship and affiliations. |

Boundary: reposting does not establish endorsement. Do not reconstruct unavailable thread context. Engagement counts are observed metadata with an observation time, not enduring facts.

### `blog`

Purpose: preserve the author's thesis, reasoning, experience and advice.

| Fields | Information to extract |
|---|---|
| `author`, `article_type`, `thesis` | Authorship and purpose: analysis, opinion, tutorial, experience report, etc. |
| `arguments`, `evidence`, `counterarguments` | Main claims, reasoning, examples and cited sources. |
| `methods_or_steps` | Ordered procedure, prerequisites, inputs and outputs for instructional content. |
| `recommendations`, `conclusions`, `limitations`, `disclosures` | Advice and conclusions with conditions, caveats and disclosed interests. |

Boundary: distinguish anecdote/opinion from measured results; evergreen explanations need not become dated events.

### `news`

Purpose: preserve the reported event, chronology, evidence and responses.

| Fields | Information to extract |
|---|---|
| `event_type`, `events`, `actors`, `affected_parties` | Who did what, when and where; roles and event dates distinct from publication. |
| `claims`, `sources`, `responses` | Reported facts, allegations, quotes, denials and attribution. |
| `metrics`, `comparisons` | Amounts, counts, changes and their comparison periods. |
| `drivers`, `observed_impacts`, `next_steps` | Explicit causal explanations, observed effects and announced follow-ups. |
| `background`, `uncertainties`, `corrections` | Context, unresolved questions and stated corrections. |

Boundary: closest fit to existing `Digest`, but causality, impact severity and outlook remain optional and evidence-dependent.

### `site`

Purpose: describe the organization, product, service or resource represented by a webpage.

| Fields | Information to extract |
|---|---|
| `page_type`, `subject`, `operator`, `description` | Homepage/about/product/pricing/directory/etc.; subject and owner/operator. |
| `offerings`, `capabilities`, `audiences`, `use_cases` | What is offered, functions, target users and uses. |
| `industries`, `geographies`, `eligibility`, `access_requirements` | Market scope, availability and access conditions. |
| `pricing`, `plans`, `specifications`, `integrations` | Listed currency, billing cadence, plan limits, technical specifications and supported integrations. |
| `resources`, `contact_channels`, `locations`, `calls_to_action`, `claims` | Useful links, contact/signup paths, locations and attributed marketing/certification claims. |

Boundary: summarize the supplied page, not an unseen entire site. Do not invent launch events or treat promotional claims as independent verification.

### `job`

Purpose: preserve the role and conditions needed to evaluate or apply for it.

| Fields | Information to extract |
|---|---|
| `job_title`, `employer`, `requisition_id`, `team` | Role identity and hiring organization. |
| `locations`, `work_arrangement`, `employment_type`, `seniority` | On-site/hybrid/remote conditions, geography, employment basis and stated level. |
| `responsibilities`, `required_qualifications`, `preferred_qualifications` | Duties, skills, credentials and experience; separate mandatory and desirable requirements. |
| `compensation`, `benefits`, `schedule`, `travel`, `eligibility` | Pay range/currency/period and location dependence, bonus/equity, hours, travel, sponsorship/authorization/clearance requirements. |
| `application_url`, `application_requirements`, `deadlines`, `posting_status` | How to apply, required materials, stated closing date and published status. |

Boundary: do not infer salary, visa sponsorship or continuing availability from silence.

### `podcast`

Purpose: preserve an episode's attributed discussion and navigable topics.

| Fields | Information to extract |
|---|---|
| `show`, `episode_title`, `episode_number`, `published_at`, `duration` | Episode identity and metadata. |
| `hosts`, `guests`, `speaker_roles` | Participants and stated affiliations. |
| `topics`, `segments`, `claims`, `arguments` | Topic summaries with available timestamps; speaker-attributed positions, examples and disagreements. |
| `takeaways`, `recommendations`, `resources`, `disclosures` | Conclusions, advice, linked resources and sponsorship separate from editorial discussion. |

Boundary: show notes cannot support claims about an unheard conversation. Record missing transcript coverage and uncertain speaker attribution.

### `contract`

Purpose: preserve who must do what, on which terms, for how long and with which exceptions.

| Fields | Information to extract |
|---|---|
| `contract_type`, `agreement_id`, `parties`, `document_status` | Identity, party roles and evidenced draft/executed/amendment status. |
| `execution_dates`, `effective_date`, `term`, `renewal` | Signing, commencement, duration, renewal and notice windows. |
| `scope`, `deliverables`, `milestones`, `acceptance`, `service_levels` | Contracted goods/services, schedule and acceptance/performance criteria. |
| `obligations`, `rights`, `conditions`, `commercial_terms` | Party duties, permissions, triggers, exceptions, price/currency, payment schedule, taxes and adjustments. |
| `ip`, `confidentiality`, `data_handling`, `restrictions` | Ownership/licensing, privacy/security duties, assignment and exclusivity. |
| `warranties`, `indemnities`, `liability`, `insurance` | Risk allocation with caps, exclusions, carve-outs and survival terms. |
| `termination`, `remedies`, `dispute_resolution`, `governing_law` | Exit rights, notice/cure periods, consequences, forum/arbitration and governing law. |
| `amendments`, `incorporated_documents`, `precedence` | Changed clauses, referenced schedules and stated order of precedence. |

Boundary: do not determine enforceability or infer missing signatures or unseen exhibits. Keep exceptions attached to their obligations.

### `procurement_notice`

Purpose: preserve an opportunity, eligibility, submission rules and selection process.

| Fields | Information to extract |
|---|---|
| `notice_type`, `notice_id`, `buyer`, `status` | RFI/solicitation/tender/award/cancellation/amendment and issuing authority. |
| `scope`, `lots`, `deliverables`, `performance_location`, `period` | Requested work, quantities, lot structure and schedule. |
| `estimated_value`, `funding`, `contract_type`, `eligibility` | Budget/currency/basis, commercial form, set-asides, qualifications and bonds/security requirements. |
| `submission_requirements`, `submission_channel`, `deadlines` | Documents, formats, portal/address, questions/site visit/bid deadlines and timezone. |
| `evaluation_criteria`, `weights`, `award_process`, `contact` | Selection rules, weighting, stages and procurement contact. |
| `amendments`, `award_details` | Changed requirements/deadlines; winner and award amount only where applicable. |

Boundary: an estimated ceiling is not committed spend; solicitation conditions are not an executed contract.

### `financial_report`

Purpose: preserve financial position, performance, cash flows and reporting context.

| Fields | Information to extract |
|---|---|
| `reporting_entity`, `report_type`, `reporting_period`, `reporting_scope` | Annual/interim/other report, covered period and consolidated/standalone scope. |
| `accounting_basis`, `currency`, `scale`, `audit_status`, `auditor_opinion` | Framework, presentation units, audit/review scope, auditor and opinion. |
| `income_statement`, `balance_sheet`, `cash_flow` | Reported line items as quantities with periods, units, basis and comparators. |
| `segments`, `geographies`, `operating_metrics` | Business/geographic breakdowns and nonfinancial measures. |
| `policies`, `restatements`, `one_time_items` | Accounting changes, restated periods, reconciliations and exceptional items. |
| `liquidity`, `debt`, `commitments`, `contingencies`, `related_parties` | Maturities, covenants, exposures and related-party disclosures. |
| `management_discussion`, `risks`, `outlook` | Attributed explanations, uncertainties, going-concern disclosures and forecasts. |

Boundary: do not assume USD, US GAAP, public-company status or an audit. Preserve table footnotes, negative values and comparative columns.

### `earnings_report`

Purpose: preserve period results, management explanations, guidance and call discussion.

| Fields | Information to extract |
|---|---|
| `issuer`, `ticker`, `document_subtype`, `fiscal_period`, `release_date` | Release/transcript/presentation/package and covered period. |
| `results`, `segment_results`, `operating_kpis` | Revenue, earnings/EPS, margins, cash flow and business metrics with comparisons. |
| `adjustments`, `reconciliations`, `performance_drivers`, `one_time_items` | Reporting basis, adjusted measures, reconciliations and management's explanations. |
| `guidance`, `guidance_changes`, `assumptions` | Forecast metric/range/period, supplied prior guidance and conditions. |
| `capital_allocation`, `liquidity`, `qna` | Dividends, buybacks, investment/financing; attributed questions, answers and unresolved questions if a transcript exists. |
| `consensus_comparisons` | Beat/miss with comparison source only if stated in supplied material. |

Boundary: separate results from forecasts; do not invent consensus surprise, market reaction or management sentiment.

### `sec_filing`

Purpose: preserve filing identity and disclosures appropriate to its form.

| Fields | Information to extract |
|---|---|
| `form_type`, `amendment_flag`, `accession_number`, `filers`, `issuer`, `cik` | Exact filing identity and party roles; distinguish reporting owner and issuer. |
| `filed_at`, `reporting_period`, `event_date`, `amends` | Submission versus covered period/event and referenced prior filing. |
| `items`, `material_disclosures`, `exhibits` | Section/item identifiers, disclosures and exhibit references; identify unavailable attachments. |
| `periodic_report` | Applicable annual/interim financials, MD&A, business, risks, controls, auditor and proceedings. |
| `current_report` | Reported items, events, parties, dates, transaction terms, management changes and other developments. |
| `offering` | Registration/prospectus securities, terms, proceeds/use, dilution, selling holders and risks. |
| `ownership` | Reporting person, security/class, holdings, transaction dates/codes, quantity, price and ownership basis. |
| `governance` | Proxy meeting, proposals, elections, compensation, voting procedures and disclosed conflicts. |

Boundary: select relevant sections by form; earnings fields cannot be mandatory for every filing. Preserve forms beyond 10-K/10-Q/8-K and unknown form text. Filing does not imply regulatory approval.

### `press_release`

Purpose: preserve an issuer's announcement, commitments and supporting claims.

| Fields | Information to extract |
|---|---|
| `issuer`, `release_date`, `announcement_type`, `headline_claim` | Issuer and principal announcement. |
| `announced_actions`, `parties`, `terms`, `metrics` | Product/deal/initiative details, roles, scope and quantities. |
| `availability`, `milestones`, `conditions`, `next_steps` | Planned versus completed action, dates, approvals and dependencies. |
| `quotations`, `claims`, `resources`, `forward_looking_statements`, `media_contact` | Attributed statements, supporting resources, forecasts/caveats and press contact. |

Boundary: announced is not completed; claimed benefits remain issuer claims. An earnings release already labeled `earnings_report` uses that financial profile.

### `official_statement`

Purpose: preserve an institution's position and expressly announced action.

| Fields | Information to extract |
|---|---|
| `issuing_body`, `speaker`, `capacity`, `issued_at` | Who speaks, in which official role and when. |
| `subject`, `occasion`, `audience`, `positions`, `rationale` | Context, audience, declared position, attributed claims and reasons. |
| `announced_actions`, `commitments`, `requests` | Decisions, promises, directives and calls for action with responsible actors. |
| `scope`, `conditions`, `dates`, `cited_authority`, `references` | Affected scope, qualifications, timeline, cited basis and linked orders/policies. |

Boundary: an official position or intention is not automatically a binding order, enacted rule or completed action.

### `enforcement_action`

Purpose: distinguish allegations, findings, procedural action and imposed remedies.

| Fields | Information to extract |
|---|---|
| `authority`, `jurisdiction`, `case_id`, `action_type`, `stage` | Agency, matter identity and complaint/order/settlement/etc. stage. |
| `respondents`, `other_parties`, `conduct_period` | Targets and roles, affected parties and relevant dates. |
| `allegations`, `cited_provisions`, `findings`, `admission_terms` | Conduct and cited rules separately from findings, admissions, denials or neither-admit-nor-deny terms. |
| `disposition`, `penalties`, `restitution`, `disgorgement`, `restrictions` | Result and remedy type, amount/currency, responsible actor and restriction scope. |
| `remediation`, `monitoring`, `deadlines`, `appeal_status` | Corrective action, reporting, compliance dates and stated review rights/status. |

Boundary: distinguish requested from ordered relief and proposed from final penalties. Settlement alone does not establish admission.

### `legislative_bill`

Purpose: preserve a formally introduced legislative text, its proposed changes and recorded progress.

| Fields | Information to extract |
|---|---|
| `bill_id`, `title`, `jurisdiction`, `legislature`, `session`, `chamber` | Bill identity and legislative context. |
| `sponsors`, `cosponsors`, `committees`, `version`, `version_date` | Responsible legislators, referrals and exact text version. |
| `purpose`, `provisions`, `affected_laws`, `amendments` | Proposed rules, new/amended/repealed sections and explicitly identified changes. |
| `covered_entities`, `obligations`, `rights`, `exceptions`, `enforcement` | Proposed applicability, duties, permissions, exemptions, enforcement bodies and penalties. |
| `proposed_effective_dates`, `implementation`, `fiscal_estimates` | Conditional commencement, implementation steps and attributed cost estimates. |
| `status`, `actions`, `votes`, `next_steps` | Source-stated introduction/referral/passage/etc., dated actions, recorded vote totals and scheduled steps. |

Boundary: a bill's proposed obligations are not current law. Do not infer passage, enactment or current status beyond the supplied record.

### `legislative_proposal`

Purpose: preserve a policy concept or draft intended for legislative consideration.

| Fields | Information to extract |
|---|---|
| `title`, `proponents`, `jurisdiction`, `proposal_form`, `version` | Sponsors/proponents, setting, concept paper/draft/framework/etc. and version. |
| `problem`, `objectives`, `proposed_measures` | Stated problem, aims and proposed mechanisms. |
| `affected_groups`, `proposed_rights`, `proposed_obligations`, `exceptions` | Intended scope, duties, benefits and exclusions. |
| `implementation_options`, `funding`, `costs`, `alternatives` | Proposed delivery, resources, attributed estimates and alternatives discussed. |
| `consultation`, `support`, `opposition`, `next_steps`, `related_bills` | Feedback process/deadlines, attributed positions and explicit links to formal legislation. |

Boundary: a policy proposal need not have a bill number, formal sponsor, introduced status or settled legislative language. Preserve unsettled options.

### `enacted_law`

Purpose: preserve an enacted instrument's provisions, scope and commencement rules.

| Fields | Information to extract |
|---|---|
| `law_id`, `citation`, `title`, `jurisdiction`, `enacting_body`, `version` | Identity, authority and text version. |
| `enacted_date`, `effective_dates`, `commencement_conditions`, `sunset` | Enactment versus section-specific commencement, conditional triggers and expiry. |
| `purpose`, `provisions`, `definitions`, `scope` | Operative rules, controlling definitions, territorial/person/entity scope and thresholds. |
| `rights`, `obligations`, `prohibitions`, `exceptions` | Who receives rights or owes duties and under which conditions/exemptions. |
| `enforcement`, `penalties`, `remedies`, `delegated_powers` | Implementing/enforcing authorities, sanctions, remedies and delegated rulemaking. |
| `amendments`, `repeals`, `transitional_rules`, `appropriations` | Changed instruments, transition/savings rules and authorized/provided resources as stated. |

Boundary: enactment does not imply every provision is already effective. Do not infer present validity or later amendments from an older text.

### `regulation`

Purpose: preserve regulatory requirements, applicability and compliance mechanics.

| Fields | Information to extract |
|---|---|
| `title`, `citation`, `rule_id`, `agency`, `jurisdiction`, `authority`, `status` | Rule identity, issuer, cited statutory basis and source-stated proposed/final/amended status. |
| `scope`, `regulated_entities`, `definitions`, `thresholds`, `exemptions` | Coverage, technical definitions, thresholds and exceptions. |
| `requirements`, `prohibitions`, `standards`, `permissions` | Actor-specific substantive rules, required standards and conditions. |
| `reporting`, `recordkeeping`, `licensing`, `procedures` | Forms, filings, retention periods, approvals and required processes. |
| `effective_dates`, `compliance_dates`, `phase_in`, `transitional_rules` | Distinct legal commencement and compliance milestones. |
| `enforcement`, `penalties`, `amendments`, `incorporated_materials` | Enforcement mechanism, sanctions, changed sections and external standards actually referenced. |

Boundary: preserve stated status even if the kind was assigned broadly. A preamble's rationale is distinct from operative text; unseen incorporated standards remain unavailable.

### `rulemaking_notice`

Purpose: preserve a rulemaking action and the public participation process.

| Fields | Information to extract |
|---|---|
| `agency`, `jurisdiction`, `docket_id`, `rule_id`, `notice_type` | Advance notice, proposed/final rule notice, hearing, extension, withdrawal or other stated action. |
| `authority`, `subject`, `proposed_changes`, `affected_rules` | Cited basis, issues and rules/sections under consideration. |
| `affected_groups`, `alternatives`, `analyses` | Covered actors, options and attributed cost/benefit or other impact estimates. |
| `questions_for_comment`, `submission_requirements`, `submission_channel` | Requested feedback, format, docket identifiers and submission method. |
| `comment_deadlines`, `hearings`, `effective_dates`, `contact` | Participation dates/timezones, hearing access, applicable effective dates and contact. |

Boundary: preserve proposal versus final action; comment deadlines and effective dates are different. A notice announcing a final rule is not necessarily the complete operative rule text.

### `court_opinion`

Purpose: preserve the questions decided, legal reasoning and disposition.

| Fields | Information to extract |
|---|---|
| `case_name`, `case_number`, `citation`, `court`, `jurisdiction`, `decision_date` | Decision identity and court. |
| `judges`, `opinion_author`, `opinion_type`, `publication_status` | Panel/author, majority/plurality/concurrence/dissent and stated publication/precedential designation. |
| `parties`, `procedural_posture`, `material_facts` | Roles, prior proceedings and facts as characterized by the court. |
| `issues`, `holdings`, `reasoning`, `standards_of_review` | Questions, decisions on those questions, rationale and expressly applied standards. |
| `cited_authorities`, `separate_opinions` | Statutes/cases relied on and separately attributed concurring/dissenting reasoning. |
| `disposition`, `relief`, `remand_instructions`, `scope_limits` | Affirmed/reversed/dismissed/etc., remedy, remand and express limits. |

Boundary: party arguments are not holdings; dissent is not the majority decision. Do not infer precedential force or extend the ruling to situations the text does not resolve.

### `lawsuit`

Purpose: preserve litigation claims, party positions, procedural history and requested relief.

| Fields | Information to extract |
|---|---|
| `case_name`, `case_number`, `court`, `jurisdiction`, `document_type`, `filed_at` | Case and complaint/answer/motion/other document identity. |
| `parties`, `counsel`, `party_roles` | Plaintiffs, defendants, intervenors, representatives and roles. |
| `allegations`, `factual_chronology`, `claims`, `legal_bases` | Attributed allegations, alleged dates and claim-by-claim cited legal grounds. |
| `defenses`, `responses`, `contested_issues` | Attributed denials, defenses, counterclaims and disagreements when present. |
| `requested_relief`, `claimed_damages`, `class_scope` | Requested injunction/declaration/damages, amounts/basis and proposed class if applicable. |
| `procedural_events`, `motions`, `orders`, `deadlines`, `status` | Dated filings, rulings, scheduled actions and source-stated posture. |
| `settlement`, `disposition`, `unresolved_matters` | Reported outcome/terms and remaining issues only when supplied. |

Boundary: allegations are not findings; requested damages are not awarded damages; a filed motion is not a granted motion. A judicial opinion warrants the `court_opinion` profile when that is the document kind.

### `government_report`

Purpose: preserve an agency's research, audit, investigation or evaluation and recommendations.

| Fields | Information to extract |
|---|---|
| `title`, `report_id`, `agency`, `report_type`, `mandate`, `published_at` | Report identity, issuer, audit/evaluation/statistical/etc. form and authority/purpose. |
| `questions`, `scope`, `covered_period`, `geography`, `subjects` | Questions investigated and limits of the review. |
| `methods`, `data_sources`, `sample`, `limitations` | Evidence base, methods, sample and stated uncertainty. |
| `findings`, `metrics`, `conclusions` | Findings with evidence, quantitative results and agency conclusions. |
| `recommendations`, `responsible_bodies`, `deadlines` | Recommended actions, assigned recipients and stated timing. |
| `agency_responses`, `disagreements`, `follow_up_status` | Acceptance/rejection, corrective actions and status reported in the document. |

Boundary: recommendations are not automatically binding requirements. Observations, causal findings and agency responses remain distinct.

### `budget_document`

Purpose: preserve public funding proposals, authorizations, allocations and actual spending without conflating them.

| Fields | Information to extract |
|---|---|
| `government`, `jurisdiction`, `document_type`, `fiscal_period`, `version`, `status` | Entity, proposal/enacted appropriation/execution report/etc., period and stage. |
| `currency`, `scale`, `accounting_basis`, `funds`, `programs` | Presentation basis and fund/agency/program hierarchy. |
| `revenues`, `expenditures`, `appropriations`, `allocations` | Line items with amount, fiscal year, category, recipient and explicit budget stage. |
| `obligations`, `outlays`, `balances`, `financing` | Committed amounts, cash spending, remaining balances, deficit/surplus, debt and financing as reported. |
| `comparisons`, `policy_changes`, `assumptions`, `forecasts` | Prior-year/baseline comparisons, proposed changes and forecast assumptions. |
| `restrictions`, `earmarks`, `conditions`, `availability_periods` | Permitted use, beneficiaries, conditions, carry-forward and expiry. |

Boundary: requested, authorized, appropriated, obligated and spent amounts are separate states. Avoid double-counting totals and their component lines; forecasts are not actuals.

### `legislative_record`

Purpose: preserve what a legislative body discussed, introduced, decided or recorded.

| Fields | Information to extract |
|---|---|
| `legislature`, `chamber`, `session`, `sitting_date`, `record_id`, `record_type` | Body and journal/debate/minutes/roll-call/etc. identity. |
| `agenda_items`, `bills`, `motions`, `amendments` | Items considered and links/identifiers to related instruments. |
| `speakers`, `remarks`, `positions` | Attributed speeches, arguments, proposals and objections. |
| `procedural_actions`, `rulings`, `referrals` | Introduction, debate, referral, withdrawal or other recorded actions with timing. |
| `votes`, `results`, `attendance` | Motion voted on, vote type/totals, individual votes when supplied, abstentions and recorded result. |
| `next_steps`, `referenced_materials` | Scheduled business and incorporated/submitted materials. |

Boundary: a speech is not the body's position; an amendment vote is not necessarily final passage of a bill. Do not infer missing individual votes.

### `hearing`

Purpose: preserve testimony, questioning, submitted evidence and recorded follow-up.

| Fields | Information to extract |
|---|---|
| `title`, `hearing_id`, `convening_body`, `jurisdiction`, `hearing_type`, `date` | Legislative/administrative/judicial/etc. hearing identity and purpose. |
| `topics`, `related_matters`, `participants`, `roles` | Issues, bill/case/docket links, chair/panel/witness/counsel roles and affiliations. |
| `testimony`, `claims`, `submitted_evidence` | Speaker-attributed assertions and exhibits, with source page/timestamp if present. |
| `questions_and_answers`, `disagreements`, `unanswered_questions` | Linked question-answer exchanges and unresolved matters. |
| `commitments`, `requested_materials`, `deadlines`, `recorded_actions` | Promised follow-ups, evidence requests, dates and actual rulings/actions if recorded. |

Boundary: witness testimony is not an adopted finding. Missing exhibits and transcript gaps must remain visible.

### `research_paper`

Purpose: preserve the research question, method, results and limits needed to assess the contribution.

| Fields | Information to extract |
|---|---|
| `title`, `authors`, `affiliations`, `identifiers`, `venue`, `version`, `publication_status` | DOI/arXiv/etc., venue and stated preprint/peer-reviewed/corrected/retracted status. |
| `research_question`, `hypotheses`, `contributions` | Problem, tested hypotheses and claimed novelty. |
| `study_design`, `methods`, `data`, `sample`, `setting` | Experimental/observational/theoretical/etc.; data origin, sample/population and setup. |
| `interventions`, `comparators`, `baselines`, `evaluation_protocol` | Treatments/models, controls, benchmark setup and evaluation conditions. |
| `results`, `uncertainty`, `statistical_tests` | Outcomes, metrics, effect sizes, intervals, significance and units as reported. |
| `conclusions`, `limitations`, `threats_to_validity` | Authors' interpretation, generalizability and limitations. |
| `reproducibility`, `ethics`, `funding`, `conflicts` | Code/data/materials availability, approvals and disclosed funding/interests. |

Boundary: association is not causation; benchmark results are conditional on setup. Preserve negative/null findings. Fields for experiments are optional for theoretical or qualitative work.

### `whitepaper`

Purpose: preserve a technical, commercial or policy thesis and the evidence behind its proposed solution.

| Fields | Information to extract |
|---|---|
| `title`, `authors`, `issuer`, `version`, `audience`, `purpose` | Document identity, intended readers and goal. |
| `problem`, `thesis`, `proposed_solution`, `architecture` | Problem statement, central position, mechanism and components. |
| `requirements`, `assumptions`, `implementation`, `adoption_steps` | Prerequisites, dependencies, deployment/adoption process and constraints. |
| `claims`, `evidence`, `case_studies`, `comparisons` | Claimed benefits, supporting methods/data, examples and competing approaches. |
| `economics`, `risks`, `limitations`, `tradeoffs` | Stated cost/benefit, risks, conditions and acknowledged drawbacks. |
| `recommendations`, `roadmap`, `references`, `disclosures` | Proposed actions, future plans, sources and commercial interests. |

Boundary: distinguish measured results, modeled estimates, hypothetical examples and marketing claims. Do not infer peer review or independent validation.

### `technical_documentation`

Purpose: preserve operational instructions and technical contracts accurately enough to use them.

| Fields | Information to extract |
|---|---|
| `product`, `component`, `version`, `doc_type`, `audience` | API reference/tutorial/how-to/concept/changelog/etc. and applicable version. |
| `purpose`, `concepts`, `prerequisites`, `compatibility` | Capabilities, concepts, supported environments and dependencies. |
| `setup`, `configuration`, `procedures` | Ordered installation/configuration/use steps; parameter names, types, defaults and allowed values. |
| `interfaces`, `inputs`, `outputs` | Endpoint/method/function/CLI signatures, request/response fields, types and semantics. |
| `examples`, `expected_results` | Source code/commands and expected output; preserve syntax rather than paraphrasing executable examples. |
| `authentication`, `permissions`, `limits`, `errors` | Auth/scopes, quotas, constraints, error codes and documented recovery. |
| `warnings`, `security_notes`, `troubleshooting` | Documented hazards, security requirements and diagnosis/remediation steps. |
| `changes`, `deprecations`, `migration`, `references` | Version changes, deadlines, replacements, migration instructions and links. |

Boundary: do not combine incompatible versions, invent defaults or present source examples as tested. This content needs instructions and interfaces, not forced events or impacts.

## Proposed output organization

A shared envelope with a typed `details` payload keeps document identity and a short summary consistent while allowing each kind to preserve its own structure. The kind-specific fields above belong in `details`; evidence is associated with their field/record paths. This shape is illustrative, not an implemented schema:

```json
{
  "kind": "lawsuit",
  "schema_version": "1",
  "document": {"title": "Example complaint"},
  "briefing": "The plaintiff alleges nonpayment and requests damages.",
  "details": {
    "document_type": "complaint",
    "allegations": [
      {"statement": "The defendant failed to pay.", "attribution": "plaintiff"}
    ],
    "requested_relief": [{"type": "damages"}]
  },
  "evidence": [
    {"path": "/details/allegations/0", "section": "Allegations", "paragraph": "12"}
  ],
  "coverage": {"input_scope": "excerpt"}
}
```

The example is synthetic. Actual source identity and evidence must come from the input; schema version and coverage come from the pipeline.

- Pros: preserves domain-specific facts and a consistent reading summary; avoids forcing every kind into event/impact fields.
- Cons: requires kind-aware validation, schema versioning, and downstream handling of nested records; larger documents need section-level extraction and merging.

## Integration considerations for a later implementation

This task adds only this reference. A subsequent implementation would select an output schema from the canonical kind, then select document subtypes where needed (particularly SEC forms). Topic labels such as AI or cybersecurity can supply optional subject fields; they should not replace document-kind selection. Unknown/missing kinds need an explicit generic fallback and a routing flag, rather than a silent assumption that the document is news.

The current analyst binds `output_model` at construction. Mixed-kind batches therefore need schema-aware grouping or a backend-supported per-request schema mechanism; merely passing the kind in the prompt does not change validation. Runtime/model sharing must be checked before constructing one GPU analyst per kind.

Current digest input is sliced to `MAX_DOCUMENT_LEN << 2` characters (16,384 at the default). Long contracts, statutes, reports and manuals can lose schedules, exceptions and tables. A future extractor should process relevant sections with stable evidence references, preserve table structure, merge duplicate records, and record skipped sections. It must never label a prefix-only extraction as complete. Required field groups should be checked for coverage, not fabricated to satisfy a schema.

Entity tagging can remain useful for search. Party roles, relationships, metrics and obligations require the richer structured extraction. Summary fields should be derived from retained records so a briefing cannot turn an allegation into a finding or a forecast into a result.

Consumers require review before adopting the proposed shape: `CupboardPorter._prep_events` currently maps `briefing` to `summary`, merges entity tags into the digest and sets `event_type` to the kind for many documents. `Consolidator._bean_to_str` serializes digest fields into its prompt, while its output is event-oriented `Briefing`. Neither behavior establishes support for arbitrary nested details or document-specific consolidation. Preserving `briefing` eases migration but does not prove downstream compatibility.

Validation examples for the eventual implementation: a complaint without a ruling yields allegations and requested relief only; a signed contract keeps notice periods and liability carve-outs; a proposed bill does not produce current obligations; a two-period financial table retains each value's period and currency; show notes do not yield invented testimony; a truncated manual records incomplete coverage and does not invent absent defaults.
