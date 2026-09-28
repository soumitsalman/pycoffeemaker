from typing import List, Literal, Optional
from functools import cached_property
from pydantic import BaseModel, Field

from .normalize import normalize_fields, merge_lists
from .formatters import model_text_schema, text_value, apply_model_json_constraints

_TAG_MAX_LEN = 50
_TAGS_MAX_COUNT = 10
_TICKER_MAX_LEN = 6

# _DIGEST_ACTIONS_MAX_COUNT = 10
# _DIGEST_CROSS_DOMAIN_IMPACTS_MAX_COUNT = 5

# _DIGEST_EVENT_TYPE_MAX_LEN = 50
# _DIGEST_IMPACT_LEVEL_MAX_LEN = 15
# _DIGEST_MACRO_CONTEXT_MAX_LEN = 50
# _DIGEST_FUTURE_OUTLOOK_MAX_LEN = 300
# _DIGEST_BRIEFING_MAX_LEN = 1000

# _TAG_LIST_ITEM_MAX_LEN = {
#     "regions": _TAG_MAX_LEN,
#     "people": _TAG_MAX_LEN,
#     "products": _TAG_MAX_LEN,
#     "companies": _TAG_MAX_LEN,
#     "entities": _TAG_MAX_LEN,
#     "stock_tickers": _TICKER_MAX_LEN,
#     "impacted_domains": _TAG_MAX_LEN,    
# }

_MODEL_DUMP_DEFAULTS = {
    "exclude_none": True,
    "exclude_unset": True,
    "exclude_defaults": True,
}

class _NLPBase(BaseModel):
    def model_post_init(self, __context):
        normalize_fields(self)

    @classmethod
    def model_text_schema(cls):
        return model_text_schema(cls)

    @classmethod
    def model_json_schema(cls):
        return apply_model_json_constraints(super().model_json_schema())

    def model_dump(self, **kwargs):
        return super().model_dump(**(_MODEL_DUMP_DEFAULTS | kwargs))

    def __str__(self):
        return text_value(self)


class _ExtractionBase(_NLPBase):
    """Common flat envelope for non-editorial content extraction."""
    briefing: str = Field(
        description=(
            "MANDATORY. Intelligence briefing of the events (<=2sentences). "
            "Include time/date, context, actors, action sequence, mechanisms, affected parties, effects, key metrics, comparisons, significance. "
        ),
    )
    key_points: list[str] = Field(
        default_factory=list,
        description=(
            "MANDATORY. "
            "list=Key points, events sequence, actions sequence, activities. "
            "max_items<=40. "
            "format=YYYY-MM-DD Actor verb object/effect with key metric if available."
        )
    )

    def __bool__(self):
        return bool(self.briefing)


CATEGORIES_LIST = Literal[
    "Artificial Intelligence",
    "Software and Data Engineering",
    "Computing Infrastructure and Hardware",
    "Cybersecurity and Privacy",
    "Consumer Electronics and Robotics",
    "Industry and Manufacturing",
    "Economics, Accounting and Finance",
    "Business Marketing and Employment",
    "Politics and Global Affairs",
    "Law, Crime and Public Safety",
    "Civil Rights, Migration and Society",
    "Health and Wellness",
    "Biology and Biotechnology",
    "Physical Sciences and Mathematics",
    "Earth, Space, Climate and Environment",
    "Agriculture and Food Production",
    "Transportation and Logistics",
    "Construction, Housing and Real Estate",
    "Education and Humanities",
    "Sports and Recreation",
    "Arts, Culture, Media and Entertainment",
    "Food, Dining and Travel",
    "Fashion, Beauty and Consumer Affairs",
    "Home, Family and Pets",
]
SENTIMENTS_LIST = Literal["highly positive", "positive", "neutral", "negative", "highly negative"]
IDEOLOGIES_LIST = Literal["left", "right", "center", "undetermined"]

class Classification(_NLPBase):
    category: CATEGORIES_LIST = Field(alias="domain_genre")
    sentiment: SENTIMENTS_LIST = Field(alias="expression_sentiment")
    ideology: IDEOLOGIES_LIST = Field(alias="political_ideology")

class Entities(_NLPBase):
    regions: List[str] = Field(default_factory=list, description="Names of countries, states, provinces, counties, cities, geographic regions, locations, neighborhoods")
    people: List[str] = Field(default_factory=list, description="Names of people/persons")
    products: List[str] = Field(default_factory=list, description="Names of physical or digital products, services, goods")
    companies: List[str] = Field(default_factory=list, description="Names of companies, organizations, institutions, political groups, business entities, government entities")
    stock_tickers: List[str] = Field(default_factory=list, description="NYSE/NASDAQ stock market ticker symbols for publicly traded companies e.g. AAPL, MSFT")    
 
    @property
    def tags(self):
        return merge_lists(self.regions, self.people, self.products, self.companies, self.stock_tickers)

# class RFPEntities(Entities):
#     document_class: Optional[str] = Field(default=None)
#     notice_id: List[str]
#     buyer: List[str]

class NewsDigest(_ExtractionBase):
    """Main digest/key points of an article/news/blog/report"""        
    
    drivers: list[str] = Field(
        default_factory=list,
        description=(
            "List of event drivers=Traceable causal relationships between initial actions and outcomes/results. "
            "max_items<=10. "
            "format=Cause produced resulting effect."
        )
    )
    event_type: Optional[str] = Field(
        None,
        description="Primary aggregated event type(<=3words).",
    )
    impacts: list[str] = Field(
        default_factory=list,
        description=(
            "MANDATORY. "
            "list=Traceable observed impacts/effects/outcomes/results on affected scope. "
            "max_items<=10. "
        )
    )
    impacted_domains: list[str] = Field(
        default_factory=list,
        description="list=Domains impacted by the events sequence. max_items<=10.",
    )
    impact_level: str = Field(
        description=(
            "Specified impact of the events on primary domain/context. "
            "allowed=null,low,medium,high,critical,transformative."
        ),
    )
    macro_context: Optional[str] = Field(
        None,
        description="Primary overarching geopolitical, trade, economic, technological context driving the events(<=4words).",
    )
    future_outlook: Optional[str] = Field(
        default=None,
        description="Traceable future outlook, trajectory or forecast. Omit if NA",
    )
    
# _BRIEFING_EVENTS_MAX_COUNT = 40
# _BRIEFING_LIST_MAX_COUNT = 10

# _BRIEFING_IMPACT_LEVEL_MAX_LEN = 15
# _BRIEFING_FORECAST_MAX_LEN = 300
# _BRIEFING_BRIEFING_MAX_LEN = 1000

class Briefing(_ExtractionBase):
    """Intelligence briefing from a stream of events."""           
    events: list[str] = Field(
        default_factory=list,
        description=(
            "MANDATORY. "
            "list=Chronological sequence of facts/events. "
            "max_items<=40. "
            "format=YYYY-MM-DD Actor verb object/effect with key metric if available."
        )
    )
    drivers: list[str] = Field(
        default_factory=list,
        description=(
            "List of event drivers=Traceable causal relationships between actions and outcomes. "
            "max_items<=10. "
            "format=Cause produced resulting effect."
        )
    )
    impacts: list[str] = Field(
        default_factory=list,
        description=(
            "MANDATORY. "
            "list=Traceable observed impacts/effects/outcomes/results on affected scope. "
            "max_items<=10."
        )
    )
    impacted_domains: list[str] = Field(
        default_factory=list,
        description="list=Domains impacted by the events sequence. max_items<=10.",
    )
    impact_level: str = Field(
        description="Combined impact of the events sequence. allowed=null,low,medium,high,critical,transformative."
    )
    future_outlook: Optional[str] = Field(
        default=None,
        description="Traceable future outlook, trajectory or forecast. Omit if NA.",
    )
    confidence: Optional[str] = Field(
        default=None,
        description=(
            "Confidence in retained events. "
            "allowed=low,medium,high. "
            "low: conflicting or isolated evidence. "
            "medium: consistent limited evidence. "
            "high: multiple corroborating events. "
            "omit if NA."
        )
    )

# class AINewsDigest(Digest):
#     benchmark_scores: List[str] = Field(default_factory=list, description="List of reported performance numbers on standard benchmarks (key: benchmark name, value: score)")
#     claimed_productivity_lift: Optional[str] = Field(None, description="Reported productivity/efficiency gain")
#     enterprise_adoption_rate: Optional[str] = Field(None, description="Reported adoption/usage rate")
#     price: Optional[str] = Field(None, description="Reported unit/subscription price")
#     valuation_or_market_size: Optional[str] = Field(None, description="Company valuation or projected market size")


# class CyberNewsDigest(Digest):
#     # malware information
#     threat_actors: List[str] = Field(default_factory=list, description="List of named or categorized attackers. Examples: LockBit, nation state, etc.",)
#     vulnerabilities: List[str] = Field(default_factory=list, description="List of CVE IDs, product names or zero-day descriptions mentioned")    
#     malware_family: Optional[str] = Field(None, description="Name of the malware family or ransomware strain if applicable")
#     attack_speed: Optional[str] = Field(None)
#     incident_type: Optional[str] = Field(None, description="Primary category of the cybersecurity event. Allowed: ransomware, supply_chain, zero_day, ai_enhanced, state_sponsored, shadow_ai, critical_infra etc.")
#     # impact information
#     products: List[str] = Field(default_factory=list, description="List of impacted/vulnerable products/services. Exclude=generic,grouped/aggregated qualifications - 3 new products.")
#     entities: List[str] = Field(default_factory=list, description="List of impacted/vulnerable organizations/sectors/users/scope. Exclude=generic,grouped/aggregated qualifications - 7 organizations.")
#     technical_impact: Optional[str] = Field(None, description="Specified quantitative technical consequences. Examples: 1000 records breach, 10h service outage etc.")
#     financial_impact: Optional[str] = Field(None, description="Specified financial damage or cost of recovery in USD.")    
#     business_impact: Optional[str] = Field(None, description="Specified operational, financial, reputational or regulatory consequences.")
#     compliance_impact: List[str] = Field(default_factory=list, description="List of specified policies,standards,laws,regulations as being triggered (SEC, CIRCIA, GDPR, etc.)")
#     # remediation
#     mitigations: List[str] = Field(default_factory=list, description="List of specified defensive/corrective/mitigation/recovery steps" )


# class HardwareNewsDigest(Digest):
#     """Summary focused on chips, accelerators, compute infrastructure"""

#     products: List[str] = Field(default_factory=list, description="List of specified products/chips (ex: NVIDIA H100, AMD MI300, AWS Trainium). Exclude=generic,grouped/aggregated qualifications - 3 new products.")
#     use_cases: List[str] = Field(default_factory=list, description="List of intended/demonstrated uses. Examples: AI training, inference, HPC, edge computing. Limit=5",)
#     performance_improvement: Optional[str] = Field(None, description="Speedup factor compared to previous generation. Include=value,unit,context (ex: 2.8x faster inference).")
#     power_efficiency: Optional[str] = Field(None, description="Power/energy usage gain/reduction. Include=unit,value (ex: 15% reduction).")
#     capex_investment: Optional[str] = Field(None,description="CapEx for data centers/fabs. Include=unit,value (ex: $2.5 billion)")
#     price: Optional[str] = Field(None, description="Reported unit/subscription price.")

# class RoboticsAVDronesNewsSummary(Digest):
#     """Summary focused on robotics systems, autonomous vehicles, drones"""

#     product_system: str = Field(
#         ..., description="Name/model of robot/AV/drone (e.g. 'Tesla Cybercab', 'Boston Dynamics Stretch')"
#     )
#     manufacturer: str = Field(
#         ..., description="Company. Format: official name. If startup: include founding year."
#     )
#     category: Optional[str] = Field(
#         None,
#         description="Broad category of the embodied system. Allowed: industrial_cobot, humanoid, autonomous_vehicle, drone_swarm, warehouse_agv.",
#     )
#     speed_or_payload_improvement: Optional[str] = Field(
#         None,
#         description="Key upgrade vs prior gen. Include unit in value. Examples: '2.5 m/s', '50% faster'.",
#     )
#     deployment_sites_count: Optional[str] = Field(
#         None,
#         description="Real-world deployments/customers mentioned. Include count in value (e.g. '12 sites').",
#     )
#     funding_raised_usd_millions: Optional[str] = Field(
#         None,
#         description="Funding amount raised. Include currency and magnitude in value (e.g. '$50 million').",
#     )
#     cost_per_unit_usd: Optional[str] = Field(
#         None, description="Estimated cost per robot / vehicle. Include currency in value (e.g. '$250,000')."
#     )
#     real_world_limitation_noted: List[str] = Field(
#         default_factory=list,
#         description="Practical limitations noted. Examples: weather sensitivity, edge cases, cost barriers. Exclude speculation. Limit: 5.",
#     )


# class StartupCorpNewsSummary(Digest):
#     """Summary focused on startups, corporate moves, funding, M&A"""

#     main_company: str = Field(..., description="Primary company (official name). No qualifiers.")
#     other_companies: List[str] = Field(
#         default_factory=list,
#         description="Co-parties: acquirers, investors, partners. Exclude=unrelated mentions. Limit: 5.",
#     )
#     lead_investors: List[str] = Field(
#         default_factory=list, description="Lead/prominent investors (official names/fund names). Exclude=passive stakeholders. Limit: 5."
#     )
#     acquirer: Optional[str] = Field(
#         None, description="Name of the acquiring company in M&A deals"
#     )
#     funding_amount_usd_millions: Optional[str] = Field(
#         None, description="Amount raised in the funding round. Include currency in value (e.g. '$25 million')."
#     )
#     round_type: Optional[str] = Field(
#         None, description="Stage of funding (Seed, Series A, Late, Debt, etc.)"
#     )
#     pre_post_valuation_usd_billions: Optional[str] = Field(
#         None, description="Pre-money and/or post-money valuation. Include currency in value (e.g. 'pre: $500M, post: $1B')."
#     )
#     deal_value_usd_millions: Optional[str] = Field(
#         None, description="Transaction value in M&A deals. Include currency in value (e.g. '$500 million')."
#     )
#     yoy_funding_growth_pct: Optional[str] = Field(
#         None,
#         description="Year-over-year change in funding volume. Include unit in value (e.g. '+15%').",
#     )
#     strategic_rationale: str = Field(
#         ...,
#         description="1-sentence rationale. Format: [Actor] seeks [capability/market] via [deal type].",
#     )
#     use_of_funds: Optional[str] = Field(
#         None, description="Stated use of capital. Categories: R&D, expansion, acquisition, operations, debt repayment. Be explicit."
#     )


# class FinancialMarketsNewsSummary(Digest):
#     """Summary focused on stocks, earnings, filings, market movements"""

#     ticker_or_index: str = Field(
#         ..., description="Primary ticker(s) or index (format: 'AAPL' or 'S&P500'). Limit: 3 tickers max."
#     )
#     companies_mentioned: List[str] = Field(
#         default_factory=list,
#         description="Companies discussed. Format: official ticker+name. Exclude=tangential mentions. Limit: 5.",
#     )
#     stock_reaction_pct: Optional[str] = Field(
#         None,
#         description="Stock price change after news. Include unit in value (e.g. '+2.5%' or '-1.3%').",
#     )
#     earnings_beat_miss_pct: Optional[str] = Field(
#         None, description="EPS/revenue beat or miss vs consensus. Include unit and direction in value (e.g. '+3% beat' or '-2% miss')."
#     )
#     revenue_or_ebitda_usd_millions: Optional[str] = Field(
#         None,
#         description="Reported or forecasted revenue / EBITDA. Include currency and magnitude in value (e.g. '$1,200 million' or '$1.2B').",
#     )
#     forward_guidance_change_pct: Optional[str] = Field(
#         None, description="FY guidance change vs prior. Include unit in value (e.g. '+5%' if raised, '-10%' if lowered, or 'reaffirmed')."
#     )
#     valuation_multiple: Optional[str] = Field(
#         None,
#         description="Forward-looking valuation metric (e.g. '32x revenue', '18x EBITDA')",
#     )
#     financial_analysis_summary: str = Field(
#         ...,
#         description="1-sentence summary. Format: [Company] [beat/miss] due to [reason], implying [outlook].",
#     )
#     sector_rotation_signal: str = Field(
#         default="",
#         description="Money flow signal. Format: 'from [sector] to [sector]' (e.g. 'from Mag7 to cyclicals').",
#     )


# class LogisticsDigest(Digest):
#     transportation_mode: str = Field(description="Allowed: N/A, air, ocean, truck, multimodal")
#     affected_routes: List[str] = Field(default_factory=list, description="Examples: Red Sea, Suez, Transpacific. Limit=5")
#     freight_rate_change: Optional[str] = Field(None, description="Include=value,unit,context. Example: +8% in 2 years")
#     order_quantity: Optional[str] = Field(None, description="Include=value,unit. Example: 30 tons")
#     shipping_delay: Optional[str] = Field(None, description="Include=value,unit,context. Example: 5 days for Transpacific route.")
#     financial_impact: Optional[str] = Field(None, description="Specified financial damage or cost of recovery in USD.")    
#     business_impact: Optional[str] = Field(None, description="Specified operational, reputational or regulatory consequences")
#     mitigations: List[str] = Field(
#         default_factory=list,
#         description="List of specified mitigations. Examples: rerouting, inventory buildup, alternative suppliers. Limit=5.",
#     )


# class MacroEconomyDigest(Digest):
#     """Summary focused on global economy, macro indicators, forecasts"""

#     gdp_growth_forecast: Optional[str] = Field(None, description="GDP growth forecast. Include=value,unit,timeframe.")
#     inflation_impact: Optional[str] = Field(None, description="Inflation impact. Include=value,unit,timeframe.")
#     oil_price: Optional[str] = Field(None, description="Oil price scenario. Include=value,unit,timeframe.")
#     gold_demand: Optional[str] = Field(None, description="Gold demand. Include=value,unit,timeframe.")
#     macro_signal: str = Field(...,description="Format: [Event] signals [risk/opportunity] for [sector/macro].",)
#     market_significance: str = Field(description="Format: affects [equities/credit/funding] via [mechanism].",)


# ────────────────────────────────────────────────
# Flat content-kind extraction models
# ────────────────────────────────────────────────
# Each list item is a self-contained source-supported record. These schemas
# contain no dicts, nested models, or list-of-object fields.
class SiteExtraction(_ExtractionBase):
    """Flat schema for a `site` page."""
    page_type: Optional[str] = Field(None, description='Source-supported page type, such as homepage, about, product, service, pricing, directory, or resource page. Describe only the supplied page; omit if unclear.')
    subject: Optional[str] = Field(None, description='Primary organization, product, service, resource, or topic represented by the supplied page. Preserve the source name and omit if unclear.')
    operator: Optional[str] = Field(None, description='Organization or person stated as operating, owning, publishing, or maintaining the represented offering or resource. Do not infer ownership from a domain name.')
    description: Optional[str] = Field(None, description='Compact source-supported description of what the represented organization, product, service, or resource is. Keep promotional language attributed to the page.')
    offerings: List[str] = Field(default_factory=list, description='Products, services, resources, or programs offered on the supplied page. Include stated names and material conditions; omit unsupported offerings.')
    capabilities: List[str] = Field(default_factory=list, description='Functions, features, or tasks the offering is stated to provide. Preserve qualifiers, limitations, and attribution; do not infer capabilities.')
    audiences: List[str] = Field(default_factory=list, description='Stated target users, customers, members, or beneficiaries for the offering.')
    use_cases: List[str] = Field(default_factory=list, description='Stated workflows, applications, or problems the offering is intended to address.')
    industries: List[str] = Field(default_factory=list, description='Industries, sectors, or verticals explicitly named as served or covered.')
    geographies: List[str] = Field(default_factory=list, description='Countries, regions, or localities where the offering is stated to operate, be available, or be relevant. Do not infer geography from contact details alone.')
    eligibility: List[str] = Field(default_factory=list, description='Stated eligibility, membership, customer, residency, age, or qualification conditions.')
    access_requirements: List[str] = Field(default_factory=list, description='Stated requirements to access, use, buy, download, join, or request the offering, including account, approval, hardware, software, or credential conditions.')
    pricing: List[str] = Field(default_factory=list, description='Source-stated prices, price ranges, currencies, billing cadence, discounts, trial terms, and stated pricing conditions. Preserve exact units and qualifiers.')
    plans: List[str] = Field(default_factory=list, description='Named plans, tiers, packages, or subscriptions with their stated audience, price, included features, and limits.')
    specifications: List[str] = Field(default_factory=list, description='Technical, product, service, or compatibility specifications explicitly stated on the page. Preserve units, versions, limits, and conditions.')
    integrations: List[str] = Field(default_factory=list, description='Third-party systems, platforms, APIs, or services stated as supported, connected, or compatible. Do not infer integration from a logo alone.')
    resources: List[str] = Field(default_factory=list, description='Named source-linked documentation, guides, downloads, case studies, help pages, legal pages, or other resources available from the supplied page.')
    contact_channels: List[str] = Field(default_factory=list, description='Source-stated contact methods and destinations, such as email, phone, form, chat, social account, or support portal. Preserve the stated purpose when available.')
    locations: List[str] = Field(default_factory=list, description='Source-stated offices, stores, facilities, service locations, or addresses, with their stated role or availability.')
    calls_to_action: List[str] = Field(default_factory=list, description='Explicit actions the page asks a visitor to take, such as sign up, request a demo, contact sales, buy, download, apply, or subscribe.')
    claims: List[str] = Field(default_factory=list, description='Marketing, certification, performance, security, compliance, customer, or comparative claims as presented by the page. Attribute each claim to the operator or page; do not treat it as independently verified.')


class JobExtraction(_ExtractionBase):
    """Flat schema for `Job` content."""
    job_title: Optional[str] = Field(None, description='Source-supported job title.')
    employer: Optional[str] = Field(None, description='Source-supported employer.')
    requisition_id: Optional[str] = Field(None, description='Source-supported requisition id.')
    team: Optional[str] = Field(None, description='Source-supported team.')
    work_arrangement: Optional[str] = Field(None, description='Source-supported work arrangement.')
    employment_type: Optional[str] = Field(None, description='Source-supported employment type.')
    seniority: Optional[str] = Field(None, description='Source-supported seniority.')
    schedule: Optional[str] = Field(None, description='Source-supported schedule.')
    travel: Optional[str] = Field(None, description='Source-supported travel.')
    application_url: Optional[str] = Field(None, description='Source-supported application url.')
    posting_status: Optional[str] = Field(None, description='Source-supported posting status.')
    locations: List[str] = Field(default_factory=list, description='List of source-supported locations.')
    responsibilities: List[str] = Field(default_factory=list, description='List of source-supported responsibilities.')
    required_qualifications: List[str] = Field(default_factory=list, description='List of source-supported required qualifications.')
    preferred_qualifications: List[str] = Field(default_factory=list, description='List of source-supported preferred qualifications.')
    compensation: List[str] = Field(default_factory=list, description='List of source-supported compensation.')
    benefits: List[str] = Field(default_factory=list, description='List of source-supported benefits.')
    eligibility: List[str] = Field(default_factory=list, description='List of source-supported eligibility.')
    application_requirements: List[str] = Field(default_factory=list, description='List of source-supported application requirements.')
    deadlines: List[str] = Field(default_factory=list, description='Source-stated deadline with required action, responsible party, trigger, timezone, and date precision when available.')


class ContractExtraction(_ExtractionBase):
    """Flat schema for `Contract` content."""
    contract_type: Optional[str] = Field(None, description='Source-supported contract type.')
    agreement_id: Optional[str] = Field(None, description='Source-supported agreement id.')
    document_status: Optional[str] = Field(None, description='Source-supported document status.')
    effective_date: Optional[str] = Field(None, description='Source-supported effective date.')
    term: Optional[str] = Field(None, description='Source-supported term.')
    renewal: Optional[str] = Field(None, description='Source-supported renewal.')
    dispute_resolution: Optional[str] = Field(None, description='Source-supported dispute resolution.')
    governing_law: Optional[str] = Field(None, description='Source-supported governing law.')
    precedence: Optional[str] = Field(None, description='Source-supported precedence.')
    parties: List[str] = Field(default_factory=list, description='List of source-supported parties.')
    execution_dates: List[str] = Field(default_factory=list, description='List of source-supported execution dates.')
    scope: List[str] = Field(default_factory=list, description='List of source-supported scope.')
    deliverables: List[str] = Field(default_factory=list, description='List of source-supported deliverables.')
    milestones: List[str] = Field(default_factory=list, description='List of source-supported milestones.')
    acceptance: List[str] = Field(default_factory=list, description='List of source-supported acceptance.')
    service_levels: List[str] = Field(default_factory=list, description='List of source-supported service levels.')
    obligations: List[str] = Field(default_factory=list, description="Duties imposed or proposed, formatted with responsible party, required action, trigger, deadline, conditions, and exceptions. Preserve 'must' versus 'may'.")
    rights: List[str] = Field(default_factory=list, description='Rights, permissions, or entitlements with holder, scope, conditions, and exceptions when stated.')
    conditions: List[str] = Field(default_factory=list, description='List of source-supported conditions.')
    commercial_terms: List[str] = Field(default_factory=list, description='List of source-supported commercial terms.')
    intellectual_property: List[str] = Field(default_factory=list, description='List of source-supported intellectual property.')
    confidentiality: List[str] = Field(default_factory=list, description='List of source-supported confidentiality.')
    data_handling: List[str] = Field(default_factory=list, description='List of source-supported data handling.')
    restrictions: List[str] = Field(default_factory=list, description='List of source-supported restrictions.')
    warranties: List[str] = Field(default_factory=list, description='List of source-supported warranties.')
    indemnities: List[str] = Field(default_factory=list, description='List of source-supported indemnities.')
    liability: List[str] = Field(default_factory=list, description='List of source-supported liability.')
    insurance: List[str] = Field(default_factory=list, description='List of source-supported insurance.')
    termination: List[str] = Field(default_factory=list, description='List of source-supported termination.')
    remedies: List[str] = Field(default_factory=list, description='List of source-supported remedies.')
    amendments: List[str] = Field(default_factory=list, description='List of source-supported amendments.')
    incorporated_documents: List[str] = Field(default_factory=list, description='List of source-supported incorporated documents.')


class ProcurementNoticeExtraction(_ExtractionBase):
    """Flat schema for `ProcurementNotice` content."""
    notice_type: Optional[str] = Field(None, description='Source-supported notice type.')
    notice_id: Optional[str] = Field(None, description='Source-supported notice id.')
    buyer: Optional[str] = Field(None, description='Source-supported buyer.')
    status: Optional[str] = Field(None, description='Source-supported status.')
    performance_period: Optional[str] = Field(None, description='Source-supported performance period.')
    contract_type: Optional[str] = Field(None, description='Source-supported contract type.')
    submission_channel: Optional[str] = Field(None, description='Source-supported submission channel.')
    scope: List[str] = Field(default_factory=list, description='List of source-supported scope.')
    lots: List[str] = Field(default_factory=list, description='List of source-supported lots.')
    deliverables: List[str] = Field(default_factory=list, description='List of source-supported deliverables.')
    performance_location: List[str] = Field(default_factory=list, description='List of source-supported performance location.')
    estimated_value: List[str] = Field(default_factory=list, description='List of source-supported estimated value.')
    funding: List[str] = Field(default_factory=list, description='List of source-supported funding.')
    eligibility: List[str] = Field(default_factory=list, description='List of source-supported eligibility.')
    submission_requirements: List[str] = Field(default_factory=list, description='List of source-supported submission requirements.')
    deadlines: List[str] = Field(default_factory=list, description='Source-stated deadline with required action, responsible party, trigger, timezone, and date precision when available.')
    evaluation_criteria: List[str] = Field(default_factory=list, description='List of source-supported evaluation criteria.')
    evaluation_weights: List[str] = Field(default_factory=list, description='List of source-supported evaluation weights.')
    award_process: List[str] = Field(default_factory=list, description='List of source-supported award process.')
    contact: List[str] = Field(default_factory=list, description='List of source-supported contact.')
    amendments: List[str] = Field(default_factory=list, description='List of source-supported amendments.')
    award_details: List[str] = Field(default_factory=list, description='List of source-supported award details.')


class FinancialReportExtraction(_ExtractionBase):
    """Flat schema for `FinancialReport` content."""
    reporting_entity: Optional[str] = Field(None, description='Source-supported reporting entity.')
    report_type: Optional[str] = Field(None, description='Source-supported report type.')
    reporting_period: Optional[str] = Field(None, description='Source-supported reporting period.')
    period_end_date: Optional[str] = Field(None, description='Source-supported period end date.')
    reporting_scope: Optional[str] = Field(None, description='Source-supported reporting scope.')
    accounting_basis: Optional[str] = Field(None, description='Source-supported accounting basis.')
    currency: Optional[str] = Field(None, description='Presentation currency explicitly stated by the source.')
    currency_scale: Optional[str] = Field(None, description='Presentation scale such as units, thousands, millions, or billions, exactly as stated by the source.')
    audit_status: Optional[str] = Field(None, description='Source-supported audit status.')
    auditor_opinion: Optional[str] = Field(None, description='Source-supported auditor opinion.')
    income_statement: List[str] = Field(default_factory=list, description='Reported income-statement line items with value, currency, scale, period, comparator, and reporting basis.')
    balance_sheet: List[str] = Field(default_factory=list, description='Reported balance-sheet line items with value, currency, scale, as-of date, and reporting basis.')
    cash_flow: List[str] = Field(default_factory=list, description='Reported cash-flow line items with value, currency, scale, period, and reporting basis.')
    segments: List[str] = Field(default_factory=list, description='List of source-supported segments.')
    geographies: List[str] = Field(default_factory=list, description='List of source-supported geographies.')
    operating_metrics: List[str] = Field(default_factory=list, description='List of source-supported operating metrics.')
    accounting_policies: List[str] = Field(default_factory=list, description='List of source-supported accounting policies.')
    restatements: List[str] = Field(default_factory=list, description='List of source-supported restatements.')
    one_time_items: List[str] = Field(default_factory=list, description='List of source-supported one time items.')
    liquidity: List[str] = Field(default_factory=list, description='List of source-supported liquidity.')
    debt: List[str] = Field(default_factory=list, description='List of source-supported debt.')
    commitments: List[str] = Field(default_factory=list, description='List of source-supported commitments.')
    contingencies: List[str] = Field(default_factory=list, description='List of source-supported contingencies.')
    related_parties: List[str] = Field(default_factory=list, description='List of source-supported related parties.')
    management_discussion: List[str] = Field(default_factory=list, description='List of source-supported management discussion.')
    risks: List[str] = Field(default_factory=list, description='List of source-supported risks.')
    outlook: List[str] = Field(default_factory=list, description='List of source-supported outlook.')
    financial_ratios: List[str] = Field(default_factory=list, description="Reported financial ratios, formatted as 'metric: value', with period, basis, and adjustment status when stated.")
    mdna_key_takeaways: List[str] = Field(default_factory=list, description='List of source-supported mdna key takeaways.')
    main_drivers: List[str] = Field(default_factory=list, description='List of source-supported main drivers.')
    top_risk_factors: List[str] = Field(default_factory=list, description='List of source-supported top risk factors.')
    legal_proceedings_status: List[str] = Field(default_factory=list, description='List of source-supported legal proceedings status.')
    business_overview_highlights: List[str] = Field(default_factory=list, description='List of source-supported business overview highlights.')
    strategic_initiatives: List[str] = Field(default_factory=list, description='List of source-supported strategic initiatives.')
    key_exhibits_filed: List[str] = Field(default_factory=list, description='List of source-supported key exhibits filed.')
    revenue: Optional[str] = Field(None, description='Reported revenue in the stated source currency and scale. Do not convert or normalize currencies.')
    revenue_growth_yoy_pct: Optional[str] = Field(None, description='Reported year-over-year revenue growth percentage for the stated period; do not calculate it.')
    net_income: Optional[str] = Field(None, description='Reported net income in the stated source currency and scale. Preserve attributable entity and reporting basis.')
    eps_basic: Optional[str] = Field(None, description='Reported basic EPS with period and accounting basis.')
    eps_diluted: Optional[str] = Field(None, description='Reported diluted EPS with period and accounting basis.')
    operating_cash_flow: Optional[str] = Field(None, description='Reported operating cash flow in the stated source currency and scale.')
    capex: Optional[str] = Field(None, description='Source-supported capex.')
    cash_equivalents: Optional[str] = Field(None, description='Source-supported cash equivalents.')
    total_debt: Optional[str] = Field(None, description='Source-supported total debt.')


class EarningsReportExtraction(FinancialReportExtraction):
    """Flat schema for `EarningsReport` content."""
    document_subtype: Optional[str] = Field(None, description='Source-supported document subtype.')
    ticker: Optional[str] = Field(None, description='Source-supported ticker.')
    release_date: Optional[str] = Field(None, description='Source-supported release date.')
    results: List[str] = Field(default_factory=list, description='Reported research, financial, vote, or hearing results with metric, units, period, and source-stated context.')
    segment_results: List[str] = Field(default_factory=list, description='List of source-supported segment results.')
    adjustments: List[str] = Field(default_factory=list, description='List of source-supported adjustments.')
    reconciliations: List[str] = Field(default_factory=list, description='List of source-supported reconciliations.')
    performance_drivers: List[str] = Field(default_factory=list, description='List of source-supported performance drivers.')
    guidance: List[str] = Field(default_factory=list, description='Issuer forecast or outlook with metric, range, period, conditions, and reporting basis. Do not treat guidance as actual results.')
    guidance_changes: List[str] = Field(default_factory=list, description='Change to prior guidance with affected metric, direction, prior/new values, period, and stated reason when available.')
    guidance_assumptions: List[str] = Field(default_factory=list, description='List of source-supported guidance assumptions.')
    capital_allocation: List[str] = Field(default_factory=list, description='List of source-supported capital allocation.')
    qna: List[str] = Field(default_factory=list, description='List of source-supported qna.')
    consensus_comparisons: List[str] = Field(default_factory=list, description='List of source-supported consensus comparisons.')
    next_quarter_guidance: List[str] = Field(default_factory=list, description='List of source-supported next quarter guidance.')
    full_year_guidance_update: Optional[str] = Field(None, description='Source-supported full year guidance update.')
    guidance_tone: Optional[str] = Field(None, description='Source-supported guidance tone.')
    key_segments_performance: List[str] = Field(default_factory=list, description='List of source-supported key segments performance.')
    strategic_priorities: List[str] = Field(default_factory=list, description='List of source-supported strategic priorities.')
    risks_updated: List[str] = Field(default_factory=list, description='List of source-supported risks updated.')
    management_tone: Optional[str] = Field(None, description='Source-supported management tone.')
    qna_hot_topics: List[str] = Field(default_factory=list, description='List of source-supported qna hot topics.')
    call_sentiment_score: Optional[str] = Field(None, description='Source-supported call sentiment score.')
    revenue_beat_miss_pct: Optional[str] = Field(None, description='Reported revenue beat or miss versus the named consensus source; omit if the source does not state the comparison.')
    eps_beat_miss_pct: Optional[str] = Field(None, description='Reported EPS beat or miss versus the named consensus source; omit if the source does not state the comparison.')
    eps_adjusted: Optional[str] = Field(None, description='Reported adjusted or non-GAAP EPS with period and basis. Do not substitute GAAP EPS.')
    gross_margin_pct: Optional[str] = Field(None, description='Reported gross margin percentage with period and adjusted or GAAP/IFRS basis.')
    operating_margin_pct: Optional[str] = Field(None, description='Reported operating margin percentage with period and adjusted or GAAP/IFRS basis.')
    free_cash_flow: Optional[str] = Field(None, description='Reported free cash flow in the stated source currency and scale; do not calculate it unless the source reports it.')


class SECFilingExtraction(FinancialReportExtraction):
    """Flat schema for `SECFiling` content."""
    form_type: Optional[str] = Field(None, description='Source-supported form type.')
    accession_number: Optional[str] = Field(None, description='Source-supported accession number.')
    issuer: Optional[str] = Field(None, description='Source-supported issuer.')
    cik: Optional[str] = Field(None, description='Source-supported cik.')
    filed_at: Optional[str] = Field(None, description='Source-supported filed at.')
    event_date: Optional[str] = Field(None, description='Source-supported event date.')
    amends: Optional[str] = Field(None, description='Source-supported amends.')
    amendment_flag: Optional[bool] = Field(None, description='Whether the filing explicitly identifies itself as an amendment. Omit if the source does not state this.')
    filers: List[str] = Field(default_factory=list, description='List of source-supported filers.')
    filing_items: List[str] = Field(default_factory=list, description='List of source-supported filing items.')
    material_disclosures: List[str] = Field(default_factory=list, description='List of source-supported material disclosures.')
    exhibits: List[str] = Field(default_factory=list, description='List of source-supported exhibits.')
    business_overview: List[str] = Field(default_factory=list, description='List of source-supported business overview.')
    controls: List[str] = Field(default_factory=list, description='List of source-supported controls.')
    legal_proceedings: List[str] = Field(default_factory=list, description='List of source-supported legal proceedings.')
    critical_accounting_estimates: List[str] = Field(default_factory=list, description='List of source-supported critical accounting estimates.')
    material_trends_uncertainties: List[str] = Field(default_factory=list, description='List of source-supported material trends uncertainties.')
    current_report_events: List[str] = Field(default_factory=list, description='List of source-supported current report events.')
    offering: List[str] = Field(default_factory=list, description='List of source-supported offering.')
    ownership: List[str] = Field(default_factory=list, description='List of source-supported ownership.')
    governance: List[str] = Field(default_factory=list, description='List of source-supported governance.')
    material_event_description: Optional[str] = Field(None, description='Source-supported material event description.')
    material_events_8k: List[str] = Field(default_factory=list, description='List of source-supported material events 8k.')


class PressReleaseExtraction(_ExtractionBase):
    """Flat schema for `PressRelease` content."""
    issuer: Optional[str] = Field(None, description='Source-supported issuer.')
    release_date: Optional[str] = Field(None, description='Source-supported release date.')
    announcement_type: Optional[str] = Field(None, description='Source-supported announcement type.')
    headline_claim: Optional[str] = Field(None, description='Source-supported headline claim.')
    announced_actions: List[str] = Field(default_factory=list, description='List of source-supported announced actions.')
    parties: List[str] = Field(default_factory=list, description='List of source-supported parties.')
    terms: List[str] = Field(default_factory=list, description='List of source-supported terms.')
    metrics: List[str] = Field(default_factory=list, description='List of source-supported metrics.')
    availability: List[str] = Field(default_factory=list, description='List of source-supported availability.')
    milestones: List[str] = Field(default_factory=list, description='List of source-supported milestones.')
    conditions: List[str] = Field(default_factory=list, description='List of source-supported conditions.')
    next_steps: List[str] = Field(default_factory=list, description='List of source-supported next steps.')
    quotations: List[str] = Field(default_factory=list, description='List of source-supported quotations.')
    claims: List[str] = Field(default_factory=list, description='Attributed claims, causes of action, or assertions. Preserve the speaker or claimant and stated uncertainty.')
    resources: List[str] = Field(default_factory=list, description='List of source-supported resources.')
    forward_looking_statements: List[str] = Field(default_factory=list, description='List of source-supported forward looking statements.')
    media_contact: List[str] = Field(default_factory=list, description='List of source-supported media contact.')


class OfficialStatementExtraction(_ExtractionBase):
    """Flat schema for `OfficialStatement` content."""
    issuing_body: Optional[str] = Field(None, description='Source-supported issuing body.')
    speaker: Optional[str] = Field(None, description='Source-supported speaker.')
    speaker_capacity: Optional[str] = Field(None, description='Source-supported speaker capacity.')
    issued_at: Optional[str] = Field(None, description='Source-supported issued at.')
    subject: Optional[str] = Field(None, description='Source-supported subject.')
    occasion: Optional[str] = Field(None, description='Source-supported occasion.')
    audience: List[str] = Field(default_factory=list, description='List of source-supported audience.')
    positions: List[str] = Field(default_factory=list, description='List of source-supported positions.')
    claims: List[str] = Field(default_factory=list, description='Attributed claims, causes of action, or assertions. Preserve the speaker or claimant and stated uncertainty.')
    rationale: List[str] = Field(default_factory=list, description='List of source-supported rationale.')
    announced_actions: List[str] = Field(default_factory=list, description='List of source-supported announced actions.')
    commitments: List[str] = Field(default_factory=list, description='List of source-supported commitments.')
    requests: List[str] = Field(default_factory=list, description='List of source-supported requests.')
    scope: List[str] = Field(default_factory=list, description='List of source-supported scope.')
    conditions: List[str] = Field(default_factory=list, description='List of source-supported conditions.')
    dates: List[str] = Field(default_factory=list, description='List of source-supported dates.')
    cited_authority: List[str] = Field(default_factory=list, description='List of source-supported cited authority.')
    references: List[str] = Field(default_factory=list, description='List of source-supported references.')


class EnforcementActionExtraction(_ExtractionBase):
    """Flat schema for `EnforcementAction` content."""
    authority: Optional[str] = Field(None, description='Source-supported authority.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction.')
    case_id: Optional[str] = Field(None, description='Source-supported case id.')
    action_type: Optional[str] = Field(None, description='Source-supported action type.')
    stage: Optional[str] = Field(None, description='Source-supported stage.')
    conduct_period: Optional[str] = Field(None, description='Source-supported conduct period.')
    appeal_status: Optional[str] = Field(None, description='Source-supported appeal status.')
    respondents: List[str] = Field(default_factory=list, description='List of source-supported respondents.')
    other_parties: List[str] = Field(default_factory=list, description='List of source-supported other parties.')
    allegations: List[str] = Field(default_factory=list, description='Attributed alleged conduct or facts. Preserve the allegation status; do not present allegations as findings.')
    cited_provisions: List[str] = Field(default_factory=list, description='List of source-supported cited provisions.')
    findings: List[str] = Field(default_factory=list, description='Findings expressly made by the issuing authority or court. Do not include allegations or party arguments.')
    admission_terms: List[str] = Field(default_factory=list, description='List of source-supported admission terms.')
    disposition: List[str] = Field(default_factory=list, description='Source-stated procedural outcome, settlement result, or court disposition; distinguish final from pending status.')
    penalties: List[str] = Field(default_factory=list, description='Final or proposed sanctions with authority, responsible party, amount, currency, and status when stated. Distinguish requested from imposed.')
    restitution: List[str] = Field(default_factory=list, description='List of source-supported restitution.')
    disgorgement: List[str] = Field(default_factory=list, description='List of source-supported disgorgement.')
    restrictions: List[str] = Field(default_factory=list, description='List of source-supported restrictions.')
    remediation: List[str] = Field(default_factory=list, description='List of source-supported remediation.')
    monitoring: List[str] = Field(default_factory=list, description='List of source-supported monitoring.')
    deadlines: List[str] = Field(default_factory=list, description='Source-stated deadline with required action, responsible party, trigger, timezone, and date precision when available.')


class LegislativeBillExtraction(_ExtractionBase):
    """Flat schema for `LegislativeBill` content."""
    bill_id: Optional[str] = Field(None, description='Source-supported bill id.')
    title: Optional[str] = Field(None, description='Source-supported title.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction.')
    legislature: Optional[str] = Field(None, description='Source-supported legislature.')
    session: Optional[str] = Field(None, description='Source-supported session.')
    chamber: Optional[str] = Field(None, description='Source-supported chamber.')
    version_date: Optional[str] = Field(None, description='Source-supported version date.')
    status: Optional[str] = Field(None, description='Source-supported status.')
    sponsors: List[str] = Field(default_factory=list, description='List of source-supported sponsors.')
    cosponsors: List[str] = Field(default_factory=list, description='List of source-supported cosponsors.')
    committees: List[str] = Field(default_factory=list, description='List of source-supported committees.')
    purpose: List[str] = Field(default_factory=list, description='List of source-supported purpose.')
    provisions: List[str] = Field(default_factory=list, description='Operative provisions or proposed changes with section, subject, affected parties, and stated effect.')
    affected_laws: List[str] = Field(default_factory=list, description='List of source-supported affected laws.')
    amendments: List[str] = Field(default_factory=list, description='List of source-supported amendments.')
    covered_entities: List[str] = Field(default_factory=list, description='List of source-supported covered entities.')
    obligations: List[str] = Field(default_factory=list, description="Duties imposed or proposed, formatted with responsible party, required action, trigger, deadline, conditions, and exceptions. Preserve 'must' versus 'may'.")
    rights: List[str] = Field(default_factory=list, description='Rights, permissions, or entitlements with holder, scope, conditions, and exceptions when stated.')
    exceptions: List[str] = Field(default_factory=list, description='Explicit exemptions, carve-outs, limitations, or conditions that qualify a rule, right, duty, or term.')
    enforcement: List[str] = Field(default_factory=list, description='List of source-supported enforcement.')
    proposed_effective_dates: List[str] = Field(default_factory=list, description='List of source-supported proposed effective dates.')
    implementation: List[str] = Field(default_factory=list, description='List of source-supported implementation.')
    fiscal_estimates: List[str] = Field(default_factory=list, description='List of source-supported fiscal estimates.')
    actions: List[str] = Field(default_factory=list, description='List of source-supported actions.')
    votes: List[str] = Field(default_factory=list, description='List of source-supported votes.')
    next_steps: List[str] = Field(default_factory=list, description='List of source-supported next steps.')


class LegislativeProposalExtraction(_ExtractionBase):
    """Flat schema for `LegislativeProposal` content."""
    title: Optional[str] = Field(None, description='Source-supported title.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction.')
    proposal_form: Optional[str] = Field(None, description='Source-supported proposal form.')
    proponents: List[str] = Field(default_factory=list, description='List of source-supported proponents.')
    problem: List[str] = Field(default_factory=list, description='List of source-supported problem.')
    objectives: List[str] = Field(default_factory=list, description='List of source-supported objectives.')
    proposed_measures: List[str] = Field(default_factory=list, description='List of source-supported proposed measures.')
    affected_groups: List[str] = Field(default_factory=list, description='List of source-supported affected groups.')
    proposed_rights: List[str] = Field(default_factory=list, description='List of source-supported proposed rights.')
    proposed_obligations: List[str] = Field(default_factory=list, description='List of source-supported proposed obligations.')
    exceptions: List[str] = Field(default_factory=list, description='Explicit exemptions, carve-outs, limitations, or conditions that qualify a rule, right, duty, or term.')
    implementation_options: List[str] = Field(default_factory=list, description='List of source-supported implementation options.')
    funding: List[str] = Field(default_factory=list, description='List of source-supported funding.')
    costs: List[str] = Field(default_factory=list, description='List of source-supported costs.')
    alternatives: List[str] = Field(default_factory=list, description='List of source-supported alternatives.')
    consultation: List[str] = Field(default_factory=list, description='List of source-supported consultation.')
    support: List[str] = Field(default_factory=list, description='List of source-supported support.')
    opposition: List[str] = Field(default_factory=list, description='List of source-supported opposition.')
    next_steps: List[str] = Field(default_factory=list, description='List of source-supported next steps.')
    related_bills: List[str] = Field(default_factory=list, description='List of source-supported related bills.')


class EnactedLawExtraction(_ExtractionBase):
    """Flat schema for `EnactedLaw` content."""
    law_id: Optional[str] = Field(None, description='Source-supported law id.')
    citation: Optional[str] = Field(None, description='Source-supported citation.')
    title: Optional[str] = Field(None, description='Source-supported title.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction.')
    enacting_body: Optional[str] = Field(None, description='Source-supported enacting body.')
    enacted_date: Optional[str] = Field(None, description='Source-supported enacted date.')
    sunset: Optional[str] = Field(None, description='Source-supported sunset.')
    effective_dates: List[str] = Field(default_factory=list, description='Effective or commencement dates with the provision or condition they apply to. Keep distinct dates as separate items.')
    commencement_conditions: List[str] = Field(default_factory=list, description='List of source-supported commencement conditions.')
    purpose: List[str] = Field(default_factory=list, description='List of source-supported purpose.')
    provisions: List[str] = Field(default_factory=list, description='Operative provisions or proposed changes with section, subject, affected parties, and stated effect.')
    definitions: List[str] = Field(default_factory=list, description='Defined terms and their source-stated meaning, including qualifying conditions or cross-references.')
    scope: List[str] = Field(default_factory=list, description='List of source-supported scope.')
    rights: List[str] = Field(default_factory=list, description='Rights, permissions, or entitlements with holder, scope, conditions, and exceptions when stated.')
    obligations: List[str] = Field(default_factory=list, description="Duties imposed or proposed, formatted with responsible party, required action, trigger, deadline, conditions, and exceptions. Preserve 'must' versus 'may'.")
    prohibitions: List[str] = Field(default_factory=list, description='List of source-supported prohibitions.')
    exceptions: List[str] = Field(default_factory=list, description='Explicit exemptions, carve-outs, limitations, or conditions that qualify a rule, right, duty, or term.')
    enforcement: List[str] = Field(default_factory=list, description='List of source-supported enforcement.')
    penalties: List[str] = Field(default_factory=list, description='Final or proposed sanctions with authority, responsible party, amount, currency, and status when stated. Distinguish requested from imposed.')
    remedies: List[str] = Field(default_factory=list, description='List of source-supported remedies.')
    delegated_powers: List[str] = Field(default_factory=list, description='List of source-supported delegated powers.')
    amendments: List[str] = Field(default_factory=list, description='List of source-supported amendments.')
    repeals: List[str] = Field(default_factory=list, description='List of source-supported repeals.')
    transitional_rules: List[str] = Field(default_factory=list, description='List of source-supported transitional rules.')
    appropriations: List[str] = Field(default_factory=list, description='List of source-supported appropriations.')


class RegulationExtraction(_ExtractionBase):
    """Flat schema for `Regulation` content."""
    title: Optional[str] = Field(None, description='Source-supported title.')
    citation: Optional[str] = Field(None, description='Source-supported citation.')
    rule_id: Optional[str] = Field(None, description='Source-supported rule id.')
    agency: Optional[str] = Field(None, description='Source-supported agency.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction.')
    status: Optional[str] = Field(None, description='Source-supported status.')
    authority: List[str] = Field(default_factory=list, description='List of source-supported authority.')
    scope: List[str] = Field(default_factory=list, description='List of source-supported scope.')
    regulated_entities: List[str] = Field(default_factory=list, description='List of source-supported regulated entities.')
    definitions: List[str] = Field(default_factory=list, description='Defined terms and their source-stated meaning, including qualifying conditions or cross-references.')
    thresholds: List[str] = Field(default_factory=list, description='List of source-supported thresholds.')
    exemptions: List[str] = Field(default_factory=list, description='List of source-supported exemptions.')
    requirements: List[str] = Field(default_factory=list, description='List of source-supported requirements.')
    prohibitions: List[str] = Field(default_factory=list, description='List of source-supported prohibitions.')
    standards: List[str] = Field(default_factory=list, description='List of source-supported standards.')
    permissions: List[str] = Field(default_factory=list, description='List of source-supported permissions.')
    reporting: List[str] = Field(default_factory=list, description='List of source-supported reporting.')
    recordkeeping: List[str] = Field(default_factory=list, description='List of source-supported recordkeeping.')
    licensing: List[str] = Field(default_factory=list, description='List of source-supported licensing.')
    procedures: List[str] = Field(default_factory=list, description='List of source-supported procedures.')
    effective_dates: List[str] = Field(default_factory=list, description='Effective or commencement dates with the provision or condition they apply to. Keep distinct dates as separate items.')
    compliance_dates: List[str] = Field(default_factory=list, description='Compliance deadlines with the affected requirement and regulated party. Do not confuse with effective dates.')
    phase_in: List[str] = Field(default_factory=list, description='List of source-supported phase in.')
    transitional_rules: List[str] = Field(default_factory=list, description='List of source-supported transitional rules.')
    enforcement: List[str] = Field(default_factory=list, description='List of source-supported enforcement.')
    penalties: List[str] = Field(default_factory=list, description='Final or proposed sanctions with authority, responsible party, amount, currency, and status when stated. Distinguish requested from imposed.')
    amendments: List[str] = Field(default_factory=list, description='List of source-supported amendments.')
    incorporated_materials: List[str] = Field(default_factory=list, description='List of source-supported incorporated materials.')


class RulemakingNoticeExtraction(_ExtractionBase):
    """Flat schema for `RulemakingNotice` content."""
    agency: Optional[str] = Field(None, description='Source-supported agency.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction.')
    docket_id: Optional[str] = Field(None, description='Source-supported docket id.')
    rule_id: Optional[str] = Field(None, description='Source-supported rule id.')
    notice_type: Optional[str] = Field(None, description='Source-supported notice type.')
    subject: Optional[str] = Field(None, description='Source-supported subject.')
    submission_channel: Optional[str] = Field(None, description='Source-supported submission channel.')
    authority: List[str] = Field(default_factory=list, description='List of source-supported authority.')
    proposed_changes: List[str] = Field(default_factory=list, description='List of source-supported proposed changes.')
    affected_rules: List[str] = Field(default_factory=list, description='List of source-supported affected rules.')
    affected_groups: List[str] = Field(default_factory=list, description='List of source-supported affected groups.')
    alternatives: List[str] = Field(default_factory=list, description='List of source-supported alternatives.')
    analyses: List[str] = Field(default_factory=list, description='List of source-supported analyses.')
    questions_for_comment: List[str] = Field(default_factory=list, description='List of source-supported questions for comment.')
    submission_requirements: List[str] = Field(default_factory=list, description='List of source-supported submission requirements.')
    comment_deadlines: List[str] = Field(default_factory=list, description='List of source-supported comment deadlines.')
    hearings: List[str] = Field(default_factory=list, description='List of source-supported hearings.')
    effective_dates: List[str] = Field(default_factory=list, description='Effective or commencement dates with the provision or condition they apply to. Keep distinct dates as separate items.')
    contact: List[str] = Field(default_factory=list, description='List of source-supported contact.')


class CourtOpinionExtraction(_ExtractionBase):
    """Flat schema for `CourtOpinion` content."""
    case_name: Optional[str] = Field(None, description='Source-supported case name.')
    case_number: Optional[str] = Field(None, description='Source-supported case number.')
    citation: Optional[str] = Field(None, description='Source-supported citation.')
    court: Optional[str] = Field(None, description='Source-supported court.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction.')
    decision_date: Optional[str] = Field(None, description='Source-supported decision date.')
    opinion_author: Optional[str] = Field(None, description='Source-supported opinion author.')
    opinion_type: Optional[str] = Field(None, description='Source-supported opinion type.')
    publication_status: Optional[str] = Field(None, description='Source-supported publication status.')
    judges: List[str] = Field(default_factory=list, description='List of source-supported judges.')
    parties: List[str] = Field(default_factory=list, description='List of source-supported parties.')
    procedural_posture: List[str] = Field(default_factory=list, description='List of source-supported procedural posture.')
    material_facts: List[str] = Field(default_factory=list, description='List of source-supported material facts.')
    issues: List[str] = Field(default_factory=list, description='List of source-supported issues.')
    holdings: List[str] = Field(default_factory=list, description='Questions decided and holdings of the court. Exclude party arguments and separate opinions unless identified as such.')
    reasoning: List[str] = Field(default_factory=list, description='List of source-supported reasoning.')
    standards_of_review: List[str] = Field(default_factory=list, description='List of source-supported standards of review.')
    cited_authorities: List[str] = Field(default_factory=list, description='List of source-supported cited authorities.')
    separate_opinions: List[str] = Field(default_factory=list, description='List of source-supported separate opinions.')
    disposition: List[str] = Field(default_factory=list, description='Source-stated procedural outcome, settlement result, or court disposition; distinguish final from pending status.')
    relief: List[str] = Field(default_factory=list, description='List of source-supported relief.')
    remand_instructions: List[str] = Field(default_factory=list, description='List of source-supported remand instructions.')
    scope_limits: List[str] = Field(default_factory=list, description='List of source-supported scope limits.')


class LawsuitExtraction(_ExtractionBase):
    """Flat schema for `Lawsuit` content."""
    case_name: Optional[str] = Field(None, description='Source-supported case name.')
    case_number: Optional[str] = Field(None, description='Source-supported case number.')
    court: Optional[str] = Field(None, description='Source-supported court.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction.')
    document_type: Optional[str] = Field(None, description='Source-supported document type.')
    filed_at: Optional[str] = Field(None, description='Source-supported filed at.')
    status: Optional[str] = Field(None, description='Source-supported status.')
    parties: List[str] = Field(default_factory=list, description='List of source-supported parties.')
    counsel: List[str] = Field(default_factory=list, description='List of source-supported counsel.')
    party_roles: List[str] = Field(default_factory=list, description='List of source-supported party roles.')
    allegations: List[str] = Field(default_factory=list, description='Attributed alleged conduct or facts. Preserve the allegation status; do not present allegations as findings.')
    factual_chronology: List[str] = Field(default_factory=list, description='List of source-supported factual chronology.')
    claims: List[str] = Field(default_factory=list, description='Attributed claims, causes of action, or assertions. Preserve the speaker or claimant and stated uncertainty.')
    legal_bases: List[str] = Field(default_factory=list, description='List of source-supported legal bases.')
    defenses: List[str] = Field(default_factory=list, description='List of source-supported defenses.')
    responses: List[str] = Field(default_factory=list, description='List of source-supported responses.')
    contested_issues: List[str] = Field(default_factory=list, description='List of source-supported contested issues.')
    requested_relief: List[str] = Field(default_factory=list, description='Relief requested by a party, including type, amount, and conditions when stated. Requested relief is not awarded relief.')
    claimed_damages: List[str] = Field(default_factory=list, description='List of source-supported claimed damages.')
    class_scope: List[str] = Field(default_factory=list, description='List of source-supported class scope.')
    procedural_events: List[str] = Field(default_factory=list, description='List of source-supported procedural events.')
    motions: List[str] = Field(default_factory=list, description='List of source-supported motions.')
    orders: List[str] = Field(default_factory=list, description='List of source-supported orders.')
    deadlines: List[str] = Field(default_factory=list, description='Source-stated deadline with required action, responsible party, trigger, timezone, and date precision when available.')
    settlement: List[str] = Field(default_factory=list, description='List of source-supported settlement.')
    disposition: List[str] = Field(default_factory=list, description='Source-stated procedural outcome, settlement result, or court disposition; distinguish final from pending status.')
    unresolved_matters: List[str] = Field(default_factory=list, description='List of source-supported unresolved matters.')


class GovernmentReportExtraction(_ExtractionBase):
    """Flat schema for `GovernmentReport` content."""
    report_id: Optional[str] = Field(None, description='Source-supported report id.')
    agency: Optional[str] = Field(None, description='Source-supported agency.')
    report_type: Optional[str] = Field(None, description='Source-supported report type.')
    covered_period: Optional[str] = Field(None, description='Source-supported covered period.')
    sample: Optional[str] = Field(None, description='Source-supported sample.')
    follow_up_status: Optional[str] = Field(None, description='Source-supported follow up status.')
    mandate: List[str] = Field(default_factory=list, description='List of source-supported mandate.')
    questions: List[str] = Field(default_factory=list, description='List of source-supported questions.')
    scope: List[str] = Field(default_factory=list, description='List of source-supported scope.')
    geography: List[str] = Field(default_factory=list, description='List of source-supported geography.')
    subjects: List[str] = Field(default_factory=list, description='List of source-supported subjects.')
    methods: List[str] = Field(default_factory=list, description='Methods, procedures, or methodology stated in the source, including relevant design, data, or analytical approach.')
    data_sources: List[str] = Field(default_factory=list, description='List of source-supported data sources.')
    limitations: List[str] = Field(default_factory=list, description='Source-stated limitations, caveats, uncertainty, or constraints. Do not infer limitations.')
    findings: List[str] = Field(default_factory=list, description='Findings expressly made by the issuing authority or court. Do not include allegations or party arguments.')
    metrics: List[str] = Field(default_factory=list, description='List of source-supported metrics.')
    conclusions: List[str] = Field(default_factory=list, description='List of source-supported conclusions.')
    recommendations: List[str] = Field(default_factory=list, description='List of source-supported recommendations.')
    responsible_bodies: List[str] = Field(default_factory=list, description='List of source-supported responsible bodies.')
    deadlines: List[str] = Field(default_factory=list, description='Source-stated deadline with required action, responsible party, trigger, timezone, and date precision when available.')
    agency_responses: List[str] = Field(default_factory=list, description='List of source-supported agency responses.')
    disagreements: List[str] = Field(default_factory=list, description='List of source-supported disagreements.')


class BudgetDocumentExtraction(_ExtractionBase):
    """Flat schema for `BudgetDocument` content."""
    government: Optional[str] = Field(None, description='Source-supported government.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction.')
    document_type: Optional[str] = Field(None, description='Source-supported document type.')
    fiscal_period: Optional[str] = Field(None, description='Source-supported fiscal period.')
    budget_status: Optional[str] = Field(None, description='Source-supported budget status.')
    currency: Optional[str] = Field(None, description='Presentation currency explicitly stated by the source.')
    currency_scale: Optional[str] = Field(None, description='Presentation scale such as units, thousands, millions, or billions, exactly as stated by the source.')
    accounting_basis: Optional[str] = Field(None, description='Source-supported accounting basis.')
    funds: List[str] = Field(default_factory=list, description='List of source-supported funds.')
    programs: List[str] = Field(default_factory=list, description='List of source-supported programs.')
    revenues: List[str] = Field(default_factory=list, description='List of source-supported revenues.')
    expenditures: List[str] = Field(default_factory=list, description='List of source-supported expenditures.')
    appropriations: List[str] = Field(default_factory=list, description='List of source-supported appropriations.')
    allocations: List[str] = Field(default_factory=list, description='List of source-supported allocations.')
    obligations: List[str] = Field(default_factory=list, description="Duties imposed or proposed, formatted with responsible party, required action, trigger, deadline, conditions, and exceptions. Preserve 'must' versus 'may'.")
    outlays: List[str] = Field(default_factory=list, description='List of source-supported outlays.')
    balances: List[str] = Field(default_factory=list, description='List of source-supported balances.')
    financing: List[str] = Field(default_factory=list, description='List of source-supported financing.')
    comparisons: List[str] = Field(default_factory=list, description='List of source-supported comparisons.')
    policy_changes: List[str] = Field(default_factory=list, description='List of source-supported policy changes.')
    assumptions: List[str] = Field(default_factory=list, description='List of source-supported assumptions.')
    forecasts: List[str] = Field(default_factory=list, description='List of source-supported forecasts.')
    restrictions: List[str] = Field(default_factory=list, description='List of source-supported restrictions.')
    earmarks: List[str] = Field(default_factory=list, description='List of source-supported earmarks.')
    conditions: List[str] = Field(default_factory=list, description='List of source-supported conditions.')
    availability_periods: List[str] = Field(default_factory=list, description='List of source-supported availability periods.')


class LegislativeRecordExtraction(_ExtractionBase):
    """Flat schema for `LegislativeRecord` content."""
    legislature: Optional[str] = Field(None, description='Source-supported legislature.')
    chamber: Optional[str] = Field(None, description='Source-supported chamber.')
    session: Optional[str] = Field(None, description='Source-supported session.')
    sitting_date: Optional[str] = Field(None, description='Source-supported sitting date.')
    record_id: Optional[str] = Field(None, description='Source-supported record id.')
    record_type: Optional[str] = Field(None, description='Source-supported record type.')
    agenda_items: List[str] = Field(default_factory=list, description='List of source-supported agenda items.')
    bills: List[str] = Field(default_factory=list, description='List of source-supported bills.')
    motions: List[str] = Field(default_factory=list, description='List of source-supported motions.')
    amendments: List[str] = Field(default_factory=list, description='List of source-supported amendments.')
    speakers: List[str] = Field(default_factory=list, description='List of source-supported speakers.')
    remarks: List[str] = Field(default_factory=list, description='List of source-supported remarks.')
    positions: List[str] = Field(default_factory=list, description='List of source-supported positions.')
    procedural_actions: List[str] = Field(default_factory=list, description='List of source-supported procedural actions.')
    rulings: List[str] = Field(default_factory=list, description='List of source-supported rulings.')
    referrals: List[str] = Field(default_factory=list, description='List of source-supported referrals.')
    votes: List[str] = Field(default_factory=list, description='List of source-supported votes.')
    results: List[str] = Field(default_factory=list, description='Reported research, financial, vote, or hearing results with metric, units, period, and source-stated context.')
    attendance: List[str] = Field(default_factory=list, description='List of source-supported attendance.')
    next_steps: List[str] = Field(default_factory=list, description='List of source-supported next steps.')
    referenced_materials: List[str] = Field(default_factory=list, description='List of source-supported referenced materials.')


class HearingExtraction(_ExtractionBase):
    """Flat schema for `Hearing` content."""
    hearing_id: Optional[str] = Field(None, description='Source-supported hearing id.')
    convening_body: Optional[str] = Field(None, description='Source-supported convening body.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction.')
    hearing_type: Optional[str] = Field(None, description='Source-supported hearing type.')
    hearing_date: Optional[str] = Field(None, description='Source-supported hearing date.')
    topics: List[str] = Field(default_factory=list, description='List of source-supported topics.')
    related_matters: List[str] = Field(default_factory=list, description='List of source-supported related matters.')
    participants: List[str] = Field(default_factory=list, description='List of source-supported participants.')
    participant_roles: List[str] = Field(default_factory=list, description='List of source-supported participant roles.')
    testimony: List[str] = Field(default_factory=list, description='Witness or participant testimony attributed to the speaker, including available timestamp or page reference.')
    claims: List[str] = Field(default_factory=list, description='Attributed claims, causes of action, or assertions. Preserve the speaker or claimant and stated uncertainty.')
    submitted_evidence: List[str] = Field(default_factory=list, description='List of source-supported submitted evidence.')
    questions_and_answers: List[str] = Field(default_factory=list, description='List of source-supported questions and answers.')
    disagreements: List[str] = Field(default_factory=list, description='List of source-supported disagreements.')
    unanswered_questions: List[str] = Field(default_factory=list, description='List of source-supported unanswered questions.')
    commitments: List[str] = Field(default_factory=list, description='List of source-supported commitments.')
    requested_materials: List[str] = Field(default_factory=list, description='List of source-supported requested materials.')
    deadlines: List[str] = Field(default_factory=list, description='Source-stated deadline with required action, responsible party, trigger, timezone, and date precision when available.')
    recorded_actions: List[str] = Field(default_factory=list, description='List of source-supported recorded actions.')


class ResearchPaperExtraction(_ExtractionBase):
    """Flat schema for `ResearchPaper` content."""
    venue: Optional[str] = Field(None, description='Source-supported venue.')
    publication_status: Optional[str] = Field(None, description='Source-supported publication status.')
    study_design: Optional[str] = Field(None, description='Source-supported study design.')
    sample: Optional[str] = Field(None, description='Source-supported sample.')
    setting: Optional[str] = Field(None, description='Source-supported setting.')
    authors: List[str] = Field(default_factory=list, description='List of source-supported authors.')
    affiliations: List[str] = Field(default_factory=list, description='List of source-supported affiliations.')
    identifiers: List[str] = Field(default_factory=list, description='List of source-supported identifiers.')
    research_question: List[str] = Field(default_factory=list, description='List of source-supported research question.')
    hypotheses: List[str] = Field(default_factory=list, description='List of source-supported hypotheses.')
    contributions: List[str] = Field(default_factory=list, description='List of source-supported contributions.')
    methods: List[str] = Field(default_factory=list, description='Methods, procedures, or methodology stated in the source, including relevant design, data, or analytical approach.')
    data: List[str] = Field(default_factory=list, description='List of source-supported data.')
    interventions: List[str] = Field(default_factory=list, description='List of source-supported interventions.')
    comparators: List[str] = Field(default_factory=list, description='List of source-supported comparators.')
    baselines: List[str] = Field(default_factory=list, description='List of source-supported baselines.')
    evaluation_protocol: List[str] = Field(default_factory=list, description='List of source-supported evaluation protocol.')
    results: List[str] = Field(default_factory=list, description='Reported research, financial, vote, or hearing results with metric, units, period, and source-stated context.')
    uncertainty: List[str] = Field(default_factory=list, description='List of source-supported uncertainty.')
    statistical_tests: List[str] = Field(default_factory=list, description='List of source-supported statistical tests.')
    conclusions: List[str] = Field(default_factory=list, description='List of source-supported conclusions.')
    limitations: List[str] = Field(default_factory=list, description='Source-stated limitations, caveats, uncertainty, or constraints. Do not infer limitations.')
    threats_to_validity: List[str] = Field(default_factory=list, description='List of source-supported threats to validity.')
    reproducibility: List[str] = Field(default_factory=list, description='List of source-supported reproducibility.')
    ethics: List[str] = Field(default_factory=list, description='List of source-supported ethics.')
    funding: List[str] = Field(default_factory=list, description='List of source-supported funding.')
    conflicts: List[str] = Field(default_factory=list, description='List of source-supported conflicts.')


class WhitepaperExtraction(_ExtractionBase):
    """Flat schema for `Whitepaper` content."""
    issuer: Optional[str] = Field(None, description='Source-supported issuer.')
    authors: List[str] = Field(default_factory=list, description='List of source-supported authors.')
    audience: List[str] = Field(default_factory=list, description='List of source-supported audience.')
    purpose: List[str] = Field(default_factory=list, description='List of source-supported purpose.')
    problem: List[str] = Field(default_factory=list, description='List of source-supported problem.')
    thesis: List[str] = Field(default_factory=list, description='List of source-supported thesis.')
    proposed_solution: List[str] = Field(default_factory=list, description='List of source-supported proposed solution.')
    architecture: List[str] = Field(default_factory=list, description='List of source-supported architecture.')
    requirements: List[str] = Field(default_factory=list, description='List of source-supported requirements.')
    assumptions: List[str] = Field(default_factory=list, description='List of source-supported assumptions.')
    implementation: List[str] = Field(default_factory=list, description='List of source-supported implementation.')
    adoption_steps: List[str] = Field(default_factory=list, description='List of source-supported adoption steps.')
    claims: List[str] = Field(default_factory=list, description='Attributed claims, causes of action, or assertions. Preserve the speaker or claimant and stated uncertainty.')
    supporting_evidence: List[str] = Field(default_factory=list, description='List of source-supported supporting evidence.')
    case_studies: List[str] = Field(default_factory=list, description='List of source-supported case studies.')
    comparisons: List[str] = Field(default_factory=list, description='List of source-supported comparisons.')
    economics: List[str] = Field(default_factory=list, description='List of source-supported economics.')
    risks: List[str] = Field(default_factory=list, description='List of source-supported risks.')
    limitations: List[str] = Field(default_factory=list, description='Source-stated limitations, caveats, uncertainty, or constraints. Do not infer limitations.')
    tradeoffs: List[str] = Field(default_factory=list, description='List of source-supported tradeoffs.')
    recommendations: List[str] = Field(default_factory=list, description='List of source-supported recommendations.')
    roadmap: List[str] = Field(default_factory=list, description='List of source-supported roadmap.')
    references: List[str] = Field(default_factory=list, description='List of source-supported references.')
    disclosures: List[str] = Field(default_factory=list, description='List of source-supported disclosures.')


class TechnicalDocumentationExtraction(_ExtractionBase):
    """Flat schema for `TechnicalDocumentation` content."""
    product: Optional[str] = Field(None, description='Source-supported product.')
    component: Optional[str] = Field(None, description='Source-supported component.')
    doc_type: Optional[str] = Field(None, description='Source-supported doc type.')
    audience: List[str] = Field(default_factory=list, description='List of source-supported audience.')
    purpose: List[str] = Field(default_factory=list, description='List of source-supported purpose.')
    concepts: List[str] = Field(default_factory=list, description='List of source-supported concepts.')
    prerequisites: List[str] = Field(default_factory=list, description='List of source-supported prerequisites.')
    compatibility: List[str] = Field(default_factory=list, description='List of source-supported compatibility.')
    setup: List[str] = Field(default_factory=list, description='List of source-supported setup.')
    configuration: List[str] = Field(default_factory=list, description='List of source-supported configuration.')
    procedures: List[str] = Field(default_factory=list, description='List of source-supported procedures.')
    interfaces: List[str] = Field(default_factory=list, description='Documented API, function, endpoint, CLI, or protocol interface with signature, purpose, inputs, and outputs.')
    inputs: List[str] = Field(default_factory=list, description='List of source-supported inputs.')
    outputs: List[str] = Field(default_factory=list, description='List of source-supported outputs.')
    examples: List[str] = Field(default_factory=list, description='Exact or faithfully preserved source examples, commands, code snippets, or usage cases. Do not invent executable output.')
    expected_results: List[str] = Field(default_factory=list, description='List of source-supported expected results.')
    authentication: List[str] = Field(default_factory=list, description='List of source-supported authentication.')
    permissions: List[str] = Field(default_factory=list, description='List of source-supported permissions.')
    limits: List[str] = Field(default_factory=list, description='List of source-supported limits.')
    errors: List[str] = Field(default_factory=list, description='Documented error codes, failure conditions, messages, and recovery guidance.')
    warnings: List[str] = Field(default_factory=list, description='List of source-supported warnings.')
    security_notes: List[str] = Field(default_factory=list, description='List of source-supported security notes.')
    troubleshooting: List[str] = Field(default_factory=list, description='List of source-supported troubleshooting.')
    changes: List[str] = Field(default_factory=list, description='List of source-supported changes.')
    deprecations: List[str] = Field(default_factory=list, description='Deprecated feature, version, replacement, deadline, and migration condition stated by the source.')
    migration: List[str] = Field(default_factory=list, description='List of source-supported migration.')
    references: List[str] = Field(default_factory=list, description='List of source-supported references.')
