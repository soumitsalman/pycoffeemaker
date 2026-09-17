from typing import List, Optional
from functools import cached_property
from pydantic import BaseModel, Field

from .normalize import normalize_fields, merge_lists
from .formatters import model_text_schema, text_value, apply_model_json_constraints

_TAG_MAX_LEN = 50
_TAGS_MAX_COUNT = 10
_TICKER_MAX_LEN = 6

_DIGEST_ACTIONS_MAX_COUNT = 10
_DIGEST_CROSS_DOMAIN_IMPACTS_MAX_COUNT = 5

_DIGEST_EVENT_TYPE_MAX_LEN = 50
_DIGEST_IMPACT_LEVEL_MAX_LEN = 15
_DIGEST_MACRO_CONTEXT_MAX_LEN = 50
_DIGEST_FUTURE_OUTLOOK_MAX_LEN = 300
_DIGEST_BRIEFING_MAX_LEN = 1000

_TAG_LIST_ITEM_MAX_LEN = {
    "regions": _TAG_MAX_LEN,
    "people": _TAG_MAX_LEN,
    "products": _TAG_MAX_LEN,
    "companies": _TAG_MAX_LEN,
    "entities": _TAG_MAX_LEN,
    "stock_tickers": _TICKER_MAX_LEN,
    "impacted_domains": _TAG_MAX_LEN,    
}

_MODEL_DUMP_DEFAULTS = {
    "exclude_none": True,
    "exclude_unset": True,
    "exclude_defaults": True,
}

class _NLPBaseModel(BaseModel):
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


class Entities(_NLPBaseModel):
    regions: List[str] = Field(default_factory=list, description="List of specified names geographic regions/locations. max_items<=10. exclude_pattern=N countries.")
    people: List[str] = Field(default_factory=list, description="List of specified names of people (CXOs,political leaders,influential figures). max_items<=10. exclude_pattern=N leaders.")
    products: List[str] = Field(default_factory=list, description="List of specified names products/services. max_items<=10. exclude_pattern=N products.")
    companies: List[str] = Field(default_factory=list, description="List of specified names companies/organizations. max_items<=10. exclude_pattern=N companies.")
    stock_tickers: List[str] = Field(default_factory=list, description="List of specified stock ticker symbols. max_items<=10. exclude_pattern=N stock tickers.")    
 
    @property
    def tags(self):
        return merge_lists(self.regions, self.people, self.products, self.companies, self.stock_tickers)

class Digest(_NLPBaseModel):
    """Main digest/key points of an article/news/blog/report"""        
    key_points: list[str] = Field(
        default_factory=list,
        description=(
            "MANDATORY. "
            "list=Key points, events sequence, actions sequence, activities. "
            "max_items<=40. "
            "format=YYYY-MM-DD Actor verb object/effect with key metric if available."
        )
    )
    drivers: list[str] = Field(
        default_factory=list,
        description=(
            "list=Traceable causal relationships between initial actions and outcomes/results. "
            "max_items<=10. "
            "format=Cause produced resulting effect."
        )
    )
    # "model_release, agent_launch, enterprise_adoption_case, safety_regulation_update, multimodal_breakthrough\n"
    # "ransomware_attack, zero_day_disclosure, supply_chain_breach, ai_enhanced_exploit, state_sponsored_campaign\n"
    # "chip_launch, platform_announcement, chip_shortage, foundry_partnership\n"
    # "humanoid_demo, warehouse_deployment, drone_swarm_test, regulation_change\n"
    # "series_a, acquisition_announced, merger_completed, strategic_partnership, ipo_filing\n"
    # "earnings_beat, stock_reaction, analyst_upgrade, sector_rotation, sec_filing_update\n"
    # "route_disruption, freight_rate_spike, aircraft_order, supply_chain_bottleneck, cyber_incident_on_cargo\n"
    # "oil_price_shock, gdp_forecast_revision, inflation_spike, rate_cut_signal, commodity_demand_shift\n"
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
    # "Examples:\n"
    # "- Cybersecurity: Increased risk of data breaches due to new vulnerabilities\n"
    # "- Aviation: Flight delays and cancellations due to air traffic control issues\n"
    # "- Hardware: Supply chain disruptions affecting chip production\n"
    # "- Startups: Emerging companies facing funding challenges\n"
    # cross_domain_impacts: List[str] = Field(
    #     default_factory=list,
    #     description=(
    #         "list=Secondary domains: impacts. "
    #         "max_items<=5. "
    #         "format=DOMAIN: 1 simple sentence impact."
    #     )
    # )
    # "Examples: US-Iran conflict, Red Sea disruption, Tariff volatility, Rare earth controls, Arctic shipping rivalry, Africa mineral conflict, Cyber arms race escalation etc."     
    macro_context: Optional[str] = Field(
        None,
        description="Primary overarching geopolitical, trade, economic, technological context driving the events(<=4words).",
    )
    future_outlook: Optional[str] = Field(
        default=None,
        description="Traceable future outlook, trajectory or forecast. Omit if NA",
    )
    briefing: str = Field(
        description=(
            "MANDATORY. Intelligence briefing of the events (<=2sentences). "
            "Include time/date, context, actors, action sequence, mechanisms, affected parties, effects, key metrics, comparisons, significance. "
        ),
    )
    

_BRIEFING_EVENTS_MAX_COUNT = 40
_BRIEFING_LIST_MAX_COUNT = 10

_BRIEFING_IMPACT_LEVEL_MAX_LEN = 15
_BRIEFING_FORECAST_MAX_LEN = 300
_BRIEFING_BRIEFING_MAX_LEN = 1000

class Briefing(_NLPBaseModel):
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
            "list=Traceable causal relationships between actions and outcomes. "
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
    briefing: str = Field(
        description=(
            "MANDATORY. Intelligence briefing of the events (<=3sentences). "
            "Include time/date, larger context, actors, events, targets/affected parties, with key metrics/comparisons. "
            "Explain mechanism/how, impact/why it matters, and effects/response/outlook. "
        )
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

    def __bool__(self):
        return bool(self.briefing)


# ────────────────────────────────────────────────
# Domain-specific models (inherit from base)
# ────────────────────────────────────────────────

class AINewsDigest(Digest):
    benchmark_scores: List[str] = Field(default_factory=list, description="List of reported performance numbers on standard benchmarks (key: benchmark name, value: score)")
    claimed_productivity_lift: Optional[str] = Field(None, description="Reported productivity/efficiency gain")
    enterprise_adoption_rate: Optional[str] = Field(None, description="Reported adoption/usage rate")
    price: Optional[str] = Field(None, description="Reported unit/subscription price")
    valuation_or_market_size: Optional[str] = Field(None, description="Company valuation or projected market size")


class CyberNewsDigest(Digest):
    # malware information
    threat_actors: List[str] = Field(default_factory=list, description="List of named or categorized attackers. Examples: LockBit, nation state, etc.",)
    vulnerabilities: List[str] = Field(default_factory=list, description="List of CVE IDs, product names or zero-day descriptions mentioned")    
    malware_family: Optional[str] = Field(None, description="Name of the malware family or ransomware strain if applicable")
    attack_speed: Optional[str] = Field(None)
    incident_type: Optional[str] = Field(None, description="Primary category of the cybersecurity event. Allowed: ransomware, supply_chain, zero_day, ai_enhanced, state_sponsored, shadow_ai, critical_infra etc.")
    # impact information
    products: List[str] = Field(default_factory=list, description="List of impacted/vulnerable products/services. Exclude=generic,grouped/aggregated qualifications - 3 new products.")
    entities: List[str] = Field(default_factory=list, description="List of impacted/vulnerable organizations/sectors/users/scope. Exclude=generic,grouped/aggregated qualifications - 7 organizations.")
    technical_impact: Optional[str] = Field(None, description="Specified quantitative technical consequences. Examples: 1000 records breach, 10h service outage etc.")
    financial_impact: Optional[str] = Field(None, description="Specified financial damage or cost of recovery in USD.")    
    business_impact: Optional[str] = Field(None, description="Specified operational, financial, reputational or regulatory consequences.")
    compliance_impact: List[str] = Field(default_factory=list, description="List of specified policies,standards,laws,regulations as being triggered (SEC, CIRCIA, GDPR, etc.)")
    # remediation
    mitigations: List[str] = Field(default_factory=list, description="List of specified defensive/corrective/mitigation/recovery steps" )


class HardwareNewsDigest(Digest):
    """Summary focused on chips, accelerators, compute infrastructure"""

    products: List[str] = Field(default_factory=list, description="List of specified products/chips (ex: NVIDIA H100, AMD MI300, AWS Trainium). Exclude=generic,grouped/aggregated qualifications - 3 new products.")
    use_cases: List[str] = Field(default_factory=list, description="List of intended/demonstrated uses. Examples: AI training, inference, HPC, edge computing. Limit=5",)
    performance_improvement: Optional[str] = Field(None, description="Speedup factor compared to previous generation. Include=value,unit,context (ex: 2.8x faster inference).")
    power_efficiency: Optional[str] = Field(None, description="Power/energy usage gain/reduction. Include=unit,value (ex: 15% reduction).")
    capex_investment: Optional[str] = Field(None,description="CapEx for data centers/fabs. Include=unit,value (ex: $2.5 billion)")
    price: Optional[str] = Field(None, description="Reported unit/subscription price.")

class RoboticsAVDronesNewsSummary(Digest):
    """Summary focused on robotics systems, autonomous vehicles, drones"""

    product_system: str = Field(
        ..., description="Name/model of robot/AV/drone (e.g. 'Tesla Cybercab', 'Boston Dynamics Stretch')"
    )
    manufacturer: str = Field(
        ..., description="Company. Format: official name. If startup: include founding year."
    )
    category: Optional[str] = Field(
        None,
        description="Broad category of the embodied system. Allowed: industrial_cobot, humanoid, autonomous_vehicle, drone_swarm, warehouse_agv.",
    )
    speed_or_payload_improvement: Optional[str] = Field(
        None,
        description="Key upgrade vs prior gen. Include unit in value. Examples: '2.5 m/s', '50% faster'.",
    )
    deployment_sites_count: Optional[str] = Field(
        None,
        description="Real-world deployments/customers mentioned. Include count in value (e.g. '12 sites').",
    )
    funding_raised_usd_millions: Optional[str] = Field(
        None,
        description="Funding amount raised. Include currency and magnitude in value (e.g. '$50 million').",
    )
    cost_per_unit_usd: Optional[str] = Field(
        None, description="Estimated cost per robot / vehicle. Include currency in value (e.g. '$250,000')."
    )
    real_world_limitation_noted: List[str] = Field(
        default_factory=list,
        description="Practical limitations noted. Examples: weather sensitivity, edge cases, cost barriers. Exclude speculation. Limit: 5.",
    )


class StartupCorpNewsSummary(Digest):
    """Summary focused on startups, corporate moves, funding, M&A"""

    main_company: str = Field(..., description="Primary company (official name). No qualifiers.")
    other_companies: List[str] = Field(
        default_factory=list,
        description="Co-parties: acquirers, investors, partners. Exclude=unrelated mentions. Limit: 5.",
    )
    lead_investors: List[str] = Field(
        default_factory=list, description="Lead/prominent investors (official names/fund names). Exclude=passive stakeholders. Limit: 5."
    )
    acquirer: Optional[str] = Field(
        None, description="Name of the acquiring company in M&A deals"
    )
    funding_amount_usd_millions: Optional[str] = Field(
        None, description="Amount raised in the funding round. Include currency in value (e.g. '$25 million')."
    )
    round_type: Optional[str] = Field(
        None, description="Stage of funding (Seed, Series A, Late, Debt, etc.)"
    )
    pre_post_valuation_usd_billions: Optional[str] = Field(
        None, description="Pre-money and/or post-money valuation. Include currency in value (e.g. 'pre: $500M, post: $1B')."
    )
    deal_value_usd_millions: Optional[str] = Field(
        None, description="Transaction value in M&A deals. Include currency in value (e.g. '$500 million')."
    )
    yoy_funding_growth_pct: Optional[str] = Field(
        None,
        description="Year-over-year change in funding volume. Include unit in value (e.g. '+15%').",
    )
    strategic_rationale: str = Field(
        ...,
        description="1-sentence rationale. Format: [Actor] seeks [capability/market] via [deal type].",
    )
    use_of_funds: Optional[str] = Field(
        None, description="Stated use of capital. Categories: R&D, expansion, acquisition, operations, debt repayment. Be explicit."
    )


class FinancialMarketsNewsSummary(Digest):
    """Summary focused on stocks, earnings, filings, market movements"""

    ticker_or_index: str = Field(
        ..., description="Primary ticker(s) or index (format: 'AAPL' or 'S&P500'). Limit: 3 tickers max."
    )
    companies_mentioned: List[str] = Field(
        default_factory=list,
        description="Companies discussed. Format: official ticker+name. Exclude=tangential mentions. Limit: 5.",
    )
    stock_reaction_pct: Optional[str] = Field(
        None,
        description="Stock price change after news. Include unit in value (e.g. '+2.5%' or '-1.3%').",
    )
    earnings_beat_miss_pct: Optional[str] = Field(
        None, description="EPS/revenue beat or miss vs consensus. Include unit and direction in value (e.g. '+3% beat' or '-2% miss')."
    )
    revenue_or_ebitda_usd_millions: Optional[str] = Field(
        None,
        description="Reported or forecasted revenue / EBITDA. Include currency and magnitude in value (e.g. '$1,200 million' or '$1.2B').",
    )
    forward_guidance_change_pct: Optional[str] = Field(
        None, description="FY guidance change vs prior. Include unit in value (e.g. '+5%' if raised, '-10%' if lowered, or 'reaffirmed')."
    )
    valuation_multiple: Optional[str] = Field(
        None,
        description="Forward-looking valuation metric (e.g. '32x revenue', '18x EBITDA')",
    )
    financial_analysis_summary: str = Field(
        ...,
        description="1-sentence summary. Format: [Company] [beat/miss] due to [reason], implying [outlook].",
    )
    sector_rotation_signal: str = Field(
        default="",
        description="Money flow signal. Format: 'from [sector] to [sector]' (e.g. 'from Mag7 to cyclicals').",
    )


class LogisticsDigest(Digest):
    transportation_mode: str = Field(description="Allowed: N/A, air, ocean, truck, multimodal")
    affected_routes: List[str] = Field(default_factory=list, description="Examples: Red Sea, Suez, Transpacific. Limit=5")
    freight_rate_change: Optional[str] = Field(None, description="Include=value,unit,context. Example: +8% in 2 years")
    order_quantity: Optional[str] = Field(None, description="Include=value,unit. Example: 30 tons")
    shipping_delay: Optional[str] = Field(None, description="Include=value,unit,context. Example: 5 days for Transpacific route.")
    financial_impact: Optional[str] = Field(None, description="Specified financial damage or cost of recovery in USD.")    
    business_impact: Optional[str] = Field(None, description="Specified operational, reputational or regulatory consequences")
    mitigations: List[str] = Field(
        default_factory=list,
        description="List of specified mitigations. Examples: rerouting, inventory buildup, alternative suppliers. Limit=5.",
    )


class MacroEconomyDigest(Digest):
    """Summary focused on global economy, macro indicators, forecasts"""

    gdp_growth_forecast: Optional[str] = Field(None, description="GDP growth forecast. Include=value,unit,timeframe.")
    inflation_impact: Optional[str] = Field(None, description="Inflation impact. Include=value,unit,timeframe.")
    oil_price: Optional[str] = Field(None, description="Oil price scenario. Include=value,unit,timeframe.")
    gold_demand: Optional[str] = Field(None, description="Gold demand. Include=value,unit,timeframe.")
    macro_signal: str = Field(...,description="Format: [Event] signals [risk/opportunity] for [sector/macro].",)
    market_significance: str = Field(description="Format: affects [equities/credit/funding] via [mechanism].",)


# ────────────────────────────────────────────────
# Flat content-kind extraction models
# ────────────────────────────────────────────────
# Each list item is a self-contained source-supported record. These schemas
# contain no dicts, nested models, or list-of-object fields.

class FlatExtraction(_NLPBaseModel):
    """Common flat envelope for non-editorial content extraction."""
    briefing: Optional[str] = Field(None, description='Compact, factual description of the document and its material substance. Use only information supported by the source.')
    key_points: List[str] = Field(default_factory=list, description='Concise source-supported key points. Do not add generic impacts, causal claims, or forecasts.')
    # actors: List[str] = Field(default_factory=list, description="Named people or organizations with their stated roles, formatted as 'role: name'.")
    # evidence: List[str] = Field(default_factory=list, description="Source support for a retained fact, formatted as 'field | locator | short supporting text'. Use only locators present in the input.")
    # coverage: List[str] = Field(default_factory=list, description='Input-scope limitations, missing sections, attachments, tables, transcript parts, or truncation affecting this extraction.')


class SiteExtraction(FlatExtraction):
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


class JobExtraction(FlatExtraction):
    """Flat schema for `Job` content."""
    job_title: Optional[str] = Field(None, description='Source-supported job title. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    employer: Optional[str] = Field(None, description='Source-supported employer. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    requisition_id: Optional[str] = Field(None, description='Source-supported requisition id. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    team: Optional[str] = Field(None, description='Source-supported team. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    work_arrangement: Optional[str] = Field(None, description='Source-supported work arrangement. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    employment_type: Optional[str] = Field(None, description='Source-supported employment type. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    seniority: Optional[str] = Field(None, description='Source-supported seniority. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    schedule: Optional[str] = Field(None, description='Source-supported schedule. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    travel: Optional[str] = Field(None, description='Source-supported travel. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    application_url: Optional[str] = Field(None, description='Source-supported application url. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    posting_status: Optional[str] = Field(None, description='Source-supported posting status. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    locations: List[str] = Field(default_factory=list, description='List of source-supported locations. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    responsibilities: List[str] = Field(default_factory=list, description='List of source-supported responsibilities. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    required_qualifications: List[str] = Field(default_factory=list, description='List of source-supported required qualifications. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    preferred_qualifications: List[str] = Field(default_factory=list, description='List of source-supported preferred qualifications. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    compensation: List[str] = Field(default_factory=list, description='List of source-supported compensation. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    benefits: List[str] = Field(default_factory=list, description='List of source-supported benefits. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    eligibility: List[str] = Field(default_factory=list, description='List of source-supported eligibility. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    application_requirements: List[str] = Field(default_factory=list, description='List of source-supported application requirements. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    deadlines: List[str] = Field(default_factory=list, description='Source-stated deadline with required action, responsible party, trigger, timezone, and date precision when available.')


class ContractExtraction(FlatExtraction):
    """Flat schema for `Contract` content."""
    contract_type: Optional[str] = Field(None, description='Source-supported contract type. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    agreement_id: Optional[str] = Field(None, description='Source-supported agreement id. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    document_status: Optional[str] = Field(None, description='Source-supported document status. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    effective_date: Optional[str] = Field(None, description='Source-supported effective date. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    term: Optional[str] = Field(None, description='Source-supported term. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    renewal: Optional[str] = Field(None, description='Source-supported renewal. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    dispute_resolution: Optional[str] = Field(None, description='Source-supported dispute resolution. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    governing_law: Optional[str] = Field(None, description='Source-supported governing law. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    precedence: Optional[str] = Field(None, description='Source-supported precedence. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    parties: List[str] = Field(default_factory=list, description='List of source-supported parties. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    execution_dates: List[str] = Field(default_factory=list, description='List of source-supported execution dates. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    scope: List[str] = Field(default_factory=list, description='List of source-supported scope. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    deliverables: List[str] = Field(default_factory=list, description='List of source-supported deliverables. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    milestones: List[str] = Field(default_factory=list, description='List of source-supported milestones. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    acceptance: List[str] = Field(default_factory=list, description='List of source-supported acceptance. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    service_levels: List[str] = Field(default_factory=list, description='List of source-supported service levels. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    obligations: List[str] = Field(default_factory=list, description="Duties imposed or proposed, formatted with responsible party, required action, trigger, deadline, conditions, and exceptions. Preserve 'must' versus 'may'.")
    rights: List[str] = Field(default_factory=list, description='Rights, permissions, or entitlements with holder, scope, conditions, and exceptions when stated.')
    conditions: List[str] = Field(default_factory=list, description='List of source-supported conditions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    commercial_terms: List[str] = Field(default_factory=list, description='List of source-supported commercial terms. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    intellectual_property: List[str] = Field(default_factory=list, description='List of source-supported intellectual property. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    confidentiality: List[str] = Field(default_factory=list, description='List of source-supported confidentiality. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    data_handling: List[str] = Field(default_factory=list, description='List of source-supported data handling. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    restrictions: List[str] = Field(default_factory=list, description='List of source-supported restrictions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    warranties: List[str] = Field(default_factory=list, description='List of source-supported warranties. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    indemnities: List[str] = Field(default_factory=list, description='List of source-supported indemnities. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    liability: List[str] = Field(default_factory=list, description='List of source-supported liability. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    insurance: List[str] = Field(default_factory=list, description='List of source-supported insurance. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    termination: List[str] = Field(default_factory=list, description='List of source-supported termination. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    remedies: List[str] = Field(default_factory=list, description='List of source-supported remedies. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    amendments: List[str] = Field(default_factory=list, description='List of source-supported amendments. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    incorporated_documents: List[str] = Field(default_factory=list, description='List of source-supported incorporated documents. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class ProcurementNoticeExtraction(FlatExtraction):
    """Flat schema for `ProcurementNotice` content."""
    notice_type: Optional[str] = Field(None, description='Source-supported notice type. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    notice_id: Optional[str] = Field(None, description='Source-supported notice id. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    buyer: Optional[str] = Field(None, description='Source-supported buyer. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    status: Optional[str] = Field(None, description='Source-supported status. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    performance_period: Optional[str] = Field(None, description='Source-supported performance period. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    contract_type: Optional[str] = Field(None, description='Source-supported contract type. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    submission_channel: Optional[str] = Field(None, description='Source-supported submission channel. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    scope: List[str] = Field(default_factory=list, description='List of source-supported scope. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    lots: List[str] = Field(default_factory=list, description='List of source-supported lots. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    deliverables: List[str] = Field(default_factory=list, description='List of source-supported deliverables. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    performance_location: List[str] = Field(default_factory=list, description='List of source-supported performance location. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    estimated_value: List[str] = Field(default_factory=list, description='List of source-supported estimated value. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    funding: List[str] = Field(default_factory=list, description='List of source-supported funding. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    eligibility: List[str] = Field(default_factory=list, description='List of source-supported eligibility. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    submission_requirements: List[str] = Field(default_factory=list, description='List of source-supported submission requirements. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    deadlines: List[str] = Field(default_factory=list, description='Source-stated deadline with required action, responsible party, trigger, timezone, and date precision when available.')
    evaluation_criteria: List[str] = Field(default_factory=list, description='List of source-supported evaluation criteria. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    evaluation_weights: List[str] = Field(default_factory=list, description='List of source-supported evaluation weights. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    award_process: List[str] = Field(default_factory=list, description='List of source-supported award process. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    contact: List[str] = Field(default_factory=list, description='List of source-supported contact. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    amendments: List[str] = Field(default_factory=list, description='List of source-supported amendments. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    award_details: List[str] = Field(default_factory=list, description='List of source-supported award details. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class FinancialReportExtraction(FlatExtraction):
    """Flat schema for `FinancialReport` content."""
    reporting_entity: Optional[str] = Field(None, description='Source-supported reporting entity. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    report_type: Optional[str] = Field(None, description='Source-supported report type. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    reporting_period: Optional[str] = Field(None, description='Source-supported reporting period. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    period_end_date: Optional[str] = Field(None, description='Source-supported period end date. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    reporting_scope: Optional[str] = Field(None, description='Source-supported reporting scope. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    accounting_basis: Optional[str] = Field(None, description='Source-supported accounting basis. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    currency: Optional[str] = Field(None, description='Presentation currency explicitly stated by the source.')
    currency_scale: Optional[str] = Field(None, description='Presentation scale such as units, thousands, millions, or billions, exactly as stated by the source.')
    audit_status: Optional[str] = Field(None, description='Source-supported audit status. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    auditor_opinion: Optional[str] = Field(None, description='Source-supported auditor opinion. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    income_statement: List[str] = Field(default_factory=list, description='Reported income-statement line items with value, currency, scale, period, comparator, and reporting basis.')
    balance_sheet: List[str] = Field(default_factory=list, description='Reported balance-sheet line items with value, currency, scale, as-of date, and reporting basis.')
    cash_flow: List[str] = Field(default_factory=list, description='Reported cash-flow line items with value, currency, scale, period, and reporting basis.')
    segments: List[str] = Field(default_factory=list, description='List of source-supported segments. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    geographies: List[str] = Field(default_factory=list, description='List of source-supported geographies. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    operating_metrics: List[str] = Field(default_factory=list, description='List of source-supported operating metrics. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    accounting_policies: List[str] = Field(default_factory=list, description='List of source-supported accounting policies. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    restatements: List[str] = Field(default_factory=list, description='List of source-supported restatements. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    one_time_items: List[str] = Field(default_factory=list, description='List of source-supported one time items. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    liquidity: List[str] = Field(default_factory=list, description='List of source-supported liquidity. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    debt: List[str] = Field(default_factory=list, description='List of source-supported debt. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    commitments: List[str] = Field(default_factory=list, description='List of source-supported commitments. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    contingencies: List[str] = Field(default_factory=list, description='List of source-supported contingencies. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    related_parties: List[str] = Field(default_factory=list, description='List of source-supported related parties. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    management_discussion: List[str] = Field(default_factory=list, description='List of source-supported management discussion. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    risks: List[str] = Field(default_factory=list, description='List of source-supported risks. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    outlook: List[str] = Field(default_factory=list, description='List of source-supported outlook. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    financial_ratios: List[str] = Field(default_factory=list, description="Reported financial ratios, formatted as 'metric: value', with period, basis, and adjustment status when stated.")
    mdna_key_takeaways: List[str] = Field(default_factory=list, description='List of source-supported mdna key takeaways. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    main_drivers: List[str] = Field(default_factory=list, description='List of source-supported main drivers. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    top_risk_factors: List[str] = Field(default_factory=list, description='List of source-supported top risk factors. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    legal_proceedings_status: List[str] = Field(default_factory=list, description='List of source-supported legal proceedings status. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    business_overview_highlights: List[str] = Field(default_factory=list, description='List of source-supported business overview highlights. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    strategic_initiatives: List[str] = Field(default_factory=list, description='List of source-supported strategic initiatives. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    key_exhibits_filed: List[str] = Field(default_factory=list, description='List of source-supported key exhibits filed. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    revenue: Optional[str] = Field(None, description='Reported revenue in the stated source currency and scale. Do not convert or normalize currencies.')
    revenue_growth_yoy_pct: Optional[str] = Field(None, description='Reported year-over-year revenue growth percentage for the stated period; do not calculate it.')
    net_income: Optional[str] = Field(None, description='Reported net income in the stated source currency and scale. Preserve attributable entity and reporting basis.')
    eps_basic: Optional[str] = Field(None, description='Reported basic EPS with period and accounting basis.')
    eps_diluted: Optional[str] = Field(None, description='Reported diluted EPS with period and accounting basis.')
    operating_cash_flow: Optional[str] = Field(None, description='Reported operating cash flow in the stated source currency and scale.')
    capex: Optional[str] = Field(None, description='Source-supported capex. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    cash_equivalents: Optional[str] = Field(None, description='Source-supported cash equivalents. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    total_debt: Optional[str] = Field(None, description='Source-supported total debt. Preserve exact names, dates, units, conditions, and status; omit if not stated.')


class EarningsReportExtraction(FinancialReportExtraction):
    """Flat schema for `EarningsReport` content."""
    document_subtype: Optional[str] = Field(None, description='Source-supported document subtype. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    ticker: Optional[str] = Field(None, description='Source-supported ticker. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    release_date: Optional[str] = Field(None, description='Source-supported release date. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    results: List[str] = Field(default_factory=list, description='Reported research, financial, vote, or hearing results with metric, units, period, and source-stated context.')
    segment_results: List[str] = Field(default_factory=list, description='List of source-supported segment results. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    adjustments: List[str] = Field(default_factory=list, description='List of source-supported adjustments. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    reconciliations: List[str] = Field(default_factory=list, description='List of source-supported reconciliations. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    performance_drivers: List[str] = Field(default_factory=list, description='List of source-supported performance drivers. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    guidance: List[str] = Field(default_factory=list, description='Issuer forecast or outlook with metric, range, period, conditions, and reporting basis. Do not treat guidance as actual results.')
    guidance_changes: List[str] = Field(default_factory=list, description='Change to prior guidance with affected metric, direction, prior/new values, period, and stated reason when available.')
    guidance_assumptions: List[str] = Field(default_factory=list, description='List of source-supported guidance assumptions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    capital_allocation: List[str] = Field(default_factory=list, description='List of source-supported capital allocation. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    qna: List[str] = Field(default_factory=list, description='List of source-supported qna. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    consensus_comparisons: List[str] = Field(default_factory=list, description='List of source-supported consensus comparisons. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    next_quarter_guidance: List[str] = Field(default_factory=list, description='List of source-supported next quarter guidance. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    full_year_guidance_update: Optional[str] = Field(None, description='Source-supported full year guidance update. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    guidance_tone: Optional[str] = Field(None, description='Source-supported guidance tone. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    key_segments_performance: List[str] = Field(default_factory=list, description='List of source-supported key segments performance. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    strategic_priorities: List[str] = Field(default_factory=list, description='List of source-supported strategic priorities. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    risks_updated: List[str] = Field(default_factory=list, description='List of source-supported risks updated. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    management_tone: Optional[str] = Field(None, description='Source-supported management tone. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    qna_hot_topics: List[str] = Field(default_factory=list, description='List of source-supported qna hot topics. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    call_sentiment_score: Optional[str] = Field(None, description='Source-supported call sentiment score. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    revenue_beat_miss_pct: Optional[str] = Field(None, description='Reported revenue beat or miss versus the named consensus source; omit if the source does not state the comparison.')
    eps_beat_miss_pct: Optional[str] = Field(None, description='Reported EPS beat or miss versus the named consensus source; omit if the source does not state the comparison.')
    eps_adjusted: Optional[str] = Field(None, description='Reported adjusted or non-GAAP EPS with period and basis. Do not substitute GAAP EPS.')
    gross_margin_pct: Optional[str] = Field(None, description='Reported gross margin percentage with period and adjusted or GAAP/IFRS basis.')
    operating_margin_pct: Optional[str] = Field(None, description='Reported operating margin percentage with period and adjusted or GAAP/IFRS basis.')
    free_cash_flow: Optional[str] = Field(None, description='Reported free cash flow in the stated source currency and scale; do not calculate it unless the source reports it.')


class SECFilingExtraction(FinancialReportExtraction):
    """Flat schema for `SECFiling` content."""
    form_type: Optional[str] = Field(None, description='Source-supported form type. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    accession_number: Optional[str] = Field(None, description='Source-supported accession number. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    issuer: Optional[str] = Field(None, description='Source-supported issuer. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    cik: Optional[str] = Field(None, description='Source-supported cik. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    filed_at: Optional[str] = Field(None, description='Source-supported filed at. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    event_date: Optional[str] = Field(None, description='Source-supported event date. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    amends: Optional[str] = Field(None, description='Source-supported amends. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    amendment_flag: Optional[bool] = Field(None, description='Whether the filing explicitly identifies itself as an amendment. Omit if the source does not state this.')
    filers: List[str] = Field(default_factory=list, description='List of source-supported filers. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    filing_items: List[str] = Field(default_factory=list, description='List of source-supported filing items. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    material_disclosures: List[str] = Field(default_factory=list, description='List of source-supported material disclosures. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    exhibits: List[str] = Field(default_factory=list, description='List of source-supported exhibits. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    business_overview: List[str] = Field(default_factory=list, description='List of source-supported business overview. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    controls: List[str] = Field(default_factory=list, description='List of source-supported controls. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    legal_proceedings: List[str] = Field(default_factory=list, description='List of source-supported legal proceedings. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    critical_accounting_estimates: List[str] = Field(default_factory=list, description='List of source-supported critical accounting estimates. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    material_trends_uncertainties: List[str] = Field(default_factory=list, description='List of source-supported material trends uncertainties. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    current_report_events: List[str] = Field(default_factory=list, description='List of source-supported current report events. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    offering: List[str] = Field(default_factory=list, description='List of source-supported offering. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    ownership: List[str] = Field(default_factory=list, description='List of source-supported ownership. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    governance: List[str] = Field(default_factory=list, description='List of source-supported governance. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    material_event_description: Optional[str] = Field(None, description='Source-supported material event description. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    material_events_8k: List[str] = Field(default_factory=list, description='List of source-supported material events 8k. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class PressReleaseExtraction(FlatExtraction):
    """Flat schema for `PressRelease` content."""
    issuer: Optional[str] = Field(None, description='Source-supported issuer. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    release_date: Optional[str] = Field(None, description='Source-supported release date. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    announcement_type: Optional[str] = Field(None, description='Source-supported announcement type. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    headline_claim: Optional[str] = Field(None, description='Source-supported headline claim. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    announced_actions: List[str] = Field(default_factory=list, description='List of source-supported announced actions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    parties: List[str] = Field(default_factory=list, description='List of source-supported parties. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    terms: List[str] = Field(default_factory=list, description='List of source-supported terms. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    metrics: List[str] = Field(default_factory=list, description='List of source-supported metrics. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    availability: List[str] = Field(default_factory=list, description='List of source-supported availability. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    milestones: List[str] = Field(default_factory=list, description='List of source-supported milestones. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    conditions: List[str] = Field(default_factory=list, description='List of source-supported conditions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    next_steps: List[str] = Field(default_factory=list, description='List of source-supported next steps. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    quotations: List[str] = Field(default_factory=list, description='List of source-supported quotations. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    claims: List[str] = Field(default_factory=list, description='Attributed claims, causes of action, or assertions. Preserve the speaker or claimant and stated uncertainty.')
    resources: List[str] = Field(default_factory=list, description='List of source-supported resources. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    forward_looking_statements: List[str] = Field(default_factory=list, description='List of source-supported forward looking statements. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    media_contact: List[str] = Field(default_factory=list, description='List of source-supported media contact. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class OfficialStatementExtraction(FlatExtraction):
    """Flat schema for `OfficialStatement` content."""
    issuing_body: Optional[str] = Field(None, description='Source-supported issuing body. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    speaker: Optional[str] = Field(None, description='Source-supported speaker. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    speaker_capacity: Optional[str] = Field(None, description='Source-supported speaker capacity. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    issued_at: Optional[str] = Field(None, description='Source-supported issued at. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    subject: Optional[str] = Field(None, description='Source-supported subject. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    occasion: Optional[str] = Field(None, description='Source-supported occasion. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    audience: List[str] = Field(default_factory=list, description='List of source-supported audience. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    positions: List[str] = Field(default_factory=list, description='List of source-supported positions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    claims: List[str] = Field(default_factory=list, description='Attributed claims, causes of action, or assertions. Preserve the speaker or claimant and stated uncertainty.')
    rationale: List[str] = Field(default_factory=list, description='List of source-supported rationale. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    announced_actions: List[str] = Field(default_factory=list, description='List of source-supported announced actions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    commitments: List[str] = Field(default_factory=list, description='List of source-supported commitments. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    requests: List[str] = Field(default_factory=list, description='List of source-supported requests. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    scope: List[str] = Field(default_factory=list, description='List of source-supported scope. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    conditions: List[str] = Field(default_factory=list, description='List of source-supported conditions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    dates: List[str] = Field(default_factory=list, description='List of source-supported dates. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    cited_authority: List[str] = Field(default_factory=list, description='List of source-supported cited authority. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    references: List[str] = Field(default_factory=list, description='List of source-supported references. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class EnforcementActionExtraction(FlatExtraction):
    """Flat schema for `EnforcementAction` content."""
    authority: Optional[str] = Field(None, description='Source-supported authority. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    case_id: Optional[str] = Field(None, description='Source-supported case id. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    action_type: Optional[str] = Field(None, description='Source-supported action type. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    stage: Optional[str] = Field(None, description='Source-supported stage. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    conduct_period: Optional[str] = Field(None, description='Source-supported conduct period. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    appeal_status: Optional[str] = Field(None, description='Source-supported appeal status. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    respondents: List[str] = Field(default_factory=list, description='List of source-supported respondents. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    other_parties: List[str] = Field(default_factory=list, description='List of source-supported other parties. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    allegations: List[str] = Field(default_factory=list, description='Attributed alleged conduct or facts. Preserve the allegation status; do not present allegations as findings.')
    cited_provisions: List[str] = Field(default_factory=list, description='List of source-supported cited provisions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    findings: List[str] = Field(default_factory=list, description='Findings expressly made by the issuing authority or court. Do not include allegations or party arguments.')
    admission_terms: List[str] = Field(default_factory=list, description='List of source-supported admission terms. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    disposition: List[str] = Field(default_factory=list, description='Source-stated procedural outcome, settlement result, or court disposition; distinguish final from pending status.')
    penalties: List[str] = Field(default_factory=list, description='Final or proposed sanctions with authority, responsible party, amount, currency, and status when stated. Distinguish requested from imposed.')
    restitution: List[str] = Field(default_factory=list, description='List of source-supported restitution. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    disgorgement: List[str] = Field(default_factory=list, description='List of source-supported disgorgement. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    restrictions: List[str] = Field(default_factory=list, description='List of source-supported restrictions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    remediation: List[str] = Field(default_factory=list, description='List of source-supported remediation. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    monitoring: List[str] = Field(default_factory=list, description='List of source-supported monitoring. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    deadlines: List[str] = Field(default_factory=list, description='Source-stated deadline with required action, responsible party, trigger, timezone, and date precision when available.')


class LegislativeBillExtraction(FlatExtraction):
    """Flat schema for `LegislativeBill` content."""
    bill_id: Optional[str] = Field(None, description='Source-supported bill id. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    title: Optional[str] = Field(None, description='Source-supported title. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    legislature: Optional[str] = Field(None, description='Source-supported legislature. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    session: Optional[str] = Field(None, description='Source-supported session. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    chamber: Optional[str] = Field(None, description='Source-supported chamber. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    version_date: Optional[str] = Field(None, description='Source-supported version date. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    status: Optional[str] = Field(None, description='Source-supported status. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    sponsors: List[str] = Field(default_factory=list, description='List of source-supported sponsors. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    cosponsors: List[str] = Field(default_factory=list, description='List of source-supported cosponsors. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    committees: List[str] = Field(default_factory=list, description='List of source-supported committees. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    purpose: List[str] = Field(default_factory=list, description='List of source-supported purpose. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    provisions: List[str] = Field(default_factory=list, description='Operative provisions or proposed changes with section, subject, affected parties, and stated effect.')
    affected_laws: List[str] = Field(default_factory=list, description='List of source-supported affected laws. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    amendments: List[str] = Field(default_factory=list, description='List of source-supported amendments. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    covered_entities: List[str] = Field(default_factory=list, description='List of source-supported covered entities. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    obligations: List[str] = Field(default_factory=list, description="Duties imposed or proposed, formatted with responsible party, required action, trigger, deadline, conditions, and exceptions. Preserve 'must' versus 'may'.")
    rights: List[str] = Field(default_factory=list, description='Rights, permissions, or entitlements with holder, scope, conditions, and exceptions when stated.')
    exceptions: List[str] = Field(default_factory=list, description='Explicit exemptions, carve-outs, limitations, or conditions that qualify a rule, right, duty, or term.')
    enforcement: List[str] = Field(default_factory=list, description='List of source-supported enforcement. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    proposed_effective_dates: List[str] = Field(default_factory=list, description='List of source-supported proposed effective dates. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    implementation: List[str] = Field(default_factory=list, description='List of source-supported implementation. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    fiscal_estimates: List[str] = Field(default_factory=list, description='List of source-supported fiscal estimates. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    actions: List[str] = Field(default_factory=list, description='List of source-supported actions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    votes: List[str] = Field(default_factory=list, description='List of source-supported votes. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    next_steps: List[str] = Field(default_factory=list, description='List of source-supported next steps. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class LegislativeProposalExtraction(FlatExtraction):
    """Flat schema for `LegislativeProposal` content."""
    title: Optional[str] = Field(None, description='Source-supported title. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    proposal_form: Optional[str] = Field(None, description='Source-supported proposal form. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    proponents: List[str] = Field(default_factory=list, description='List of source-supported proponents. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    problem: List[str] = Field(default_factory=list, description='List of source-supported problem. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    objectives: List[str] = Field(default_factory=list, description='List of source-supported objectives. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    proposed_measures: List[str] = Field(default_factory=list, description='List of source-supported proposed measures. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    affected_groups: List[str] = Field(default_factory=list, description='List of source-supported affected groups. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    proposed_rights: List[str] = Field(default_factory=list, description='List of source-supported proposed rights. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    proposed_obligations: List[str] = Field(default_factory=list, description='List of source-supported proposed obligations. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    exceptions: List[str] = Field(default_factory=list, description='Explicit exemptions, carve-outs, limitations, or conditions that qualify a rule, right, duty, or term.')
    implementation_options: List[str] = Field(default_factory=list, description='List of source-supported implementation options. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    funding: List[str] = Field(default_factory=list, description='List of source-supported funding. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    costs: List[str] = Field(default_factory=list, description='List of source-supported costs. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    alternatives: List[str] = Field(default_factory=list, description='List of source-supported alternatives. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    consultation: List[str] = Field(default_factory=list, description='List of source-supported consultation. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    support: List[str] = Field(default_factory=list, description='List of source-supported support. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    opposition: List[str] = Field(default_factory=list, description='List of source-supported opposition. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    next_steps: List[str] = Field(default_factory=list, description='List of source-supported next steps. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    related_bills: List[str] = Field(default_factory=list, description='List of source-supported related bills. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class EnactedLawExtraction(FlatExtraction):
    """Flat schema for `EnactedLaw` content."""
    law_id: Optional[str] = Field(None, description='Source-supported law id. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    citation: Optional[str] = Field(None, description='Source-supported citation. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    title: Optional[str] = Field(None, description='Source-supported title. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    enacting_body: Optional[str] = Field(None, description='Source-supported enacting body. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    enacted_date: Optional[str] = Field(None, description='Source-supported enacted date. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    sunset: Optional[str] = Field(None, description='Source-supported sunset. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    effective_dates: List[str] = Field(default_factory=list, description='Effective or commencement dates with the provision or condition they apply to. Keep distinct dates as separate items.')
    commencement_conditions: List[str] = Field(default_factory=list, description='List of source-supported commencement conditions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    purpose: List[str] = Field(default_factory=list, description='List of source-supported purpose. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    provisions: List[str] = Field(default_factory=list, description='Operative provisions or proposed changes with section, subject, affected parties, and stated effect.')
    definitions: List[str] = Field(default_factory=list, description='Defined terms and their source-stated meaning, including qualifying conditions or cross-references.')
    scope: List[str] = Field(default_factory=list, description='List of source-supported scope. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    rights: List[str] = Field(default_factory=list, description='Rights, permissions, or entitlements with holder, scope, conditions, and exceptions when stated.')
    obligations: List[str] = Field(default_factory=list, description="Duties imposed or proposed, formatted with responsible party, required action, trigger, deadline, conditions, and exceptions. Preserve 'must' versus 'may'.")
    prohibitions: List[str] = Field(default_factory=list, description='List of source-supported prohibitions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    exceptions: List[str] = Field(default_factory=list, description='Explicit exemptions, carve-outs, limitations, or conditions that qualify a rule, right, duty, or term.')
    enforcement: List[str] = Field(default_factory=list, description='List of source-supported enforcement. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    penalties: List[str] = Field(default_factory=list, description='Final or proposed sanctions with authority, responsible party, amount, currency, and status when stated. Distinguish requested from imposed.')
    remedies: List[str] = Field(default_factory=list, description='List of source-supported remedies. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    delegated_powers: List[str] = Field(default_factory=list, description='List of source-supported delegated powers. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    amendments: List[str] = Field(default_factory=list, description='List of source-supported amendments. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    repeals: List[str] = Field(default_factory=list, description='List of source-supported repeals. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    transitional_rules: List[str] = Field(default_factory=list, description='List of source-supported transitional rules. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    appropriations: List[str] = Field(default_factory=list, description='List of source-supported appropriations. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class RegulationExtraction(FlatExtraction):
    """Flat schema for `Regulation` content."""
    title: Optional[str] = Field(None, description='Source-supported title. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    citation: Optional[str] = Field(None, description='Source-supported citation. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    rule_id: Optional[str] = Field(None, description='Source-supported rule id. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    agency: Optional[str] = Field(None, description='Source-supported agency. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    status: Optional[str] = Field(None, description='Source-supported status. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    authority: List[str] = Field(default_factory=list, description='List of source-supported authority. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    scope: List[str] = Field(default_factory=list, description='List of source-supported scope. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    regulated_entities: List[str] = Field(default_factory=list, description='List of source-supported regulated entities. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    definitions: List[str] = Field(default_factory=list, description='Defined terms and their source-stated meaning, including qualifying conditions or cross-references.')
    thresholds: List[str] = Field(default_factory=list, description='List of source-supported thresholds. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    exemptions: List[str] = Field(default_factory=list, description='List of source-supported exemptions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    requirements: List[str] = Field(default_factory=list, description='List of source-supported requirements. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    prohibitions: List[str] = Field(default_factory=list, description='List of source-supported prohibitions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    standards: List[str] = Field(default_factory=list, description='List of source-supported standards. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    permissions: List[str] = Field(default_factory=list, description='List of source-supported permissions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    reporting: List[str] = Field(default_factory=list, description='List of source-supported reporting. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    recordkeeping: List[str] = Field(default_factory=list, description='List of source-supported recordkeeping. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    licensing: List[str] = Field(default_factory=list, description='List of source-supported licensing. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    procedures: List[str] = Field(default_factory=list, description='List of source-supported procedures. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    effective_dates: List[str] = Field(default_factory=list, description='Effective or commencement dates with the provision or condition they apply to. Keep distinct dates as separate items.')
    compliance_dates: List[str] = Field(default_factory=list, description='Compliance deadlines with the affected requirement and regulated party. Do not confuse with effective dates.')
    phase_in: List[str] = Field(default_factory=list, description='List of source-supported phase in. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    transitional_rules: List[str] = Field(default_factory=list, description='List of source-supported transitional rules. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    enforcement: List[str] = Field(default_factory=list, description='List of source-supported enforcement. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    penalties: List[str] = Field(default_factory=list, description='Final or proposed sanctions with authority, responsible party, amount, currency, and status when stated. Distinguish requested from imposed.')
    amendments: List[str] = Field(default_factory=list, description='List of source-supported amendments. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    incorporated_materials: List[str] = Field(default_factory=list, description='List of source-supported incorporated materials. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class RulemakingNoticeExtraction(FlatExtraction):
    """Flat schema for `RulemakingNotice` content."""
    agency: Optional[str] = Field(None, description='Source-supported agency. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    docket_id: Optional[str] = Field(None, description='Source-supported docket id. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    rule_id: Optional[str] = Field(None, description='Source-supported rule id. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    notice_type: Optional[str] = Field(None, description='Source-supported notice type. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    subject: Optional[str] = Field(None, description='Source-supported subject. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    submission_channel: Optional[str] = Field(None, description='Source-supported submission channel. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    authority: List[str] = Field(default_factory=list, description='List of source-supported authority. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    proposed_changes: List[str] = Field(default_factory=list, description='List of source-supported proposed changes. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    affected_rules: List[str] = Field(default_factory=list, description='List of source-supported affected rules. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    affected_groups: List[str] = Field(default_factory=list, description='List of source-supported affected groups. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    alternatives: List[str] = Field(default_factory=list, description='List of source-supported alternatives. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    analyses: List[str] = Field(default_factory=list, description='List of source-supported analyses. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    questions_for_comment: List[str] = Field(default_factory=list, description='List of source-supported questions for comment. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    submission_requirements: List[str] = Field(default_factory=list, description='List of source-supported submission requirements. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    comment_deadlines: List[str] = Field(default_factory=list, description='List of source-supported comment deadlines. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    hearings: List[str] = Field(default_factory=list, description='List of source-supported hearings. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    effective_dates: List[str] = Field(default_factory=list, description='Effective or commencement dates with the provision or condition they apply to. Keep distinct dates as separate items.')
    contact: List[str] = Field(default_factory=list, description='List of source-supported contact. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class CourtOpinionExtraction(FlatExtraction):
    """Flat schema for `CourtOpinion` content."""
    case_name: Optional[str] = Field(None, description='Source-supported case name. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    case_number: Optional[str] = Field(None, description='Source-supported case number. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    citation: Optional[str] = Field(None, description='Source-supported citation. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    court: Optional[str] = Field(None, description='Source-supported court. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    decision_date: Optional[str] = Field(None, description='Source-supported decision date. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    opinion_author: Optional[str] = Field(None, description='Source-supported opinion author. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    opinion_type: Optional[str] = Field(None, description='Source-supported opinion type. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    publication_status: Optional[str] = Field(None, description='Source-supported publication status. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    judges: List[str] = Field(default_factory=list, description='List of source-supported judges. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    parties: List[str] = Field(default_factory=list, description='List of source-supported parties. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    procedural_posture: List[str] = Field(default_factory=list, description='List of source-supported procedural posture. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    material_facts: List[str] = Field(default_factory=list, description='List of source-supported material facts. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    issues: List[str] = Field(default_factory=list, description='List of source-supported issues. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    holdings: List[str] = Field(default_factory=list, description='Questions decided and holdings of the court. Exclude party arguments and separate opinions unless identified as such.')
    reasoning: List[str] = Field(default_factory=list, description='List of source-supported reasoning. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    standards_of_review: List[str] = Field(default_factory=list, description='List of source-supported standards of review. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    cited_authorities: List[str] = Field(default_factory=list, description='List of source-supported cited authorities. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    separate_opinions: List[str] = Field(default_factory=list, description='List of source-supported separate opinions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    disposition: List[str] = Field(default_factory=list, description='Source-stated procedural outcome, settlement result, or court disposition; distinguish final from pending status.')
    relief: List[str] = Field(default_factory=list, description='List of source-supported relief. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    remand_instructions: List[str] = Field(default_factory=list, description='List of source-supported remand instructions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    scope_limits: List[str] = Field(default_factory=list, description='List of source-supported scope limits. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class LawsuitExtraction(FlatExtraction):
    """Flat schema for `Lawsuit` content."""
    case_name: Optional[str] = Field(None, description='Source-supported case name. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    case_number: Optional[str] = Field(None, description='Source-supported case number. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    court: Optional[str] = Field(None, description='Source-supported court. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    document_type: Optional[str] = Field(None, description='Source-supported document type. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    filed_at: Optional[str] = Field(None, description='Source-supported filed at. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    status: Optional[str] = Field(None, description='Source-supported status. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    parties: List[str] = Field(default_factory=list, description='List of source-supported parties. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    counsel: List[str] = Field(default_factory=list, description='List of source-supported counsel. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    party_roles: List[str] = Field(default_factory=list, description='List of source-supported party roles. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    allegations: List[str] = Field(default_factory=list, description='Attributed alleged conduct or facts. Preserve the allegation status; do not present allegations as findings.')
    factual_chronology: List[str] = Field(default_factory=list, description='List of source-supported factual chronology. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    claims: List[str] = Field(default_factory=list, description='Attributed claims, causes of action, or assertions. Preserve the speaker or claimant and stated uncertainty.')
    legal_bases: List[str] = Field(default_factory=list, description='List of source-supported legal bases. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    defenses: List[str] = Field(default_factory=list, description='List of source-supported defenses. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    responses: List[str] = Field(default_factory=list, description='List of source-supported responses. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    contested_issues: List[str] = Field(default_factory=list, description='List of source-supported contested issues. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    requested_relief: List[str] = Field(default_factory=list, description='Relief requested by a party, including type, amount, and conditions when stated. Requested relief is not awarded relief.')
    claimed_damages: List[str] = Field(default_factory=list, description='List of source-supported claimed damages. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    class_scope: List[str] = Field(default_factory=list, description='List of source-supported class scope. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    procedural_events: List[str] = Field(default_factory=list, description='List of source-supported procedural events. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    motions: List[str] = Field(default_factory=list, description='List of source-supported motions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    orders: List[str] = Field(default_factory=list, description='List of source-supported orders. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    deadlines: List[str] = Field(default_factory=list, description='Source-stated deadline with required action, responsible party, trigger, timezone, and date precision when available.')
    settlement: List[str] = Field(default_factory=list, description='List of source-supported settlement. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    disposition: List[str] = Field(default_factory=list, description='Source-stated procedural outcome, settlement result, or court disposition; distinguish final from pending status.')
    unresolved_matters: List[str] = Field(default_factory=list, description='List of source-supported unresolved matters. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class GovernmentReportExtraction(FlatExtraction):
    """Flat schema for `GovernmentReport` content."""
    report_id: Optional[str] = Field(None, description='Source-supported report id. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    agency: Optional[str] = Field(None, description='Source-supported agency. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    report_type: Optional[str] = Field(None, description='Source-supported report type. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    covered_period: Optional[str] = Field(None, description='Source-supported covered period. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    sample: Optional[str] = Field(None, description='Source-supported sample. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    follow_up_status: Optional[str] = Field(None, description='Source-supported follow up status. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    mandate: List[str] = Field(default_factory=list, description='List of source-supported mandate. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    questions: List[str] = Field(default_factory=list, description='List of source-supported questions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    scope: List[str] = Field(default_factory=list, description='List of source-supported scope. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    geography: List[str] = Field(default_factory=list, description='List of source-supported geography. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    subjects: List[str] = Field(default_factory=list, description='List of source-supported subjects. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    methods: List[str] = Field(default_factory=list, description='Methods, procedures, or methodology stated in the source, including relevant design, data, or analytical approach.')
    data_sources: List[str] = Field(default_factory=list, description='List of source-supported data sources. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    limitations: List[str] = Field(default_factory=list, description='Source-stated limitations, caveats, uncertainty, or constraints. Do not infer limitations.')
    findings: List[str] = Field(default_factory=list, description='Findings expressly made by the issuing authority or court. Do not include allegations or party arguments.')
    metrics: List[str] = Field(default_factory=list, description='List of source-supported metrics. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    conclusions: List[str] = Field(default_factory=list, description='List of source-supported conclusions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    recommendations: List[str] = Field(default_factory=list, description='List of source-supported recommendations. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    responsible_bodies: List[str] = Field(default_factory=list, description='List of source-supported responsible bodies. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    deadlines: List[str] = Field(default_factory=list, description='Source-stated deadline with required action, responsible party, trigger, timezone, and date precision when available.')
    agency_responses: List[str] = Field(default_factory=list, description='List of source-supported agency responses. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    disagreements: List[str] = Field(default_factory=list, description='List of source-supported disagreements. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class BudgetDocumentExtraction(FlatExtraction):
    """Flat schema for `BudgetDocument` content."""
    government: Optional[str] = Field(None, description='Source-supported government. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    document_type: Optional[str] = Field(None, description='Source-supported document type. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    fiscal_period: Optional[str] = Field(None, description='Source-supported fiscal period. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    budget_status: Optional[str] = Field(None, description='Source-supported budget status. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    currency: Optional[str] = Field(None, description='Presentation currency explicitly stated by the source.')
    currency_scale: Optional[str] = Field(None, description='Presentation scale such as units, thousands, millions, or billions, exactly as stated by the source.')
    accounting_basis: Optional[str] = Field(None, description='Source-supported accounting basis. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    funds: List[str] = Field(default_factory=list, description='List of source-supported funds. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    programs: List[str] = Field(default_factory=list, description='List of source-supported programs. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    revenues: List[str] = Field(default_factory=list, description='List of source-supported revenues. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    expenditures: List[str] = Field(default_factory=list, description='List of source-supported expenditures. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    appropriations: List[str] = Field(default_factory=list, description='List of source-supported appropriations. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    allocations: List[str] = Field(default_factory=list, description='List of source-supported allocations. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    obligations: List[str] = Field(default_factory=list, description="Duties imposed or proposed, formatted with responsible party, required action, trigger, deadline, conditions, and exceptions. Preserve 'must' versus 'may'.")
    outlays: List[str] = Field(default_factory=list, description='List of source-supported outlays. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    balances: List[str] = Field(default_factory=list, description='List of source-supported balances. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    financing: List[str] = Field(default_factory=list, description='List of source-supported financing. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    comparisons: List[str] = Field(default_factory=list, description='List of source-supported comparisons. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    policy_changes: List[str] = Field(default_factory=list, description='List of source-supported policy changes. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    assumptions: List[str] = Field(default_factory=list, description='List of source-supported assumptions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    forecasts: List[str] = Field(default_factory=list, description='List of source-supported forecasts. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    restrictions: List[str] = Field(default_factory=list, description='List of source-supported restrictions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    earmarks: List[str] = Field(default_factory=list, description='List of source-supported earmarks. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    conditions: List[str] = Field(default_factory=list, description='List of source-supported conditions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    availability_periods: List[str] = Field(default_factory=list, description='List of source-supported availability periods. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class LegislativeRecordExtraction(FlatExtraction):
    """Flat schema for `LegislativeRecord` content."""
    legislature: Optional[str] = Field(None, description='Source-supported legislature. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    chamber: Optional[str] = Field(None, description='Source-supported chamber. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    session: Optional[str] = Field(None, description='Source-supported session. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    sitting_date: Optional[str] = Field(None, description='Source-supported sitting date. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    record_id: Optional[str] = Field(None, description='Source-supported record id. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    record_type: Optional[str] = Field(None, description='Source-supported record type. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    agenda_items: List[str] = Field(default_factory=list, description='List of source-supported agenda items. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    bills: List[str] = Field(default_factory=list, description='List of source-supported bills. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    motions: List[str] = Field(default_factory=list, description='List of source-supported motions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    amendments: List[str] = Field(default_factory=list, description='List of source-supported amendments. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    speakers: List[str] = Field(default_factory=list, description='List of source-supported speakers. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    remarks: List[str] = Field(default_factory=list, description='List of source-supported remarks. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    positions: List[str] = Field(default_factory=list, description='List of source-supported positions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    procedural_actions: List[str] = Field(default_factory=list, description='List of source-supported procedural actions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    rulings: List[str] = Field(default_factory=list, description='List of source-supported rulings. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    referrals: List[str] = Field(default_factory=list, description='List of source-supported referrals. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    votes: List[str] = Field(default_factory=list, description='List of source-supported votes. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    results: List[str] = Field(default_factory=list, description='Reported research, financial, vote, or hearing results with metric, units, period, and source-stated context.')
    attendance: List[str] = Field(default_factory=list, description='List of source-supported attendance. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    next_steps: List[str] = Field(default_factory=list, description='List of source-supported next steps. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    referenced_materials: List[str] = Field(default_factory=list, description='List of source-supported referenced materials. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class HearingExtraction(FlatExtraction):
    """Flat schema for `Hearing` content."""
    hearing_id: Optional[str] = Field(None, description='Source-supported hearing id. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    convening_body: Optional[str] = Field(None, description='Source-supported convening body. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    jurisdiction: Optional[str] = Field(None, description='Source-supported jurisdiction. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    hearing_type: Optional[str] = Field(None, description='Source-supported hearing type. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    hearing_date: Optional[str] = Field(None, description='Source-supported hearing date. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    topics: List[str] = Field(default_factory=list, description='List of source-supported topics. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    related_matters: List[str] = Field(default_factory=list, description='List of source-supported related matters. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    participants: List[str] = Field(default_factory=list, description='List of source-supported participants. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    participant_roles: List[str] = Field(default_factory=list, description='List of source-supported participant roles. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    testimony: List[str] = Field(default_factory=list, description='Witness or participant testimony attributed to the speaker, including available timestamp or page reference.')
    claims: List[str] = Field(default_factory=list, description='Attributed claims, causes of action, or assertions. Preserve the speaker or claimant and stated uncertainty.')
    submitted_evidence: List[str] = Field(default_factory=list, description='List of source-supported submitted evidence. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    questions_and_answers: List[str] = Field(default_factory=list, description='List of source-supported questions and answers. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    disagreements: List[str] = Field(default_factory=list, description='List of source-supported disagreements. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    unanswered_questions: List[str] = Field(default_factory=list, description='List of source-supported unanswered questions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    commitments: List[str] = Field(default_factory=list, description='List of source-supported commitments. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    requested_materials: List[str] = Field(default_factory=list, description='List of source-supported requested materials. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    deadlines: List[str] = Field(default_factory=list, description='Source-stated deadline with required action, responsible party, trigger, timezone, and date precision when available.')
    recorded_actions: List[str] = Field(default_factory=list, description='List of source-supported recorded actions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class ResearchPaperExtraction(FlatExtraction):
    """Flat schema for `ResearchPaper` content."""
    venue: Optional[str] = Field(None, description='Source-supported venue. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    publication_status: Optional[str] = Field(None, description='Source-supported publication status. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    study_design: Optional[str] = Field(None, description='Source-supported study design. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    sample: Optional[str] = Field(None, description='Source-supported sample. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    setting: Optional[str] = Field(None, description='Source-supported setting. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    authors: List[str] = Field(default_factory=list, description='List of source-supported authors. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    affiliations: List[str] = Field(default_factory=list, description='List of source-supported affiliations. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    identifiers: List[str] = Field(default_factory=list, description='List of source-supported identifiers. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    research_question: List[str] = Field(default_factory=list, description='List of source-supported research question. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    hypotheses: List[str] = Field(default_factory=list, description='List of source-supported hypotheses. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    contributions: List[str] = Field(default_factory=list, description='List of source-supported contributions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    methods: List[str] = Field(default_factory=list, description='Methods, procedures, or methodology stated in the source, including relevant design, data, or analytical approach.')
    data: List[str] = Field(default_factory=list, description='List of source-supported data. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    interventions: List[str] = Field(default_factory=list, description='List of source-supported interventions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    comparators: List[str] = Field(default_factory=list, description='List of source-supported comparators. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    baselines: List[str] = Field(default_factory=list, description='List of source-supported baselines. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    evaluation_protocol: List[str] = Field(default_factory=list, description='List of source-supported evaluation protocol. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    results: List[str] = Field(default_factory=list, description='Reported research, financial, vote, or hearing results with metric, units, period, and source-stated context.')
    uncertainty: List[str] = Field(default_factory=list, description='List of source-supported uncertainty. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    statistical_tests: List[str] = Field(default_factory=list, description='List of source-supported statistical tests. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    conclusions: List[str] = Field(default_factory=list, description='List of source-supported conclusions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    limitations: List[str] = Field(default_factory=list, description='Source-stated limitations, caveats, uncertainty, or constraints. Do not infer limitations.')
    threats_to_validity: List[str] = Field(default_factory=list, description='List of source-supported threats to validity. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    reproducibility: List[str] = Field(default_factory=list, description='List of source-supported reproducibility. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    ethics: List[str] = Field(default_factory=list, description='List of source-supported ethics. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    funding: List[str] = Field(default_factory=list, description='List of source-supported funding. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    conflicts: List[str] = Field(default_factory=list, description='List of source-supported conflicts. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class WhitepaperExtraction(FlatExtraction):
    """Flat schema for `Whitepaper` content."""
    issuer: Optional[str] = Field(None, description='Source-supported issuer. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    authors: List[str] = Field(default_factory=list, description='List of source-supported authors. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    audience: List[str] = Field(default_factory=list, description='List of source-supported audience. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    purpose: List[str] = Field(default_factory=list, description='List of source-supported purpose. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    problem: List[str] = Field(default_factory=list, description='List of source-supported problem. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    thesis: List[str] = Field(default_factory=list, description='List of source-supported thesis. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    proposed_solution: List[str] = Field(default_factory=list, description='List of source-supported proposed solution. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    architecture: List[str] = Field(default_factory=list, description='List of source-supported architecture. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    requirements: List[str] = Field(default_factory=list, description='List of source-supported requirements. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    assumptions: List[str] = Field(default_factory=list, description='List of source-supported assumptions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    implementation: List[str] = Field(default_factory=list, description='List of source-supported implementation. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    adoption_steps: List[str] = Field(default_factory=list, description='List of source-supported adoption steps. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    claims: List[str] = Field(default_factory=list, description='Attributed claims, causes of action, or assertions. Preserve the speaker or claimant and stated uncertainty.')
    supporting_evidence: List[str] = Field(default_factory=list, description='List of source-supported supporting evidence. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    case_studies: List[str] = Field(default_factory=list, description='List of source-supported case studies. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    comparisons: List[str] = Field(default_factory=list, description='List of source-supported comparisons. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    economics: List[str] = Field(default_factory=list, description='List of source-supported economics. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    risks: List[str] = Field(default_factory=list, description='List of source-supported risks. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    limitations: List[str] = Field(default_factory=list, description='Source-stated limitations, caveats, uncertainty, or constraints. Do not infer limitations.')
    tradeoffs: List[str] = Field(default_factory=list, description='List of source-supported tradeoffs. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    recommendations: List[str] = Field(default_factory=list, description='List of source-supported recommendations. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    roadmap: List[str] = Field(default_factory=list, description='List of source-supported roadmap. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    references: List[str] = Field(default_factory=list, description='List of source-supported references. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    disclosures: List[str] = Field(default_factory=list, description='List of source-supported disclosures. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


class TechnicalDocumentationExtraction(FlatExtraction):
    """Flat schema for `TechnicalDocumentation` content."""
    product: Optional[str] = Field(None, description='Source-supported product. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    component: Optional[str] = Field(None, description='Source-supported component. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    doc_type: Optional[str] = Field(None, description='Source-supported doc type. Preserve exact names, dates, units, conditions, and status; omit if not stated.')
    audience: List[str] = Field(default_factory=list, description='List of source-supported audience. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    purpose: List[str] = Field(default_factory=list, description='List of source-supported purpose. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    concepts: List[str] = Field(default_factory=list, description='List of source-supported concepts. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    prerequisites: List[str] = Field(default_factory=list, description='List of source-supported prerequisites. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    compatibility: List[str] = Field(default_factory=list, description='List of source-supported compatibility. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    setup: List[str] = Field(default_factory=list, description='List of source-supported setup. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    configuration: List[str] = Field(default_factory=list, description='List of source-supported configuration. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    procedures: List[str] = Field(default_factory=list, description='List of source-supported procedures. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    interfaces: List[str] = Field(default_factory=list, description='Documented API, function, endpoint, CLI, or protocol interface with signature, purpose, inputs, and outputs.')
    inputs: List[str] = Field(default_factory=list, description='List of source-supported inputs. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    outputs: List[str] = Field(default_factory=list, description='List of source-supported outputs. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    examples: List[str] = Field(default_factory=list, description='Exact or faithfully preserved source examples, commands, code snippets, or usage cases. Do not invent executable output.')
    expected_results: List[str] = Field(default_factory=list, description='List of source-supported expected results. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    authentication: List[str] = Field(default_factory=list, description='List of source-supported authentication. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    permissions: List[str] = Field(default_factory=list, description='List of source-supported permissions. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    limits: List[str] = Field(default_factory=list, description='List of source-supported limits. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    errors: List[str] = Field(default_factory=list, description='Documented error codes, failure conditions, messages, and recovery guidance.')
    warnings: List[str] = Field(default_factory=list, description='List of source-supported warnings. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    security_notes: List[str] = Field(default_factory=list, description='List of source-supported security notes. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    troubleshooting: List[str] = Field(default_factory=list, description='List of source-supported troubleshooting. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    changes: List[str] = Field(default_factory=list, description='List of source-supported changes. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    deprecations: List[str] = Field(default_factory=list, description='Deprecated feature, version, replacement, deadline, and migration condition stated by the source.')
    migration: List[str] = Field(default_factory=list, description='List of source-supported migration. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')
    references: List[str] = Field(default_factory=list, description='List of source-supported references. Preserve names, roles, dates, units, conditions, and attribution when stated; omit unsupported items.')


# Flat aliases for the existing financial/SEC public schemas.
FlatFinancialDocumentSummary = EarningsReportExtraction
FlatEarningsReportSummary = EarningsReportExtraction
FlatSECFilingSummary = SECFilingExtraction
