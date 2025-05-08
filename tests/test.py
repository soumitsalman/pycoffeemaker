import os, sys
from dotenv import load_dotenv
load_dotenv()
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import logging
logging.basicConfig(level=logging.WARNING, format="%(asctime)s|%(name)s|%(levelname)s|%(message)s|%(source)s|%(num_items)s")
logger = logging.getLogger("app")
logger.setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.simplecollector").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.fullstack").setLevel(logging.INFO)
logging.getLogger("coffeemaker.collectors.collector").setLevel(logging.INFO)
logging.getLogger("jieba").propagate = False
logging.getLogger("coffeemaker.nlp.digestors").propagate = False
logging.getLogger("coffeemaker.nlp.embedders").propagate = False
logging.getLogger("asyncprawcore").propagate = False
logging.getLogger("asyncpraw").propagate = False
logging.getLogger("dammit").propagate = False
logging.getLogger("UnicodeDammit").propagate = False
logging.getLogger("urllib3").propagate = False
logging.getLogger("connectionpool").propagate = False
logging.getLogger("asyncio").propagate = False

import json, re, random
import asyncio
from datetime import datetime
from coffeemaker.pybeansack.mongosack import *
from coffeemaker.pybeansack.models import *
from coffeemaker.collectors import collector
from coffeemaker.orchestrators.utils import log_runtime, END_OF_STREAM
from coffeemaker.nlp import digestors, embedders

os.makedirs(".test", exist_ok=True)

def url_to_filename(url: str) -> str:
    return "./.test/" + re.sub(r'[^a-zA-Z0-9]', '-', url)

def load_json(filename):
    with open(filename, 'r') as file:
        return json.load(file)

def save_markdown(url, markdown):
    filename = url_to_filename(url)+".md"
    with open(filename, 'w') as file:
        file.write(markdown)

def save_json(url, items):
    filename = url_to_filename(url)+".json"
    with open(filename, 'w') as file:
        json.dump(items, file)

def save_models(items: list[Bean|Chatter], file_name: str = None):
    if items:
        with open(f".test/{file_name or items[0].source}.json", 'w') as file:
            json.dump([item.model_dump(exclude_unset=True, exclude_none=True) for item in items], file)
        return ic(len(items))

def _create_fullstack_orchestrator():
    from coffeemaker.orchestrators.fullstack import Orchestrator
    return Orchestrator(
        os.getenv("DB_REMOTE_TEST"),
        os.getenv("DB_LOCAL"),
        os.getenv("DB_NAME"),
        embedder_path=os.getenv("EMBEDDER_PATH"),
        digestor_path=os.getenv("DIGESTOR_PATH"),
        clus_eps=0.5
    )


  
# def test_index_and_augment():
#     sources = [
#         # "https://www.freethink.com/feed/all",
#         # "https://www.testingcatalog.com/rss/",
#         # "https://startupnews.fyi/feed/",
#         # "https://spectrum.ieee.org/customfeeds/feed/all-topics/rss",
#         # "https://dailyhodl.com/feed/",
#         # "https://www.investors.com/feed/",
#         # "https://www.datacenterknowledge.com/rss.xml",
#         # "https://www.gamedeveloper.com/rss.xml",
#         # "https://singularityhub.com/feed/",
#         # "https://www.nextbigfuture.com/feed",        
#         # "https://blog.mozilla.org/attack-and-defense/feed/",
#         # "https://www.nccgroup.com/us/research-blog/feed/",
#         # "https://blog.oversecured.com/feed.xml",
#         # "https://rtx.meta.security/feed.xml",
#         # "https://windows-internals.com/feed/",
#         # "https://secret.club/feed.xml",
#         # "https://research.securitum.com/feed/",
#         "https://feeds.feedburner.com/positiveTechnologiesResearchLab",
#         # "https://microsoftedge.github.io/edgevr/feed.xml",
#         "https://github.blog/tag/github-security-lab/feed/"
#     ]
#     collected = lambda beans: print(f"collected {len(beans)} from {beans[0].source}" if beans else "no beans collected")
#     rssfeed.collect(collected, sources=sources)
#     redditor.collect(collected)
#     # rssfeed.collect(sources=sources, store_func=lambda beans: write_datamodels(orch._index(orch._download(beans[:3]))))
  
# def test_search():
#     query = "profession: pilot"
#     # write_datamodels(ic(orch.remotesack.query_unique_beans(filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "QUERY_BEANS")
#     # write_datamodels(ic(orch.remotesack.text_search_beans(query=query, filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "TEXT_SEARCH")
#     save_models(orch.remotesack.vector_search_beans(query=query, filter=updated_after(3), sort_by=LATEST, projection={K_EMBEDDING: 0, K_ID: 0}), "VECTOR_SEARCH")

# def test_clustering():  
#     def _collect(beans: list[Bean]):
#         ic(len(beans))
#         beans = orch.localsack.not_exists(beans)
#         if beans:
#             beans = orch._index(orch.deep_collect(random.sample(beans, min(3, ic(len(beans))))))
#             orch.localsack.store_beans(beans)
#             ic(orch._cluster([bean.url for bean in beans]))        
    
#     sources = [
#         "https://www.ghacks.net/feed/",
#         "https://thenewstack.io/feed",
#         "https://scitechdaily.com/feed/"
#     ]
#     rssfeed.collect(store_beans=_collect, sources = sources)

# def test_clustering_live(): 
#     orch.cluster_beans()

# def test_trend_ranking():    
#     # redditor.collect(
#     #     store_beans=lambda beans: print(len(beans), "beans collected from", beans[0].source),
#     #     store_chatters=orch._collect_chatters)
#     # ychackernews.collect(
#     #     store_beans=lambda beans: print(len(beans), "beans collected from", beans[0].source),
#     #     store_chatters=orch._collect_chatters)    
#     orch._trend_rank()

def test_collection_and_download():
    from coffeemaker.orchestrators.fullstack import Orchestrator
    async def _run(orch: Orchestrator):
        orch._init_run()
        await orch.run_collections_async("/home/soumitsr/codes/pycoffeemaker/tests/test-sources-1.yaml")
        await orch.download_queue.put(END_OF_STREAM)
        await orch.run_downloading_async()

    orch = _create_fullstack_orchestrator()
    asyncio.run(_run(orch))
    orch.close()

def test_embedder():
    inputs = load_json("/home/soumitsr/codes/pycoffeemaker/tests/digests-for-embedder.json") 
    embedder = embedders.from_path(ic(os.getenv("EMBEDDER_PATH")))
    vecs = embedder([i['digest'] for i in inputs])
    ic(len(vecs), [(len(vec), vec[0]) for vec in vecs])

@log_runtime
def test_digestor():
    inputs = load_json("/home/soumitsr/codes/pycoffeemaker/tests/texts-for-digestor-1.json")
    digestor = digestors.from_path(
        "google/gemma-3-27b-it",
        int(os.getenv("DIGESTOR_CONTEXT_LEN", 8192)), 
        os.getenv("DIGESTOR_BASE_URL"), 
        os.getenv("DIGESTOR_API_KEY"), 
        use_short_digest=lambda text: False)
    [print(resp if resp else "N/A", '\n') for resp in digestor([b['text'] for b in inputs])]

def test_digest_parser():
    responses = [
        "P:AI model training acceleration|Data compression advancements|AIOps framework rollout|Channel sales leadership change|Real-time AI data store|Storage system upgrade|Log data infrastructure replacement|CMO appointment|Sales leadership expansion|File collaboration VP appointment|CRO appointment|RAID integration for AI;E:Alluxio introduces Cache Only Write Mode|Atombeam raises $20M in A+ funding|CloudFabrix rebrands to Fabrix.ai|DDN appoints Wendy Stusrud|GridGain launches GridGain for AI|Hitachi Vantara wins pharmaceutical customer|Hydrolix ships Spark connector for Databricks|MinIO appoints Erik Frieberg|Quobyte expands to New York|Resilio appoints Eric Soffin|Scality appoints Emilio Roman|Xinnor wins customer with BeeGFS integration;D:Atombeam funding: $35M total|$84 patents issued to Atombeam|115 patents pending for Atombeam|HP overlap: Riahi & Ignomirello - 4 years|Read speeds: 29.2 GBps|Write speeds: 25.8 GBps;R:Financial sector (Quobyte expansion);N:Alluxio|Atombeam|CloudFabrix/Fabrix.ai|DDN|GridGain|Hitachi Vantara|Hydrolix|MinIO|Quobyte|Resilio|Scality|Xinnor|Asghar Riahi|Brian Ignomirello|Wendy Stusrud|Erik Frieberg|Emilio Roman|Peter Brennan;C:AI|Storage|Data Management|Cloud Computing|AIOps;S:neutral",

        """P:Public GitHub repository;Saved searches available;Notification & Fork options;Sign-in/Sign-up prompts;Session management alerts
        E:Repository maintenance;User authentication requests;Account switching detected
        D:2025-05-01 18:48:18|1 Fork|7 Stars
        R:GitHub
        N:stanlee000|spellbound|GitHub
        C:Software Engineering|DevOps|Cybersecurity
        S:neutral""",

        "P:Contractors fired from State Department bureau;Bureau programs criticized by authoritarian regimes;Loss of technical expertise impacts program implementation;Focus on civil society in countries with limited US diplomatic presence;Efforts to address forced labor in China|S:negative;E:Dismissal of ~60 contractors;D:$150M-$200M annual bureau budget;R:Russia|China|Iran|North Korea|Venezuela|Cuba|US;N:State Department|National Endowment for Democracy|Uyghur Muslims|China;C:Politics|Human Rights|International Relations|Labor|Technology;S:negative|critical",

        "P:NewExpropriationAct|LandInequality|PropertyRightsDebate|ConstitutionalAlignment|LimitedEffectiveness|LandownerProtection|NilCompensationClause;E:ActPassage2024|RepealsApartheidAct|DebateOverProvisions|FocusOnCompensation|NilCompensationConsideration;D:Act13of2024|Act63of1975Repealed|Section12(3)NilCompensation|LandOnlyForNilCompensation|PublicInterestForNilCompensation|LimitedNilCompensationCircumstances;R:SouthAfrica;N:Zsa-ZsaTemmersBoggenpoel|SouthAfricanGovernment|Landowners|ExpropriatingAuthority;C:Law|LandReform|PropertyRights|Politics;S:neutral|concerned|analytical",

        """P:Challenges to Bill 21 reach Supreme Court|Law restricts religious symbols for public sector employees|Bill 21 enacted with popular support but raises constitutional concerns|Section 33 (notwithstanding clause) shields law from challenges|Unwritten constitutional principles, like minority protection, may limit Section 33 use|Law impacts Muslim women disproportionately, hindering employment & access to services|Court must define limits of Section 33 to ensure Charter rights are meaningful
        E:Supreme Court announces hearing of Bill 21 challenge|Québec government invoked Section 33 & 52 preemptively|Superior Court found Bill 21 has cruel & dehumanizing impact
        D:Bill 21 passed in 2019|Section 33 declaration valid for 5 years, renewable|Examples of past discriminatory laws: Indian Act, Chinese Exclusion Act, Japanese internment
        R:Québec|Canada
        N:Supreme Court of Canada|Bill 21|Section 33 (notwithstanding clause)|Muslim women|Natasha Bakht|Lynda Collins|National Council of Canadian Muslims|Women's Legal Education and Action Fund
        C:Law|Politics|Human Rights|Religion|Constitutional Law|Discrimination
        S:negative|critical|concerned""",

        "P:Burundi faces climate/environmental challenges due to biomass dependence & outdated agriculture|Deforestation increases vulnerability to climate shocks|Government needs balanced “carrot & stick” approach for compliance|Legal framework crucial for effective climate regulation|Regional applicability to East Central African countries;E:Population increase (8M to 12M, 2008-2021) drives demand for firewood/land|Severe droughts (1998-2005: 35% livestock mortality) & floods (2006, 2007) experienced|Loss of 2,350 hectares of natural forest in 2023;D:76% population below poverty line (US$2.15/day, 2023)|Burundi ranks 22nd most vulnerable to climate change|CO₂ emissions <0.02% of global total|90% rural households use three-stone fires|99% households use solid fuels for cooking|<2% access to improved cookstoves|465,000 hectares natural forest (2020);R:Lake Tanganyika region|East Central African region (Uganda, Rwanda, DR Congo, Zambia, CAR, Malawi);N:Burundi|Kikelomo Kila;C:Climate Change|Environmental Policy|Sustainable Development|Law & Regulation;S:concerned|informative ",

        "P:Economic potential of plastic waste|Informal collectors key to goals|Perceptions shaped by socio-cultural factors|Education improves business understanding|Family size impacts feasibility|Religion promotes resource management|Gender influences perception|Age affects motivation|Income drives engagement|Social connections foster collaboration;E:Exploratory survey in Ijebu, Ogun State|Study of 86 participants with 5+ years experience|Highlighting education, family, religion, gender, age, & economic factors|Identifying barriers & opportunities for formalization;D:86 participants surveyed|5+ years experience in plastic waste industry|Age range: Younger (<14) to Adults (33-38);R:Nigeria|Ijebu area of Ogun State;N:Samuel Oludare Awobona (Osun State University)|Islamic teachings (_israf_, _zakat_)|African traditional religions;C:Waste Management|Sustainable Development|Environmental Governance|Economics|Sociology|Gender Studies;S:positive ",

        "P:Addo Elephant Park wetlands endangered|New wetland inventory completed|Wetlands crucial for biodiversity & water sources|Climate change threatens wetland function|Past farming practices impact wetland recovery|Elephant population impacts wetlands; trade-off with conservation|Importance of monitoring & preventing invasive species;E:Inventory of Addo's rivers & wetlands co-authored|Discovery of previously undocumented wetlands (dune-slack, springs, depression wetlands)|Rangers' local knowledge crucial for discovery|National assessment shows wetlands highly threatened|Climate change disrupting weather patterns & water resources;D:Park established 94 years ago|Park size: 155,000 hectares|437 wetlands in Addo Park|600+ elephants in Addo Park|National biodiversity assessment every 6 years|Ramsar Convention recognizes importance of small wetlands;R:Eastern Cape region, South Africa|Addo Elephant National Park|Coastal dunes|Forested gorges (\"kloofs\")|Mountainous plateaus;N:Nancy Job|Dirk Roux|Nicholas Cole (SANParks)|South African National Parks|South African National Biodiversity Institute|Elephants|Rangers;C:Conservation|Ecology|Environmental Science|Climate Change|Biodiversity|Water Resources;S:concerned|informative",

        """P:Trump proposes relocating Palestinians|Ultranationlists seek "Greater Israel"|Shift towards right-wing coalitions in Israel|Religious Zionists combine faith & nationalism|Settler violence destabilizes West Bank;US removes sanctions;Netanyahu's upcoming White House visit ominous|Oslo Accords derailed by assassination & intifada|Netanyahu's longevity enables rightward shift;E:Trump suggests clearing Gaza|Smotrich aims to enact relocation policy|Ben Gvir calls Gaza evacuation "humanitarian";R:Gaza|West Bank ("Judea & Samaria")|Israel|Egypt|Jordan|Lebanon|Syria|Iraq|Saudi Arabia;N:Donald Trump|Bezalel Smotrich|Itamar Ben Gvir|Yitzhak Rabin|Yasser Arafat|Ehud Barak|Benjamin Netanyahu|Yigal Amir|Religious Zionists|Palestinians|Israelis|Pew Research Center|Israel Democracy Institute;C:Politics|International Relations|Israeli-Palestinian Conflict|Religion|Ideology|US Foreign Policy;D:2024 Pew Research: 45% Israelis unfavorable view of Ben-Gvir|2024 Pew Research: 41% Israelis unfavorable view of Smotrich|2022 Elections: Religious Zionist party won 10.84%|1995: Oslo II support at 72% in Israel|1995: Rabin assassinated|2022: Religious Zionists hold balance of power in Knesset|2024: Religious Zionists represent 22% of Jewish population in Israel|S:negative|critical """,

        "P:Australia considering social media ban for under-16s;Adversarial framing of tech debate hinders solutions;Students express awareness of excessive app use & desire for self-discipline;Education should foster critical reflection on digital world;Banning tech may increase its allure;Content curation by tech companies is key issue|E:Australian government proposes social media ban;Research project collaborates with students, tech companies, policymakers, & ethicists;Callaghan's 1976 Ruskin Speech questioned education's focus;Rowland claims need for immediate action on content exposure|D:Students use apps ~8 hours/day;Students report difficulty reading >5 pages;UK education shifted from 'how to live' to 'how to make a living';Survey prioritizes online safety & harm prevention|R:Australia;UK|N:Anthony Albanese;Michelle Rowland;James Callaghan;Bernard Crick;Lawrence Stenhouse;James Conroy|C:Social Media;Education;Digital Citizenship;Technology Policy;Online Safety|S:concerned;critical;nuanced ",

        "P:PM exposure linked to decreased melanoma risk|Study shows association, not causation|Benefits of clean air outweigh potential melanoma risk reduction|Further research needed to confirm findings;E:New study finds potential link between air pollution & melanoma|Researchers acknowledge study limitations;D:PM10 & PM2.5 are particulate matter sizes|Long-term PM exposure causes millions of premature deaths annually|UV radiation is primary risk factor for melanoma;R:Italy (study location);N:Particulate Matter (PM10, PM2.5)|Melanoma|Ultraviolet (UV) Radiation;C:Health|Environmental Science|Cancer Research|Dermatology;S:cautionary|informative|negative ",

        """P:Art,music,science exhibition at Winchester Cathedral|Whale sculptures made from "ghost gear" highlight ocean pollution|AI used to translate whale calls|Whale foraging areas overlap with krill fisheries|Global "whale superhighways" mapped, revealing threats|Performance piece "Echolocations" responds to cathedral acoustics & whale communication|Human impact on oceans: pollution, overfishing, climate change|Imagination needed for sustainable relations with planet;E:Exhibition open until Feb 26 2025|Performance "Echolocations" on Feb 6|Discovery of complex sperm whale codas|Reports of stranded whales with plastic-filled stomachs|D:Whales weigh up to 45 tonnes|Sculptures 3-5m long|Sperm whale found with 25kg debris in stomach|Cathedral is 170m long|Over 1,000 whale tracks used for "superhighway" map|40,000+ subscribers to "Imagine" newsletter|R:Winchester Cathedral, Hampshire, UK|Pas-de-Calais, France|North Carolina, US|Western Antarctic Peninsula|Global oceans|N:Tessa Campbell Fraser (artist)|Sperm whales|Blue whales|Herman Melville (author of Moby Dick)|Philip Hoare (author of Leviathan)|Liz Gre (vocalist)|Ben Oliver (pianist)|Pablo Galaz (electronics)|Drew Crawford (electronics)|Ryan Reisinger (researcher)|Ghost Fishing UK (charity)|University of Southampton|WWF|C:Art|Music|Science|Marine Biology|Conservation|Climate Change|Pollution|Technology|Literature|Acoustics|S:concerned|hopeful|critical """,

        "P:Slow growth strategy|Immediate impact lacking|Focus on supply-side reforms|Infrastructure investment key|Long timeframe for results|Political risk of unfulfilled promises|Financing reliant on pension funds & private capital|Planning reform faces local opposition|Tension between growth & environmental goals|Deregulation concerns in finance & industry|Skills shortage a limiting factor|Trade dependency & Brexit impact;E:Reeves shifts economic outlook|OBR forecasts minimal short-term growth boost|HS2 debacle cited as example of project delays|Starmer emphasizes deregulation|Head of Competition and Markets Authority sacked;D:£160 billion available in defined benefit pension schemes|0.1% growth boost per 1% public investment|IMF projects 1.6% UK growth this year|Heathrow expansion unlikely before 2030|Lower Thames crossing & Sizewell C planned for ~20 years;R:UK|Kent|Essex|Suffolk|EU|USA|France|Germany;N:Rachel Reeves|Keir Starmer|Liz Truss|Office for Budget Responsibility (OBR)|International Monetary Fund (IMF)|Dinendra Haria|Steve Schifferes;C:Economics|Politics|Infrastructure|Finance|Regulation|Trade|Brexit|Environmental Policy;S:cautious|concerned|critical ",

        "P:4 distinct giraffe species confirmed|Skull morphology supports 4 species|Conservation efforts need species-specific focus|Skull variations affect reproductive success|Study confirms previous genetic findings;E:Study of 500+ giraffe skulls|DNA, ecology, behavior, health & coat patterns previously studied|IUCN still recognizes only one giraffe species;D:45,000 southern giraffe|50,000 Masai giraffe|16,000 reticulated giraffe|6,000 northern giraffe|3D scanning used on 500+ skulls;R:Africa|Europe|US;N:Giraffa giraffa|Giraffa tippelskirchi|Giraffa reticulata|Giraffa camelopardalis|Giraffe Conservation Foundation (GCF)|International Union for the Conservation of Nature (IUCN);C:Biology|Taxonomy|Conservation|Zoology;S:positive",

        """P:Labour aims for economic growth|Chancellor Reeves seeks "faster" growth|Growth elusive since election|Speech on Jan 29 was economic reset attempt|Ambitions positive, execution key|Fiscal constraints limit investment|Brexit remains a drag|Need private sector support|Immediate stimulus needed
        E:Reeves' speech on Jan 29|Means-testing of winter fuel payments criticized|Budget framing dented confidence|Announcement of £28M investment in Cornish Metals|£63M for advanced fuels in Teesside|Plans for redevelopment around Old Trafford
        D:Public investment to rise to 2.6% of GDP|Previous government planned 1.9% GDP investment|UK exports down 9% since 2020|Similar economies exports up 1% since 2020|Planning law reform commitment|
        R:South-East may benefit from investment|Cornwall (investment in Cornish Metals)|Teesside (advanced fuels investment)|Manchester (Old Trafford redevelopment)|Oxford-Cambridge ("Silicon Valley" plan)
        N:Rachel Reeves|Labour government|Heathrow Airport|Luton Airport|Gatwick Airport|Cornish Metals|Phil Tomlinson|David Bailey
        C:UK Economy|Economic Policy|Brexit|Industrial Strategy|Investment|Infrastructure|Regional Development
        S:mixed|cautiously optimistic|critical of past "own goals"|concerned about Brexit impact|skeptical about execution|""",

        """P:Anti-gender movements restrict equality & sex education globally|Trump presidency may limit school knowledge|Movements target schools to influence long-term norms|Funding from US/European conservative sources|Movements utilize anti-colonial language in Africa/Latin America|Misinformation & parental anxieties are exploited|Legal frameworks can protect education access;E:Book bans in US schools|CitizenGO's "anti-trans" bus campaign in Europe/Americas|Outrage manufactured around comprehensive sex education|Parental protests against sex education in South Africa, Peru, Ghana|Strategic litigation overturned laws in Mexico/Brazil|Policies banning adolescent mothers reversed in Sierra Leone;D:US$6.2 billion revenue (2008-2017) for US anti-gender orgs|US$1 billion+ funneled abroad (2008-2017)|US$54 million spent in Africa (2007-2020) by US Christian groups|Global Philanthropy Project aims to advance LGBTI+ rights;R:US|Europe|Africa|Latin America|Global South|Mexico|Brazil|Sierra Leone|Peru|Ghana|South Africa|Argentina|Chile|Colombia;N:Trump|CitizenGO|ODI Global|Global Philanthropy Project|Con Mis Hijos No Te Metas;C:Education|Politics|LGBTQ+ Rights|Gender Equality|Human Rights|Social Movements|Sex Education|Misinformation;S:negative """,

        """P:Surging bird flu cases in UK|Human case detected in England|Risk to humans remains low|Virus evolving & spreading aggressively|Outbreak impacting wild birds & poultry|H5N1 strain prominent|Previous UK outbreak subsided mid-2023, resurged autumn 2024|Prevention zones declared in England, Scotland, Wales|Wild birds spread to poultry|Global H5N1 outbreak ongoing|Virus infecting mammals in South America (seals/sea lions) & US (dairy cattle)|Mild human infections reported in US farm workers|Severe cases & fatality in US/Canada|No human-to-human transmission reported|UK has vaccine/antiviral reserves
        E:Avian influenza control zones implemented|Restrictions placed on wild birds & poultry|H5N1 evolved in 2020|Outbreak in seabird colonies in UK (2021)|Outbreaks in farmed birds|Case of bird flu in poultry worker in England|Sightings of dead/sick birds reported to agencies
        D:H5N1 strain causing major poultry die-offs since late 1990s|Three seasonal influenza types in humans (H1N1, H3N2, influenza B)|US experiencing major H5N1 outbreak|Potential for temporary egg supply difficulties & price increases
        R:England|Scotland|Wales|South America|US|UK
        N:H5N1|Influenza viruses|Wild birds|Poultry|Seals|Sea lions|Dairy cattle|Ed Hutchinson
        C:Avian influenza|Public health|Animal health|Global outbreak|Influenza viruses
        S:neutral|concerned|informative|cautionary """,

        "P:Shipping companies hesitant to return to Red Sea despite Gaza ceasefire|Increased shipping costs & delays due to rerouting|Ceasefire considered “fragile”|Long-term security needed for route resumption|Supply chain disruptions likely to continue in 2025;E:Houthi attacks on ~190 ships since Nov 2023|Ship diversions around Africa began Nov 2023|Tesla & Volvo temporarily suspended manufacturing due to component delays|Houthi leader warned of continued attacks Jan 20;D:12% of global trade normally passes through Red Sea|Suez Canal usage dropped from 26k (2023) to 13.2k (2024)|Shanghai-Rotterdam container cost surged from $4.4k (Jan) to $8k (Aug), then to $4.9k (Dec)|On-time container ship arrival dropped from 60% (2023) to 50% (2024)|Fuel use increased 33% on Africa route|Emissions increased by ~13.6M tonnes CO₂ (Dec 2023-Apr 2024)|Air freight CO₂ emissions 50x higher than container shipping;R:Red Sea|Bab al-Mandab Strait|Suez Canal|Africa (Southern Tip)|Europe|China|Gaza Strip|Netherlands (Rotterdam);N:CMA CGM|Houthi militants|Tesla|Volvo|Abdul-Malik al-Houthi|Gokcay Balci;C:Shipping|Supply Chain|Geopolitics|Environment|Economics;S:negative ",

        "P:Support for authoritarianism increasing among UK Gen Z|Disillusionment with current political systems|Distrust of politicians prioritizing self/corporate interests|Young people want engagement but feel unheard|Democracy seen as potentially improvable, not inherently rejected|Need for political/media literacy & democratic education|Youth desire systemic political reform|Democracy extends beyond governance to social organization;S:concerned|negative|hopeful;E:Channel 4 research shows 52% Gen Z favor strong leader bypassing Parliament|Open Society Foundations study: 42% global youth see military rule as good|12 discussion groups conducted with 101 young people|Youth network formed to re-imagine democracy;D:2000 UK 13-27 year olds surveyed|86% of young people still want to live in a democracy|73% of Gen Z view democracy positively|Liam (Sunderland) highlights lack of democratic education|Chloe (Liverpool) describes performative politician engagement;R:UK|Europe|Global;N:Gen Z|Melissa Butcher|Channel 4|Open Society Foundations|Cumberland Lodge|Green Party|DimaBerlin/Shutterstock;C:Politics|Youth Engagement|Democracy|Authoritarianism|Social Issues|Education|Mental Health|Climate Change|Artificial Intelligence|Housing ",

        "P:Trump invites influencers to briefings|Broadening media access is a good idea in principle|Concerns about repeating past briefing practices|Potential to undermine public trust in journalism|Polarization of media expected to increase|Legal threats to journalists may increase|Rise of partisan influencers with dangerous language;E:Karoline Leavitt announces new briefing policy|White House received 7,400 accreditation applications|Trump favored friendly media (Fox News) in first term|Breitbart & Axios selected for first questions|Acosta leaves CNN;D:7400 applications received|2017-2021: Trump's first term|January 28: Acosta's final CNN broadcast;R:United States|White House;N:Donald Trump|Karoline Leavitt|Elon Musk|Tucker Carlson|Jim Acosta|George Stephanopoulos|Taylor Lorenz|Ken Klippenstein|Mark Zuckerberg|Steven Buckley;C:Politics|Media|Journalism|Misinformation|Social Media;S:negative|concerned|critical"
    ]
    [ic(digestors.parse_compressed_digest(resp)) for resp in responses]

@log_runtime
def test_run_async():
    orch = _create_fullstack_orchestrator() 
    sources = "/home/soumitsr/codes/pycoffeemaker/tests/sources-2.yaml"
    asyncio.run(orch.run_async(sources))
    orch.close()

lower_case = lambda items: {"$in": [item.lower() for item in items]} if isinstance(items, list) else items.lower()
case_insensitive = lambda items: {"$in": [re.compile(item, re.IGNORECASE) for item in items]} if isinstance(items, list) else re.compile(items, re.IGNORECASE)

def download_beans():
    from coffeemaker.orchestrators.fullstack import Orchestrator
    orch = Orchestrator(
        os.getenv("DB_REMOTE"),
        os.getenv("DB_LOCAL"),
        os.getenv("DB_NAME"), 
        embedder_path=os.getenv("EMBEDDER_PATH"),    
        digestor_path=os.getenv("DIGESTOR_PATH")
    )
    beans = orch.remotesack.query_beans(
        filter = {
            "collected": { "$gte": (datetime.now() - timedelta(hours=8))},
            "gist": {"$exists": True},
            "highlights": {"$exists": True}
        },
        limit=256
    )
    to_write = [{"digest": bean.digest()} for bean in beans]
    with open("embedder-test-data.json", "w") as file:
        json.dump(to_write, file)

def download_markdown(q: str = None, accuracy = DEFAULT_VECTOR_SEARCH_SCORE, keywords: str|list[str] = None, limit = 100):
    orch = _create_fullstack_orchestrator()
    filter = {K_KIND: { "$ne": POST}}
    if keywords: 
        filter.update(
            { 
                "tags": case_insensitive(keywords) 
            }
        )
    projection = {K_SUMMARY: 1, K_URL: 1}

    if q:
        beans = orch.remotesack.vector_search_beans(
            embedding=orch.embedder.embed_query(f"query: {q}"),
            min_score=accuracy,
            filter=filter,
            sort_by=NEWEST,
            skip=0,
            limit=limit,
            projection=projection
        )
    else:
        beans = orch.remotesack.query_sample_beans(filter, NEWEST, limit, projection)

    markdown = "\n\n".join([bean.digest() for bean in beans])

    filename = q or datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    if keywords: filename = "-".join(keywords)
    save_markdown(filename, markdown)

def test_simple_collector():
    from coffeemaker.orchestrators.simplecollector import Orchestrator
    orch = Orchestrator(
        "mongodb://localhost:27017/",
        "test", 
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;",
        "index-queue"
    )
    orch.run("/home/soumitsr/codes/pycoffeemaker/tests/sources-2.yaml")

if __name__ == "__main__":
    test_simple_collector()

    # test_run_async()
    # test_embedder()
    # test_digestor()
    # test_digest_parser()
    # test_collection_and_download()
    # test_run_async()
    # topics['topics'][0]
    # download_markdown(limit = 110)
    # download_markdown(keywords =  ["Cardano", "$0.62 support", "RSI oversold", "Fibonacci retracement", "volume confirmation"], limit=110)
    # download_markdown("North Korean operatives generate $250 million to $600 million annually through remote IT job fraud (May 2, 2025).", accuracy=0.8)
    # [download_markdown(q = topic['verdict'], limit = 50) for topic in topics['topics']]
    
