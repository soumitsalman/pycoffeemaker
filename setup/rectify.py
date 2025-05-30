import sys
import json
import os
import re
import asyncio
from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()

from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin
from itertools import chain
from icecream import ic
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack.mongosack import VALUE_EXISTS
from coffeemaker.pybeansack.ducksack import SQL_NOT_WHERE_URLS
from coffeemaker.orchestrators.fullstack import Orchestrator

K_RELATED = "related"
LIMIT=40000

ndays_ago = lambda n: (datetime.now() - timedelta(days=n))
make_id = lambda text: re.sub(r'[^a-zA-Z0-9]', '-', text.lower())
create_orch = lambda: Orchestrator(
    os.getenv("DB_REMOTE"),
    os.getenv("DB_LOCAL"),
    os.getenv("DB_NAME"),
    embedder_path = os.getenv("EMBEDDER_PATH"),    
    digestor_path = os.getenv("DIGESTOR_PATH")
)

def setup_categories():   
    updates = []
    def _make_category_entry(predecessors, entry):        
        if isinstance(entry, str):
            path = predecessors + [entry]
            id = make_id(entry)
            updates.append({
                K_ID: id,
                K_CONTENT: entry, 
                K_RELATED: list({make_id(item) for item in path}),
                K_DESCRIPTION: " >> ".join(path), 
                K_EMBEDDING:  orch.remotesack.embedder.embed( "category: " + (" >> ".join(path))), 
                K_SOURCE: "__SYSTEM__"
            })
            return [id]
        if isinstance(entry, list):
            return list(chain(*(_make_category_entry(predecessors, item) for item in entry)))
        if isinstance(entry, dict):
            res = []
            for key, value in entry.items():
                id = make_id(key)
                related = list(set(_make_category_entry(predecessors + [key], value)))
                updates.append({
                    K_ID: id,
                    K_CONTENT: key,
                    K_RELATED: related,
                    K_SOURCE: "__SYSTEM__"
                })
                res.extend(related+[id])
            return res    
        
    with open("factory_settings.json", 'r') as file:
        _make_category_entry([], json.load(file)['categories'])
    # orch.categorystore.delete_many({K_SOURCE: "__SYSTEM__"})
    # orch.categorystore.insert_many(list({item[K_ID]: item for item in updates}.values()))   
    orch.localsack.store_categories(list({item[K_ID]: item for item in updates}.values()))

def setup_baristas():   
    baristas = MongoClient(os.getenv("DB_CONNECTION_STRING"))['espresso']['baristas']
    updates = [
        {
            K_ID: "hackernews", 
            K_TITLE: "Hackernews (by Y Combinator)", 
            K_DESCRIPTION: "News, blogs and posts shared in Y Combinator's Hackernews.", 
            K_SOURCE: ychackernews.YC,
            "owner": "__SYSTEM__"
        },
        {
            K_ID: "reddit", 
            K_TITLE: "Reddit", 
            K_DESCRIPTION: "News, blogs and posts shared in Reddit.", 
            K_SOURCE: redditor.REDDIT,
            "owner": "__SYSTEM__"
        }
    ]
    # updates = []
    # def make_barista_entry(entry) -> list[str]:        
    #     if isinstance(entry, str):
    #         return [entry]
    #     if isinstance(entry, list):
    #         return list(chain(*(make_barista_entry(item) for item in entry)))
    #     if isinstance(entry, dict):
    #         res = []
    #         for key, value in entry.items():
    #             tags = list(set([key] + make_barista_entry(value)))
    #             barista = {
    #                 K_ID: make_id(key),
    #                 K_TITLE: key,
    #                 K_DESCRIPTION: f"News, blogs and posts on {', '.join(tags)}.",
    #                 K_TAGS: tags,
    #                 K_EMBEDDING: orch.remotesack.embedder.embed(f"News, blogs and posts on domain/genre/topics such as {', '.join(tags)}."),
    #                 "owner": "__SYSTEM__"
    #             }
    #             updates.append(barista)
    #             res.extend(tags)
    #         return res    
        
    # with open("factory_settings.json", 'r') as file:
    #     make_barista_entry(json.load(file)['categories'])

    # with open(".test/baristas.json", 'w') as file:
    #     json.dump(updates, file)
    
    # baristas.delete_many({"owner": "__SYSTEM__"})
    baristas.insert_many(list({item[K_ID]: item for item in updates}.values())) 

# def embed_categories():
#     cats = orch.categorystore.find({K_EMBEDDING: {"$exists": False}})
#     ic(orch.categorystore.bulk_write(
#         [UpdateOne(filter={K_ID: cat[K_ID]}, update={"$set": {K_EMBEDDING: orch.remotesack.embedder.embed("topic: " + cat[K_TEXT])}}) for cat in cats],
#         ordered=False
#     ).modified_count)

# def rectify_categories():
#     beans = orch.remotesack.get_beans(filter={K_EMBEDDING: {"$exists": True}}, projection={K_URL: 1, K_TITLE: 1, K_EMBEDDING: 1})
#     updates = []
#     for bean in beans:
#         cats = orch._find_categories(bean)
#         if cats:
#             updates.append(UpdateOne(
#                 filter = {K_URL: bean.url},
#                 update = {"$set": {K_CATEGORIES: cats}}
#             ))
#         else:
#             updates.append(UpdateOne(
#                 filter = {K_URL: bean.url},
#                 update = {"$unset": {K_CATEGORIES: ""}}
#             ))
#     return orch.remotesack.beanstore.bulk_write(updates, False).modified_count

# def rectify_ranking():
#     orch.trend_rank_beans()

# def port_categories_to_localsack():
#     orch.localsack.store_categories(list(orch.categorystore.find()))


def port_beans_to_localsack():
    orch = create_orch()
    beans = orch.remotesack.query_beans(
        filter={
            K_EMBEDDING: {"$exists": True}
        }
    )
    print(datetime.now(), "porting beans|%d", len(beans))
    BATCH_SIZE = 2000
    
    async def store_locally(beans: list[Bean]):
        tasks = [asyncio.to_thread(orch.localsack.store_beans, beans[i:i+BATCH_SIZE]) for i in range(0, len(beans), BATCH_SIZE)]
        await asyncio.gather(*tasks) 
           
    asyncio.run(store_locally(beans))   
    orch.close()
    
    print(datetime.now(), "finished porting beans")

def port_chatters_to_localsack():
    orch = create_orch()
    
    print("finished porting beans")

# def port_chatters_to_localsack():
#     chatters = []
#     items = list(orch.remotesack.chatterstore.find(filter={K_UPDATED: {"$exists": True}}, sort={K_UPDATED: -1}))
#     print("porting chatters|%d", len(items))
#     for item in items:
#         if isinstance(item.get(K_UPDATED), int):
#             item[K_COLLECTED] = dt.fromtimestamp(item[K_UPDATED])
#         item["chatter_url"] = item[K_CONTAINER_URL]
#         chatters.append(Chatter(**item))

#     orch.localsack.store_chatters(chatters)
#     print("finished porting chatters")

def refresh_localsack():
    orch = create_orch()
    orch_new = Orchestrator("mongodb://localhost:27017/", ".db-new", "beansackV2")

    BATCH_SIZE = 256
    # count = orch.remotesack.beanstore.count_documents(filter={})
    # ported_urls = set()
    # print(datetime.now(), "porting remote beans|%d", count)
    # from tqdm import tqdm
    # progress_bar = tqdm(total=count, desc="Progress", unit="beans")
    # for i in range(43008, count, BATCH_SIZE):
    #     beans = orch.remotesack.get_beans(
    #         filter = {},
    #         skip = i,
    #         limit = BATCH_SIZE
    #     )
    #     vecs = orch.embedder.embed_documents([bean.digest() for bean in beans])
    #     for bean, v in zip(beans, vecs):
    #         bean.embedding = v

    #     orch_new.localsack.store_beans(beans)
    #     orch.remotesack.update_beans([UpdateOne(filter={"_id": bean.url}, update={"$set": {"embedding": bean.embedding}}) for bean in beans])
    #     ported_urls.update([bean.url for bean in beans])

    #     progress_bar.update(len(beans))

    # progress_bar.close()

    # print(datetime.now(), "recomputed vectors and saved|%d", len(ported_urls))

    # print("porting chatters")

    # orch_new.localsack.store_chatters(orch.localsack.get_chatters())    
    
    beans = orch.localsack.get_beans(filter = "updated <= CURRENT_TIMESTAMP - INTERVAL '2 months'")
    print(datetime.now(), "porting local beans")   
    orch_new.localsack.store_beans(beans)  

    # from tqdm import tqdm
    # progress_bar = tqdm(total=len(beans), desc="Progress", unit="bean")

    # def load_and_store(batch):
    #     orch_new.localsack.store_beans(batch)        
    #     progress_bar.update(len(batch))
   
    # with ThreadPoolExecutor(max_workers=os.cpu_count()*os.cpu_count()) as executor:
    #     executor.map(load_and_store, [beans[i:i+BATCH_SIZE] for i in range(0, len(beans), BATCH_SIZE)])    
    
    # progress_bar.close()
    orch.close()
    orch_new.close()
    
    print(datetime.now(), "refresh complete")

def download_sources():
    from coffeemaker.orchestrators.collectororch import Orchestrator
    from coffeemaker.collectors.collector import extract_base_url, extract_source

    local = Orchestrator(
        mongodb_conn_str="mongodb://localhost:27017/",
        db_name="test3"
    )
    
    # SCRAPE FOR DETAILS
    sources = local.db.beanstore.distinct("source", filter = {K_SOURCE: {"$ne": ""}, K_KIND: {"$ne": POST}})[1950:]
    
    batch_size = 128
    for i in range(0, ic(len(sources)), batch_size):
        beans = local.db.query_beans({K_SOURCE: {"$in": sources[ic(i) : i+batch_size]}}, distinct_field=K_SOURCE, project = {K_URL: 1})
        urls = list(set([bean.url for bean in beans]+["https://"+extract_base_url(bean.url) for bean in beans]))
        exists = [e[K_ID] for e in local.db.sourcestore.find({K_ID: {"$in": urls}}, projection = {K_ID: 1})]
        urls = list(filter(lambda url: url not in exists, urls))
        if not urls: continue

        results = [res for res in asyncio.run(local.webscraper.scrape_urls(urls, collect_metadata=True)) if res]
        for res in results:
            res[K_ID] = res[K_URL]
            res[K_SITE_BASE_URL] = extract_base_url(res[K_URL])
            res[K_SOURCE] = extract_source(res[K_URL])
            res.pop('markdown')

            site = "https://"+res[K_SITE_BASE_URL]
            if 'site_name' not in res:
                res['site_name'] = res.get('title')
            if "rss_feed" in res and not res['rss_feed'].startswith('http'):
                res['rss_feed'] = urljoin(site, res['rss_feed'])
            if "favicon" in res and not res['favicon'].startswith('http'):
                res['favicon'] = urljoin(site, res['favicon'])
                if not res['favicon'].startswith('http'): res.pop('favicon')
            if 'favicon' not in res:
                res['favicon'] = urljoin(site, "/favicon.ico")

        local.db.sourcestore.insert_many([res for res in results if res])


def merge_feeds():
    from coffeemaker.orchestrators.collectororch import Orchestrator
    from coffeemaker.collectors.collector import parse_sources
    import yaml

    local = Orchestrator(
        mongodb_conn_str="mongodb://localhost:27017/",
        db_name="test3"
    )
    
    # SCRAPE FOR DETAILS
    rss_feeds = local.db.sourcestore.distinct("rss_feed", filter = {"rss_feed": {"$nin": to_ignore}})
    
    existing = parse_sources("/home/soumitsr/codes/pycoffeemaker/coffeemaker/collectors/feeds.yaml")
    existing['rss'] = list(set(existing['rss']+rss_feeds))
    existing = {"sources": existing}
    
    with open("/home/soumitsr/codes/pycoffeemaker/coffeemaker/collectors/feeds.yaml", "w") as file:
        yaml.dump(existing, file)

def remove_non_functional_feeds():
    import csv, yaml
    csv_path = "/home/soumitsr/codes/pycoffeemaker/coffeemaker/collectors/non-functional-rss.csv"
    yaml_path = "/home/soumitsr/codes/pycoffeemaker/coffeemaker/collectors/feeds.yaml"

    # Read non-functional RSS feeds from CSV (handling quoted values)
    with open(csv_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        non_functional = [row[0].strip().strip('"') for row in reader if row and row[0].strip()]

    ic(len(non_functional))
    # Load YAML feeds
    with open(yaml_path, 'r') as file:
        feeds_data = yaml.safe_load(file)

    # Remove non-functional feeds from the rss list
    if 'sources' in feeds_data and 'rss' in feeds_data['sources']:
        original_count = len(feeds_data['sources']['rss'])
        feeds_data['sources']['rss'] = sorted([
            feed for feed in feeds_data['sources']['rss'] if feed not in non_functional
        ])
        print(f"Removed {original_count - len(feeds_data['sources']['rss'])} feeds.")


    # Save the updated YAML
    with open(yaml_path, 'w') as file:
        yaml.dump(feeds_data, file)



to_ignore = [
"feed://bjango.com/rss/articles.xml",
"http://www.belgrade-news.com/search/?f=rss&t=article&c=business&l=50&s=start_time&sd=desc",
    "http://www.kake.com/search/?f=rss&t=article&l=50&s=start_time&sd=desc&k%5B%5D=%23topstory",
  "http://www.nola.com/search/?f=rss&t=article&l=50&s=start_time&sd=desc&k%5B%5D=%23topstory",

  "http://www.stcatharinesstandard.ca/search/?f=rss&t=article&c=news/niagara-region&l=50&s=start_time&sd=desc",
  "http://www.stltoday.com/search/?f=rss&t=article&l=50&s=start_time&sd=desc&k%5B%5D=%23topstory",

  "http://www.thecentersquare.com/search/?f=rss&t=article&c=national&l=50&s=start_time&sd=desc",
  "http://www.thecentersquare.com/search/?f=rss&t=article&l=50&s=start_time&sd=desc&k%5B%5D=%23topstory",
  "http://www.thecincinnatisubway.com/feeds/posts/default?alt=rss",
  "http://www.thestar.com/search/?f=rss&t=article&c=news/gta&l=50&s=start_time&sd=desc",
  "http://www.thestar.com/search/?f=rss&t=article&l=50&s=start_time&sd=desc&k%5B%5D=%23topstory",
  "http://www.wdel.com/search/?f=rss&t=article&l=50&s=start_time&sd=desc&k%5B%5D=%23topstory",
 
  "https://99percentinvisible.org/episode/episode-76-the-modern-moloch/feed/",
  "https://99percentinvisible.org/episode/the-real-book/feed/",

   "https://blog.logrocket.com/product-management/goodbye-skype-product-insider-take/feed/",

   "https://blog.meccabingo.com/bingo-calls-complete-list/feed/",
 "https://blogs.lse.ac.uk/europpblog/2025/03/26/turkish-protests-is-this-the-end-for-erdogan/feed/",
  "https://blogs.lse.ac.uk/impactofsocialsciences/2025/03/03/bluesky-will-trap-academics-in-the-same-way-twitter-x-did/feed/",
 "https://blogs.opera.com/news/2025/03/opera-browser-operator-ai-agentics/feed/",
  "https://blogs.windows.com/msedgedev/2025/03/19/minding-the-gaps-a-new-way-to-draw-separators-in-css/feed/",
   "https://bolor.me/introducing-qipai-quantum-inspired-particle-ai-for-the-next-wave-of-intelligence/feed/",
"https://business.inquirer.net/518369/green-lane-investment-pipeline-breaches-p-5-t-mark/feed",
   "https://canadianbusiness.com/ideas/molson-non-alcoholic-drinks/feed/",

  "https://carnewschina.com/2025/04/21/battery-giant-catl-showcases-three-innovations-1500km-range-battery-520km-in-5-minutes-ultra-fast-charging-and-2025-mass-production-sodium-ion-battery/feed/",
  "https://carnewschina.com/homepage/feed/", 
 "https://chemistry.princeton.edu/news/dinca-lab-researchers-demonstrate-high-performance-sodium-ion-cathode-towards-new-battery-technology/feed/",
  "https://collective.flashbots.net/t/beyond-yocto-exploring-mkosi-for-tdx-images/4739.rss",
    "https://consequence.net/2025/03/neil-young-free-concert-ukraine/feed/",
  "https://consequence.net/2025/05/trump-warns-springsteen-keep-mouth-shut/feed/",
  "https://courage.media/2025/03/21/the-heathrow-fires-accident-incompetence-or-accelerationism/feed/",
 "https://devblogs.microsoft.com/azure-sdk/rust-in-time-announcing-the-azure-sdk-for-rust-beta/feed/",
  "https://devblogs.microsoft.com/cppblog/cpp-dynamic-debugging-full-debuggability-for-optimized-builds/feed/",
  "https://devblogs.microsoft.com/devops/new-overlapping-secrets-on-azure-devops-oauth/feed/",
  "https://devblogs.microsoft.com/directx/announcing-directx-raytracing-1-2-pix-neural-rendering-and-more-at-gdc-2025/feed/",
  "https://devblogs.microsoft.com/dotnet/modernizing-push-notification-api-for-teams/feed/",
  "https://devblogs.microsoft.com/identity/openid-connect-external-identity-provider-support-ga/feed/",
  "https://devblogs.microsoft.com/java/java-openjdk-april-2025-patch-security-update/feed/",
  "https://devblogs.microsoft.com/oldnewthing/20250429-00/?p=111127/feed",
      "https://domainincite.com/30979-the-soviet-union-might-be-safe-after-all/feed",
 "https://earthsky.org/earth/fire-season-early-start-canada-and-minnesota-may-2025/feed/",
  "https://earthsky.org/space/blaze-star-nova-corona-borealis-how-to-see-it/feed/",
 "https://ethereum-magicians.org/t/long-term-l1-execution-layer-proposal-replace-the-evm-with-risc-v/23617.rss",
  "https://foreignpolicy.com/2025/03/14/carney-mincemeat-second-raters-trump/feed/",
  "https://foreignpolicy.com/2025/03/25/america-kleptocracy-trump-musk-corruption/feed/",
  "https://foreignpolicy.com/2025/05/14/elon-musk-donald-trump-doge-russell-vought/feed/",
  "https://habr.com/en/rss/post/454376/?fl=en",
  "https://habr.com/en/rss/post/905288/?fl=en", 
   "https://halifax.citynews.ca/2025/03/04/nova-scotia-hits-back-at-u-s-tariffs-with-procurement-limits-toll-hike-and-alcohol-ban/feed/",
  "https://iai.tv/articles/information-and-data-will-never-deliver-creativity-auid-3132/rss",
   "https://ifp.org/an-action-plan-for-american-leadership-in-ai/feed/",
     "https://inl.int/neuromorphic-photonic-neuron-light-sensing/feed/",
   "https://insider-gaming.com/final-fantasy-9-remake-seemingly-teased-by-square-enix/feed/",
  "https://insights.samsung.com/2025/02/12/a-fresh-approach-to-secure-mobile-device-usage-in-classified-areas/feed/",
 "https://internals.rust-lang.org/t/pre-rfc-explicit-proper-tail-calls/3797.rss",
 "https://ipwatchdog.com/2025/04/20/fox-succeeds-scrapping-macine-learning-claims-cafc-101/id=188305/feed/",
  "https://kernelnewbies.org/FirstKernelPatch?diffs=1&show_att=1&action=rss_rc&unique=0&page=FirstKernelPatch&ddiffs=1",
  "https://kernelnewbies.org/Linux_Kernel_Newbies?action=rss_rc&unique=1&ddiffs=1",
  "https://kk.org/thetechnium/1000-true-fans/feed/",
 "https://krisp.ai/blog/improving-turn-taking-of-ai-voice-agents-with-background-voice-cancellation/feed/",
   "https://ktar.com/arizona-news/mesa-butterfly/5684819/feed/",
  "https://lethbridgenewsnow.com/2025/03/15/israeli-airstrikes-killed-8-people-in-the-gaza-strip-palestinian-medics-say/feed/",
 "https://lostcity.rs/t/singleplayer-main-branch-scripts-and-desktop-start-launchers-on-windows-linux-freebsd/54.rss",
  "https://metatalk.metafilter.com/26610/Introducing-Socky-An-AI-Assistant-for-MetaFilter/rss",

"https://militarnyi.com/en/news/usa-unable-to-make-drones-without-components-from-china/feed/",
   "https://mobilesyrup.com/2025/03/24/canadian-streaming-report-convergence-research/feed/",
 
"https://mynorthwest.com/local/seattle-cruise-season/4075820/feed",
   "https://nasawatch.com/education/you-can-still-read-nasas-deleted-first-woman-graphic-novels/feed/",
 
"https://newsroom.arm.com/blog/arm-malaysia-silicon-vision/feed",
  "https://nintendoeverything.com/nintendo-has-changed-how-the-switch-eshop-charts-work/feed/",
  "https://openziti.discourse.group/t/netfoundry-raises-new-venture-round/4449.rss",
    "https://panow.com/2025/03/20/liberals-revoke-aryas-nomination-after-removing-him-from-leadership-race/feed/",
 "https://pears.com/news/introducing-bare-actually-run-javascript-everywhere/feed/",
 
  "https://redmonk.com/jgovernor/2025/02/21/ai-disruption-code-editors-are-up-for-grabs/feed/",
  "https://redmonk.com/kfitzpatrick/2025/04/11/three-things-that-matter/feed/",
  "https://redmonk.com/kholterhoff/2025/04/02/is-frontend-observability-hipster-rum/feed/",
  "https://redmonk.com/rstephens/2025/05/02/heroku/feed/",
   "https://ritholtz.com/2014/02/the-joy-and-freedom-of-working-until-death/feed/",
"https://securityaffairs.com/175344/hacking/coordinated-surge-exploitation-attempts-ssrf-vulnerabilities.html/feed",
  "https://securityaffairs.com/176937/data-breach/yale-new-haven-health-ynhhs-data-breach-impacted-5-5-million-patients.html/feed",
  "https://securityaffairs.com/177784/data-breach/marks-and-spencer-confirms-data-breach-after-april-cyber-attack.html/feed",
  "https://techchannel.com/operating-system/ibm-i-7-6-and-7-5-tr6/feed/",
   "https://thehistoryoftheweb.com/the-innovative-designs-of-1995/feed/",
    "https://tosc.iacr.org/index.php/ToSC/gateway/plugin/APP%5Cplugins%5Cgeneric%5CwebFeed%5CWebFeedGatewayPlugin/rss2",
    "https://uk.pcmag.com/lenovo-thinkpad-x1-carbon-gen-13-aura-edition.xml",
  "https://web.archive.org/web/20080415175105/http://blogs.msdn.com/jensenh/rss.xml",
  "https://web.archive.org/web/20190721030122/http://blog.modernmechanix.com/feed/",
  "https://web.archive.org/web/20230204195941/https://simonberens.me/blog?format=rss",
  "https://web.archive.org/web/20240618215938/https://thecrimereport.org/feed/",
 "https://wenewsenglish.pk/israel-will-finish-the-job-against-iran-with-us-support-netanyahu/feed/",
 "https://www.climatechangenews.com/2025/04/02/the-amazon-rainforest-emerges-as-the-new-global-oil-frontier/feed/",
  "https://www.climatechangenews.com/2025/05/13/trump-shifts-us-energy-funding-from-shutting-down-foreign-fossil-fuels-to-expanding-them/feed/",
 "https://www.deploymentresearch.com/delivery-optimization-not-listening/feed/",
 "https://www.dezeen.com/2025/03/20/final-usonian-home-riverrock-frank-lloyd-wright-ohio-completed/feed/",
  "https://www.electrive.com/2025/03/03/h2-mobility-to-shut-down-22-hydrogen-fuel-stations-in-germany/feed/",
  "https://www.fiercebiotech.com/rss/Fierce Biotech Homepage/xml",
  "https://www.fiercehealthcare.com/rss/Fierce Healthcare Homepage/xml",
  "https://www.fiercepharma.com/rss/Fierce Pharma Homepage/xml",
   "https://www.gematsu.com/2025/03/bexide-announces-fruit-mountain-party-for-pc/feed",
  "https://www.lean.org/the-lean-post/articles/30-plus-years-with-hoshin-kanri/feed/",
  "https://www.lean.org/the-lean-post/articles/hoshin-kanri-as-a-foundational-piece-of-a-lean-management-system/feed/",
"https://www.lifenews.com/2025/02/26/trump-nominee-harmeet-dhillon-confirms-doj-will-stop-targeting-pro-life-christians/feed/",
  "https://www.lifenews.com/2025/05/12/planned-parenthood-makes-over-2-billion-killing-a-record-402000-babies-in-abortions/feed/",
  "https://www.metafilter.com/207938/Im-invasive-and-delicious-EAT-ME-Please/rss",
   "https://www.newsshooter.com/2025/03/20/davinci-resolve-19-1-4-update/feed/",
 "https://www.pushsquare.com/feeds/comments/news/2025/03/ps2-the-most-popular-console-ever-celebrates-its-25th-anniversary",
  "https://www.pushsquare.com/feeds/comments/news/2025/05/ps5-fans-furious-as-microsoft-ships-doom-the-dark-ages-with-just-85mb-on-the-disc",
  "https://www.themarysue.com/adam-kinzinger-roasts-elon-musk-for-losing-bigly-after-wisconsin-political-stunt/feed/",
 "https://www.thetransmitter.org/human-neurotechnology/functional-mri-can-do-more-than-you-think/feed/",
 "https://www.timeextension.com/feeds/comments/news/2025/03/activision-comes-under-fire-for-using-ai-art-to-gauge-interest-in-new-mobile-games",
  "https://www.timesnownews.com/feeds/gns-en-latest.xml",
  "https://www.timesofalarab.com/uae-promises-to-invest-1-4-trillion-in-the-us-over-10-years-report/feed/"
]


def port_beans_locally():
    from coffeemaker.orchestrators.collectororch import Orchestrator
    orch = Orchestrator(
        os.getenv('MONGODB_CONN_STR'),
        "test"
    )
    local_orch = Orchestrator(
        "mongodb://localhost:27017/",
        "test"
    )
    
    batch_size = 1000
    def port(skip):
        beans = orch.db.beanstore.find(
            filter={
                K_GIST: VALUE_EXISTS,
                K_EMBEDDING: VALUE_EXISTS,
                K_CREATED: {"$gte": ndays_ago(15)}
            },
            skip=skip,
            limit=batch_size,
            projection={K_EMBEDDING: 0}
        )
        local_orch.db.beanstore.insert_many(beans, ordered=False)
        print(datetime.now(), "ported beans|%d", skip)

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        executor.map(port, range(0, 100000, batch_size))

def port_pages_locally():
    from coffeemaker.orchestrators.analyzerorch import Orchestrator
    orch = Orchestrator(
        os.getenv('DB_REMOTE'),
        "beansackV2",
        os.getenv('QUEUE_PATH_TEST'),
        "test"
    )
    local_orch = Orchestrator(
        os.getenv('DB_REMOTE_TEST'),
        "test",
        os.getenv('QUEUE_PATH_TEST'),
        "test"
    )

    pages = list(orch.db.db['baristas'].find({}))
    for page in pages:
        if K_EMBEDDING in page:
            page[K_EMBEDDING] = local_orch.embedder.embed("category/classification/domain: "+page[K_DESCRIPTION])
    local_orch.db.db['pages'].insert_many(pages)

    print(datetime.now(), "ported pages|%d", len(pages))

def port_sources_locally():
    from coffeemaker.orchestrators.collectororch import Orchestrator
    orch = Orchestrator(
        os.getenv('DB_REMOTE_TEST'),
        "test3"
    )
    local_orch = Orchestrator(
        os.getenv('DB_REMOTE_TEST'),
        "test"
    )
    sources = {}
    allowed_fields = [K_ID, K_SOURCE, K_SITE_BASE_URL, K_SITE_NAME, K_SITE_RSS_FEED, K_SITE_FAVICON, K_DESCRIPTION]

    for new in orch.db.sourcestore.find({}):
        new[K_ID] = new[K_SITE_BASE_URL]
        new[K_SITE_FAVICON] = new.get("favicon")
        new[K_SITE_RSS_FEED] = new.get('rss_feed')
        new[K_SITE_NAME] = new.get(K_SITE_NAME) or new.get(K_TITLE)
        new = {k:v for k,v in new.items() if v and (k in allowed_fields)}
        
        if new[K_ID] in sources: 
            updated = {**sources[new[K_ID]], **new}
            if any(not v for v in updated.values()): ic(updated, sources[new[K_ID]], new)
            if (K_SITE_NAME in updated) and (K_SITE_NAME in new): updated[K_SITE_NAME] = updated[K_SITE_NAME] if len(updated[K_SITE_NAME]) < len(new[K_SITE_NAME]) else new[K_SITE_NAME]
            if (K_DESCRIPTION in updated) and (K_DESCRIPTION in new): updated[K_DESCRIPTION] = updated[K_DESCRIPTION] if len(updated[K_DESCRIPTION] ) < len(new[K_DESCRIPTION]) else new[K_DESCRIPTION]
            if (K_SITE_RSS_FEED in updated) and (K_SITE_RSS_FEED in new): updated[K_SITE_RSS_FEED] = updated[K_SITE_RSS_FEED] if len(updated[K_SITE_RSS_FEED]) < len(new[K_SITE_RSS_FEED]) else new[K_SITE_RSS_FEED]
            sources[new[K_ID]] = updated
        else:
            sources[new[K_ID]] = new

    local_orch.db.sourcestore.insert_many(list(sources.values()))

def port_stuff_to_remote():
    from coffeemaker.orchestrators.collectororch import Orchestrator
    orch = Orchestrator(
        os.getenv('DB_REMOTE'),
        "test"
    )
    local_orch = Orchestrator(
        os.getenv('DB_REMOTE_TEST'),
        "test"
    )

    orch.db.pagestore.insert_many(local_orch.db.pagestore.find({}))
    orch.db.sourcestore.insert_many(local_orch.db.sourcestore.find({}))

def create_sentiments_locally():
    # from coffeemaker.orchestrators.chainable import Orchestrator
    # orch = Orchestrator(
    #     os.getenv('MONGODB_CONN_STR'),
    #     "test"
    # )
    # sentiments = orch.db.beanstore.distinct("sentiments", filter={"sentiments": VALUE_EXISTS})
    # categories = orch.db.beanstore.distinct("categories", filter={"categories": VALUE_EXISTS})
    # with open("setup/sentiments-temp.txt", "r") as file:
    #     sentiments = file.readlines()

    # sentiments = [s.strip() for s in sentiments if s.strip()]
    # for i in range(len(sentiments)):
    #     sentiments[i] = sentiments[i].split()[0]

    # sentiments = [s.strip()+"\n" for s in sentiments if s.isalpha()]
    # sentiments = list(set(sentiments))
    # with open("setup/sentiments-proc-1.txt", "w") as file:
    #     file.writelines(sentiments)

    # with open("setup/sentiments-proc-4.txt", "r") as file:
    #     sentiments = file.readlines()

    
    from coffeemaker.nlp import embedders

    embedder = embedders.from_path(os.getenv('EMBEDDER_PATH'), 512)
    
    import yaml

    with open("./coffeemaker/nlp/classifications.yaml", "r") as file:
        classifications = yaml.safe_load(file)

    sentiments = classifications['sentiments']
    vecs = embedder(["sentiment: "+s for s in sentiments])

    import pandas as pd

    df = pd.DataFrame(
        {
            K_ID: sentiments,
            K_EMBEDDING: vecs
        }
    )
    ic(df.sample(n=3))
    df.to_parquet("setup/sentiments.parquet", engine='pyarrow')
    
def create_categories_locally():  
    # with open("setup/categories-temp.txt", "r") as file:
    #     categories = file.readlines()

    # categories = [s.strip() for s in categories if s.strip()]
    # for i in range(len(categories)):
    #     categories[i] = categories[i].split("(")[0].strip()

    # cat_filter = lambda cat: cat and re.fullmatch(r"[a-zA-Z]+", cat.replace(' ', '')) and len(cat) > 3 and cat[0].isupper()
    # categories = sorted(list(set([cat+"\n" for cat in categories if cat_filter(cat)])))
    
    # with open("setup/categories-proc-1.txt", "w") as file:
    #     file.writelines(categories)

    from coffeemaker.nlp import embedders

    embedder = embedders.from_path(os.getenv('EMBEDDER_PATH'), 512)
    
    import yaml

    with open("./coffeemaker/nlp/classifications.yaml", "r") as file:
        classifications = yaml.safe_load(file)

    categories = classifications['categories']
    vecs = embedder(["domain: "+cat for cat in categories])

    import pandas as pd

    df = pd.DataFrame(
        {
            K_ID: categories,
            K_EMBEDDING: vecs
        }
    )
    ic(df.sample(n=3))
    df.to_parquet("setup/categories.parquet", engine='pyarrow')


def merge_to_classification():
    import yaml
    # with open("coffeemaker/nlp/categories.txt", "r") as file:
    #     categories = file.readlines()
    # with open("coffeemaker/nlp/sentiments.txt", "r") as file:
    #     sentiments = file.readlines()
    
    # classifications = {
    #     K_SENTIMENTS: [s.strip() for s in sentiments],
    #     K_CATEGORIES: [cat.strip() for cat in categories]
    # }

    # with open("coffeemaker/nlp/classifications.yaml", "w") as file:
    #     yaml.dump(classifications, file)

    with open("coffeemaker/nlp/classifications.yaml", "r") as file:
        classifications = yaml.safe_load(file)
    ic(classifications)


# adding data porting logic
if __name__ == "__main__":
    # merge_to_classification()
    create_categories_locally()
    create_sentiments_locally()