## MAIN FUNC ##
from icecream import ic
import os
from dotenv import load_dotenv
    
load_dotenv()

import json
from datetime import datetime as dt
from pybeansack.beansack import *
from pybeansack.datamodels import *
from collectors import rssfeed, ychackernews, individual
from coffeemaker import orchestrator as orch
from coffeemaker.chains import *
from sklearn.cluster import DBSCAN
from pymongo import UpdateMany


def write_datamodels(items, file_name: str = None):
    if items:
        with open(f".test/{file_name or ic(items[0].source)}.json", 'w') as file:
            json.dump([bean.model_dump(exclude_unset=True, exclude_none=True) for bean in items], file)
            
def write_text(text, file_name):
    with open(f"test/{file_name}", 'w') as file:
        file.write(text)

def test_chains():
    sources = [
        # "https://dev.to/feed",
        "https://www.ghacks.net/feed/",
        "https://gearpatrol.com/feed/"
    ]
    rssfeed.collect(sources=sources, store_func=lambda beans: write_datamodels(orch._augment(beans), "TEST-CHAIN-"+beans[0].source))
   
def test_collection():
    sources = [
        # "https://dev.to/feed",
        "https://techxplore.com/rss-feed/"
        "https://spacenews.com/feed/",
        "https://crypto.news/feed/"
    ]
    rssfeed.collect(sources=sources, store_func=write_datamodels)
    # [rssfeed.collect_from(src) for src in sources]
    # ychackernews.collect(store_func=write_datamodels)

def test_search():
    write_datamodels(ic(orch.remotesack.query_unique_beans(filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "QUERY_BEANS")
    write_datamodels(ic(orch.remotesack.text_search_beans(query="kamala harris election", filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "TEXT_SEARCH")
    write_datamodels(ic(orch.remotesack.vector_search_beans(query="kamala harris election", filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID: 0})), "VECTOR_SEARCH")

def test_clustering(urls):    
    beans = orch.remotesack.get_beans(filter={K_URL: {"$in": urls}})
    dbscan = DBSCAN(eps = orch.cluster_eps, min_samples=1, metric="cosine")
    labels = dbscan.fit([bean.embedding for bean in beans]).labels_
    
    groups = {}
    for index, label in enumerate(labels):
        label = int(label)
        if label not in groups:
            groups[label] = []
        groups[label].append(index)

    make_update = lambda indexes: print("[", len(indexes), "]", beans[indexes[0]].title, "====\n", "\n".join(["\t"+beans[index].title for index in indexes]),"\n")
    [make_update(indexes) for indexes in groups.values()]
        

urls = [
    "https://scitechdaily.com/laser-view-into-the-avocado-new-method-exposes-cellular-secrets/" ,
    "https://scitechdaily.com/harnessing-the-power-of-micro-bunching-a-new-frontier-in-synchrotron-radiation/",
    "https://investorplace.com/2024/08/trip-stock-alert-whats-going-on-with-tripadvisor-today/",
    "https://qz.com/air-canada-earnings-olympics-1851615675",
    "https://www.theverge.com/2024/8/7/24215275/google-tv-streamer-chromecast-gemini-interview",
    "https://www.huffpost.com/entry/election-2024-live-updates_n_66b3ac49e4b05d0bc280816b",
    "https://www.prnewswire.com/news-releases/chipmos-reports-12-4-yoy-growth-in-july-2024-revenue-up-7-6-mom-302218606.html",
    "https://scitechdaily.com/protecting-earths-biodiversity-scientists-propose-bold-plan-to-create-moon-based-bio-vault/",
    "https://www.prnewswire.com/news-releases/bybit-report-crypto-market-rollercoaster---did-the-worst-sell-off-already-happen-302218710.html",
    "https://betanews.com/2024/08/09/bridging-the-gap-innovations-in-ai-safety-and-public-understanding/",
    "https://www.businessinsider.com/top-uber-lyft-driver-concerns-low-earnings-declining-pay-safety-2024-8",
    "https://investorplace.com/earning-results/2024/08/aclx-stock-earnings-arcellx-for-q2-of-2024/",
    "https://investorplace.com/earning-results/2024/08/alxo-stock-earnings-alx-oncology-holdings-for-q2-of-2024/",
    "https://www.businessinsider.com/rto-amazon-relocate-remote-work2024-8",
    "https://techxplore.com/news/2024-08-picotaur-unrivaled-microrobot.html",
    "https://techxplore.com/news/2024-08-survey-sexually-explicit-deepfakes.html",
    "https://www.prnewswire.com/news-releases/nike-inc-sued-for-securities-law-violations---contact-levi--korsinsky-before-august-19-2024-to-discuss-your-rights--nke-302218416.html",
    "https://www.businessinsider.com/turbulence-more-frequent-stronger-expert-aviation-explains-why-2024-7",
    "https://www.businessinsider.com/korean-air-stop-serving-instant-noodles-burns-increased-turbulence-2024-8",
    "https://www.huffpost.com/entry/ap-us-korean-air-noodles_l_66abef82e4b029f42a095bbf",
    "https://www.businessinsider.com/how-turbulence-scientist-deals-with-bumpy-flights-fear-anxiety-2024-7",
    "https://www.prnewswire.com/news-releases/subaru-of-america-reports-july-sales-increase-302212615.html",
    "https://jalopnik.com/national-park-service-will-cite-drivers-of-awd-cars-for-1851617315",
    "https://www.prnewswire.com/news-releases/empresa-nacional-del-petroleo-announces-pricing-and-final-results-of-its-cash-tender-offer-relating-to-its-3-450-notes-due-2031--302214577.html",
    "https://www.prnewswire.com/news-releases/quanta-services-announces-pricing-of-senior-notes-offering-302217241.html",
    "https://www.prnewswire.com/news-releases/noble-corporation-plc-announces-pricing-and-upsizing-of-offering-of-an-additional-800-million-principal-amount-of-8-000-senior-notes-due-2030--302218494.html",
    "https://www.prnewswire.com/news-releases/adecoagro-sa-announces-early-results-of-cash-tender-offer-for-up-to-us100-0-million-aggregate-principal-amount-of-6-000-senior-notes-due-2027--302213758.html",
    "https://www.ghacks.net/2024/07/19/google-is-shutting-down-its-url-shortener-service-goo-gl/",
    "https://dev.to/zeeshanali0704/design-a-url-shortner-tiny-url-4cb5",
    "https://kittycal.net/",
    "https://www.marktechpost.com/2024/08/08/top-calendar-tools-for-meetings-2023/",
    "https://www.marktechpost.com/2024/08/08/comparative-evaluation-of-sam2-and-sam1-for-2d-and-3d-medical-image-segmentation-performance-insights-and-transfer-learning-potential/",
    "https://www.latent.space/p/sam2",
    "https://www.marktechpost.com/2024/08/05/11-versatile-use-cases-of-metas-segment-anything-model-2-sam-2/",
    "https://www.marktechpost.com/2024/08/05/cc-sam-achieving-superior-medical-image-segmentation-with-85-20-dice-score-and-27-10-hausdorff-distance-using-convolutional-neural-network-cnn-and-vit-integration/",
    "https://blog.roboflow.com/sam-2-video-segmentation/",
    "https://ai.meta.com/sam2/",
    "https://ai.meta.com/blog/segment-anything-2/",
    "https://github.com/facebookresearch/segment-anything-2",
    "https://hackaday.com/2024/08/08/kickflips-and-buffer-slips-an-exploit-in-tony-hawks-pro-skater/",
    "https://icode4.coffee/?p=954",
    "https://www.prnewswire.com/news-releases/cloudpay-secures-120-million-funding-to-further-strengthen-its-global-customer-base-302216896.html",
    "https://techcrunch.com/2024/08/07/payoneer-is-buying-5-year-old-global-payroll-startup-skaud-for-61m-cash/",
    "https://www.prnewswire.com/news-releases/owlting-unveils-integration-with-stellar-to-support-usdc-stablecoin-on-owlpay-wallet-pro-302212686.html",
    "https://www.techradar.com/pro/good-news-your-google-meet-call-will-soon-be-able-to-take-notes-for-you",
    "https://www.wired.com/story/does-jewelry-slow-down-olympic-runners/",
    "https://www.huffpost.com/entry/shacarri-richardson-heat-paris-olympics_n_66acac64e4b0bc1c990d25bd",
    "https://futurism.com/neoscope/microplastics-cancer-young-people",
    "https://scitechdaily.com/new-study-reveals-disturbing-rise-in-cancer-among-gen-x-and-millennials/",
    "https://www.yahoo.com/lifestyle/cancer-rates-in-millennials-gen-x-ers-have-risen-starkly-in-recent-years-study-finds-experts-have-1-prime-suspect-223840496.html",
    "https://www.prnewswire.com/news-releases/senestech-to-report-second-quarter-2024-financial-results-on-thursday-august-8-2024-302211544.html",
    "https://www.prnewswire.com/news-releases/senestech-announces-second-quarter-2024-financial-results-302218295.html",
    "https://thenewstack.io/get-certified-in-platform-engineering-starting-aug-6/",
    "https://dev.to/danielbryantuk/what-is-platform-decay-and-why-should-platform-engineers-care-12o9",
    "https://thenewstack.io/platform-owners-must-master-platform-optimization-to-drive-innovation/"
]

orch.initialize(
    os.getenv("DB_CONNECTION_STRING"), 
    "/workspaces/coffeemaker-2/pycoffeemaker", 
    os.getenv("EMBEDDER_FILE"),
    os.getenv("GROQ_API_KEY"),    
    float(0.105),
    float(os.getenv('CATEGORY_EPS')))
test_clustering(urls)
    
# beans = []
# def collect_beans(new_items):    
#     if isinstance(new_items[0], Bean):
#         beans.extend(orch._augment(orch._download_beans(new_items)))
#         print("feed collection finished")
#     elif isinstance(new_items[0], tuple):
#         beans.extend(orch._augment([item[0] for item in new_items]))
#         print("yc collection finished")
# yc_urls = [
#     "41163382",
#     "41182823",
#     "41130620",
#     "41127706",
#     "41171060",
#     "41150317",
#     "41154135",
#     "41165255",
#     "41156872",
#     "41144755"
# ]        

# feeds = [
#     "https://www.theverge.com/rss/index.xml",
#     "https://scitechdaily.com/feed/",
#     # "https://qz.com/rss",
#     "https://chaski.huffpost.com/us/auto/vertical/politics"
# ]
# # rssfeed.collect(feeds, collect_beans)
# collect_beans([ychackernews._extract(int(id), int(dt.now().timestamp())) for id in yc_urls])
# write_datamodels(beans, "DEBUGGING-DATA-2")

### TEST CALLS
# test_writing()
# test_chains()
# test_collection_local()
# test_collection_live()
# test_rectify_beansack()
# test_search()


