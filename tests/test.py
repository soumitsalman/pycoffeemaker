import os
import pytest
from icecream import ic

DB_NAME_TEST = "test3"
AZSTORAGE_PATH_TEST = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
INDEXER_IN_QUEUE = "indexing-queue"
DIGESTOR_IN_QUEUE = "digesting-queue"
COLLECTOR_OUT_QUEUES = [INDEXER_IN_QUEUE, DIGESTOR_IN_QUEUE]
EMBEDDER_CONTEXT_LEN = 512

import asyncio
import json
import random
import re
from datetime import datetime, timedelta

from pybeansack.models import *
from pybeansack import *
from pycupboard.pgcupboard import Cupboard
from pycupboard.models import ID, Sip

def url_to_filename(url: str) -> str:
    return "./.test/" + re.sub(r"[^a-zA-Z0-9]", "-", url)


def load_json(filename):
    with open(filename, "r") as file:
        return json.load(file)


def save_markdown(url, markdown):
    filename = url_to_filename(url) + ".md"
    with open(filename, "w") as file:
        file.write(markdown)


def save_json(url, items):
    filename = url_to_filename(url) + ".json"
    with open(filename, "w") as file:
        json.dump(items, file)


def save_models(items: list[Bean | Chatter], file_name: str = None):
    if items:
        with open(f".test/{file_name or items[0].source}.json", "w") as file:
            json.dump(
                [
                    item.model_dump(
                        by_alias=True,
                        exclude_none=True,
                        exclude_unset=True,
                        exclude_defaults=True,
                    )
                    for item in items
                ],
                file,
            )
        return ic(items)


@pytest.mark.integration
@pytest.mark.collect
def test_collector():
    from datacollectors import APICollectorAsync

    async def run():
        async with APICollectorAsync(batch_size=128) as col:
            items = await col.collect_subreddit("InfoSecNews")
            ic(items)
            items = await col.collect_ychackernews("https://hacker-news.firebaseio.com/v0/showstories.json")
            ic(len(items))
    asyncio.run(run())


@pytest.mark.integration
@pytest.mark.scrape
def test_scraper():
    from datacollectors import AsyncWebScraper

    urls = [
        "https://financebuzz.com/retirees-should-buy-at-bjs-4",
        "https://financebuzz.com/southern-lake-towns-afford-social-security",
        "https://financebuzz.com/professional-skills-more-valuable-after-60",
        "https://financebuzz.com/avoid-buying-rv-in-retirement",
        "https://financebuzz.com/trader-joes-pantry-items-june-2025",
        "https://financebuzz.com/cities-getting-caseys-june-2025",
        "https://financebuzz.com/costco-home-decor-guests-want-june-2025",
    ]

    async def run():
        async with AsyncWebScraper() as scraper:
            for url in urls:
                ic(await scraper.scrape_page(url))

    asyncio.run(run())


@pytest.mark.integration
@pytest.mark.scrapepubs
def test_publisher_scraper():
    from datacollectors import AsyncWebScraper
    from pybeansack.ducksack import DuckSack

    urls = [
        "https://financebuzz.com/retirees-should-buy-at-bjs-4",
        "https://financebuzz.com/southern-lake-towns-afford-social-security",
        "https://financebuzz.com/professional-skills-more-valuable-after-60",
        "https://www.cyberark.com/blog/distinguishing-authn-and-authz/",
        "http://jalammar.github.io/ai-image-generation-tools/",
        "https://aws.amazon.com/blogs/aws/introducing-aws-capabilities-by-region-for-easier-regional-planning-and-faster-global-deployments/",
        "https://www.camilleroux.com/la-veille-technologique-ma-methode-complete-pour-rester-a-jour/",
        "https://www.neelc.org/posts/freebsd-signal-proxy/",
        "https://aws.amazon.com/blogs/storage/cross-account-amazon-s3-bulk-transfers-with-enhanced-aws-kms-support/",
        "https://aws.amazon.com/blogs/storage/cross-account-disaster-recovery-setup-using-aws-elastic-disaster-recovery-in-secured-networks-part-1-architecture-and-network-setup/",
        "https://aws.amazon.com/blogs/storage/mastering-cross-account-amazon-efs-seamlessly-mount-amazon-efs-on-amazon-eks-cluster/",
    ]
    db = DuckSack(
        catalog_db=os.getenv("DUCKLAKE_CATALOG", os.getenv("PG_CONNECTION_STRING")),
        storage_path=os.getenv("DUCKLAKE_STORAGE", os.getenv("STORAGE_DATAPATH")),
    )

    async def run():
        async with AsyncWebScraper() as scraper:
            ic(await scraper.scrape_sites(urls))
            pubs = db.query_publishers(
                conditions=["rss_feed IS NULL", "favicon IS NULL", "site_name IS NULL"],
                limit=5,
            )
            ic(await scraper.scrape_publishers(pubs))

    asyncio.run(run())


@pytest.mark.integration
@pytest.mark.skip(reason="fullstack orchestrator removed; use run.py worker modes")
def test_fullstack_orch():
    raise NotImplementedError("fullstack orchestrator was removed; use run.py worker modes instead")

@pytest.mark.integration
def test_static_db():
    import pandas as pd
    from nlp import create_embedder
    from pybeansack import SimpleVectorDB

    print("starting")
    categories_df = pd.read_parquet("./factory/categories.parquet")
    sentiments_df = pd.read_parquet("./factory/sentiments.parquet")
    db = SimpleVectorDB(
        ".test/staticdb-test",
        {},
        categories=categories_df.to_dict(orient="records"),
        sentiments=sentiments_df.to_dict(orient="records"),
    )
    print("db loaded")
    embedder = create_embedder(os.getenv("EMBEDDER_PATH"), 512)
    print("embedder loaded")
    with embedder:
        ic(
            db.search(
                "categories",
                embedder.embed_query("category/domain classification: artificial intelligence"),
                distance_func="cosine",
                limit=10,
            )
        )
        ic(
            db.search(
                "sentiments",
                embedder.embed_query("sentiment classification: ecstatic"),
                distance_func="cosine",
                limit=5,
            )
        )
    db.close()


@pytest.mark.integration
@pytest.mark.orch_classifier
def test_classifier_static_label_search():
    import pandas as pd
    from workers.analyzerorch import (
        CATEGORIES,
        CLASSIFICATION_LIMIT,
        EMBEDDING,
        SENTIMENTS,
        URL,
        Classifier,
    )
    from workers.utils import BEANS, EMBEDDED

    class DummyCache:
        pass

    categories_df = pd.read_parquet("./factory/categories.parquet")
    sentiments_df = pd.read_parquet("./factory/sentiments.parquet")
    cache = _analyzer_test_cache()
    classifier = Classifier(cache=cache, cls_cache=DummyCache(), batch_size=2)

    category_embedding = categories_df[EMBEDDING].iloc[0]
    sentiment_embedding = sentiments_df[EMBEDDING].iloc[0]

    try:
        categories = classifier._label_batch_search(
            classifier.category_index,
            [category_embedding],
            CLASSIFICATION_LIMIT,
        )[0]
        sentiments = classifier._label_batch_search(
            classifier.sentiment_index,
            [sentiment_embedding],
            CLASSIFICATION_LIMIT,
        )[0]

        assert categories[0] == categories_df["category"].iloc[0]
        assert sentiments[0] == sentiments_df["sentiment"].iloc[0]

        beans = [
            bean
            for bean in cache.get(BEANS, states=EMBEDDED, limit=10)
            if bean.get(EMBEDDING)
        ]
        if not beans:
            pytest.skip("No embedded beans available in PROCESSING_CACHE")

        updates = list(classifier.classify_beans(beans))
        flat_updates = [update for chunk in updates for update in chunk]
        output_block = "\nCLASSIFIER_OUTPUT\n" + json.dumps(flat_updates, indent=2)
        print(output_block)
        assert len(flat_updates) == len(beans)
        assert {update[URL] for update in flat_updates} == {bean[URL] for bean in beans}
        assert all(update[CATEGORIES] for update in flat_updates)
        assert all(update[SENTIMENTS] for update in flat_updates)
    finally:
        cache.close()


@pytest.mark.integration
@pytest.mark.orch_collector
def test_collector_orch():
    from workers.collectororch import Collector
    from workers.workercache.pgcache import AsyncStateCache

    cache_settings = {
        BEANS: {"id_key": K_URL},
        PUBLISHERS: {"id_key": K_BASE_URL},
        CHATTERS: {"id_key": "id"}
    }
    orch = Collector(
        AsyncStateCache(os.getenv("PROCESSING_CACHE"), cache_settings),
        batch_size=16,
    )
    # sources = """/home/soumitsr/codes/pycoffeemaker/factory/feeds.yaml"""
    # sources = f"{os.path.dirname(__file__)}/sources-1.yaml"
    # sources = """
    # sources:
    #     reddit:
    #         - news
    #         - worldnews
    #         - InternationalNews
    #         - GlobalNews
    #         - GlobalMarketNews
    #         - FinanceNews
    #         - StockNews
    #         - CryptoNews
    #         - energyStocks
    #     rss:
    #         - https://newatlas.com/index.rss
    #         - https://www.channele2e.com/feed/topic/latest
    #         - https://www.ghacks.net/feed/
    #         - https://thenewstack.io/feed
    #         - https://scitechdaily.com/feed/
    #         - https://www.techradar.com/feeds/articletype/news
    #         - https://www.geekwire.com/feed/
    #         - https://investorplace.com/content-feed/
    #     ychackernews:
    #         - https://hacker-news.firebaseio.com/v0/newstories.json
    # """
    sources = """
    sources:
        reddit:
            - news
            - worldnews
            - InternationalNews
            - GlobalNews
            - GlobalMarketNews
            - FinanceNews
            - StockNews
            - CryptoNews
            - energyStocks       
    """
    # sources = """
    # sources:
    #     rss:
    #         - https://newatlas.com/index.rss
    #         - https://www.channele2e.com/feed/topic/latest
    #         - https://www.ghacks.net/feed/
    #         - https://thenewstack.io/feed
    #         - https://scitechdaily.com/feed/
    #         - https://www.techradar.com/feeds/articletype/news
    #         - https://www.geekwire.com/feed/
    #         - https://investorplace.com/content-feed/
    # """

    asyncio.run(orch.run(sources))
    

def _analyzer_test_cache():
    from workers.workercache.pgcache import StateCache

    return StateCache(
        os.getenv("PROCESSING_CACHE"),
        {
            BEANS: {"id_key": K_URL},
            PUBLISHERS: {"id_key": K_BASE_URL},
            CHATTERS: {"id_key": "id"}
        }
    )


def _analyzer_cls_cache():
    from workers.workercache.clscache import ClassificationCache

    return ClassificationCache(
        ".test/clsstore-test",
        table_settings={
            BEANS: {"id_key": K_URL, "distance_func": "l2"},
            "categories": {"id_key": "category", "distance_func": "cosine"},
            "sentiments": {"id_key": "sentiment", "distance_func": "cosine"},
        },
    )


@pytest.mark.integration
@pytest.mark.orch_embedder
def test_embedder_orch():
    from workers.analyzerorch import Embedder

    cache = _analyzer_test_cache()
    orch = Embedder(
        cache=cache,
        model_path=os.getenv(
            "EMBEDDER_PATH", "vllm://avsolatorio/GIST-small-Embedding-v0"
        ),
        context_len=int(os.getenv("EMBEDDER_CONTEXT_LEN", EMBEDDER_CONTEXT_LEN)),
    )
    orch.run(batch_size=32)
    cache.close()


@pytest.mark.integration
@pytest.mark.orch_extractor
def test_extractor_orch():
    from workers.analyzerorch import Extractor

    cache = _analyzer_test_cache()
    orch = Extractor(
        cache=cache,
        model_path=os.getenv("EXTRACTOR_PATH"),
        context_len=int(os.getenv("EXTRACTOR_CONTEXT_LEN", 4096)),
    )
    orch.run(batch_size=16)
    cache.close()


@pytest.mark.integration
@pytest.mark.orch_digestor
def test_digestor_orch():
    from workers.analyzerorch import Digestor

    cache = _analyzer_test_cache()
    orch = Digestor(
        cache=cache,
        model_path=os.getenv(
            "DIGESTOR_PATH", "vllm://LiquidAI/LFM2.5-1.2B-Instruct"
        ),
        context_len=int(os.getenv("DIGESTOR_CONTEXT_LEN", 32768)),
    )
    orch.run(batch_size=16)
    cache.close()


@pytest.mark.integration
@pytest.mark.orch_classifier
def test_classifier_orch():
    from workers.analyzerorch import Classifier

    cache = _analyzer_test_cache()
    cls_cache = _analyzer_cls_cache()
    Classifier(cache=cache, cls_cache=cls_cache).run(batch_size=32)
    cls_cache.close()
    cache.close()


@pytest.mark.integration
@pytest.mark.group_to_str
def test_group_to_str_text_lengths():
    from workers.analyzerorch import _group_to_str
    from workers.utils import BEANS, COLLECTED, DIGESTED, DIGEST

    cache = _analyzer_test_cache()
    try:
        beans = cache.get(BEANS, states=[COLLECTED, DIGESTED])
        beans = [b for b in beans if b.get(DIGEST)]
        for size in (32, 48, 64, 96):
            if len(beans) < size:
                pytest.skip(f"need at least {size} digested beans, got {len(beans)}")
            group = {"data": random.sample(beans, size)}
            text = _group_to_str(group)
            lines = text.splitlines()
            print(f"--- group size {size} (top 100 lines) ---")
            print("\n".join(lines[:100]))
            print(f"group size {size}: text len = {len(text)} ({len(lines)} lines)")
    finally:
        cache.close()


@pytest.mark.integration
@pytest.mark.orch_porter
@pytest.mark.parametrize("beansack_or_cupboard", ["beansack", "cupboard"])
def test_porter_orch(beansack_or_cupboard):
    from workers.porterorch import BeansackPorter, CupboardPorter
    from workers.utils import COMPOSITES
    from workers.workercache.pgcache import AsyncStateCache

    cache_settings = {
        BEANS: {"id_key": K_URL},
        PUBLISHERS: {"id_key": K_BASE_URL},
        CHATTERS: {"id_key": "id"},
        COMPOSITES: {"id_key": "id"}
    }
    cache = AsyncStateCache(os.getenv('PROCESSING_CACHE'), cache_settings)    
    
    if beansack_or_cupboard == "cupboard":
        db = Cupboard(os.getenv('CUPBOARD_CONNECTION_STRING'))
        async def run():
            porter = CupboardPorter(cache=cache)
            async with cache, db:
                await porter.hydrate_signals(db, "cupboarded")

        asyncio.run(run())

    elif beansack_or_cupboard == "beansack":
        db = create_client(db_type="pg", pg_connection_string=os.getenv('PG_CONNECTION_STRING'))
        async def run():
            async with cache:
                await BeansackPorter(cache=cache).hydrate_beans(db, "beansacked")
            db.close()

        asyncio.run(run())

@pytest.mark.integration
@pytest.mark.vector
def test_vector_search():
    from pycupboard.pgcupboard import Cupboard
    from pycupboard.models import URL
    from nlp import create_embedder
    from faker import Faker

    fake = Faker()

    # db = create_client(db_type="pg", pg_connection_string=os.getenv('PG_CONNECTION_STRING'))
    # Cupboard.create_db("/home/soumitsr/codes/pycoffeemaker/.test/cupboard.db")
    db = Cupboard(os.getenv('CUPBOARD_CONNECTION_STRING'))
    embedder = create_embedder("avsolatorio/GIST-small-Embedding-v0", 512)

    queries = fake.sentences(256)
    with embedder:
        vecs = embedder.embed_documents(queries)
        queries = queries
        vecs = vecs

    async def run():        
        async with db:            
            async def search(q, vec):
                result = await db.query_sips(embedding=vec, limit=10, columns=[URL])
                print("#", q, "=======")
                [print(item.url) for item in result]
            start = now()
            await asyncio.gather(*(search(q, vec) for q, vec in zip(queries, vecs)))            
            print((now() - start).total_seconds()/len(vecs))

    asyncio.run(run())


@pytest.mark.integration
@pytest.mark.cache
def test_cache():
    from workers.workercache.base import DEFAULT_WINDOW
    from workers.workercache.clscache import ClassificationCache
    import pandas as pd
    from nlp import create_embedder
    from workers.workercache.pgcache import StateCache
    from faker import Faker

    fake = Faker()
    conn_str = os.getenv("PROCESSING_CACHE")
    if not conn_str:
        raise RuntimeError("PROCESSING_CACHE must be set in .env")

    test_state = "test_cache_collected"
    exclude_state = "test_cache_beansacked"

    st_cache = StateCache(
        conn_str,
        {BEANS: {"id_key": K_URL}, PUBLISHERS: {"id_key": K_BASE_URL}},
    )
    try:
        beans = [
            {
                K_URL: fake.unique.url(),
                K_CONTENT: fake.paragraph(3),
                K_SOURCE: fake.domain_name(),
            }
            for _ in range(10)
        ]
        urls = [b[K_URL] for b in beans]
        st_cache.set(BEANS, test_state, beans)

        subset = urls[2:7]
        by_ids = st_cache.get(BEANS, test_state, exclude_state, ids=subset)
        returned_urls = {item[K_URL] for item in by_ids}
        assert returned_urls == set(subset), f"get by ids: {returned_urls} != {set(subset)}"
        ic("get by ids ok", len(by_ids))

        old_urls, recent_urls = urls[:5], urls[5:]
        with st_cache.pool.connection() as conn:
            conn.execute(
                f"UPDATE {BEANS} SET ts = CURRENT_TIMESTAMP - INTERVAL '10 days' "
                "WHERE id = ANY(%(ids)s) AND state = %(state)s",
                {"ids": old_urls, "state": test_state},
            )
        within_window = st_cache.get(
            BEANS, test_state, exclude_state, ids=urls, window=7
        )
        in_window = {item[K_URL] for item in within_window}
        assert set(recent_urls) <= in_window
        assert not set(old_urls) & in_window, f"window=7 should exclude: {old_urls & in_window}"
        ic("window=7 ok", sorted(in_window))

        wide = st_cache.get(
            BEANS, test_state, exclude_state, ids=old_urls, window=14
        )
        assert {item[K_URL] for item in wide} == set(old_urls)
        ic("window=14 ok", len(wide))

        st_cache.set(BEANS, exclude_state, beans)
        excluded = st_cache.get(
            BEANS, test_state, exclude_state, ids=urls, window=DEFAULT_WINDOW
        )
        assert not excluded
        ic("exclude_states ok")
    finally:
        st_cache.close()

    if False:
        VECTOR_LEN = 384
        cls_cache = ClassificationCache(
            ".test/cache",
            {
                BEANS: {"id_key": K_URL, "distance_func": "l2", "vector_length": VECTOR_LEN},
                "categories": {"id_key": "category", "distance_func": "cosine", "vector_length": VECTOR_LEN},
                "sentiments": {"id_key": "sentiment", "distance_func": "cosine", "vector_length": VECTOR_LEN},
            }
        )
        cls_cache.store("categories", pd.read_parquet( "/home/soumitsr/codes/pycoffeemaker-cache/factory/categories.parquet").to_dict(orient="records"))
        cls_cache.store("sentiments", pd.read_parquet("/home/soumitsr/codes/pycoffeemaker-cache/factory/sentiments.parquet").to_dict(orient="records"))
        
        categories = ["AI", "information security", "software engineering", "cloud computing", "arts and entertainment"]*100
        with create_embedder(os.getenv('EMBEDDER_PATH'), 512) as embedder:        
            embs = embedder.embed_documents(categories)
            ic(cls_cache.batch_search("categories", embs, distance=0.17, top_n=5))
        
        cls_cache.close()


@pytest.mark.integration
def test_orch_on_lancesack():
    from datacollectors import APICollector
    from nlp import Digest, create_digestor, create_embedder
    from pybeansack import create_db
    from pybeansack.lancesack import _Bean
    from workers.collectororch import parse_sources

    db = create_db("lance", lancedb_storage=".beansack/lancesack_v2")
    if False:
        feeds = parse_sources(f"{os.path.dirname(__file__)}/sources-1.yaml")
        collector = APICollector(batch_size=64)
        for rss in random.sample(feeds["rss"], 5):
            items = collector.collect_rssfeed(rss)
            ic(db.store_beans([item["bean"] for item in items if item.get("bean")]))
            ic(
                db.store_publishers(
                    [item["publisher"] for item in items if item.get("publisher")]
                )
            )
            ic(
                db.store_chatters(
                    [item["chatter"] for item in items if item.get("chatter")]
                )
            )

    if True:
        with create_embedder(
            os.getenv("EMBEDDER_PATH"), EMBEDDER_CONTEXT_LEN
        ) as embedder:
            while beans := db.query_latest_beans(
                collected=ndays_ago(2),
                conditions=["embedding IS NULL", "content_length >= 200"],
                limit=32,
                columns=[K_URL, K_CONTENT, K_SOURCE],
            ):
                vectors = embedder.embed_documents([bean.content for bean in beans])
                updates = [
                    Bean(url=bean.url, embedding=vec)
                    for bean, vec in zip(beans, vectors)
                    if vec
                ]
                ic(db.update_embeddings(updates))

    if False:
        with create_digestor(
            os.getenv("DIGESTOR_PATH"),
            context_len=4096,
            output_model=Digest,
        ) as digestor:
            while beans := db.query_latest_beans(
                collected=ndays_ago(2),
                conditions=["gist IS NULL", "content_length >= 200"],
                limit=2,
                columns=[K_URL, K_CONTENT, K_SOURCE],
            ):
                digests = digestor.run_batch([bean.content for bean in beans])
                updates = [
                    Bean(
                        url=bean.url,
                        gist=json.dumps(d.model_dump(exclude_none=True)),
                        regions=d.regions,
                        entities=[*d.people, *d.companies, *d.products, *d.stock_tickers],
                    )
                    for bean, d in zip(beans, digests)
                    if d
                ]
                ic(db.update_beans(updates, columns=[K_GIST, K_REGIONS, K_ENTITIES]))

    if True:
        print("===========")
        beans = db.query_latest_beans(
            conditions=["embedding IS NOT NULL"],
            limit=5,
            columns=[K_URL, K_TITLE, K_CREATED, K_CATEGORIES, K_SENTIMENTS],
        )
        [print(bean.created, bean.title, bean.categories) for bean in beans]

        print("===========")
        beans = db.query_aggregated_beans(conditions=["embedding IS NOT NULL"], limit=5)
        [
            print(bean.created, bean.title, bean.base_url, bean.cluster_size)
            for bean in beans
        ]

        with create_embedder(
            os.getenv("EMBEDDER_PATH"), EMBEDDER_CONTEXT_LEN
        ) as embedder:
            vec = embedder.embed_query(
                "latest trends in artificial intelligence and machine learning"
            )
            print("===========")
            beans = db.query_latest_beans(
                kind=BLOG,
                embedding=vec,
                distance=0.3,
                limit=10,
                columns=[K_URL, K_TITLE, K_CREATED, K_CATEGORIES, K_SENTIMENTS],
            )
            [print(bean.created, bean.title, bean.categories) for bean in beans]
            print("===========")
            beans = db.query_aggregated_beans(embedding=vec, distance=0.3, limit=5)
            [
                print(bean.created, bean.title, bean.base_url, bean.cluster_size)
                for bean in beans
            ]

