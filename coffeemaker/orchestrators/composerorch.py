import io
import os
import random
import yaml
import json
import numpy as np
import pandas as pd
from coffeemaker.nlp import *
from coffeemaker.pybeansack.cdnstore import CDNStore
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack.mongosack import *
from coffeemaker.orchestrators.utils import *
from azure.storage.blob import BlobType
from icecream import ic

log = logging.getLogger(__name__)

_LAST_NDAYS = 1

_SCOPE_FILTER = {
    K_GIST: VALUE_EXISTS,
    K_CREATED: {"$gte": ndays_ago(_LAST_NDAYS)}
}
_PROJECTION = {
    K_URL: 1, 
    K_EMBEDDING: 1, 
    K_GIST: 1, 
    K_ENTITIES: 1, 
    K_REGIONS: 1, 
    K_CATEGORIES: 1, 
    K_SENTIMENTS: 1, 
    K_CREATED:1
}

CLUSTER_EPS = float(os.getenv('CLUSTER_EPS', 1.4))
MIN_CLUSTER_SIZE = int(os.getenv('MIN_CLUSTER_SIZE', 24))
MAX_CLUSTER_SIZE = int(os.getenv('MAX_CLUSTER_SIZE', 128))
MAX_ARTICLES = int(os.getenv('MAX_ARTICLES', 100)) # larger number for infinite
MAX_ARTICLE_LEN = 3072

make_article_id = lambda title: re.sub(r'[^a-zA-Z0-9]', '-', f"{title}-{int(now().timestamp())}-{random.randint(1000,9999)}").lower()
def _make_bean(comp: GeneratedArticle): 
    current = now()
    bean_id = make_article_id(comp.title)
    summary = comp.verdict
    content = comp.raw
    return GeneratedBean(
        _id=bean_id,
        url=bean_id,
        kind=GENERATED,
        created=current,
        updated=current,
        collected=current,
        title=comp.title,
        summary=summary,
        content=content,
        entities=comp.keywords,
        tags=merge_tags(comp.keywords),
        num_words_in_title=num_words(comp.title),
        num_words_in_summary=num_words(summary),
        num_words_in_content=num_words(content),
        author="Barista AI",
        source="cafecito",
        site_base_url="cafecito.tech",
        site_name="Cafecito",
        site_favicon="https://cafecito.tech/images/favicon.ico",
        intro=comp.intro or None,
        analysis=comp.analysis or None,
        insights=comp.insights or None,
        verdict=comp.verdict or None,
        predictions=comp.predictions or None
    )

def _parse_topics(topics: list|dict|str):
    if isinstance(topics, list): return topics
    if isinstance(topics, dict): return list(topics.values())
    if os.path.exists(topics):
        if topics.endswith(".parquet"): return pd.read_parquet(topics).to_dict(orient="records")
        if topics.endswith(".yaml"): 
            with open(topics, 'r') as file:
                return list(yaml.safe_load(file).values())
        if topics.endswith(".json"): 
            with open(topics, 'r') as file:
                return list(json.load(file).values())
        raise ValueError(f"unsupported file type: {topics}")
    return list(yaml.safe_load(topics).values())

class Orchestrator:
    db: Beansack
    cdn = None
    cdn_folder = ""

    def __init__(self, 
        mongodb_conn_str: str, 
        db_name: str,        
        cdn_endpoint: str,
        cdn_access_key: str,
        cdn_secret_key: str,
        composer_path: str, 
        composer_base_url: str,
        composer_api_key: str,
        composer_context_len: int,
        banner_model: str, 
        banner_base_url: str,
        banner_api_key: str,
        embedder_path: str = None,
        embedder_context_len: int = 0,       
        backup_azstorage_conn_str: str = None,
        max_articles: int = MAX_ARTICLES
    ):
        self.db = Beansack(mongodb_conn_str, db_name)
        self.cdn = CDNStore(cdn_endpoint, cdn_access_key, cdn_secret_key) 
        self.max_articles = max_articles

        self.news_writer = agents.from_path(
            composer_path, composer_base_url, composer_api_key, 
            max_input_tokens=composer_context_len, max_output_tokens=MAX_ARTICLE_LEN, 
            system_prompt=NEWSRECAP_SYSTEM_PROMPT, output_parser=GeneratedArticle.parse_markdown, json_mode=False
            # system_prompt=NEWSRECAP_SYSTEM_PROMPT_JSON, output_parser=GeneratedArticle.parse_json, json_mode=True
        )
        self.blog_writer = agents.from_path(
            composer_path, composer_base_url, composer_api_key, 
            max_input_tokens=composer_context_len, max_output_tokens=MAX_ARTICLE_LEN, 
            system_prompt=OPINION_SYSTEM_PROMPT, output_parser=GeneratedArticle.parse_markdown, json_mode=False
            # system_prompt=OPINION_SYSTEM_PROMPT_JSON, output_parser=GeneratedArticle.parse_json, json_mode=True
        )
        if banner_model: self.banner_maker = SimpleImageGenerationAgent(
            banner_model, banner_base_url, banner_api_key, 
            BANNER_IMAGE_SYSTEM_PROMPT
        )
        if embedder_path: self.embedder = embedders.from_path(embedder_path, embedder_context_len)
        if backup_azstorage_conn_str: self.backup_container = initialize_azblobstore(backup_azstorage_conn_str, "composer")

    def get_beans(self, embedding: list[float] = None,  kind: str = None, tags: list[str] = None, last_ndays: int = None, limit: int = None):
        filter = _SCOPE_FILTER
        # NOTE: temporarily disabling kind filter to see what happens
        # if kind: filter[K_KIND] = lower_case(kind)
        if tags and not embedding: filter[K_TAGS] = lower_case(tags) # if embedding is provided do not use tags
        if embedding: return self.db.vector_search_beans(embedding, similarity_score=0.7, filter=filter, group_by=K_CLUSTER_ID, sort_by=TRENDING, limit=limit, project=_PROJECTION)
        else: return self.db.query_beans(filter=filter, group_by=K_CLUSTER_ID, sort_by=TRENDING, limit=limit, project=_PROJECTION)
    
    def _kmeans_cluster(self, beans)-> list[list[Bean]]:
        from sklearn.cluster import KMeans
        n_clusters = len(beans)//(MIN_CLUSTER_SIZE>>1)
        return KMeans(n_clusters=n_clusters, random_state=666)
    
    def _hdbscan_cluster(self, beans):
        from hdbscan import HDBSCAN
        return HDBSCAN(
            min_cluster_size=MIN_CLUSTER_SIZE, 
            min_samples=2, 
            max_cluster_size=MAX_CLUSTER_SIZE, 
            cluster_selection_epsilon_max=CLUSTER_EPS
        )
            
    def _dbscan_cluster(self, beans):
        from sklearn.cluster import DBSCAN
        return DBSCAN(min_samples=MIN_CLUSTER_SIZE>>1, eps=CLUSTER_EPS)
    
    def _affinity_cluster(self, beans):
        from sklearn.cluster import AffinityPropagation
        return AffinityPropagation(copy=False, damping=0.55, random_state=666)
    
    def get_clusters(self, topics = None):
        def try_get_beans(**kwargs):
            limit = kwargs.pop('limit', None)
            topic = kwargs.pop('_id', self.run_id)
            try: 
                beans = self.get_beans(embedding = kwargs.get(K_EMBEDDING), kind = kwargs.get(K_KIND), tags = kwargs.get(K_TAGS), limit=limit)
                log.info("found beans", extra={"source": topic, "num_items": len(beans)}) 
                return beans
            except Exception as e: log.warning(f"finding beans failed - {e}", extra={"source": topic, "num_items": 0})

        if topics:
            topics = _parse_topics(topics)
            clusters = run_batch(
                lambda topic: (topic[K_ID], topic.get(K_KIND, NEWS), try_get_beans(**topic, limit=MAX_CLUSTER_SIZE)), 
                topics,
                num_threads=len(topics)
            )
            clusters = list(filter(lambda x: x[2] and MIN_CLUSTER_SIZE <= len(x[2]), clusters))
        else:
            clusters = self.cluster_beans(self.get_beans(kind = NEWS), "HDBSCAN")
        return random.sample(clusters, min(self.max_articles, len(clusters)))

    def cluster_beans(self, beans: list[Bean], method: str = "KMEANS")-> list[list[Bean]]:
        if not beans: return 

        if method == "HDBSCAN": method = self._hdbscan_cluster(beans) 
        elif method == "DBSCAN": method = self._dbscan_cluster(beans) 
        elif method == "AFFINITY": method = self._affinity_cluster(beans)
        else: method = self._kmeans_cluster(beans)

        labels = method.fit_predict(np.array([bean.embedding for bean in beans]))
        clusters = {}
        for bean, label in zip(beans, labels):
            if label != -1: clusters.setdefault(label, []).append(bean)
        clusters = clusters.values()

        if clusters: log.info("found clusters", extra={'source': self.run_id, 'num_items': len(clusters)})
        else: log.info(f"no cluster found", extra={'source': self.run_id, 'num_items': 0})
        # return sorted(clusters, key=len)
        clusters = filter(lambda x: MIN_CLUSTER_SIZE <= len(x) <= MAX_CLUSTER_SIZE, clusters)
        return list(map(lambda x: (x[0].categories[0], x[0].kind, x), clusters))

    def compose_article(self, topic: str, kind: str, beans: list[Bean]):  
        import time
        time.sleep(random.randint(65, 85)) # NOTE: this is a hack to avoid rate limiting
        if not topic or not beans: return 

        input_text = f"Topic: {topic}\n\n"+"\n".join([bean.digest() for bean in beans])
        if kind == BLOG: article = self.blog_writer.run(input_text)
        else: article = self.news_writer.run(input_text)

        if not article: return 

        bean = _make_bean(article)
        try: bean.url = self.cdn.upload_article(bean.content, bean.id)
        except Exception as e: log.warning(f"article upload failed - {e}", extra={'source': bean.id, 'num_items': 1})
        self._backup_composer_response(input_text, article.raw)
        return bean
    
    def _backup_composer_response(self, input_text: str, response_text: str):
        if not self.backup_container or not response_text: return 
        trfile = "articles-"+now().strftime("%Y-%m-%d")+".jsonl"
        try: self.backup_container.upload_blob(trfile, json.dumps({'input': input_text, 'output': response_text})+"\n", BlobType.APPENDBLOB)           
        except Exception as e: log.warning(f"backup failed - {e}", extra={'source': trfile, 'num_items': 1})

    def create_banner(self, bean: Bean):
        if bean:
            image_data = self.banner_maker.run(f"\n- Title: {bean.title}\n- Keywords: {', '.join(bean.entities)}\n")
            try: bean.image_url = self.cdn.upload_image(image_data, bean.id+".png")
            except Exception as e: log.warning(f"image upload failed - {e}", extra={'source': bean.id, 'num_items': 1})
        return bean  

    def _compose_banner_and_store(self, topic: str, kind: str, beans: list[Bean]):
        bean = self.compose_article(topic, kind, beans)
        if not bean: return
        if self.banner_maker: bean = self.create_banner(bean)
        self.db.store_beans([bean])
        log.info(f"composed and stored {kind}", extra={'source': topic, 'num_items': 1})
        return bean
       
    def run(self, topics = None):
        self.run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("starting composer", extra={"source": self.run_id, "num_items": 1})

        clusters = self.get_clusters(topics)
        if not clusters: return    
        beans = map(lambda c: self._compose_banner_and_store(topic=c[0], kind=c[1], beans=c[2]), clusters)
        beans = [bean for bean in beans if bean]
        if beans: log.info("total articles", extra={'source': self.run_id, 'num_items': len(beans)})
        return beans
