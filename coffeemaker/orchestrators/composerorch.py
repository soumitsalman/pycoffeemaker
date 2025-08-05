from concurrent.futures import ThreadPoolExecutor
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
def _make_bean(article: GeneratedArticle): 
    current = now()
    bean_id = make_article_id(article.title)
    return GeneratedBean(
        _id=bean_id,
        url=bean_id,
        kind=GENERATED,
        created=current,
        updated=current,
        collected=current,
        title=article.title,
        summary=article.summary,
        content=article.raw,
        entities=article.keywords,
        tags=merge_tags(article.keywords),
        num_words_in_title=num_words(article.title),
        num_words_in_summary=num_words(article.summary),
        num_words_in_content=num_words(article.raw),
        author="Barista AI",
        source="cafecito",
        site_base_url="cafecito.tech",
        site_name="Cafecito",
        site_favicon="https://cafecito.tech/images/favicon.ico",
        intro=article.intro or None,
        analysis=article.analysis or None,
        insights=article.insights or None,
        verdict=article.summary or None,
        predictions=article.predictions or None
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
    reporter = None
    editor = None
    highlighter = None
    embedder = None
    backup_container = None

    def __init__(self, 
        mongodb_conn_str: str, 
        db_name: str,        
        cdn_endpoint: str,
        cdn_access_key: str,
        cdn_secret_key: str,
        composer_path: str, 
        composer_base_url: str = None,
        composer_api_key: str = None,
        composer_context_len: int = 0,
        banner_model: str = None, 
        banner_base_url: str = None,
        banner_api_key: str = None,
        embedder_path: str = None,
        embedder_context_len: int = 0,       
        backup_azstorage_conn_str: str = None,
        max_articles: int = MAX_ARTICLES
    ):
        self.db = Beansack(mongodb_conn_str, db_name)
        self.cdn = CDNStore(cdn_endpoint, cdn_access_key, cdn_secret_key) 
        self.max_articles = max_articles
        self.run_id = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

        # self.news_writer = agents.from_path(
        #     composer_path, composer_base_url, composer_api_key, 
        #     max_input_tokens=composer_context_len, max_output_tokens=MAX_ARTICLE_LEN, 
        #     system_prompt=NEWSRECAP_SYSTEM_PROMPT, output_parser=GeneratedArticle.parse_markdown, json_mode=False
        #     # system_prompt=NEWSRECAP_SYSTEM_PROMPT_JSON, output_parser=GeneratedArticle.parse_json, json_mode=True
        # )
        # self.blog_writer = agents.from_path(
        #     composer_path, composer_base_url, composer_api_key, 
        #     max_input_tokens=composer_context_len, max_output_tokens=MAX_ARTICLE_LEN, 
        #     system_prompt=OPINION_SYSTEM_PROMPT, output_parser=GeneratedArticle.parse_markdown, json_mode=False
        #     # system_prompt=OPINION_SYSTEM_PROMPT_JSON, output_parser=GeneratedArticle.parse_json, json_mode=True
        # )
        self.reporter = agents.from_path(
            composer_path, composer_base_url, composer_api_key, 
            max_input_tokens=composer_context_len, max_output_tokens=MAX_ARTICLE_LEN, 
            system_prompt=JOURNALIST_SYSTEM_PROMPT, json_mode=False
        )
        self.editor = agents.from_path(
            composer_path, composer_base_url, composer_api_key, 
            max_input_tokens=MAX_ARTICLE_LEN, max_output_tokens=MAX_ARTICLE_LEN, 
            system_prompt=EDITOR_SYSTEM_PROMPT, output_parser=cleanup_markdown, json_mode=False
        )
        self.highlighter = agents.from_path(
            composer_path, composer_base_url, composer_api_key, 
            max_input_tokens=MAX_ARTICLE_LEN, max_output_tokens=MAX_ARTICLE_LEN, 
            system_prompt=HIGHLIGHTER_SYSTEM_PROMPT, output_parser=GeneratedArticle.parse_json, json_mode=True
        )

        if banner_model: self.banner_maker = agents.image_agent_from_path(banner_model, banner_base_url, banner_api_key)
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
            clusters = map(
                lambda topic: (topic[K_ID], topic.get(K_KIND, NEWS), try_get_beans(**topic, limit=MAX_CLUSTER_SIZE)), 
                topics
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
        if not topic or not beans: return 

        prompt = f"Topic: {topic}\n\n"+"\n".join([bean.digest() for bean in beans])
        draft = self.reporter.run(prompt)
        with ThreadPoolExecutor(max_workers=3) as executor:
            content = executor.submit(self.editor.run, draft)
            highlights = executor.submit(self.highlighter.run, draft)
            
        article = highlights.result()
        article.raw = content.result()
        self._backup_composer_response(prompt, article.raw)
            
        return article

    def save_article(self, article: GeneratedArticle):
        bean = _make_bean(article)
        try: bean.url = self.cdn.upload_article(bean.content, bean.id)
        except Exception as e: log.warning(f"article upload failed - {e}", extra={'source': bean.id, 'num_items': 1})  
        return bean
    
    def _backup_composer_response(self, input_text: str, response_text: str):
        if not self.backup_container or not response_text: return 
        trfile = "articles-"+now().strftime("%Y-%m-%d")+".jsonl"
        try: self.backup_container.upload_blob(trfile, json.dumps({'input': input_text, 'output': response_text})+"\n", BlobType.APPENDBLOB)           
        except Exception as e: log.warning(f"backup failed - {e}", extra={'source': trfile, 'num_items': 1})

    def create_banner(self, bean: Bean):
        if not bean: return

        user_prompt = bean.title if bean.title else ", ".join(bean.entities)
        image_data = self.banner_maker.run(user_prompt)
        try: bean.image_url = self.cdn.upload_image(image_data, bean.id+".png")
        except Exception as e: log.warning(f"image upload failed - {e}", extra={'source': bean.id, 'num_items': 1, 'prompt': user_prompt})
        return bean  

    def _compose_banner_and_store(self, topic: str, kind: str, beans: list[Bean]):
        article = self.compose_article(topic, kind, beans)
        bean = self.save_article(article)
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

    
