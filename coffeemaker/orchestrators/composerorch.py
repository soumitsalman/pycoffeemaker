import json
import os
import random

import numpy as np
from coffeemaker.nlp import OPINION_SYSTEM_PROMPT, NEWSRECAP_SYSTEM_PROMPT, agents, embedders, SimpleAgent, GeneratedArticle, batch_run
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack.mongosack import *
from coffeemaker.orchestrators.utils import *
from icecream import ic

log = logging.getLogger(__name__)

CLUSTER_EPS = float(os.getenv('CLUSTER_EPS', 1.4))
MIN_CLUSTER_SIZE = int(os.getenv('MIN_CLUSTER_SIZE', 24))
MAX_CLUSTER_SIZE = int(os.getenv('MAX_CLUSTER_SIZE', 128))
MAX_ARTICLES = int(os.getenv('MAX_ARTICLES', 8))
MAX_ARTICLE_LEN = 2048

LAST_NDAYS = 1
BEAN_FILTER = {
    K_GIST: {"$exists": True}, 
    K_EMBEDDING: {"$exists": True},  
    K_CREATED: {"$gte": ndays_ago(LAST_NDAYS)}
}
BEAN_PROJECT = {
    K_URL: 1, 
    K_EMBEDDING: 1, 
    K_GIST: 1, 
    K_ENTITIES: 1, 
    K_REGIONS: 1, 
    K_CATEGORIES: 1, 
    K_SENTIMENTS: 1, 
    K_CREATED:1
}

make_article_id = lambda title, current: title.lower().replace(' ', '-')+current.strftime("-%Y-%m-%d-%H-%M-%S")+".md"
def _make_bean(comp: GeneratedArticle): 
    current = now()
    bean_id = make_article_id(comp.title, current)
    summary = "\n\n".join(comp.verdict)
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
        intro=comp.intro,
        analysis=comp.analysis,
        insights=comp.insights,
        verdict=comp.verdict,
        predictions=comp.predictions
    )

class Orchestrator:
    db: Beansack
    news_writer: SimpleAgent
    blog_writer: SimpleAgent

    embedder: embedders.Embeddings
    backup_container: ContainerClient

    def __init__(self, 
        mongodb_conn_str: str, 
        db_name: str, 
        composer_path: str, 
        composer_base_url: str,
        composer_api_key: str,
        composer_context_len: int,
        embedder_path: str = None,
        embedder_context_len: int = 0,
        backup_azstorage_conn_str: str = None
    ):
        self.db = Beansack(mongodb_conn_str, db_name)
        self.news_writer = agents.from_path(composer_path, composer_base_url, composer_api_key, max_input_tokens=composer_context_len, max_output_tokens=MAX_ARTICLE_LEN, system_prompt=NEWSRECAP_SYSTEM_PROMPT, output_parser=GeneratedArticle.parse_markdown, json_mode=False)
        self.blog_writer = agents.from_path(composer_path, composer_base_url, composer_api_key, max_input_tokens=composer_context_len, max_output_tokens=MAX_ARTICLE_LEN, system_prompt=OPINION_SYSTEM_PROMPT, output_parser=GeneratedArticle.parse_markdown, json_mode=False)

        if embedder_path: self.embedder = embedders.from_path(embedder_path, embedder_context_len)
        if backup_azstorage_conn_str: self.backup_container = initialize_azblobstore(backup_azstorage_conn_str, "composer")

    def get_beans(self, query: str = None, embedding: list[float] = None,  filter: dict = None):
        if filter: filter.update(BEAN_FILTER)
        else: filter = BEAN_FILTER

        if query: embedding = self.embedder.embed(query)

        if embedding: beans = self.db.vector_search_beans(embedding, filter=filter, project=BEAN_PROJECT)
        else: beans = self.db.query_beans(filter, project=BEAN_PROJECT)
        log.info("found beans", extra={"source": self.run_id, "num_items": len(beans)})
        return beans if len(beans) >= MIN_CLUSTER_SIZE else None
    
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
        return list(filter(lambda x: MIN_CLUSTER_SIZE <= len(x) <= MAX_ARTICLE_LEN, clusters))

    def compose_article(self, beans: list[Bean], kind = NEWS):  
        if not beans: return 
        # NOTE: this is an internal hack to take random items when there are too many items
        if len(beans) > 512: beans = random.sample(beans, 512) 
        input_text = "\n".join([bean.digest() for bean in beans])
        if kind == BLOG: response = self.blog_writer.run(input_text)
        else: response = self.news_writer.run(input_text)

        self.backup_response("articles", input_text, response.raw)
        return response

    def backup_response(self, prefix: str, input_text: str, response_text: str):
        if not self.backup_container or not response_text: return 
        trfile = prefix+"-"+now().strftime("%Y-%m-%d")+".jsonl"
        try: self.backup_container.upload_blob(trfile, json.dumps({'input': input_text, 'output': response_text})+"\n", BlobType.APPENDBLOB)           
        except Exception as e: log.warning("backup failed", extra={'source': trfile, 'num_items': 1})

    def store_beans(self, responses: list):      
        if not responses: return

        beans = list(map(_make_bean, responses))
        batch_upload(self.backup_container, beans)
        count = self.db.store_beans(beans)
        log.info("stored articles", extra={'source': self.run_id, 'num_items': count})
        return beans
       
    def run(self):
        self.run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("starting composer", extra={"source": self.run_id, "num_items": 1})

        beans = self.get_beans(None, None, filter={K_KIND: NEWS})
        clusters = self.cluster_beans(beans, "HDBSCAN")
        if not clusters: return    

        num_articles = min(MAX_ARTICLES, len(clusters))
        articles = batch_run(self.compose_article, random.sample(clusters, num_articles))
        # articles = list(map(self.compose_article, random.sample(clusters, num_articles)))
        self.store_beans(articles)
