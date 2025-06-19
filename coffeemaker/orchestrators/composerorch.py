import os
import random
import yaml
import json
import numpy as np
from coffeemaker.nlp import OPINION_SYSTEM_PROMPT, NEWSRECAP_SYSTEM_PROMPT, OPINION_SYSTEM_PROMPT_JSON, NEWSRECAP_SYSTEM_PROMPT_JSON, agents, embedders, GeneratedArticle, run_batch
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack.mongosack import *
from coffeemaker.orchestrators.utils import *
from icecream import ic

log = logging.getLogger(__name__)

CLUSTER_EPS = float(os.getenv('CLUSTER_EPS', 1.4))
MIN_CLUSTER_SIZE = int(os.getenv('MIN_CLUSTER_SIZE', 24))
MAX_CLUSTER_SIZE = int(os.getenv('MAX_CLUSTER_SIZE', 128))
MAX_ARTICLES = int(os.getenv('MAX_ARTICLES', 8))
MAX_ARTICLE_LEN = 3072

LAST_NDAYS = 1
BEAN_FILTER = {
    K_GIST: {"$exists": True}, 
    # K_EMBEDDING: {"$exists": True},  
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

make_article_id = lambda title, current: title.lower().replace(' ', '-')+current.strftime("-%Y-%m-%d-%H-%M-%S-")+str(random.randint(1000,9999))+".md"
def _make_bean(comp: GeneratedArticle): 
    current = now()
    bean_id = make_article_id(comp.title, current)
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

def _parse_topics(topics):
    if isinstance(topics, dict): return topics
    if os.path.exists(topics):
        with open(topics, 'r') as file:
            return yaml.safe_load(file)
    else: return yaml.safe_load(topics)

class Orchestrator:
    db: Beansack

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
        if embedder_path: self.embedder = embedders.from_path(embedder_path, embedder_context_len)
        if backup_azstorage_conn_str: self.backup_container = initialize_azblobstore(backup_azstorage_conn_str, "composer")

    def get_beans(self, query: str = None, embedding: list[float] = None,  kind: str = None, tags: list[str] = None, last_ndays: int = None, limit: int = None):
        filter = BEAN_FILTER
        if kind: filter[K_KIND] = lower_case(kind)
        # if last_ndays: filter.update(created_after(last_ndays=last_ndays))

        if query: embedding = self.embedder("topic: "+ (query+": "+", ".join(tags) if tags else query))
        elif tags: filter[K_TAGS] = lower_case(tags)
        
        if embedding: beans = self.db.vector_search_beans(embedding, similarity_score=DEFAULT_VECTOR_SEARCH_SCORE, filter=filter, group_by=K_CLUSTER_ID, sort_by=TRENDING, limit=limit, project=BEAN_PROJECT)
        else: beans = self.db.query_beans(filter=filter, group_by=K_CLUSTER_ID, sort_by=TRENDING, limit=limit, project=BEAN_PROJECT)
        log.info("found beans", extra={"source": query or self.run_id, "num_items": len(beans)})
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
    
    def get_topic_clusters(self, topics = None):
        if topics:
            topics = _parse_topics(topics)
            clusters = map(lambda topic: (topic[0], topic[1][K_KIND], self.get_beans(query = topic[0], kind = topic[1][K_KIND], tags = topic[1][K_TAGS], limit=MAX_CLUSTER_SIZE)), topics.items())
            return list(filter(lambda x: x[2] and len(x[2]) >= MIN_CLUSTER_SIZE, clusters))
        else:
            beans = self.get_beans(kind = NEWS)
            return self.cluster_beans(beans, "HDBSCAN")

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
        clusters = filter(lambda x: MIN_CLUSTER_SIZE <= len(x) <= MAX_ARTICLE_LEN, clusters)
        return list(map(lambda x: (x[0].categories[0], x[0].kind, x), clusters))

    def compose_article(self, topic: str, kind: str, beans: list[Bean]):  
        import time
        time.sleep(65) # NOTE: this is a hack to avoid rate limiting
        if not topic or not beans: return 

        input_text = f"Topic: {topic}\n\n"+"\n".join([bean.digest() for bean in beans])
        if kind == BLOG: response = self.blog_writer.run(input_text)
        else: response = self.news_writer.run(input_text)

        if response: self._backup_article(input_text, response.raw)
        return response

    def _backup_article(self, input_text: str, response_text: str):
        if not self.backup_container or not response_text: return 
        trfile = "articles-"+now().strftime("%Y-%m-%d")+".jsonl"
        try: self.backup_container.upload_blob(trfile, json.dumps({'input': input_text, 'output': response_text})+"\n", BlobType.APPENDBLOB)           
        except Exception as e: log.warning(f"backup failed - {e}", extra={'source': trfile, 'num_items': 1})

    def store_beans(self, responses: list):      
        if not responses: return
        beans = list(map(_make_bean, (r for r in responses if r)))
        batch_upload(self.backup_container, beans)
        count = self.db.store_beans(beans)
        log.info("stored articles", extra={'source': self.run_id, 'num_items': count})
        return beans
       
    def run(self, topics = None):
        self.run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("starting composer", extra={"source": self.run_id, "num_items": 1})

        clusters = self.get_topic_clusters(topics)
        if not clusters: return    
        clusters = random.sample(clusters, min(MAX_ARTICLES, len(clusters)))
        articles = list(map(lambda c: self.compose_article(topic=c[0], kind=c[1], beans=c[2]), clusters))
        # articles = batch_run(lambda c: self.compose_article(topic=c[0], kind=c[1], beans=c[2]), clusters)
        return self.store_beans(articles)
