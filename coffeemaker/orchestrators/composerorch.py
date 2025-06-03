import json
import os
import random

import numpy as np
from sklearn.cluster import KMeans
from coffeemaker.nlp.src.prompts import *
from coffeemaker.nlp.src.models import *
from coffeemaker.nlp.src.utils import *
from coffeemaker.nlp.src.agents import *
from coffeemaker.nlp.src import embedders
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack.mongosack import *
from coffeemaker.orchestrators.utils import *
from azure.storage.blob import BlobType
from icecream import ic

log = logging.getLogger(__name__)

MAX_TOPICS =  int(os.getenv('MAX_TOPICS', 32))
MAX_ARTICLES = int(os.getenv('MAX_ARTICLES', MAX_TOPICS))
MAX_ARTICLE_LEN = 2048

LAST_NDAYS = 1
MIN_BEANS_THRESHOLD = 8
BEAN_FILTER = {
    K_GIST: {"$exists": True}, 
    K_EMBEDDING: {"$exists": True},  
    K_CREATED: {"$gte": ndays_ago(LAST_NDAYS)}
}
BEAN_PROJECT = {K_URL: 1, K_EMBEDDING: 1, K_GIST: 1}
# _PAGES = ["Cybersecurity", "Artificial Intelligence", "Government", "Innovation and Startups" ]

make_article_id = lambda title, current: title.lower().replace(' ', '-')+current.strftime("-%Y-%m-%d-%H-%M-%S")+".md"
def _make_bean(comp: GeneratedArticle): 
    current = now()
    bean_id = make_article_id(comp.title, current)
    summary = "\n\n".join(comp.intro)
    content = "\n\n".join(comp.intro+comp.insights+comp.analysis+comp.verdict)
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
        tags=[kw.lower() for kw in comp.keywords] if comp.keywords else None,
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
        verdict=comp.verdict
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
        self.news_writer = from_path(composer_path, composer_base_url, composer_api_key, max_input_tokens=composer_context_len, max_output_tokens=MAX_ARTICLE_LEN, system_prompt=NEWRECAPT_SYSTEM_PROMPT, output_parser=GeneratedArticle.parse_markdown, json_mode=False)
        self.blog_writer = from_path(composer_path, composer_base_url, composer_api_key, max_input_tokens=composer_context_len, max_output_tokens=MAX_ARTICLE_LEN, system_prompt=OLD_OPINION_COMPOSER_SYSTEM_PROMPT, output_parser=GeneratedArticle.parse_markdown, json_mode=False)

        if embedder_path: self.embedder = embedders.from_path(embedder_path, embedder_context_len)
        if backup_azstorage_conn_str: self.backup_container = initialize_azblobstore(backup_azstorage_conn_str, "composer")

    def _get_beans(self, query: str = None, embedding: list[float] = None,  filter: dict = None):
        if filter: filter.update(BEAN_FILTER)
        else: filter = BEAN_FILTER

        if query: embedding = self.embedder.embed(query)

        if embedding: beans = self.db.vector_search_beans(embedding, filter=filter, project=BEAN_PROJECT)
        else: beans = self.db.query_beans(filter, project=BEAN_PROJECT)
        return beans if len(beans) >= MIN_BEANS_THRESHOLD else None
    
    def extract_clusters(self, beans):
        if not beans: return 

        log.info("found beans", extra={"source": self.run_id, "num_items": len(beans)})
        # Step 1: Prepare embeddings matrix
        embeddings = np.array([bean.embedding for bean in beans])
        # Step 2: Fit KMeans
        kmeans = KMeans(n_clusters=MAX_TOPICS, random_state=666, n_init='auto')
        labels = kmeans.fit_predict(embeddings)        
        # Step 3: Group beans by cluster
        clusters = [[] for _ in range(MAX_TOPICS)]
        for bean, label in zip(beans, labels):
            clusters[label].append(bean)        
        # Step 4: Sort clusters by size descending
        clusters = sorted(filter(lambda x: len(x) >= MIN_BEANS_THRESHOLD, clusters), key=len, reverse=True)

        if clusters: log.info("found topics", extra={'source': self.run_id, 'num_items': len(clusters)})
        else: log.info(f"no topic found", extra={'source': self.run_id, 'num_items': 0})
        return clusters
            
    def compose_article(self, beans: list, kind = NEWS):  
        if not beans: return 
        # NOTE: this is an internal hack to take random items when there are too many items
        if len(beans) > 512: beans = random.sample(beans, 512) 
        input_text = "\n".join([bean.gist.replace(' ', '') for bean in beans])
        if kind == BLOG: response = self.blog_writer.run(input_text)
        else: response = self.news_writer.run(input_text)

        self.backup_response("articles", input_text, response.raw)
        return response

    def backup_response(self, prefix: str, input_text: str, response_text: str):
        if not self.backup_container or not response_text: return 
        trfile = prefix+"-"+now().strftime("%Y-%m-%d")+".jsonl"
        try: self.backup_container.upload_blob(trfile, json.dumps({'input': input_text, 'output': response_text})+"\n", BlobType.APPENDBLOB)           
        except Exception as e:
            ic(e) 
            log.warning("backup failed", extra={'source': trfile, 'num_items': 1})

    def store_beans(self, responses: list):      
        if not responses: return

        beans = list(map(_make_bean, responses))
        batch_run(lambda bean: self.backup_container.upload_blob(bean.url, bean.content, BlobType.BLOCKBLOB), beans)
        count = self.db.store_beans(beans)
        log.info("stored articles", extra={'source': self.run_id, 'num_items': count})
        return beans
       
    def run(self):
        self.run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("starting composer", extra={"source": self.run_id, "num_items": 1})

        beans = self._get_beans(None, None, filter={K_KIND: NEWS})
        clusters = self.extract_clusters(beans)
        if not clusters: return    

        num_articles = min(MAX_ARTICLES, len(clusters))
        # articles = batch_run(self.compose_article, random.sample(clusters, num_articles))
        articles = list(map(self.compose_article, random.sample(clusters, num_articles)))
        self.store_beans(articles)
