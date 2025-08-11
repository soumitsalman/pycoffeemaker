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
_SIMILARITY_SCORE = 0.73
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
BATCH_SIZE = 16

make_article_id = lambda title: re.sub(r'[^a-zA-Z0-9]', '-', f"{title}-{int(now().timestamp())}-{random.randint(1000,9999)}").lower()
def _make_bean(article: ArticleMetadata, content: str, banner_url: str): 
    current = now()
    bean_id = make_article_id(article.headline)
    return GeneratedBean(
        _id=bean_id,
        url=bean_id,
        kind=GENERATED,
        created=current,
        updated=current,
        collected=current,
        title=article.headline,
        summary=article.summary or article.intro,
        content=content,
        image_url=banner_url,
        entities=article.keywords,
        tags=merge_tags(article.keywords),
        num_words_in_title=num_words(article.headline),
        num_words_in_summary=num_words(article.summary or article.intro),
        num_words_in_content=num_words(content),
        author="Barista AI",
        source="cafecito",
        site_base_url="cafecito.tech",
        site_name="Cafecito",
        site_favicon="https://cafecito.tech/images/favicon.ico",
        intro=article.intro or article.summary,
        highlights=article.highlights or None,
        insights=article.insights or None,
        verdict=article.summary or None,
        predictions=article.predictions or None
    )

def _process_banner(banner):
    if hasattr(banner, "save"): 
        filename = ".cache/"+re.sub(r'[^a-zA-Z0-9]', '-', f"banner-{int(now().timestamp())}-{random.randint(1000,9999)}").lower()+".png"
        banner.save(filename)
        return filename
    return banner

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
    journalist = None
    editor = None
    extractor = None
    embedder = None
    backup_container = None

    def __init__(self, 
        mongodb_conn_str: str, 
        db_name: str,        
        cdn_endpoint: str,
        cdn_access_key: str,
        cdn_secret_key: str,
        composer_path: str, 
        extractor_path: str,
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

        client = agents.text_generator_client(composer_path, composer_base_url, composer_api_key, composer_context_len, MAX_ARTICLE_LEN)
        self.journalist = agents.TextGeneratorAgent(client, composer_context_len, JOURNALIST_SYSTEM_PROMPT, lambda x: x.strip())
        self.editor = agents.TextGeneratorAgent(client, composer_context_len, EDITOR_SYSTEM_PROMPT, cleanup_markdown)
        self.extractor = agents.text_generator_agent(composer_path, composer_base_url, composer_api_key, composer_context_len, MAX_ARTICLE_LEN, SUMMARIZER_SYSTEM_PROMPT, ArticleMetadata.parse_json, True)

        if banner_model: self.banner_maker = agents.image_agent_from_path(banner_model, banner_base_url, banner_api_key, _process_banner, num_inference_steps=40, height=512, width=1024)
        if embedder_path: self.embedder = embedders.from_path(embedder_path, embedder_context_len)
        if backup_azstorage_conn_str: self.backup_container = initialize_azblobstore(backup_azstorage_conn_str, "composer")

    def get_beans(self, embedding: list[float] = None,  kind: str = None, tags: list[str] = None, last_ndays: int = None, limit: int = None):
        filter = _SCOPE_FILTER
        if kind: filter[K_KIND] = lower_case(kind)
        if tags and not embedding: filter[K_TAGS] = lower_case(tags) # if embedding is provided do not use tags
        if embedding: return self.db.vector_search_beans(embedding, similarity_score=_SIMILARITY_SCORE, filter=filter, sort_by=BY_TRENDSCORE, limit=limit, project=_PROJECTION)
        else: return self.db.query_beans(filter=filter, sort_by=BY_TRENDSCORE, limit=limit, project=_PROJECTION)
    
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
        clusters = filter(lambda x: MIN_CLUSTER_SIZE <= len(x) <= MAX_CLUSTER_SIZE, clusters)
        return list(map(lambda x: (x[0].categories[0], x[0].kind, x), clusters))
    
    def stage0_get_clusters(self, topics = None):
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
            beans_list = run_batch(lambda topic: try_get_beans(**topic, limit=MAX_CLUSTER_SIZE), topics, len(topics))
            clusters = [(topic, beans) for topic, beans in zip(topics, beans_list) if beans and MIN_CLUSTER_SIZE <= len(beans)]
        else:
            clusters = self.cluster_beans(self.get_beans(kind = NEWS), "HDBSCAN")

        log.info("found clusters", extra={'source': self.run_id, 'num_items': len(clusters)})
        return clusters

    def stage1_create_drafts(self, topic: dict, beans: list[Bean]):
        batches = [beans[i:i+MIN_CLUSTER_SIZE] for i in range(0, len(beans), MIN_CLUSTER_SIZE) if i+(MIN_CLUSTER_SIZE>>1) <= len(beans)]
        if len(batches) <= 0: return
        try:
            drafts = self.journalist.run_batch([f"Topic: {topic[K_ID]}\n\n"+"\n".join([b.digest() for b in batch]) for batch in batches])
            log.info("created drafts", extra={'source': topic[K_ID], 'num_items': len(drafts)})
            return drafts
        except Exception as e: log.warning(f"drafts creation failed - {e}", extra={'source': topic[K_ID], 'num_items': len(batches)})
    
    def stage2_create_metadata(self, topic: dict, drafts: list[str]):
        drafts_text = "\n-------- DRAFT --------\n".join(drafts)
        try:
            metadata = self.extractor.run(f"Topic: {topic[K_ID]}\n\n{drafts_text}")
            log.info("created metadata", extra={'source': topic[K_ID], 'num_items': 1})
            return metadata
        except Exception as e: log.warning(f"metadata creation failed - {e}", extra={'source': topic[K_ID], 'num_items': len(drafts)})

    def stage3_create_content(self, topic: dict, metadata: ArticleMetadata, drafts: list[str]):
        drafts_text = "\n-------- DRAFT --------\n".join(drafts)    
        prompt = f"Topic: {topic[K_ID]}\n\nHeadline: {metadata.headline}\n\nCurrentDate: {now().strftime('%Y-%m-%d')}\n\n{drafts_text}"
        try:
            article = self.editor.run(prompt)
            log.info("created content", extra={'source': topic[K_ID], 'num_items': 1})
            return article
        except Exception as e: log.warning(f"content creation failed - {e}", extra={'source': topic[K_ID], 'num_items': 1})

    

    def stage4_create_bean(self, topic: dict, metadata: ArticleMetadata, content: str):
        bean = _make_bean(metadata, content, None)
        try: bean.url = self.cdn.upload_article(bean.content, bean.id)
        except Exception as e: log.warning(f"article store/upload failed - {e}", extra={'source': topic[K_ID], 'num_items': 1})  
        return bean

    def stage5_create_banner(self, bean: Bean):
        result = self.banner_maker.run(f"Create a realistic image depicting: {bean.title}")
        log.info("created banner", extra={'source': bean.id, 'num_items': 1})
        bean.image_url = self.cdn.upload_image_file(result, bean.id+".png")
        log.info("uploaded banner", extra={'source': self.run_id, 'num_items': 1})
        return bean

    def stage5_batch_create_banner(self, beans: list[Bean]):
        local_files = self.banner_maker.run_batch([f"Create a realistic image depicting: {bean.title}" for bean in beans])
        log.info("created banners", extra={'source': self.run_id, 'num_items': len(local_files)})
        banner_urls = run_batch(self.cdn.upload_image_file, local_files, len(beans))
        for bean, banner_url in zip(beans, banner_urls):
            bean.image_url = banner_url
        log.info("uploaded banners", extra={'source': self.run_id, 'num_items': len(banner_urls)})
        return beans    

    # def compose_article(self, topic: str, kind: str, beans: list[Bean]):  
    #     if not topic or not beans: return 

    #     prompt = f"Topic: {topic}\n\n"+"\n".join([bean.digest() for bean in beans])
    #     draft = self.journalist.run(prompt)
    #     with ThreadPoolExecutor(max_workers=3) as executor:
    #         content = executor.submit(self.editor.run, draft)
    #         highlights = executor.submit(self.extractor.run, draft)
            
    #     article = highlights.result()
    #     article.raw = content.result()
    #     self._backup_composer_response("article", prompt, article.raw)
            
    #     return article

    # def save_article(self, metadata: ArticleMetadata, content: str, banner_url: str = None):
    #     bean = _make_bean(metadata, content, banner_url)
    #     try: bean.url = self.cdn.upload_article(bean.content, bean.id)
    #     except Exception as e: log.warning(f"article upload failed - {e}", extra={'source': bean.id, 'num_items': 1})  
    #     return bean
    
    # def _backup_composer_response(self, content_type: str, input_text: str, response_text: str):
    #     if not self.backup_container or not response_text: return 
    #     trfile = content_type+"-"+now().strftime("%Y-%m-%d")+".jsonl"
    #     try: self.backup_container.upload_blob(trfile, json.dumps({'input': input_text, 'output': response_text})+"\n", BlobType.APPENDBLOB)           
    #     except Exception as e: log.warning(f"backup failed - {e}", extra={'source': trfile, 'num_items': 1})

    # def create_banner(self, bean: Bean):
    #     if not bean: return

    #     user_prompt = bean.title if bean.title else ", ".join(bean.entities)
    #     image_data = self.banner_maker.run(user_prompt)
    #     try: bean.image_url = self.cdn.upload_image(image_data, bean.id+".png")
    #     except Exception as e: log.warning(f"image upload failed - {e}", extra={'source': bean.id, 'num_items': 1, 'prompt': user_prompt})
    #     return bean  

    # def _compose_banner_and_store(self, topic: str, kind: str, beans: list[Bean]):
    #     article = self.compose_article(topic, kind, beans)
    #     bean = self.save_article(article)
    #     if not bean: return
    #     if self.banner_maker: bean = self.create_banner(bean)
    #     self.db.store_beans([bean])
    #     log.info(f"composed and stored {kind}", extra={'source': topic, 'num_items': 1})
    #     return bean

    def compose_article(self, topic: dict, beans: list[Bean]):
        if not topic or not beans: return

        # create drafts
        drafts = self.stage1_create_drafts(topic, beans)
        # create metadata
        metadata = self.stage2_create_metadata(topic, drafts)
        if not metadata: return
        # create content
        content = self.stage3_create_content(topic, metadata, drafts)
        # create bean
        bean = self.stage4_create_bean(topic, metadata, content)
        return bean
       
    def run(self, topics = None):
        self.run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("starting composer", extra={"source": self.run_id, "num_items": 1})

        clusters = self.stage0_get_clusters(topics)
        if not clusters: return    

        beans = run_batch(lambda c: self.compose_article(c[0], c[1]), clusters, len(clusters))
        beans = [b for b in beans if b]
        if not beans: return

        beans = self.stage5_batch_create_banner(beans)
        self.db.store_beans(beans)
        log.info("stored articles", extra={'source': self.run_id, 'num_items': len(beans)})
        return beans

    
