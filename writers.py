import json
import os
from heapq import nlargest
from coffeemaker.nlp.prompts import *
from coffeemaker.nlp.models import *
from coffeemaker.nlp.agents import SimpleAgent, from_path as agent_from_path
from coffeemaker.nlp.embedders import Embeddings, from_path as embedder_from_path
from coffeemaker.nlp.utils import *
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack.mongosack import *
from coffeemaker.orchestrators.utils import *
from azure.storage.blob import BlobType

from icecream import ic

log = logging.getLogger(__name__)

from dotenv import load_dotenv

load_dotenv()


CONTEXT_LEN = 117760
MIN_ITEMS_THRESHOLD = 10
GIST_PROJECT = {K_GIST: 1, K_URL: 1}
GIST_FILTER = {K_GIST: VALUE_EXISTS, K_CREATED: {"$gte": ndays_ago(10)}}
# _PAGES = ["Cybersecurity", "Startups and Innovation", "Government and Politics", "Artification Intelligence", "Aviation"]
_PAGES = ["Cybersecurity", "Artificial Intelligence", "Government", "Innovation and Startups" ]

make_article_id = lambda title, current: title.lower().replace(' ', '-')+current.strftime("-%Y-%m-%d-%H-%M-%S")+".md"
def _make_bean(comp: Composition): 
    current = now()
    bean_id = make_article_id(comp.title, current)
    summary = "\n\n".join(comp.intro)
    md_content = "\n\n".join(
        [f"> "+v for v in comp.verdict]+
        comp.intro+
        ["**Insights**"]+comp.insights+
        ["**Analysis**"]+comp.analysis
    )
    return Bean(
        _id=bean_id,
        url=bean_id,
        kind=GENERATED,
        created=current,
        updated=current,
        collected=current,
        title=comp.title,
        summary=summary,
        content=md_content,
        num_words_in_title=num_words(comp.title),
        num_words_in_summary=num_words(summary),
        num_words_in_content=num_words(md_content),
        author="Barista AI",
        source="cafecito",
        site_base_url="cafecito.tech",
        site_name="Cafecito",
        site_favicon="https://cafecito.tech/images/favicon.ico"
    )

class Orchestrator:
    db: Beansack
    embedder: Embeddings

    topic_extractor: SimpleAgent
    news_writer: SimpleAgent
    blog_writer: SimpleAgent

    backup_container: ContainerClient

    def __init__(self, 
        mongodb_conn_str: str, 
        db_name: str, 
        embedder_path: str, 
        embedder_context_len: str,
        bean_distance: float,
        writer_path: str, 
        writer_base_url: str,
        writer_api_key: str,
        writer_context_len: int,
        max_articles_per_page: int = 5,
        backup_azstorage_conn_str: str = None
    ):
        self.db = Beansack(mongodb_conn_str, db_name)
        self.embedder = embedder_from_path(embedder_path, embedder_context_len)
        self.bean_similarity = 1 - bean_distance
        
        self.topic_extractor = agent_from_path("gpt-4.1-nano", writer_base_url, writer_api_key, max_input_tokens=writer_context_len, system_prompt=TOPIC_EXTRACTOR_SYSTEM_PROMPT, output_parser=TopicsExtractionResponse.parse_json, json_mode=True)
        self.news_writer = agent_from_path("gpt-4.1-mini", writer_base_url, writer_api_key, max_input_tokens=writer_context_len, system_prompt=NEWRECAPT_SYSTEM_PROMPT, json_mode=False)
        self.blog_writer = agent_from_path("gpt-4.1-mini", writer_base_url, writer_api_key, max_input_tokens=writer_context_len, system_prompt=OLD_OPINION_COMPOSER_SYSTEM_PROMPT, json_mode=False)

        self.max_articles_per_page = max_articles_per_page
        if backup_azstorage_conn_str: self.backup_container = initialize_azblobstore(os.getenv("AZSTORAGE_CONN_STR"), "composer")

    def create_input(self, query: str = None, embedding: list[float] = None,  filter: dict = None):
        if filter: filter.update(GIST_FILTER)
        else: filter = GIST_FILTER

        if query: embedding = self.embedder.embed(query)
        if embedding: beans = self.db.vector_search_beans(embedding, self.bean_similarity, filter, limit=225, project=GIST_PROJECT)
        else: beans = self.db.query_beans(filter, project=GIST_PROJECT)

        if ic(len(beans)) < MIN_ITEMS_THRESHOLD: return

        gists = "\n".join([bean.gist for bean in beans])
        return f"{query},{gists}"
    
    def extract_topics(self, page, kind = None):
        topic_input = self.create_input(
            # query=f"domain: {page}",
            filter={K_KIND: kind, K_TAGS: lower_case(page)} if kind else None
        )
        if not topic_input: return 
        response = self.topic_extractor.run(topic_input)

        self.backup_content("topics", topic_input, response.raw)
        return response
            
    def compose_article(self, topic, kind = NEWS):  
        article_input = self.create_input(
            # query=f"topic: {topic[0]}",
            filter={K_KIND: kind, K_TAGS: lower_case(ic(topic[1].keywords))} if kind else None
        )
        if not article_input: return 
        if kind == BLOG: response = self.blog_writer.run(article_input)
        else: response = self.news_writer.run(article_input)

        self.backup_content("articles", article_input, response.raw)
        return response

    def backup_content(self, prefix: str, input_text: str, response_text: str):
        if not self.backup_container or not response_text: return 
        trfile = prefix+"-"+now().strftime("%Y-%m-%d")+".jsonl"
        try: self.backup_container.upload_blob(trfile, json.dumps({'input': input_text, 'output': response_text})+"\n", BlobType.APPENDBLOB)           
        except Exception as e:
            ic(e) 
            log.warning("backup failed", extra={'source': trfile, 'num_items': 1})

    def store_beans(self, responses: list):      
        if not responses: return

        beans = list(map(_make_bean, responses))
        ic([bean.url for bean in beans])
        batch_run(lambda bean: self.backup_container.upload_blob(bean.url, bean.content, BlobType.BLOCKBLOB), beans)
        count = self.db.store_beans(beans)
        log.info("stored articles", extra={'source': 'articles', 'num_items': count})
        return beans
       
    def run(self):
        total = 0
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("starting composer", extra={"source": run_id, "num_items": 1})

        for page in _PAGES:
            topics = self.extract_topics(page)
            if not topics: 
                log.info(f"no topic found", extra={'source': page, 'num_items': 1})
                continue
            
            top_topics = nlargest(self.max_articles_per_page, topics.topics.items(), key=lambda item: item[1].frequency or 0)
            articles = list(map(self.compose_article, ic(top_topics)))
            self.store_beans(articles, top_topics)
            total += len(articles)            
        
        log.info("total composed", extra={"source": run_id, "num_items": total})

logging.basicConfig(level=logging.INFO)
if __name__ == "__main__":
    orch = Orchestrator(
        "mongodb://localhost:27017",
        "test",
        embedder_path=os.getenv("EMBEDDER_PATH"),
        embedder_context_len=int(os.getenv("EMBEDDER_CONTEXT_LEN")),
        bean_distance=0.3,
        writer_path=os.getenv("WRITER_PATH"), 
        writer_base_url=os.getenv("WRITER_BASE_URL"),
        writer_api_key=os.getenv("WRITER_API_KEY"),
        writer_context_len=CONTEXT_LEN,
        max_articles_per_page=2,
        backup_azstorage_conn_str=os.getenv('AZSTORAGE_CONN_STR')
    )
    # orch.run()

    files = [
        "/home/soumitsr/codes/pycoffeemaker/.test/startups venture capital and fundraising-2025-05-28-16-05-Venture Capital & Fundraising.md",
        "/home/soumitsr/codes/pycoffeemaker/.test/startups venture capital and fundraising-2025-05-28-16-05-Startups & Ecosystem Development.md",
        "/home/soumitsr/codes/pycoffeemaker/.test/startups venture capital and fundraising-2025-05-28-16-05-Market & Economic Conditions.md",
        "/home/soumitsr/codes/pycoffeemaker/.test/startups venture capital and fundraising-2025-05-28-16-05-Funding & Investment Trends.md",
        "/home/soumitsr/codes/pycoffeemaker/.test/startups venture capital and fundraising-2025-05-28-16-05-Blockchain & Cryptocurrency.md",
        "/home/soumitsr/codes/pycoffeemaker/.test/startups venture capital and fundraising-2025-05-28-16-05-AI & Technology Infrastructure.md",
        "/home/soumitsr/codes/pycoffeemaker/.test/startups venture capital and fundraising-2025-05-28-14-52-Venture Financing and Startup Support.md",
        "/home/soumitsr/codes/pycoffeemaker/.test/startups venture capital and fundraising-2025-05-28-14-52-Venture Capital and Fundraising.md"
    ]
    comps = []
    for name in files:
        with open(name, "r") as file:
            comps.append(Composition.parse_markdown(file.read()))
    
    orch.store_beans(comps)
