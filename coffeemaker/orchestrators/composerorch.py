from itertools import count
import os
import aiohttp
import re
import yaml
import json
import asyncio
import numpy as np
import pandas as pd
import logfire
from coffeemaker.nlp import *
from coffeemaker.pybeansack.cdnstore import *
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack.warehouse import *
from coffeemaker.pybeansack.lancesack import Beansack as Cupboard
from coffeemaker.pybeansack.utils import *
from coffeemaker.orchestrators.utils import *
from icecream import ic
from pydantic_ai import Agent, ModelSettings
from pydantic_ai.retries import AsyncTenacityTransport, RetryConfig, wait_retry_after
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider
from httpx import AsyncClient, HTTPStatusError
from tenacity import retry_if_exception_type, stop_after_attempt
from slugify import slugify


log = logging.getLogger(__name__)

BARISTA = "barista@cafecito.tech"

K_ID = "id"

EMBEDDER_CTX_LEN = 512
COMPOSER_CTX_LEN = 32768
LAST_NDAYS = int(os.getenv('COMPOSER_LAST_NDAYS', 1))

MIN_BEANS_PER_TOPIC = int(os.getenv('MIN_BEANS_PER_TOPIC', 4))
MAX_BEANS_PER_TOPIC = int(os.getenv('MAX_BEANS_PER_TOPIC', 24))
MAX_DISTANCE_PER_TOPIC = 0.2
MIN_BEANS_PER_DOMAIN = int(os.getenv('MIN_BEANS_PER_DOMAIN', 24))
MAX_BEANS_PER_DOMAIN = int(os.getenv('MAX_BEANS_PER_DOMAIN', 128))
MAX_DISTANCE_PER_DOMAIN = 0.3

# OUTPUT=JSON;{"headlines":List<Headline>};Headline=String;Length<=20Words;
ANALYST_INSTRUCTIONS = """
ROLE=NewsAnalyst;
TASK=ExtractHeadlines
INPUT=Topic:String\n\nList<NewsString>;NewsString=Format<U:Date;P:KeyPoints|...;E:Events|...;D:Datapoints|...;R:Regions|...;N:Entities|...;C:Categories|...;S:Sentiment|...>
OUTPUT=JSON;
- headlines (List<String>): Top 3 - 7 headline statements.
INSTRUCTIONS:
1.AnalyzeArticles;UseFields=U,P,E,D,N;GenerateHeadlines=Dynamic,Specific,Granular;Cluster=SemanticSimilarity;Avoid=GenericCategoriesFromC;AllowMultiTagging=True
2.CountFrequency;Frequency=NumArticlesPerHeadline
3.MeasureTopicAdherence;TopicAdherence=SemanticAndThematicRelevanceToTopic
4.Filter=FrequencyMin2,TopicAdherenceHigh;KeepHeadlines=Frequency>=2,TopicAdherence=Strict
5.MergeSimilar;SelectDistinct
6.OrderBy=Actor/Subject
7.Headline=Length<=25Words;MustInclude=Action/Subject,Action/Event,Object/Target,Impact,;Avoid=Clickbait,Sensationalism,Ambiguity,Vagueness;Tone=Neutral,Informative,Objective;IncludeKeywords;Keywords=Specific,Searchable,Entities,Phrases;MinimizeFalsePositives=True
"""

COLUMNIST_INSTRUCTIONS = f"""
ROLE=Columnist;
TASK=WriteTechnicalReport; 
TODAYS_DATE: {datetime.now().strftime('%Y-%m-%d')}     
INPUT=Topic:String\n\nList<NewsString>;NewsString=Format<U:Date;P:KeyPoints|...;E:Events|...;D:Datapoints|...;R:Regions|...;N:Entities|...;C:Categories|...;S:Sentiment|...>
OUTPUT=TechnicalReport;Markdown;IncludeBodyOnly=True;ContentLength<600Words;Language=AmericanEnglish en-US;
STEPS:
    1.ANALYZE=AnalyzeDatastreams;UseFields:U,P,E,D,R,N,S
    2.IDENTIFY=Patterns,Themes,Insights,EmergingTrends,Predictions
    3.GROUNDING=Normative,MultiNews
    4.FOCUS=TopicRelevance
    5.INCLUDE=Events,DataPoints,Trends,AnalyzedPatterns,SpecifiedPredictions
    6.OPTIONAL=if (conflicting viewpoints or contradictory data): INCLUDE comparison; else: pass
    7.OPTIONAL=if (time-based trends or events): INCLUDE timeline progression; else: pass
    8.PHRASING=direct,technical,factual,data-centric,1st-person
    9.REMOVE=speculative,narrative,emotive language
"""

EDITOR_INSTRUCTIONS = f"""
ROLE=Editor;
TASK=WriteOpinionPiece;
INPUT=DraftTechnicalReport;
OUTPUT=OP-ED;HTML;IncludeBodyOnly=True;ContentLength<450Words;
STEPS=
    - REMOVE:SelfReferencingLines,RedundantInformation,RepetitiveStatements,OffTopicContent,InconsistentNarratives,SelfContradictoryStatements,IncompleteSentences,NonContentBearingSections,ReportTitle,ReportDate,IntroductoryPhrases,ConclusionPhrases;
    - REMOVE_SAMPLES:
        - I’ve sifted through independent feeds ...
        - I tried to keep this concise ...
        - My analysis draws exclusively from ...
        - I note that the 20 Oct 2025 ...
        - My assessment of the October 2025 ...
        - By the author, 21 Oct 2025 – 670 words ...
        - Prepared 10 Oct 2025 – Technical Briefing
        - AWS Outage – Technical Report (10 Oct 2025)
        - Conflicting Viewpoints: The data are not contradictory ...
        - Subject: OpenAI’s claim that ...
        - Recent announcements (22 Oct 2025) describe a ...
    - MAINTAIN:TechnicalDetails;DataCentricity
    - REFINE:ContentStructure,ContentPhrasing=OP-EDStyle;SectionLayout=ReadFlowOptimized;HeaderPhrasing=SearchEngineOptimized;Timelines=ChronologicalOrder;CorrectArithmaticErrors;
    - REMOVE:SectionHeaders=Introduction,Conclusion,Verdict,ConflictingViewpoints,ExecutiveSummary;Verbiage=Speculative,Narrative,EmotiveStatements
    - HTML_TAGS:OPEDHeader=<h2>;SectionHeaders=<h3>;Tables=<ul>;Timelines=<ul>;
"""

SYNTHESIZER_INSTRUCTIONS = """
ROLE=NewsSynthesizer;
TASK=CreateHeadline,Question,Keywords
INPUT=ListOfNewsHighlights
OUTPUT=JSON
1. headline (String): A headline / title for a daily news recap + podcast that captures the primary actors and associated actions. Length < 25 Words
2. question (String): A precision question answer to which results in the INPUT event highlights. Length < 25 Words
3. keywords (List<String>): Name of top 2-5 people, organizations, geographic regions
"""

# PROMPT DATE: 10-21-2025
# EDITOR_INSTRUCTIONS = f"""
# ROLE=Editor;
# TASK=WriteSectionEditorial;
# TODAYS_DATE: {datetime.now().strftime('%Y-%m-%d')}
# INPUT=Domain:String\n\n\nTechnicalReports:List<String>
# OUTPUT=HTML;IncludeBodyOnly=True
# STEPS:
#     1. CONTENT_SOURCE=Use the technical reports as the ONLY assertable source of authoritative information;    
#     2. CONTENT_STRUCTURE=Leverage HTML syntax to optimize the layout and structure for better readability and flow;
#     3. FOCUS=DomainRelevance;
#     4. [OPTIONAL] Show side-by-side comparison of conflicting viewpoints and data
#     5. PHRASING=1st-person,direct,technical,factual,data-centric
#     6. AVOID=speculative,narrative,emotive language,self-describing verbiage, input references;
#     7. CLEANUP=Remove inconsistent narratives, self-contradictory statements, incomplete sentences, headers like 'Introduction' and 'Conclusion'
#     8. CONTENT_LENGTH<=1000Words
# """

#PROMPT DATE: 10-21-2025
# SYNTHESIZER_INSTRUCTIONS = """
# ROLE=NewsSynthesizer;
# TASK=Extract Headline,Question,Highlights,Keywords,BannerPrompt;
# INPUT=Article(HTML)
# OUTPUT=JSON
# 1. headline: One line capturing the primary who, what, whom and where. Length <= 20 Words.
# 2. question: A specific question that the article addresses. Length <= 20 Words
# 3. highlights (List<String>): Top 5 main events, trends and takeaways
# 4. keywords (List<String>): Top 5 names of people, organizations, geographic regions
# 5. banner_prompt: A prompt that can be passed on to a text-to-image LLM for generating article banner. Length <= 20 Words
# """

ARTICLE_TEMPLATE = """
<section style="border:1px solid lightgray;border-radius:8px;padding:8px;">
    <h4>TL;DR</h4>
    <ul>
        {headlines}
    </ul>
</section>
{reports}
"""

_create_id = lambda title: slugify(now().strftime("%Y-%m-%d")+" "+title)

class ShortlistedTopics(BaseModel):
    headlines: list[str] = Field(description="List of shortlisted headlines. each headline length <= 25 Words")

class Orchestrator:
    db: Beansack
    cdn = None
    embedder = None
    analyst = None
    columnist = None
    editor = None
    synthesizer = None
    sketcher = None
    publisher_conn = None
    cupboard = None

    def __init__(self, 
        db_conn: tuple[str, str],    
        embedder_model: str,          
        analyst_model: str,
        writer_model: str,
        composer_conn: tuple[str, str],
        banner_model: str = None,
        banner_conn: tuple[str, str] = None,
        publisher_conn: tuple[str, str] = None,
        cupboard_conn_str: str = None
    ):
        self.db = initialize_db(db_conn)

        logfire.configure(token=os.getenv("PUBLICATIONS_LOGFIRE_TOKEN"))
        logfire.instrument_pydantic_ai()
        
        self.embedder = embedders.from_path(embedder_model, EMBEDDER_CTX_LEN)

        transport = AsyncTenacityTransport(
            RetryConfig(
                wait=wait_retry_after(max_wait=120),
                stop=stop_after_attempt(3),
                reraise=True
            ),
            validate_response=lambda r: r.raise_for_status()
        )
        composer_provider = OpenAIProvider(*composer_conn, http_client=AsyncClient(transport=transport)) 
        analyst_model = OpenAIChatModel(
            analyst_model, 
            provider=composer_provider, 
            settings=ModelSettings(temperature=0.3, timeout=120, seed=666)
        )
        writer_model = OpenAIChatModel(
            writer_model, 
            provider=composer_provider, 
            settings=ModelSettings(temperature=0.3, timeout=120, seed=666)
        )

        self.analyst = Agent(
            name="Analyst",
            model=analyst_model,
            system_prompt=ANALYST_INSTRUCTIONS,
            output_type=ShortlistedTopics
        )
        self.columnist = Agent(
            name="Columnist",
            model=writer_model,
            system_prompt=COLUMNIST_INSTRUCTIONS
        )
        self.editor = Agent(
            name="Editor",
            model=writer_model,
            system_prompt=EDITOR_INSTRUCTIONS
        )
        self.synthesizer = Agent(
            name="Synthesizer",
            model=analyst_model,
            system_prompt=SYNTHESIZER_INSTRUCTIONS,
            output_type=Metadata
        )
        if banner_model: self.sketcher = Agent(
            name="BannerMaker",
            model=OpenAIChatModel(banner_model, provider=OpenAIProvider(*banner_conn)),
        )

        self.publisher_conn = publisher_conn
        if cupboard_conn_str: self.cupboard = Cupboard(cupboard_conn_str)


    async def _query_beans(self, trending: bool, kind: str, last_ndays: int, query_text: str, query_emb: list[float], distance: float, limit: int):
        if not query_emb and query_text:           
            query_emb = self.embedder.embed_query(query_text)

        trending_beans = lambda: self.db.query_trending_beans(
            kind=kind,
            updated=ndays_ago(last_ndays),
            embedding=query_emb,
            distance=distance if query_emb else None,
            limit=limit,
            columns=DIGEST_COLUMNS
        )
        latest_beans = lambda: self.db.query_latest_beans(
            kind=kind,
            created=ndays_ago(last_ndays),
            embedding=query_emb,
            distance=distance if query_emb else None,
            limit=limit,
            columns=DIGEST_COLUMNS
        )
        return await asyncio.to_thread(trending_beans if trending else latest_beans)  
    
    def _cluster_beans(self, beans: list[Bean], method: str = "KMEANS")-> list[list[Bean]]:
        if not beans: return 

        if method == "HDBSCAN": method = _hdbscan_cluster(beans) 
        elif method == "DBSCAN": method = _dbscan_cluster(beans) 
        elif method == "AFFINITY": method = _affinity_cluster(beans)
        else: method = _kmeans_cluster(beans)

        labels = method.fit_predict(np.array([bean.embedding for bean in beans]))
        clusters = {}
        for bean, label in zip(beans, labels):
            if label != -1: clusters.setdefault(label, []).append(bean)
        clusters = clusters.values()

        if clusters: log.info("found clusters", extra={'source': self.run_id, 'num_items': len(clusters)})
        else: log.info(f"no cluster found", extra={'source': self.run_id, 'num_items': 0})
        clusters = filter(lambda x: MIN_BEANS_PER_DOMAIN <= len(x) <= MAX_BEANS_PER_DOMAIN, clusters)
        return list(map(lambda x: (x[0].categories[0], x[0].kind, x), clusters))
    
    # processing stages for each topic
    # Option 1:
        # get the beans
        # identify section to write on
        # for each section create a draft
        # for each draft create a title
        # merge drafts and pass it on to editor to rewrite
        # create headline, highlights, summary, prompt for image, tags
    # Option 2:
        # get the beans
        # identify section headers to write on in N batches
        # for each section header find related beans
        # for each section header create a technical report
        # merge the sections and pass it on to editor to rewrite
        # create headline, highlights, summary, prompt for image, tags
    
    async def _get_beans_for_topic(self, topic: dict):
        beans = await self._query_beans(False, kind=NEWS, last_ndays=LAST_NDAYS, query_text=topic.get(K_DESCRIPTION), query_emb=topic.get(K_EMBEDDING), distance=MAX_DISTANCE_PER_DOMAIN, limit=MAX_BEANS_PER_DOMAIN)
        log.info("found beans", extra={'source': topic[K_ID], 'num_items': len(beans)})
        return beans if beans and len(beans) >= MIN_BEANS_PER_DOMAIN else None

    async def _create_headlines(self, topic: dict):
        beans = await self._get_beans_for_topic(topic)
        if not beans: return

        prompt = f"# Topic: {topic[K_DESCRIPTION]}\n\n\n" + "\n\n".join([b.digest for b in beans])
        res = await self.analyst.run(prompt)
        return res.output.headlines

    async def _get_beans_for_headline(self, topic: str, emb: list[float] = None) -> list[Bean]:
        beans = await self._query_beans(False, kind=None, last_ndays=LAST_NDAYS+1, query_text=topic, query_emb=emb, distance=MAX_DISTANCE_PER_TOPIC, limit=MAX_BEANS_PER_TOPIC)
        log.info("found beans", extra={'source': topic, 'num_items': len(beans)})
        return beans if beans and len(beans) >= MIN_BEANS_PER_TOPIC else None

    # async def _write_report(self, headline: str, beans: list[Bean]):    
    #     if not beans: return    

    #     prompt = f"# Topic: {headline}\n\n\n" + "\n\n".join([b.digest for b in beans])
    #     draft = await self.columnist.run(prompt)
    #     if not draft.output: return
    #     report = await self.editor.run(draft.output)
    #     return report.output
    
    async def _create_section(self, topic: dict, headline: str, headline_emb: list[float]):
        beans = await self._get_beans_for_headline(headline, headline_emb)
        if not beans: return

        prompt = f"# Topic: {headline}\n\n\n" + "\n\n".join([b.digest for b in beans])
        draft = await self.columnist.run(prompt)
        if not draft.output: return
        section = await self.editor.run(draft.output)
        if not (section and section.output): return
        log.info("created report", extra={'source': headline, 'num_items': 1})
        return {
            K_ID: _create_id(headline),
            K_TITLE: headline, 
            K_CONTENT: re.sub(r'<body[^>]*>', '', section.output).replace("</body>", ""), # cleaning up body tags
            "beans": [bean.url for bean in beans]
        } 
                
    async def _synthesize_content(self, headlines: list[str]):  
        if not headlines: return

        prompt = f"# Highlights:\n\n" + "\n".join([f"- {hl}" for hl in headlines])
        res = await self.synthesizer.run(prompt)
        return res.output

    async def _create_banner(self, banner_query: str):
        res = await self.sketcher.run(banner_query)
        return bytes(res.output)

    async def compose_article(self, topic: dict):
        try:
            headlines = await self._create_headlines(topic)
            log.info("created headlines", extra={'source': topic[K_ID], 'num_items': len(headlines)})
            if not headlines: return

            headline_embs = self.embedder([f"topic: {t}" for t in headlines])
            sections = await asyncio.gather(*[self._create_section(topic, headline, emb) for headline, emb in zip(headlines, headline_embs)])
            sections = [s for s in sections if s]
            if not sections: return
            
            # body = await self._write_main_content(domain, reports)
            # log.info("created body", extra={'source': domain[K_ID], 'num_items': 1})            
            # if not body: return

            metadata = await self._synthesize_content([section[K_TITLE] for section in sections])
            log.info("synthesized metadata", extra={'source': topic[K_ID], 'num_items': 1})

            return {
                K_ID: _create_id(metadata.headline),
                K_TITLE: metadata.headline,
                K_SUMMARY: metadata.question,
                K_CONTENT: ARTICLE_TEMPLATE.format(
                    headlines="\n".join([f"<li>{hl}</li>" for hl in headlines]),
                    reports="\n".join([section[K_CONTENT] for section in sections])
                ),
                K_TAGS: [topic[K_ID]]+topic.get(K_TAGS, [])+metadata.keywords,
                K_AUTHOR: BARISTA,
                "highlights": headlines,
                "sections": sections,
                "type": "html",
            }
        except Exception as e:
            log.warning(f"compose article failed - {e}", extra={'source': topic[K_ID], 'num_items': 1}, exc_info=True)

    async def _publish(self, session, article: dict):
        async with session.post("articles", json=article) as resp:
            res = await resp.json()                
        if res: log.info("published", extra={'source': article[K_TAGS][0], 'num_items': 1})      
        else: log.warning("publish failed", extra={'source': article[K_TAGS][0], 'num_items': 1})
        return res    

    async def publish(self, article: dict):
        if not self.publisher_conn: return
        async with aiohttp.ClientSession(base_url=self.publisher_conn[0], headers={"X_API_KEY": self.publisher_conn[1] }, raise_for_status=True) as session:            
            return await self._publish(session, article)

    async def bulk_publish(self, articles: list[dict]):
        if not self.publisher_conn: return
        async with aiohttp.ClientSession(base_url=self.publisher_conn[0], headers={"X_API_KEY": self.publisher_conn[1] }, raise_for_status=True) as session:
            published = await asyncio.gather(*[self._publish(session, a) for a in articles])
        return published
    
    def bulk_store(self, articles: list[dict]):
        new_mugs, new_sips = [], []
        for a in articles:
            sections = a.get("sections", [])
            vecs = self.embedder.embed_documents([s[K_CONTENT] for s in sections])
            sips = [
                Sip(
                    **s, 
                    created=now(), 
                    embedding=vec, 
                    mug=a[K_ID]
                ) 
                for s, vec in zip(sections, vecs)
            ]
            new_sips.extend(sips)
            new_mugs.append(Mug(
                **a, 
                created=now(), 
                embedding=np.mean(vecs, axis=0).tolist(), 
                sips=[s.id for s in sips]
            ))

        total = self.cupboard.store_sips(new_sips)
        log.info("stored sips", extra={'source': self.run_id, 'num_items': total})
        total = self.cupboard.store_mugs(new_mugs)
        log.info("stored mugs", extra={'source': self.run_id, 'num_items': total})

    async def _compose_and_publish(self, domain: dict):
        article = await self.compose_article(domain)
        if not article: log.info("no article", extra={'source': domain[K_ID], 'num_items': 1})
        else: await self.publish(article)
        return article

    def _create_topic_embeddings(self, domains: list[dict])-> list[dict]:
        if any(K_EMBEDDING not in d for d in domains):
            domain_embs = self.embedder([f"topic: {d.get(K_DESCRIPTION)}" for d in domains])
            for d, emb in zip(domains, domain_embs): d[K_EMBEDDING] = emb
        return domains
          
    async def run_async(self, topics):
        self.run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("starting composer", extra={"source": self.run_id, "num_items": 1})
        self.db.refresh_aggregated_chatters()
        topics = parse_topics(topics)
        topics = self._create_topic_embeddings(topics)

        articles = await asyncio.gather(*[self._compose_and_publish(topic) for topic in topics])
        articles = [a for a in articles if a]
        log.info("composed and published", extra={"source": self.run_id, "num_items": len(articles)})
        self.bulk_store(articles)
        self.db.close()
        return articles

def _process_banner(banner):
    if hasattr(banner, "save"): 
        filename = ".cache/"+re.sub(r'[^a-zA-Z0-9]', '-', f"banner-{int(now().timestamp())}-{random.randint(1000,9999)}").lower()+".png"
        banner.save(filename)
        return filename
    return banner

def parse_topics(topics: list|dict|str):
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


def _kmeans_cluster(self, beans)-> list[list[Bean]]:
    from sklearn.cluster import KMeans
    n_clusters = len(beans)//(MIN_BEANS_PER_DOMAIN>>1)
    return KMeans(n_clusters=n_clusters, random_state=666)
    
def _hdbscan_cluster(self, beans):
    from hdbscan import HDBSCAN
    return HDBSCAN(
        min_cluster_size=MIN_BEANS_PER_DOMAIN, 
        min_samples=2, 
        max_cluster_size=MAX_BEANS_PER_DOMAIN, 
        cluster_selection_epsilon_max=MAX_DISTANCE_PER_DOMAIN
    )
        
def _dbscan_cluster(self, beans):
    from sklearn.cluster import DBSCAN
    return DBSCAN(min_samples=MIN_BEANS_PER_DOMAIN>>1, eps=MAX_DISTANCE_PER_DOMAIN)

def _affinity_cluster(self, beans):
    from sklearn.cluster import AffinityPropagation
    return AffinityPropagation(copy=False, damping=0.55, random_state=666)    

    # def _make_bean(metadata: Metadata, content: str, banner_url: str): 
    # current = now()
    # bean_id = random_filename(metadata.headline)
    # return BeanCore(
    #     url=f"https://publications.cafecito.tech/articles/{bean_id}.html",
    #     kind=OPED,
    #     title=metadata.headline,
    #     summary="\n".join(metadata.highlights),
    #     content=content,
    #     restricted_content=True,
    #     author="Barista AI",
    #     source="cafecito",
    #     image_url=banner_url,
    #     created=current,        
    #     collected=current
    # )

    # def stage0_get_clusters(self, topics = None):
    #     if topics:
    #         topics = _parse_topics(topics)
    #         beans_list = run_batch(lambda topic: self._get_beans(kind=NEWS, last_ndays=LAST_NDAYS, categories=topic.get(K_TAGS), embedding=topic.get(K_EMBEDDING), limit=MAX_CLUSTER_SIZE), topics, len(topics))
    #         clusters = [(topic, beans) for topic, beans in zip(topics, beans_list) if beans and MIN_CLUSTER_SIZE <= len(beans)]
    #     else:
    #         clusters = self._cluster_beans(self._get_beans(kind = NEWS, last_ndays=LAST_NDAYS), "HDBSCAN")

    #     log.info("found clusters", extra={'source': self.run_id, 'num_items': len(clusters)})
    #     return clusters

    # def stage1_create_drafts(self, topic: dict, beans: list[Bean]):
    #     batches = [beans[i:i+MIN_CLUSTER_SIZE] for i in range(0, len(beans), MIN_CLUSTER_SIZE) if i+(MIN_CLUSTER_SIZE>>1) <= len(beans)]
    #     if len(batches) <= 0: return
    #     try:
    #         drafts = self.journalist.run_batch([f"Topic: {topic[K_ID]}\n\n"+"\n".join([b.digest() for b in batch]) for batch in batches])
    #         log.info("created drafts", extra={'source': topic[K_ID], 'num_items': len(drafts)})
    #         return drafts
    #     except Exception as e: log.warning(f"drafts creation failed - {e}", extra={'source': topic[K_ID], 'num_items': len(batches)})
    
    # def stage2_create_metadata(self, topic: dict, drafts: list[str]):
    #     drafts_text = "\n-------- DRAFT --------\n".join(drafts)
    #     try:
    #         metadata = self.extractor.run(f"Topic: {topic[K_ID]}\n\n{drafts_text}")
    #         log.info("created metadata", extra={'source': topic[K_ID], 'num_items': 1})
    #         return metadata
    #     except Exception as e: log.warning(f"metadata creation failed - {e}", extra={'source': topic[K_ID], 'num_items': len(drafts)})

    # def stage3_create_content(self, topic: dict, metadata: ArticleMetadata, drafts: list[str]):
    #     drafts_text = "\n-------- DRAFT --------\n".join(drafts)    
    #     prompt = f"Topic: {topic[K_ID]}\n\nHeadline: {metadata.headline}\n\nCurrentDate: {now().strftime('%Y-%m-%d')}\n\n{drafts_text}"
    #     try:
    #         article = self.editor.run(prompt)
    #         log.info("created content", extra={'source': topic[K_ID], 'num_items': 1})
    #         return article
    #     except Exception as e: log.warning(f"content creation failed - {e}", extra={'source': topic[K_ID], 'num_items': 1})

    # def stage4_create_bean(self, topic: dict, metadata: ArticleMetadata, content: str):
    #     bean = _make_bean(metadata, content, None)
    #     try: bean.url = self.cdn.upload_article(bean.content, bean.id)
    #     except Exception as e: log.warning(f"article store/upload failed - {e}", extra={'source': topic[K_ID], 'num_items': 1})  
    #     return bean

    # def stage5_create_banner(self, bean: Bean):
    #     result = self.banner_maker.run(f"Create a realistic image depicting: {bean.title}")
    #     log.info("created banner", extra={'source': bean.id, 'num_items': 1})
    #     bean.image_url = self.cdn.upload_image_file(result, bean.id+".png")
    #     log.info("uploaded banner", extra={'source': self.run_id, 'num_items': 1})
    #     return bean

    # def stage5_batch_create_banner(self, beans: list[Bean]):
    #     local_files = self.banner_maker.run_batch([f"Create a realistic image depicting: {bean.title}" for bean in beans])
    #     log.info("created banners", extra={'source': self.run_id, 'num_items': len(local_files)})
    #     banner_urls = run_batch(lambda x: self.cdn.upload_image_file(x[0], x[1].id+".png"), zip(local_files, beans), len(beans))
    #     for bean, banner_url in zip(beans, banner_urls):
    #         bean.image_url = banner_url
    #     log.info("uploaded banners", extra={'source': self.run_id, 'num_items': len(banner_urls)})
    #     return beans    

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

    # def compose_article(self, topic: dict, beans: list[Bean]):
    #     if not topic or not beans: return

    #     # create drafts
    #     drafts = self.stage1_create_drafts(topic, beans)
    #     # create metadata
    #     metadata = self.stage2_create_metadata(topic, drafts)
    #     if not metadata: return
    #     # create content
    #     content = self.stage3_create_content(topic, metadata, drafts)
    #     # create bean
    #     bean = self.stage4_create_bean(topic, metadata, content)
    #     return bean

    # async def _save_article(self, domain: dict, metadata: Metadata, article: str, banner: bytes = None):
    #     filename = random_filename(metadata.headline)
    #     article_url = await asyncio.to_thread(self.cdn.upload_article, article, filename)
    #     banner_url = None
    #     if banner: banner_url = await asyncio.to_thread(self.cdn.upload_image, banner, filename)  
    #     log.info("saved", extra={'source': domain[K_ID], 'num_items': 1})      
    #     return ic(article_url, banner_url)