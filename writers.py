import json
import os
import random

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
from dotenv import load_dotenv

load_dotenv()

# """
# # topic generator
# TASK:INPUT=List<ArticleString>;ArticleString=Format<U:YYYY-MM-DD;P:Summary|...;N:Entities|...;E:Events|...;C:Categories|...;S:Sentiment|...>;OUTPUT=Dict<Topic,Dict<frequency:Int,keywords:List<String>>>;
# INSTRUCTIONS:1=AnalyzeArticles;UseFields=P,N,E;GenerateTopics=Dynamic,Specific,Granular;Cluster=SemanticSimilarity;NotUse=GenericCategoriesFromC;AllowMultiTagging=True;
# 2=CountFrequency;Frequency=NumArticlesPerTopic;
# 3=FilterFrequency=Min2;KeepTopics=Frequency>=2;
# 4=GenerateKeywords;Keywords=Specific,Searchable,FromP,N,E;MinimizeFalsePositives=True;Include=Entities,Events,Dates,Phrases;
# 5=OutputFormat=Dict;Key=Topic;Value=Dict;ValueFormat=frequency:Int,keywords:List<String>;
# EXAMPLE_OUTPUT={"Topic1":{"frequency":4,"keywords":["kw1","kw2"]},"Topic2":{"frequency":2,"keywords":["kw3","kw4"]}}
# """




# class TopicItemSchema(BaseModel):
#     frequency: Optional[int] = Field(default=None)
#     keywords: Optional[list[str]] = Field(default=None)

# class TopicExtractionSchema(BaseIOSchema):
#     """Extracted topics of articles that can be written"""
#     topics: dict[str, TopicItemSchema] = Field(default=None, description="Dict<Topic,Dict<frequency:Int,keywords:List<String>>>")


# topic_agent = BaseAgent(
#     config=BaseAgentConfig(
#         client=instructor.from_openai(openai.OpenAI(api_key=os.getenv('WRITER_API_KEY')), mode=instructor.Mode.JSON),
#         model="gpt-4.1-nano",
#         system_prompt_generator=SystemPromptGenerator(
#             background=[
#                 "INPUT=List<ArticleString>;ArticleString=Format<U:YYYY-MM-DD;P:Summary|...;N:Entities|...;E:Events|...;C:Categories|...;S:Sentiment|...>",
#                 "OUTPUT=Dict<Topic,Dict<frequency:Int,keywords:List<String>>>"
#             ],
#             steps=[
#                 "1=AnalyzeArticles;UseFields=U,P,N,E;GenerateTopics=Dynamic,Specific,Granular;Cluster=SemanticSimilarity;NotUse=GenericCategoriesFromC;AllowMultiTagging=True",
#                 "2=CountFrequency;Frequency=NumArticlesPerTopic",
#                 "3=FilterFrequency=Min2;KeepTopics=Frequency>=2",
#                 "4=GenerateKeywords;Keywords=Specific,Searchable,FromP,N,E;MinimizeFalsePositives=True;Include=Entities,Events,Dates,Phrases"
#             ],
#             output_instructions=[
#                 "OUTPUT_FORMAT=Dict;Key=Topic;Value=Dict;ValueFormat=frequency:Int,keywords:List<String>",
#                 "EXAMPLE_OUTPUT={\"Topic1\":{\"frequency\":4,\"keywords\":[\"kw1\",\"kw2\"]},\"Topic2\":{\"frequency\":2,\"keywords\":[\"kw3\",\"kw4\"]}}"
#             ]
#         ),
#         input_schema=ArticlesGistsSchema,
#         output_schema=TopicExtractionSchema
#     )
# )


# """TASK:INPUT=Topic:String,Articles:List<ArticleString>;ArticleString=Format<U:YYYY-MM-DD;P:Summary|...;N:Entities|...;E:Events|...;C:Categories|...;S:Sentiment|...>;OUTPUT=OpinionPiece:Markdown;
# INSTRUCTIONS:
# 1=AnalyzeArticles;UseFields=P,N,E,S;Identify=Patterns,Themes,Insights;Grounding=Normative,MultiArticle;Focus=TopicRelevance;
# 2=GenerateOpinionPiece;Structure=Introduction,Analysis,Takeaways,Verdict;Introduction=Context,TopicOverview;Analysis=SynthesizePatterns,ReportEntitiesEvents,PresentSentiment;Takeaways=KeyInsights,Implications;Verdict=TechnicalSummary;Content=CoreFindings,KeyData;Style=Direct,Technical,Factual;Length=400-600Words;Avoid=Speculation,Narrative,EmotiveLanguage;VerdictLength=10-20Words;
# 3=OutputFormat=Markdown;Sections=#Introduction,##Analysis,##KeyTakeaways,##Verdict;Include=TopicInTitle;
# EXAMPLE_OUTPUT=#OpinionPiece:TopicName\n#Introduction\nContext...\n##Analysis\nPatterns...\n##KeyTakeaways\n-Insight1\n-Insight2\n##Verdict\nSummary..."""


# writer_agent = BaseAgent(
#     config=BaseAgentConfig(
#         client=instructor.from_openai(
#             openai.OpenAI(api_key=os.getenv('DIGESTOR_API_KEY'), base_url=os.getenv('DIGESTOR_BASE_URL')), 
#             mode=instructor.Mode.MD_JSON
#         ),
#         model=os.getenv('DIGESTOR_PATH'),
#         system_prompt_generator=SystemPromptGenerator(
#             background=[
#                 "TASK:INPUT=Topic:String,Articles:List<ArticleString>;ArticleString=Format<U:YYYY-MM-DD;P:Summary|...;N:Entities|...;E:Events|...;C:Categories|...;S:Sentiment|...>;OUTPUT=OpinionPiece:Markdown;"
#             ],
#             steps=[
#                 """
#                 INSTRUCTIONS:
#                 1=AnalyzeArticles;UseFields=P,N,E,S;Identify=Patterns,Themes,Insights;Grounding=Normative,MultiArticle;Focus=TopicRelevance;
#                 2=GenerateOpinionPiece;Structure=Introduction,Analysis,Takeaways,Verdict;Introduction=Context,TopicOverview;Analysis=SynthesizePatterns,ReportEntitiesEvents,PresentSentiment;Takeaways=KeyInsights,Implications;Verdict=TechnicalSummary;Content=CoreFindings,KeyData;Style=Direct,Technical,Factual;Length=400-600Words;Avoid=Speculation,Narrative,EmotiveLanguage;VerdictLength=10-20Words;
#                 3=OutputFormat=Markdown;Sections=#Introduction,##Analysis,##KeyTakeaways,##Verdict;Include=TopicInTitle;
#                 """
#             ],
#             output_instructions=[
#                 "EXAMPLE_OUTPUT=#OpinionPiece:TopicName\n#Introduction\nContext...\n##Analysis\nPatterns...\n##KeyTakeaways\n-Insight1\n-Insight2\n##Verdict\nSummary..."
#             ]
#         ),
#         input_schema=ArticlesGistsSchema
#     )
# )

# """
# # topic generator
# TASK:INPUT=List<ArticleString>;ArticleString=Format<U:YYYY-MM-DD;P:Summary|...;N:Entities|...;E:Events|...;C:Categories|...;S:Sentiment|...>;OUTPUT=Dict<Topic,Dict<frequency:Int,keywords:List<String>>>;
# INSTRUCTIONS:1=AnalyzeArticles;UseFields=P,N,E;GenerateTopics=Dynamic,Specific,Granular;Cluster=SemanticSimilarity;NotUse=GenericCategoriesFromC;AllowMultiTagging=True;
# 2=CountFrequency;Frequency=NumArticlesPerTopic;
# 3=FilterFrequency=Min2;KeepTopics=Frequency>=2;
# 4=GenerateKeywords;Keywords=Specific,Searchable,FromP,N,E;MinimizeFalsePositives=True;Include=Entities,Events,Dates,Phrases;
# 5=OutputFormat=Dict;Key=Topic;Value=Dict;ValueFormat=frequency:Int,keywords:List<String>;
# EXAMPLE_OUTPUT={"Topic1":{"frequency":4,"keywords":["kw1","kw2"]},"Topic2":{"frequency":2,"keywords":["kw3","kw4"]}}
# """

# CONTEXT_LEN = 111072
# MIN_PROCESSING_THRESHOLD = 20
# GIST_PROJECT = {K_GIST: 1, K_URL: 1}
# GIST_FILTER = {K_GIST: VALUE_EXISTS}

# TOPIC_EXTRACTION_SYSTEM_PROMPT=SystemPromptGenerator(
#     background=[
#         "TASK:INPUT=List<ArticleString>;ArticleString=Format<U:YYYY-MM-DD;P:Summary|...;N:Entities|...;E:Events|...;C:Categories|...;S:Sentiment|...>;OUTPUT=Dict<Topic,Dict<frequency:Int,keywords:List<String>>>;"
#     ],
#     steps=[
#         """
#         INSTRUCTIONS:
#         1=AnalyzeArticles;UseFields=P,N,E;GenerateTopics=Dynamic,Specific,Granular;Cluster=SemanticSimilarity;NotUse=GenericCategoriesFromC;AllowMultiTagging=True;
#         2=CountFrequency;Frequency=NumArticlesPerTopic;
#         3=FilterFrequency=Min2;KeepTopics=Frequency>=2;
#         4=GenerateKeywords;Keywords=Specific,Searchable,FromP,N,E;MinimizeFalsePositives=True;Include=Entities,Events,Dates,Phrases;
#         5=OutputFormat=Dict;Key=Topic;Value=Dict;ValueFormat=frequency:Int,keywords:List<String>;
#         """
#     ],
#     output_instructions=[
#         "EXAMPLE_OUTPUT={\"Topic1\":{\"frequency\":4,\"keywords\":[\"kw1\",\"kw2\"]},\"Topic2\":{\"frequency\":2,\"keywords\":[\"kw3\",\"kw4\"]}}"
#     ]
# )

# OPINION_GENERATOR_SYSTEM_PROMPT=SystemPromptGenerator(
#     background=[
#         "TASK:INPUT=Topic:String,Articles:List<ArticleString>;ArticleString=Format<U:YYYY-MM-DD;P:Summary|...;N:Entities|...;E:Events|...;C:Categories|...;S:Sentiment|...>;OUTPUT=OpinionPiece:Markdown;"
#     ],
#     steps=[
#         """
#         INSTRUCTIONS:
#         1=AnalyzeArticles;UseFields=P,N,E,S;Identify=Patterns,Themes,Insights;Grounding=Normative,MultiArticle;Focus=TopicRelevance;
#         2=GenerateOpinionPiece;Structure=Introduction,Analysis,Takeaways,Verdict;Introduction=Context,TopicOverview;Analysis=SynthesizePatterns,ReportEntitiesEvents,PresentSentiment;Takeaways=KeyInsights,Implications;Verdict=TechnicalSummary;Content=CoreFindings,KeyData;Style=Direct,Technical,Factual;Length=400-600Words;Avoid=Speculation,Narrative,EmotiveLanguage;VerdictLength=10-20Words;
#         3=OutputFormat=Markdown;Sections=#Introduction,##Analysis,##KeyTakeaways,##Verdict;Include=TopicInTitle;
#         """
#     ],
#     output_instructions=[
#         "EXAMPLE_OUTPUT=#OpinionPiece:TopicName\n#Introduction\nContext...\n##Analysis\nPatterns...\n##KeyTakeaways\n-Insight1\n-Insight2\n##Verdict\nSummary..."
#     ]
# )


# NEWSRECAP_GENERATOR_SYSTEM_PROMPT=SystemPromptGenerator(
#     background=[
#         "TASK:INPUT=Topic:String,Articles:List<ArticleString>;ArticleString=Format<U:YYYY-MM-DD;P:Summary|...;N:Entities|...;E:Events|...;C:Categories|...;S:Sentiment|...>;OUTPUT=NewsRecap:Markdown;"
#     ],
#     steps=[
#         """
#         INSTRUCTIONS:
#         1=AnalyzeArticles;UseFields=P,N,E,S;Identify=Patterns,Themes,Insights;Grounding=Normative,MultiArticle;Focus=TopicRelevance;
#         2=GenerateNewsRecap;Structure=Introduction,Analysis,Datapoints,Verdict;Introduction=Context,TopicOverview;Analysis=SynthesizePatterns,ReportEntitiesEvents,PresentSentiment;Datapoints=KeyData,Implications;Verdict=TechnicalSummary;Content=CoreFindings,KeyData;Style=Direct,Technical,Factual;Length=400-600Words;Avoid=Speculation,Narrative,EmotiveLanguage;VerdictLength=10-20Words;
#         3=OutputFormat=Markdown;Sections=# Introduction,## Analysis,## KeyDatapoints,## Verdict;Include=TopicInTitle;
#         """
#     ],
#     output_instructions=[
#         "EXAMPLE_OUTPUT=#News Recap: TopicName\n# Introduction\nContext...\n## Analysis\nPatterns...\n## Key Datapoints\n-Insight1\n-Insight2\n## Verdict\nSummary..."
#     ]
# )

# class DigestsInputSchema(BaseIOSchema):
#     """Compressed gist of articles"""
#     domain: str = Field(default=None)
#     articles: list[str] = Field(default=None)

# class TopicItemSchema(BaseModel):
#     frequency: Optional[int] = Field(default=None)
#     keywords: Optional[list[str]] = Field(default=None)

# class TopicExtractionSchema(BaseIOSchema):
#     """Extracted topics of articles that can be written"""
#     topics: dict[str, TopicItemSchema] = Field(default=None, description="Dict<Topic,Dict<frequency:Int,keywords:List<String>>>")



# class DigestorPlusPlus:
#     agent: BaseAgent
#     context_len: int
#     db: Beansack
#     embedder: Embeddings

#     def __init__(self, db, model_path, context_len = CONTEXT_LEN, api_key = None, base_url = None, system_prompt = None, output_schema = None, embedder = None):
#         self.db = db
#         self.agent = BaseAgent(
#             config=BaseAgentConfig(
#                 client=instructor.from_openai(
#                     openai.OpenAI(api_key=api_key, base_url=base_url), 
#                     mode=instructor.Mode.JSON
#                 ),
#                 model=model_path,
#                 system_prompt_generator=system_prompt,
#                 input_schema=DigestsInputSchema,
#                 output_schema=output_schema
#             )
#         )
#         self.context_len = context_len
#         self.embedder = embedder

#     def run(self, query: str = None, vector: list[float] = None, similarity_score: float = None, filter: dict = None):
#         if filter: filter.update(GIST_FILTER)
#         else: filter = GIST_FILTER

#         if query: vector = self.embedder.embed(query)
#         if vector: beans = self.db.vector_search_beans(vector, similarity_score, filter, project=GIST_PROJECT)
#         else: beans = self.db.query_beans(filter, project=GIST_PROJECT)

#         if ic(len(beans)) < MIN_PROCESSING_THRESHOLD: return
#         gists = self.truncate_list([bean.gist for bean in beans])
#         return self.agent.run(DigestsInputSchema(domain=query, articles=gists))

#     def truncate_list(self, gists: list[str]):
#         total_token = 0
#         for i in range(len(gists)):
#             count = count_tokens(gists[i])
#             if total_token + count >= self.context_len: break
#             else: total_token += count
#         return gists[:i]


# orch = Orchestrator(
#     os.getenv('MONGODB_CONN_STR'),
#     "test",
#     embedder_path=os.getenv('EMBEDDER_PATH'),
#     embedder_context_len=512
# )
# topic_agent = DigestorPlusPlus(orch.db, "gpt-4.1-nano", context_len=CONTEXT_LEN, api_key=os.getenv('WRITER_API_KEY'), system_prompt=TOPIC_EXTRACTION_SYSTEM_PROMPT, output_schema=TopicExtractionSchema, embedder=orch.embedder)
# opinion_agent = DigestorPlusPlus(orch.db, "gpt-4.1-mini", context_len=CONTEXT_LEN, api_key=os.getenv('WRITER_API_KEY'), system_prompt=OPINION_GENERATOR_SYSTEM_PROMPT, embedder=orch.embedder)
# news_agent = DigestorPlusPlus(orch.db, "gpt-4.1-mini", context_len=CONTEXT_LEN, api_key=os.getenv('WRITER_API_KEY'), system_prompt=NEWSRECAP_GENERATOR_SYSTEM_PROMPT, embedder=orch.embedder)

# def run_generation(category, kind):
#     fileprefix=f"{now().strftime('%Y-%m-%d-%H-%M')}-{category}"

#     topics_response = topic_agent.run(
#         query=f"category/domain:{category}",
#         similarity_score=0.73,
#         filter={
#             K_KIND: kind,
#             K_CREATED: {"$gte": ndays_ago(7)}
#         }
#     )
#     if not topics_response: 
#         print("NO TOPICS")
#         return

#     save_json(fileprefix, topics_response.model_dump())
#     for k,v in topics_response.topics.items():
#         if v.frequency <= 3: 
#             ic(k, v)
#             continue

#         if kind == BLOG:
#             write_response = opinion_agent.run(
#                 query = f"topic:{k}", 
#                 similarity_score=0.77, 
#                 filter={
#                     K_KIND: kind,
#                     K_CREATED: {"$gte": ndays_ago(7)},
#                     K_TAGS: lower_case(v.keywords)
#                 }
#             )
#         else:
#             write_response = news_agent.run(
#                 query = f"topic:{k}", 
#                 similarity_score=0.77, 
#                 filter={
#                     K_KIND: kind,
#                     K_CREATED: {"$gte": ndays_ago(7)},
#                     K_TAGS: lower_case(v.keywords)
#                 }
#             )

#         if write_response: save_markdown(fileprefix+"-"+k, write_response.chat_message)

# for category in ["government and politics", "mergers aquisitions and IPO"]:
#     run_generation(category, NEWS)
#     run_generation(category, BLOG)



CONTEXT_LEN = 117760
MIN_ITEMS_THRESHOLD = 10
GIST_PROJECT = {K_GIST: 1, K_URL: 1}
GIST_FILTER = {K_GIST: VALUE_EXISTS, K_COLLECTED: {"$gte": ndays_ago(2)}}
_PAGES = ["Cybersecurity", "Startups and Innovation", "Government and Politics", "Artification Intelligence", "Aviation"]

make_article_id = lambda topic, current: f"article-"+current.strftime("%Y-%m-%d-%H-%M-%S-")+topic.lower().replace(' ', '-')+".md"
def make_bean(response): 
    current = now()
    _id = make_article_id(response.topic, current)
    return Bean(
        _id=_id,
        url=_id,
        kind=GENERATED,
        created=current,
        updated=current,
        collected=current,
        title=response.title,
        summary=response.summary,
        content=response.content,
        num_words_in_title=num_words(response.title),
        num_words_in_summary=num_words(response.summary),
        num_words_in_content=num_words(response.content),
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
        self.news_writer = agent_from_path("gpt-4.1-mini", writer_base_url, writer_api_key, max_input_tokens=writer_context_len, system_prompt=NEWSRECAP_COMPOSER_SYSTEM_PROMPT, json_mode=False)
        self.blog_writer = agent_from_path("gpt-4.1-mini", writer_base_url, writer_api_key, max_input_tokens=writer_context_len, system_prompt=OPINION_COMPOSER_SYSTEM_PROMPT, json_mode=False)

        self.max_articles_per_page = max_articles_per_page
        if backup_azstorage_conn_str: self.backup_container = initialize_azblobstore(os.getenv("AZSTORAGE_CONN_STR"), "composer")

    def create_input(self, query: str = None, embedding: list[float] = None,  filter: dict = None):
        if filter: filter.update(GIST_FILTER)
        else: filter = GIST_FILTER

        if query: embedding = self.embedder.embed(query)
        if embedding: beans = self.db.vector_search_beans(embedding, self.bean_similarity, filter, project=GIST_PROJECT)
        else: beans = self.db.query_beans(filter, project=GIST_PROJECT)

        if ic(len(beans)) < MIN_ITEMS_THRESHOLD: return

        gists = "\n".join([bean.gist for bean in beans])
        return f"{query},{gists}"
    
    def extract_topics(self, page, kind = None):
        topic_input = self.create_input(
            query=f"domain: {page}",
            filter={K_KIND: kind} if kind else None
        )
        if not topic_input: return 
        response = self.topic_extractor.run(topic_input)

        self.backup_topics(topic_input, response)
        return response
            
    def compose_article(self, topic, kind = None):  
        article_input = self.create_input(
            query=f"topic: {topic}",
            filter={K_KIND: kind} if kind else None
        )
        if not article_input: return 
        if kind == BLOG: response = self.blog_writer.run(article_input)
        else: response = self.news_writer.run(article_input)

        self.backup_article(article_input, response)
        return response

    def backup_topics(self, topic_input: str, response):
        if not self.backup_container or not response: return 

        try:
            trfile = "topics-"+now().strftime("%Y-%m-%d")+".jsonl" 
            self.backup_container.upload_blob(trfile, json.dumps({'input': topic_input, 'output': response.raw})+"\n", BlobType.APPENDBLOB)
            log.info("backed up", extra={'source': trfile, 'num_items': 1})
        except Exception as e: 
            log.warning("backup failed", extra={"source": trfile, "num_items": 1})

    def backup_article(self, article_input: str, response):
        if not self.backup_container or not response: return 
        current = now()

        try:
            srvfile = make_article_id(response.title, current)
            self.backup_container.upload_blob(srvfile, response.raw, BlobType.BLOCKBLOB)
            
            trfile = "articles-"+current.strftime("%Y-%m-%d")+".jsonl"
            self.backup_container.upload_blob(trfile, json.dumps({'input': article_input, 'output': response.raw})+"\n", BlobType.APPENDBLOB)           
            log.info("backed up", extra={'source': trfile, 'num_items': 1})
            return srvfile
        except: 
            log.warning("backup failed", extra={'source': trfile, 'num_items': 1})

    def store_articles(self, responses: list):      
        if not responses: return
        beans = list(map(make_bean, responses))
        count = self.db.store_beans(beans)
        log.info("stored articles", extra={'source': responses[0].topic, 'num_items': count})
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
            
            sample_topics = sorted(topics.topics, min(self.max_articles_per_page, len(topics.topics)))
            ic(sample_topics)
            # articles = list(map(self.compose_article, sample_topics))
            # self.store_articles(articles)
            # total += len(articles)            
        
        log.info("total composed", extra={"source": run_id, "num_items": total})

        # def truncate_list(self, gists: list[str]):
        #     total_token = 0
        #     for i in range(len(gists)):
        #         count = count_tokens(gists[i])
        #         if total_token + count >= self.context_len: break
        #         else: total_token += count
        #     return gists[:i]


if __name__ == "__main__":
    orch = Orchestrator(
        os.getenv('MONGODB_CONN_STR'),
        os.getenv('DB_NAME'),
        embedder_path=os.getenv("EMBEDDER_PATH"),
        embedder_context_len=int(os.getenv("EMBEDDER_CONTEXT_LEN")),
        bean_distance=0.3,
        writer_path=os.getenv("WRITER_PATH"), 
        writer_base_url=os.getenv("WRITER_BASE_URL"),
        writer_api_key=os.getenv("WRITER_API_KEY"),
        writer_context_len=CONTEXT_LEN,
        max_articles_per_page=3,
        backup_azstorage_conn_str=os.getenv('AZSTORAGE_CONN_STR')
    )
    orch.run()
