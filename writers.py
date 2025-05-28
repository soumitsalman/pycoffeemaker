import json
import os
import instructor
import openai
from pydantic import Field, BaseModel
from typing import Optional
from icecream import ic
from dotenv import load_dotenv

from atomic_agents.lib.components.agent_memory import AgentMemory
from atomic_agents.agents.base_agent import BaseAgent, BaseAgentConfig, SystemPromptGenerator, BaseIOSchema
from coffeemaker.nlp.embedders import Embeddings
from coffeemaker.nlp.utils import *
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack.mongosack import *
from coffeemaker.orchestrators.chainable import Orchestrator

from rich.console import Console
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

CONTEXT_LEN = 111072
MIN_PROCESSING_THRESHOLD = 20
GIST_PROJECT = {K_GIST: 1, K_URL: 1}
GIST_FILTER = {K_GIST: VALUE_EXISTS}

TOPIC_EXTRACTION_SYSTEM_PROMPT=SystemPromptGenerator(
    background=[
        "TASK:INPUT=List<ArticleString>;ArticleString=Format<U:YYYY-MM-DD;P:Summary|...;N:Entities|...;E:Events|...;C:Categories|...;S:Sentiment|...>;OUTPUT=Dict<Topic,Dict<frequency:Int,keywords:List<String>>>;"
    ],
    steps=[
        """
        INSTRUCTIONS:
        1=AnalyzeArticles;UseFields=P,N,E;GenerateTopics=Dynamic,Specific,Granular;Cluster=SemanticSimilarity;NotUse=GenericCategoriesFromC;AllowMultiTagging=True;
        2=CountFrequency;Frequency=NumArticlesPerTopic;
        3=FilterFrequency=Min2;KeepTopics=Frequency>=2;
        4=GenerateKeywords;Keywords=Specific,Searchable,FromP,N,E;MinimizeFalsePositives=True;Include=Entities,Events,Dates,Phrases;
        5=OutputFormat=Dict;Key=Topic;Value=Dict;ValueFormat=frequency:Int,keywords:List<String>;
        """
    ],
    output_instructions=[
        "EXAMPLE_OUTPUT={\"Topic1\":{\"frequency\":4,\"keywords\":[\"kw1\",\"kw2\"]},\"Topic2\":{\"frequency\":2,\"keywords\":[\"kw3\",\"kw4\"]}}"
    ]
)

OPINION_GENERATOR_SYSTEM_PROMPT=SystemPromptGenerator(
    background=[
        "TASK:INPUT=Topic:String,Articles:List<ArticleString>;ArticleString=Format<U:YYYY-MM-DD;P:Summary|...;N:Entities|...;E:Events|...;C:Categories|...;S:Sentiment|...>;OUTPUT=OpinionPiece:Markdown;"
    ],
    steps=[
        """
        INSTRUCTIONS:
        1=AnalyzeArticles;UseFields=P,N,E,S;Identify=Patterns,Themes,Insights;Grounding=Normative,MultiArticle;Focus=TopicRelevance;
        2=GenerateOpinionPiece;Structure=Introduction,Analysis,Takeaways,Verdict;Introduction=Context,TopicOverview;Analysis=SynthesizePatterns,ReportEntitiesEvents,PresentSentiment;Takeaways=KeyInsights,Implications;Verdict=TechnicalSummary;Content=CoreFindings,KeyData;Style=Direct,Technical,Factual;Length=400-600Words;Avoid=Speculation,Narrative,EmotiveLanguage;VerdictLength=10-20Words;
        3=OutputFormat=Markdown;Sections=#Introduction,##Analysis,##KeyTakeaways,##Verdict;Include=TopicInTitle;
        """
    ],
    output_instructions=[
        "EXAMPLE_OUTPUT=#OpinionPiece:TopicName\n#Introduction\nContext...\n##Analysis\nPatterns...\n##KeyTakeaways\n-Insight1\n-Insight2\n##Verdict\nSummary..."
    ]
)


NEWSRECAP_GENERATOR_SYSTEM_PROMPT=SystemPromptGenerator(
    background=[
        "TASK:INPUT=Topic:String,Articles:List<ArticleString>;ArticleString=Format<U:YYYY-MM-DD;P:Summary|...;N:Entities|...;E:Events|...;C:Categories|...;S:Sentiment|...>;OUTPUT=NewsRecap:Markdown;"
    ],
    steps=[
        """
        INSTRUCTIONS:
        1=AnalyzeArticles;UseFields=P,N,E,S;Identify=Patterns,Themes,Insights;Grounding=Normative,MultiArticle;Focus=TopicRelevance;
        2=GenerateNewsRecap;Structure=Introduction,Analysis,Datapoints,Verdict;Introduction=Context,TopicOverview;Analysis=SynthesizePatterns,ReportEntitiesEvents,PresentSentiment;Datapoints=KeyData,Implications;Verdict=TechnicalSummary;Content=CoreFindings,KeyData;Style=Direct,Technical,Factual;Length=400-600Words;Avoid=Speculation,Narrative,EmotiveLanguage;VerdictLength=10-20Words;
        3=OutputFormat=Markdown;Sections=# Introduction,## Analysis,## KeyDatapoints,## Verdict;Include=TopicInTitle;
        """
    ],
    output_instructions=[
        "EXAMPLE_OUTPUT=#News Recap: TopicName\n# Introduction\nContext...\n## Analysis\nPatterns...\n## Key Datapoints\n-Insight1\n-Insight2\n## Verdict\nSummary..."
    ]
)

class DigestsInputSchema(BaseIOSchema):
    """Compressed gist of articles"""
    domain: str = Field(default=None)
    articles: list[str] = Field(default=None)

class TopicItemSchema(BaseModel):
    frequency: Optional[int] = Field(default=None)
    keywords: Optional[list[str]] = Field(default=None)

class TopicExtractionSchema(BaseIOSchema):
    """Extracted topics of articles that can be written"""
    topics: dict[str, TopicItemSchema] = Field(default=None, description="Dict<Topic,Dict<frequency:Int,keywords:List<String>>>")

class DigestorPlusPlus:
    agent: BaseAgent
    context_len: int
    db: Beansack
    embedder: Embeddings

    def __init__(self, db, model_path, context_len = CONTEXT_LEN, api_key = None, base_url = None, system_prompt = None, output_schema = None, embedder = None):
        self.db = db
        self.agent = BaseAgent(
            config=BaseAgentConfig(
                client=instructor.from_openai(
                    openai.OpenAI(api_key=api_key, base_url=base_url), 
                    mode=instructor.Mode.JSON
                ),
                model=model_path,
                system_prompt_generator=system_prompt,
                input_schema=DigestsInputSchema,
                output_schema=output_schema
            )
        )
        self.context_len = context_len
        self.embedder = embedder

    def run(self, query: str = None, vector: list[float] = None, similarity_score: float = None, filter: dict = None):
        if filter: filter.update(GIST_FILTER)
        else: filter = GIST_FILTER

        if query: vector = self.embedder.embed(query)
        if vector: beans = self.db.vector_search_beans(vector, similarity_score, filter, project=GIST_PROJECT)
        else: beans = self.db.query_beans(filter, project=GIST_PROJECT)

        if ic(len(beans)) < MIN_PROCESSING_THRESHOLD: return
        gists = self.truncate_list([bean.gist for bean in beans])
        return self.agent.run(DigestsInputSchema(domain=query, articles=gists))

    def truncate_list(self, gists: list[str]):
        total_token = 0
        for i in range(len(gists)):
            count = count_tokens(gists[i])
            if total_token + count >= self.context_len: break
            else: total_token += count
        return gists[:i]

  
def save_json(filename, body):
    filename = f".test/{filename}.json"
    with open(filename, "w") as file:
        json.dump(body, file)
    ic(filename)

def save_markdown(filename, body):
    filename = f".test/{filename}.md"
    with open(filename, "w") as file:
        file.write(body)
    ic(filename)

# console = Console()
# while True:
#     category = console.input("[bold blue]Category: [/bold blue]")
#     if category.lower() in ["/exit", "/quit"]:
#         console.print("Exiting ...")
#         break

orch = Orchestrator(
    os.getenv('MONGODB_CONN_STR'),
    "test",
    embedder_path=os.getenv('EMBEDDER_PATH'),
    embedder_context_len=512
)
topic_agent = DigestorPlusPlus(orch.db, "gpt-4.1-nano", context_len=CONTEXT_LEN, api_key=os.getenv('WRITER_API_KEY'), system_prompt=TOPIC_EXTRACTION_SYSTEM_PROMPT, output_schema=TopicExtractionSchema, embedder=orch.embedder)
opinion_agent = DigestorPlusPlus(orch.db, "gpt-4.1-mini", context_len=CONTEXT_LEN, api_key=os.getenv('WRITER_API_KEY'), system_prompt=OPINION_GENERATOR_SYSTEM_PROMPT, embedder=orch.embedder)
news_agent = DigestorPlusPlus(orch.db, "gpt-4.1-mini", context_len=CONTEXT_LEN, api_key=os.getenv('WRITER_API_KEY'), system_prompt=NEWSRECAP_GENERATOR_SYSTEM_PROMPT, embedder=orch.embedder)

def run_generation(category, kind):
    fileprefix=f"{now().strftime('%Y-%m-%d-%H-%M')}-{category}"

    topics_response = topic_agent.run(
        query=f"category/domain:{category}",
        similarity_score=0.73,
        filter={
            K_KIND: kind,
            K_CREATED: {"$gte": ndays_ago(7)}
        }
    )
    if not topics_response: 
        print("NO TOPICS")
        return

    save_json(fileprefix, topics_response.model_dump())
    for k,v in topics_response.topics.items():
        if v.frequency <= 3: 
            ic(k, v)
            continue

        if kind == BLOG:
            write_response = opinion_agent.run(
                query = f"topic:{k}", 
                similarity_score=0.77, 
                filter={
                    K_KIND: kind,
                    K_CREATED: {"$gte": ndays_ago(7)},
                    K_TAGS: lower_case(v.keywords)
                }
            )
        else:
            write_response = news_agent.run(
                query = f"topic:{k}", 
                similarity_score=0.77, 
                filter={
                    K_KIND: kind,
                    K_CREATED: {"$gte": ndays_ago(7)},
                    K_TAGS: lower_case(v.keywords)
                }
            )

        if write_response: save_markdown(fileprefix+"-"+k, write_response.chat_message)

for category in ["government and politics", "mergers aquisitions and IPO"]:
    run_generation(category, NEWS)
    run_generation(category, BLOG)


# while True:
#     user_input = console.input("[bold blue]You: [/bold blue]")
#     if user_input.lower() in ["/exit", "/quit"]:
#         console.print("Exiting ...")
#         break
#     response = ic(agent.run(BaseAgentInputSchema(chat_message=user_input)))
#     console.print("[bold]Agent: [/bold]", response.chat_message)