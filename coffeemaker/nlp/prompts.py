DIGESTOR_SYSTEM_PROMPT = """
TASK:
INPUT=news/blog article
OUTPUT=compressed token-efficient, lossless format
INSTRUCTIONS:
1=Extract KeyPoints, KeyEvents, DataPoints, Regions, Entities
2=Use semicolon-separated key-value pairs with single-letter prefixes (P:KeyPoints;E:KeyEvents;D:DataPoints;R:Regions;N:Entities). 
3=Pipe-separate values within sections. 
4=Skip empty fields. 
5=Retain all data for >98% recovery. 
6=Avoid JSON, nesting. 
EXAMPLE_OUTPUT=P:Key Point 1|Key Point 2;E:Key Event 1|Key Event 2;D:Datapoint 1|Datapoint 2;R:Country|City|Continent;N:Entity 1|Entity 2
"""

TOPIC_EXTRACTOR_SYSTEM_PROMPT="""
TASK:
INPUT=Domain:String,Articles:List<ArticleString>;ArticleString=Format<U:YYYY-MM-DD;P:Summary|...;E:Events|...;D:Datapoints|...;R:Regions|...;N:Entities|...;C:Categories|...;S:Sentiment|...>
OUTPUT=Dict<TopicTitle,Dict<frequency:Int,keywords:List<String>>>:JSON
INSTRUCTIONS:
1=AnalyzeArticles;UseFields=U,P,E,D,N;GenerateTopics=Dynamic,Specific,Granular;Cluster=SemanticSimilarity;Avoid=GenericCategoriesFromC;AllowMultiTagging=True
2=CountFrequency;Frequency=NumArticlesPerTopic
3=FilterFrequency=Min2;KeepTopics=Frequency>=2
4=GenerateKeywords;Keywords=Specific,Searchable;From=N,R;MinimizeFalsePositives=True;Include=Entities,Phrases
5=OutputFormat=Dict;Key=TopicTitle;Value=Dict;ValueFormat=frequency:Int,keywords:List<String>
EXAMPLE_OUTPUT={"TopicTitle1":{"frequency":4,"keywords":["kw1","kw2"]},"TopicTitle2":{"frequency":2,"keywords":["kw3","kw4"]}}
"""

OLD_OPINION_COMPOSER_SYSTEM_PROMPT="""
TASK:INPUT=Topic:String,Articles:List<ArticleString>;ArticleString=Format<U:YYYY-MM-DD;P:Summary|...;N:Entities|...;E:Events|...;C:Categories|...;S:Sentiment|...>;OUTPUT=OpinionPiece:Markdown;"
INSTRUCTIONS:
1=AnalyzeArticles;UseFields=P,N,E,S;Identify=Patterns,Themes,Insights;Grounding=Normative,MultiArticle;Focus=TopicRelevance;
2=GenerateOpinionPiece;Structure=Introduction,Analysis,Takeaways,Verdict;Introduction=Context,TopicOverview;Analysis=SynthesizePatterns,ReportEntitiesEvents,PresentSentiment;Takeaways=KeyInsights,Implications;Verdict=TechnicalSummary;Content=CoreFindings,KeyData;Style=Direct,Technical,Factual;Length=400-600Words;Avoid=Speculation,Narrative,EmotiveLanguage;VerdictLength=10-20Words;
3=OutputFormat=Markdown;Sections=#Introduction,##Analysis,##KeyTakeaways,##Verdict;Include=TopicInTitle;
EXAMPLE_OUTPUT=# Title\n## Introduction\nContext...\n## Analysis\nPatterns...\n## KeyTakeaways\n- Insight1\n- Insight2\n## Verdict\nSummary...
"""

NEWRECAPT_SYSTEM_PROMPT="""
TASK:INPUT=List<NewsDigest>;NewsDigest=Format<U:YYYY-MM-DD;P:Summary|...;E:Events|...;D:Datapoints|...;R:Regions|...;N:Entities|...;S:Sentiment|...>;OUTPUT=NewsRecap:Markdown
INSTRUCTIONS:
1=AnalyzeArticles;UseFields=U,P,E,D,R,N,S;Identify=Patterns,Themes,Insights,TimeTrends,Topics,NamedEntities;Grounding=Normative,MultiArticle;
2=GenerateNewsRecap;Structure=Introduction,Analysis,Datapoints,Verdict,Keywords;Introduction=Context,AnalysisOverview;Analysis=SynthesizePatterns,ReportEntitiesEvents,PresentSentiment;Datapoints=KeyData,Implications;Verdict=TechnicalSummary;Keywords=NamedEntities,NamedRegions,CommaSeparated;Content=CoreFindings,KeyData;Style=Direct,Technical,Factual,DataCentric;Length=400-600Words;Avoid=Speculation,Narrative,EmotiveLanguage;VerdictLength=10-20Words;
3=OutputFormat=Markdown;Sections=## Introduction,## Analysis,## KeyDatapoints,## Verdict,## Keywords;Include=TopicsInTitle;
EXAMPLE_OUTPUT=# Topics\n## Introduction\nContext...\n## Analysis\nPatterns...\n## Key Datapoints\n- Insight1\n- Insight2\n## Verdict\nSummary...\n## Keywords\nkw1,kw2,...
"""
