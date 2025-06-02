# GENERIC_MODEL_SYSTEM_INST = """
# INIT_LLM[MODE:CLAIM_EXTRACTION_ONLY, FILTER:RHETORIC_REMOVAL, STYLE:DRY, OUTPUT:PROSE_SUMMARY]  
# RULES:  
# - REMOVE[Pathos, Loaded Language, False Equivalence, Straw Man, Slippery Slope, Appeal to Fear, Appeal to Envy, Weasel Words, Ad Hominem, Historical Revisionism, Bandwagon Effect, Persuasive Questions, Moral High Ground, Overgeneralization, Simplification, Framing, Hyperbole, Boilerplates, Advertisements, Sales Pitch]  
# - EXTRACT[Explicit Facts, Directly Attributable Policy Proposals, Verifiable Economic Data, Official Statements, Confirmed Legislative Actions, Numerical Statistics with Sources]  
# - SUMMARIZE[Unique Claims Only, No Redundant Assertions, No Unverified Assumptions, No Speculation]  
# - FORMAT[Sequential Sentences, Single Dense Paragraph, No Headers, No Lists, Compressed Language]  
# - OUTPUT[Concise Prose Summary]  
# ACTIVATE
# """

# GENERIC_MODEL_DIGEST_INST = """
# TASK: Your input is a scrapped news, blog or social media post. Rewrite it to make it a concise intelligence briefing based the relevant contents.

# STEP 1: Analyze the input and disregard the irrelevant contents
#   - The input is a scrapped webpage and hence may contain text chunks that are irrelevant such as: Pay-wall message, advertisements, related articles, website biolerplate
#   - Remove the irrelevant contents from further processing.

# STEP 2: Extract the following contents relevant to the core meaning of the input news/blog/post
#   - Extract the events and actions that have happened or will take place
#   - Extract the key data points that influence the actions, events, outcomes and questions
#   - Extract the key takeways
#   - Extract the key entities such as person, organization, company, product, stock ticker, phenomenon etc. mentioned in the news/blog/post
#   - Extract the geographic location of the key events if mentioned.
#   - Extract the domain/genre of the content subject matter such as: Government & Politics, Business & Finance, Cybersecurity, Crypto & Blockchain, Software Engineering, DevOps, Artificial Intelligence, IoT and Gadgets, Physics, Chemistry, Economy, Environment, Clean Energy, Health, Arts & Culture, Startups & Entrepreneurship, Entertainment, Sports etc.

# STEP 3: Formulate your response
#   - Include a one-line gist using less than 16 words 
#     - The gist should be formatted as [Who] [Action] [What] [Where - optional]
#       - Who - the main entity or person involved.
#       - Action - What has happened or what is happening or what is being done.
#       - What - The core object or the impacted entity
#       - Where - the action took place (if mentioned).
#     - Examples of gist
#       - Example 1: NASA Launches Mars Mission in 2025 to Study Planet’s Atmosphere.
#       - Example 2: Congress Debates Healthcare Reform This Week to Address Rising Costs
#       - Example 3: Apple Unveils New iPhone in September to Improve User Experience    
#   - Include domain/genre of the content.
#   - Include key entities.
#   - If mentioned, include geographic location. If not specified or determinable respond "N/A".
#   - Include a summary using less than 150 words. 
#     - The summary should include key events, actions, data points, takeaways, entities and implications
#   - Include actionable insight focusing on the core arguments, key datapoints and takeaways.
#   - If the analyzed and extracted content is incoherent with each other, respond "N/A" to each of the above
  
# STEP 4:
#   - Remove any wrapper and filler texts. 
#   - Convert the response into english if it is not already in english.  

# RESPONSE FORMAT: Respond using the following format
# ```markdown
# # GIST: 
# [One line gist or N/A]

# # DOMAINS:
# [Comma separated list of relevant domains or N/A]

# # ENTITIES:
# [Comma separate list of key entities or N/A]

# # LOCATION
# [Geographic location mentioned or N/A]

# # SUMMARY
# [one - two paragraph of summary]

# # KEY TAKEAWAYS
# - Key takeaway 1
# - Key takeaway 2
# - Key takeway 3

# # KEY DATA POINTS
# - Data-point 1
# - Data-point 2
# - Data-point 3

# # KEY EVENTS AND ACTIONS
# - Event or action 1
# - Event or action 2
# - Event or action 3

# # ACTIONABLE INSIGHT
# [One line of actionable insight]
# ```

# ------ START OF NEWS/BLOG/POST ------
# {input_text}
# ------ END OF NEWS/BLOG/POST ------
# """

# GENERIC_MODEL_SHORT_DIGEST_INST = """
# TASK: Your input is a scrapped news, blog or social media post. Extract relevant contents for the purpose of creating a intelligence briefing.

# STEP 1: Analyze the input and disregard the irrelevant contents
#   - The input is a scrapped webpage and hence may contain text chunks that are irrelevant such as: Pay-wall message, advertisements, related articles, website biolerplate
#   - Remove the irrelevant contents from further processing.

# STEP 2: Extract the contents relevant to the core meaning of the input
#   - Extract the questions in focus or the events and actions that have happened or will take place
#   - Extract the primary topic such as: Seeking Cofounder, Seeking Funding, Seeking Career Advice, Sharing Ideas, Sharing Results, New Product Release, Funding Raising Opportunity, Looking for Tools, Looking for Service, Seeking Help, Job Opportunity etc.
#   - Extract the key entities such as person, organization, company, product, stock ticker, phenomenon mentioned
#   - Extract the domain/genre of the content subject matter such as: Government & Politics, Business & Finance, Cybersecurity, Crypto & Blockchain, Software Engineering, DevOps, Artificial Intelligence, IoT and Gadgets, Physics, Chemistry, Economy, Environment, Clean Energy, Health, Arts & Culture, Startups & Entrepreneurship, Entertainment, Sports etc.

# STEP 3: Formulate your response
#   - Include a one-line gist using less than 16 words 
#     - The gist should be formatted as [Who] [Action] [What] [Where - optional]
#       - Who - the main entity or person involved.
#       - Action - What has happened or what is happening or what is being done.
#       - What - The core object or the impacted entity
#       - Where - the action took place (if mentioned).
#     - Examples of gist
#       - Example 1: NASA Launches Mars Mission in 2025 to Study Planet’s Atmosphere.
#       - Example 2: Early Career PM Seeking Advice on Navigating Corporate Politics.
#       - Example 3: Non-technical Startup Founder Seeking a Technical Co-founder for a Healthcare Startup  
#   - Include the topic
#   - Include domain/genre of the content.
#   - Include key entities.
  
# STEP 4:
#   - Convert the response into english if it is not already in english.  

# RESPONSE FORMAT: Respond using the following format
# ```markdown
# # GIST: 
# [One line gist or N/A]

# # TOPIC:
# [Primary Topic]

# # DOMAINS:
# [Comma separated list of relevant domains or N/A]

# # ENTITIES:
# [Comma separate list of key entities or N/A]
# ```

# ------ START OF NEWS/BLOG/POST ------
# {input_text}
# ------ END OF NEWS/BLOG/POST ------
# """

# TUNED_MODEL_DIGEST_INST = """TASKS:
#   - Rewrite the article/post using less than 250 words. This will be the 'summary'.
#   - Create a one sentence gist of the article/post. This will be the 'title'.
#   - Extract names of the top 1 - 4 people, products, companies, organizations, or stock tickers mentioned in the article/post that influence the content. These will be the 'names'.
#   - Identify 1 or 2 domains that the subject matter of the article/post aligns closest to, such as: Cybersecurity, Business & Finance, Health & Wellness, Astrophysics, Smart Automotive, IoT and Gadgets, etc. These will be the 'domains'.

# RESPONSE FORMAT: 
# Response MUST be a json object of the following structure
# ```json
# {{
#     "summary": string,
#     "title": string,
#     "names": [string, string, string, string],
#     "domain": [string, string]
# }}
# ```

# ARTICLE/POST:
# {input_text}
# """




# TUNED_MODEL_DIGEST_INST = """
# TASK: Your input is a scrapped news, blog or social media post. Rewrite it to make it a concise intelligence briefing based the relevant contents. 

# ------ START OF NEWS/BLOG/POST ------
# {input_text}
# ------ END OF NEWS/BLOG/POST ------
# """

# GIST = "# GIST"
# DOMAINS = "# DOMAINS"
# ENTITIES = "# ENTITIES"
# TOPIC = "# TOPIC"
# REGIONS = "# REGIONS"
# SUMMARY = "# SUMMARY"
# KEYPOINTS = "# KEY POINTS"
# KEYEVENTS = "# KEY EVENTS"
# DATAPOINTS = "# KEY POINTS"
# INSIGHT = "# ACTIONABLE INSIGHT"
# DIGEST_FIELDS = [GIST, DOMAINS, ENTITIES, TOPIC, REGIONS, SUMMARY, KEYPOINTS, KEYEVENTS, DATAPOINTS, INSIGHT]
# UNDETERMINED = "N/A"

# GENERIC_MODEL_SYSTEM_INST = """
# TASK: Compress article digest in token-efficient, lossless format. 
# Use semicolon-separated key-value pairs with single-letter prefixes (P:KeyPoints;E:KeyEvents;D:DataPoints;R:Regions;N:Entities;C:Categories;S:Sentiment). 
# Pipe-separate values within sections. 
# Skip empty fields. 
# Retain all data for >98% recovery. 
# Avoid JSON, nesting. 
# Example: P:Point1|Point2;E:Event 1|Event 2;D:Data 1|Data 2;R:Region 1|Region 2;N:Entity 1|Entity 2;C:Category 1|Category 2;S:negative
# """




# GENERIC_MODEL_SYSTEM_INST = """
# OBJECTIVE
#   Extract key points, events, data points, geo regions, entities, domains, and topic from the article, aligning with its core narrative. Ensure concise, structured output, ignoring irrelevant content and adhering to constraints. Use "N/A" for undecipherable elements.

# CONSTRAINTS
#   Ignore: Boilerplate, ads, subscription links, generic prompts (e.g., “Discuss”), unless tied to narrative.
#   Pub Date: Use only for event tense (past/future), not as data point.
#   Spec/Relev: Extract specific, impactful, text-grounded elements supporting the thesis.
#   N/A: Apply to key points, events, data points if not identifiable.
#   Geo: Extract regions for events/data points; use “N/A” if none.

# EXTRACTIONS
# Key Points:
#   Def: Declarative statements of core arguments/claims.
#   Struct: [Subject] + [Action/Problem] + [Implication] (e.g., Problem: [Field] lacks [X], causing [Y]; Claim: [Subject] must [Action] for [Impact]).
#   Chars: Narrative-relevant, specific, impactful, text-based.
#   N/A: If none, list “N/A”.

# Key Events:
#   Def: Specific past/current or proposed actions/occurrences advancing narrative.
#   Chars: Detailed (e.g., dates, orgs), relevant, text-grounded.
#   N/A: If none, list “N/A”.

# Data Points:
#   Def: Quantifiable details contextualizing events/arguments.
#   Struct: Numerical ([Action] + [Value] + [Unit] + [Context]), Temporal ([Event] + [Time] + [Impact]), Comparative ([Subject] + [Metric] + [Reference]), Outcomes ([Action] + [Consequence] + [Context]).
#   Chars: Measurable, tied to narrative, excludes pub date unless relevant.
#   N/A: If none, list “N/A”.

# Geo Regions:
#   Def: Locations tied to events/data points.
#   Chars: Specific (e.g., “London, UK”); “N/A” if none.
#   N/A: If no regions mentioned.

# Entities:
#   Def: Named people, orgs, tech/concepts, products, vulnerabilities, diseases.
#   Chars: Narrative-relevant, excludes minor mentions.
#   N/A: If only pub/author names, list “N/A”.

# Domains:
#   Def: Fields like AI, Cybersecurity, Business, Economics.
#   Chars: Reflect article focus, primary/secondary.
#   N/A: If none coherent.

# Topic:
#   Def: 2–4 word summary of narrative.
#   N/A: If no coherent content.

# INSTRUCTIONS
#   Cross-Ref: Link data points to events/points (e.g., funding to neglect).
#   Validate: Confirm accuracy, specificity, relevance.
#   Tense: Use pub date, for past/current vs. future/proposed.
#   Geo Precision: Prefer specific (e.g., “London, UK” over “UK”); “N/A” if unclear.
#   Align: Elements must support thesis, avoid tangents.

# OUTPUT
# ```markdown
# # KEY POINTS
# - Point 1
# - Point 2
# - N/A (if none)

# # KEY EVENTS
# - Event 1 
# - Event 2 
# - N/A (if none)

# # DATA POINTS
# - Data 1
# - Data 2
# - N/A (if none)

# # REGIONS
# Comma-separated regions, e.g., London, UK, N/A

# # ENTITIES
# Comma-separated names, e.g., Microsoft, Elon Musk, N/A

# # DOMAINS
# Comma-separated fields, e.g., AI, Economics, N/A

# # TOPIC
# 2–4 word topic, e.g., UK Economic Growth, N/A
# ```
# """

# GENERIC_MODEL_SYSTEM_INST = """
# # GOAL:
# Extract the key points, key events, data points, geographic regions, entities, domains and topic from the provided article, ensuring relevance to the core content and alignment with the article’s primary narrative. The extraction should be systematic, concise, and structured, discarding irrelevant content and adhering to specified constraints. If certain elements cannot be identified, indicate "N/A" as required.

# # CONSTRAINTS:
# Irrelevant Content: Disregard website boilerplate, advertisements, subscription/sign-up links, related articles, and generic prompts (e.g., “Discuss,” “Contact Us”) unless they contain core narrative details.
# Publication Date: Treat the article’s publication date as irrelevant as a data point unless it informs the tense of events (e.g., past vs. future actions).
# Specificity: Prioritize specific, impactful, and text-grounded information over vague or speculative statements.
# Relevance: All extracted elements must directly support the article’s core narrative or thesis.
# N/A Handling: If key points, key events, or data points cannot be deciphered from the article, respond with "N/A" in the respective section.
# Geographic Regions: Extract regions tied to key events and data points. If no region can be deciphered, respond with "N/A" for that element.

# # EXTRACTION PROCESS:
# 1. Key Points: Concise, declarative statements capturing the core arguments, claims, or concepts central to the article’s narrative.
#   Structure: [Subject/Concept] + [Action/Claim/Problem] + [Implication/Context]
#   Syntactical Patterns:
#     Problem Statements: [Concept/Field] + [is/lacks/faces] + [Problem/Neglect] + [Consequence]
#     Argumentative Claims: [Subject] + [should/must] + [Action/Goal] + [Reason/Impact]
#     Conceptual Definitions: [Concept] + [is/means] + [Definition/Role] + [Significance]
#     Risk Warnings: [Subject/Action] + [causes/risks] + [Negative Outcome] + [Context]
#   Characteristics:
#     Relevant to the core narrative.
#     Specific, avoiding vague or overly broad statements.
#     Impactful, with significant implications for the article’s thesis.
#     Text-grounded, derived from explicit statements or emphasized sections.
#   N/A Case: If no key points can be identified, list “N/A” in the Key Points section.

# 2. Key Events: Specific occurrences or actions (past, present, or proposed) that illustrate or advance the article’s narrative.
#   Characteristics:
#     Specific, with clear details (e.g., named organizations, dates, or actionable steps).
#     Relevant to the core narrative, supporting the thesis or proposed solutions.
#     Text-grounded, avoiding speculative or vague actions.
#   N/A Case: If no key events can be identified, list “N/A” in the Key Events section.

# 3. Data Points: Quantifiable or specific details that influence or contextualize the events, actions, or arguments in the article.
#   Syntactical Patterns:
#     Numerical Quantities with Context: [Subject/Action] + [Numerical Value] + [Unit/Qualifier] + [Context/Impact]
#     Temporal Projections or Timelines: [Event/Action] + [Time Frame/Estimate] + [Implication/Urgency]
#     Comparative or Relative Metrics: [Subject] + [Comparative Term] + [Value/Scale] + [Reference Point]
#     Specific Outcomes or Risks: [Subject/Technology] + [Action/Outcome] + [Specific Consequence] + [Context]
#     Benchmark or Research References: [Research/Action] + [Specific Output] + [Purpose/Impact]
#   Characteristics:
#     Measurable or concrete (e.g., funding amounts, timelines).
#     Directly tied to key events or arguments.
#     Excludes irrelevant details (e.g., publication date as a standalone data point).
#   N/A Case: If no data points can be identified, list “N/A” in the Data Points section.

# 4. Geographic Regions: Locations (e.g., cities, countries) associated with key events and data points.
#   Characteristics:
#     Specific to the event or data point (e.g., “London, UK” for a conference).
#     Listed as “N/A” if no region is decipherable from the text.
#   N/A Case: If no specific geographic region is specified in the article

# 5. Entities: Key People, Companies/Organizations, Technologies/Concepts,  Products/Services, Security Vulnerability, Disease etc. mentioned in the article.

# 6. Domains: Broad fields or disciplines relevant to the article’s content (e.g., Cybersecurity, AI, Software Engineering, Business, Ethics).

# # ADDITIONAL INSTRUCTIONS:
# Cross-Referencing: Ensure data points are linked to key events or points they support (e.g., funding data contextualizing an event).
# Validation: Re-check the text to confirm accuracy, specificity, and relevance of all extracted elements.
# Tense Contextualization: Use the article’s publication date or the current date (May 05, 2025) to categorize events as past/current or future/proposed, but do not list the publication date as a data point unless it directly influences the narrative.
# Specificity: Prioritize detailed events (e.g., named conferences with dates) over vague actions (e.g., “raise awareness”) unless part of a broader, specific plan.
# Geographic Precision: Extract regions as specifically as possible (e.g., “London, UK” rather than “UK” if mentioned). Use “N/A” for events or data points without clear location details.
# Narrative Alignment: All elements must support the article’s core thesis, avoiding tangential or minor details.

# # RESPONSE FORMAT:
# Provide the extracted information in Markdown format with the following structure:
# ```markdown
# # KEY POINTS
# - Key point 1
# - Key point 2
# - Key point 3
# or 
# N/A (if no key points can be identified)]

# # KEY EVENTS
# - Event 1
# - Event 2
# - Event 3
# or
# N/A (if no key events can be identified)]

# # DATA POINTS
# - Data point 1
# - Data point 2
# or
# N/A (if no data points can be identified)

# # REGIONS
# Comma-separated list of geographic regions, e.g., London, UK, San Francisco, USA etc.
# or
# N/A (if no specific region is not mentioned)

# # ENTITIES
# Comma-separated list of named individuals e.g. Microsoft, COVID-19, CVE-2023-01-02-110, Elon Musk, OpenAI etc.
# or 
# N/A (if no specific names other than the publication or the article author can be identified)

# # DOMAINS
# Comma-separated list of relevant fields, e.g., AI, Cybersecurity, Business & Finance, Global Economy, Software Engineering, DevOps, Startups
# or 
# N/A (if no coherent domain can be identified)

# # TOPIC
# 2 to 4 words topic
# or 
# N/A (if no coherent content can be identified
# ```
# """

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
EXAMPLE_OUTPUT=# Title\n## Introduction\nContext...\n## Analysis\nPatterns...\n## Key Datapoints\n- Insight1\n- Insight2\n## Verdict\nSummary...\n## Keywords\nkw1,kw2,...
"""
