GENERIC_MODEL_SYSTEM_INST = """
INIT_LLM[MODE:CLAIM_EXTRACTION_ONLY, FILTER:RHETORIC_REMOVAL, STYLE:DRY, OUTPUT:PROSE_SUMMARY]  
RULES:  
- REMOVE[Pathos, Loaded Language, False Equivalence, Straw Man, Slippery Slope, Appeal to Fear, Appeal to Envy, Weasel Words, Ad Hominem, Historical Revisionism, Bandwagon Effect, Persuasive Questions, Moral High Ground, Overgeneralization, Simplification, Framing, Hyperbole]  
- EXTRACT[Explicit Facts, Directly Attributable Policy Proposals, Verifiable Economic Data, Official Statements, Confirmed Legislative Actions, Numerical Statistics with Sources]  
- SUMMARIZE[Unique Claims Only, No Redundant Assertions, No Unverified Assumptions, No Speculation]  
- FORMAT[Sequential Sentences, Single Dense Paragraph, No Headers, No Lists, Compressed Language]  
- OUTPUT[Concise Prose Summary]  
ACTIVATE
"""

GENERIC_MODEL_DIGEST_INST = """
TASK: Your input is a scrapped news, blog or social media post. Rewrite it to make it a concise intelligence briefing based the relevant contents.

STEP 1: Analyze the input and disregard the irrelevant contents
  - The input is a scrapped webpage and hence may contain text chunks that are irrelevant such as: Pay-wall message, advertisements, related articles, website biolerplate
  - Remove the irrelevant contents from further processing.

STEP 2: Extract the relevant contents presented by the core of the article
  - Extract the events and actions that have happened or will take place
  - Extract the key data points that influence the actions, events, outcomes and questions
  - Extract the key takeways presented by the article
  - Extract the key entities such as person, organization, company, product, stock ticker, phenomenon mentioned in the article
  - Extract the geographic location of the key events if mentioned.
  - Extract the domain/genre of the content subject matter such as: Government & Politics, Business & Finance, Cybersecurity, Crypto & Blockchain, Software Engineering, DevOps, Artificial Intelligence, IoT and Gadgets, Physics, Chemistry, Economy, Environment, Clean Energy, Health, Arts & Culture, Startups & Entrepreneurship, Entertainment, Sports etc.

STEP 3: Formulate your response
  - Include a one-line gist using less than 16 words 
    - The gist should be formatted as [Who] [Action] [What] [Where - optional]
      - Who - the main entity or person involved.
      - Action - What has happened or what is happening or what is being done.
      - What - The core object of the article or the impacted entity
      - Where - the action took place (if mentioned).
    - Examples of gist
      - Example 1: NASA Launches Mars Mission in 2025 to Study Planet’s Atmosphere.
      - Example 2: Congress Debates Healthcare Reform This Week to Address Rising Costs
      - Example 3: Apple Unveils New iPhone in September to Improve User Experience    
  - Include domain/genre of the content.
  - Include key entities.
  - If mentioned, include geographic location. If not specified or determinable respond "N/A".
  - Include a summary using less than 150 words. 
    - The summary should include key events, actions, data points, takeaways, entities and implications
  - Include actionable insight focusing on the core arguments, key datapoints and takeaways.
  - If the analyzed and extracted content is incoherent with each other, respond "N/A" to each of the above
  
STEP 4:
  - Remove any wrapper and filler texts. 
  - Convert the response into english if it is not already in english.  

RESPONSE FORMAT: Respond using the following format
```markdown
# GIST: 
[One line gist or N/A]

# DOMAINS:
[Comma separated list of relevant domains or N/A]

# ENTITIES:
[Comma separate list of key entities or N/A]

# LOCATION
[Geographic location mentioned or N/A]

# SUMMARY
[one - two paragraph of summary]

# KEY TAKEAWAYS
- Key takeaway 1
- Key takeaway 2
- Key takeway 3

# ACTIONABLE INSIGHT
[One line of actionable insight]
```

------ START OF NEWS/BLOG/POST ------
{input_text}
------ END OF NEWS/BLOG/POST ------
"""

GENERIC_MODEL_SHORT_DIGEST_INST = """
TASK: Your input is a scrapped news, blog or social media post. Extract relevant contents for the purpose of creating a intelligence briefing.

STEP 1: Analyze the input and disregard the irrelevant contents
  - The input is a scrapped webpage and hence may contain text chunks that are irrelevant such as: Pay-wall message, advertisements, related articles, website biolerplate
  - Remove the irrelevant contents from further processing.

STEP 2: Extract the contents relevant to the core meaning of the input
  - Extract the questions in focus or the events and actions that have happened or will take place
  - Extract the primary topic such as: Seeking Cofounder, Seeking Funding, Seeking Career Advice, Sharing Ideas, Sharing Results, New Product Release, Funding Raising Opportunity, Looking for Tools, Looking for Help, Hiring etc.
  - Extract the key entities such as person, organization, company, product, stock ticker, phenomenon mentioned
  - Extract the domain/genre of the content subject matter such as: Government & Politics, Business & Finance, Cybersecurity, Crypto & Blockchain, Software Engineering, DevOps, Artificial Intelligence, IoT and Gadgets, Physics, Chemistry, Economy, Environment, Clean Energy, Health, Arts & Culture, Startups & Entrepreneurship, Entertainment, Sports etc.

STEP 3: Formulate your response
  - Include a one-line gist using less than 16 words 
    - The gist should be formatted as [Who] [Action] [What] [Where - optional]
      - Who - the main entity or person involved.
      - Action - What has happened or what is happening or what is being done.
      - What - The core object or the impacted entity
      - Where - the action took place (if mentioned).
    - Examples of gist
      - Example 1: NASA Launches Mars Mission in 2025 to Study Planet’s Atmosphere.
      - Example 2: Early Career PM Seeking Advice on Navigating Corporate Politics.
      - Example 3: Non-technical Startup Founder Seeking a Technical Co-founder for a Healthcare Startup  
  - Include the topic
  - Include domain/genre of the content.
  - Include key entities.
  
STEP 4:
  - Convert the response into english if it is not already in english.  

RESPONSE FORMAT: Respond using the following format
```markdown
# GIST: 
[One line gist or N/A]

# TOPIC:
[Primary Topic]

# DOMAINS:
[Comma separated list of relevant domains or N/A]

# ENTITIES:
[Comma separate list of key entities or N/A]
```

------ START OF NEWS/BLOG/POST ------
{input_text}
------ END OF NEWS/BLOG/POST ------
"""

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

TUNED_MODEL_DIGEST_INST = """
TASK: Your input is a scrapped news, blog or social media post. Rewrite it to make it a concise intelligence briefing based the relevant contents. 

------ START OF NEWS/BLOG/POST ------
{input_text}
------ END OF NEWS/BLOG/POST ------
"""

GIST = "# GIST"
DOMAINS = "# DOMAINS"
ENTITIES = "# ENTITIES"
TOPIC = "# TOPIC"
LOCATION = "# LOCATION"
SUMMARY = "# SUMMARY"
TAKEAWAYS = "# KEY TAKEAWAYS"
INSIGHT = "# ACTIONABLE INSIGHT"
DIGEST_FIELDS = [GIST, DOMAINS, ENTITIES, TOPIC, LOCATION, SUMMARY, TAKEAWAYS, INSIGHT]
UNDETERMINED = "N/A"