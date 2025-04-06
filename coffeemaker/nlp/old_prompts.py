
# DIGESTOR_PROMPT = """<|begin_of_text|><|start_header_id|>system<|end_header_id|>response_format:json_object<|eot_id|>
# <|start_header_id|>user<|end_header_id|>
# TASK: generate summary, title, tags (e.g. company, organization, person, catastrophic event, product, technology, security vulnerability, stock ticker symbol, geographic location).
# INPUT:\n```\n{text}\n```
# OUTPUT FORMAT: A json object with fields title (string), summary (string) and tags (string of comma separated phrases)<|eot_id|>
# <|start_header_id|>assistant<|end_header_id|>"""
# DIGESTOR_PROMPT = """<|im_start|>system\n\nresponse_format:json_object<|im_end|>

# <|im_start|>user

# TASK: generate summary, title, tags (such as company, organization, person). 
# INPUT:\n```\n{text}\n```
# OUTPUT FORMAT: A json object with fields title (string), summary (string) and tags (string of comma separated phrases)

# <|im_end|>

# <|im_start|>assistant\n\n"""

# DIGEST_TEMPLATE = """<|im_start|>system

# response_format:json_object
# <|im_end|>

# <|im_start|>user

# TASKS:
#     - Create a one sentence gist of the article/post. This will be the 'title'.
#     - Extract names of the top 1 - 4 people, products, companies, organizations, or stock tickers mentioned in the article/post that influence the content. These will be the 'names'.
#     - Identify 1 or 2 domains that the subject matter of the article/post aligns closest to, such as: Cybersecurity, Business & Finance, Health & Wellness, Astrophysics, Smart Automotive, IoT and Gadgets, etc. These will be the 'domains'.

# RESPONSE FORMAT: 
# Response MUST be a json object of the following structure
# ```json
# {{
#     "title": string,
#     "names": [string, string, string, string],
#     "domains": [string, string]
# }}
# ```

# ARTICLE/POST:
# {input_text}
# <|im_end|>

# <|im_start|>assistant\n\n
# """

# DIGEST_TEMPLATE = """<|im_start|>system

# response_format:json_object
# <|im_end|>

# <|im_start|>user

# TASKS:
#     - Rewrite the article/post using less than 250 words. This will be the 'summary'.
#     - Extract top 3 sentences that are most relevant to the main points and core meaning of the entire article/post. These will be the 'highlights'.
#     - Create a one sentence gist of the article/post. This will be the 'title'.
#     - Extract names of the top 1 - 4 people, products, companies, organizations, or stock tickers mentioned in the article/post that influence the content. These will be the 'names'.
#     - Identify 1 or 2 domains that the subject matter of the article/post aligns closest to, such as: Cybersecurity, Business & Finance, Health & Wellness, Astrophysics, Smart Automotive, IoT and Gadgets, etc. These will be the 'domains'.

# RESPONSE FORMAT: 
# Response MUST be a json object of the following structure
# ```json
# {{
#     "summary": string,
#     "highlights": [string, string, string],
#     "title": string,
#     "names": [string, string, string, string],
#     "domains": [string, string]
# }}
# ```

# ARTICLE/POST:
# {input_text}
# <|im_end|>

# <|im_start|>assistant\n\n
# """

# DIGEST_TEMPLATE = """TASKS:
#     - Rewrite the article/post using less than 250 words. This will be the 'summary'.
#     - Extract top 3 sentences that are most relevant to the main points and core meaning of the entire article/post. These will be the 'highlights'.
#     - Create a one sentence gist of the article/post. This will be the 'title'.
#     - Extract names of the top 1 - 4 people, products, companies, organizations, or stock tickers mentioned in the article/post that influence the content. These will be the 'names'.
#     - Identify 1 or 2 domains that the subject matter of the article/post aligns closest to, such as: Cybersecurity, Business & Finance, Health & Wellness, Astrophysics, Smart Automotive, IoT and Gadgets, etc. These will be the 'domains'.

# RESPONSE FORMAT: 
# Response MUST be a json object of the following structure
# ```json
# {{
#     "summary": string,
#     "highlights": [string, string, string],
#     "title": string,
#     "names": [string, string, string, string],
#     "domains": [string, string]
# }}
# ```

# ARTICLE/POST:
# {input_text}
# """
