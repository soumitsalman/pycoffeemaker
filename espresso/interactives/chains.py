####################
## ARTICLE WRITER ##
####################
from retry import retry
from pybeansack.chains import combine_texts
from collectors.utils import create_logger
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_community.llms.deepinfra import DeepInfra

WRITER_TEMPLATE = """<|begin_of_text|><|start_header_id|>system<|end_header_id|>
You are a {content_type} writer. Your task is to rewrite one section of a {content_type} on a given topic from the drafts provided by the user. 
From the drafts extract ONLY the contents that are strictly relevant to the topic and write the section based on ONLY that. You MUST NOT use your own knowledge for this. 
The section should have a title and body. The section should be less that 400 words. Output MUST be in markdown format.
<|eot_id|><|start_header_id|>user<|end_header_id|>
Rewrite a section on topic '{topic}' ONLY based on the following drafts:\n{drafts}
<|eot_id|><|start_header_id|>assistant<|end_header_id|>"""
WRITER_MODEL = "meta-llama/Meta-Llama-3-8B-Instruct"
WRITER_BATCH_SIZE = 6144
DEFAULT_CONTENT_TYPE = "blog"

class ArticleWriter:
    def __init__(self, api_key: str):
        prompt = PromptTemplate.from_template(template=WRITER_TEMPLATE)
        llm = DeepInfra(deepinfra_api_token=api_key, model_id=WRITER_MODEL, verbose=False)
        self.chain = prompt | llm | StrOutputParser()

    # highlights, coontents and sources should havethe same number of items
    def write_article(self, highlights: list, drafts:list, sources: list, content_type: str = DEFAULT_CONTENT_TYPE):                  
        article = "## Trending Highlights\n"+"\n".join(['- '+item for item in highlights])+"\n\n"
        for i in range(len(drafts)):                                         
            article += (
                self.write_section(highlights[i], drafts[i], content_type) +
                "\n**Sources:** "+ 
                ", ".join({src[0]:f"[{src[0]}]({src[1]})" for src in sources[i]}.values()) + 
                "\n\n")   
        return article

    # highlights, coontents and sources should havethe same number of items
    def stream_article(self, highlights: list, drafts:list, sources: list, content_type: str = DEFAULT_CONTENT_TYPE):                  
        yield "## Trending Highlights\n"+"\n".join(['- '+item for item in highlights])
        for i in range(len(drafts)):                                   
           yield self.write_section(highlights[i], drafts[i], content_type)
           yield "**Sources:** "+ ", ".join({src[0]:f"[{src[0]}]({src[1]})" for src in sources[i]}.values())

    @retry(tries=3, jitter=10, delay=10, logger=create_logger("article writer"))
    def write_section(self, topic: str, drafts: list[str], content_type: str = DEFAULT_CONTENT_TYPE) -> str:        
        while True:         
            # run it once at least   
            texts = combine_texts(drafts, WRITER_BATCH_SIZE, "\n\n\n")
            # these are the new drafts
            drafts = [self.chain.invoke({"content_type": content_type, "topic": topic, "drafts": text}) for text in texts]                           
            if len(drafts) <= 1:
                return drafts[0]
            
