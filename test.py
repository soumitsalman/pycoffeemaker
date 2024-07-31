## MAIN FUNC ##
from icecream import ic
import os
from dotenv import load_dotenv
    
load_dotenv()

import json
from datetime import datetime as dt
from langchain_groq import ChatGroq
from pybeansack.embedding import BeansackEmbeddings
from pybeansack.beansack import *
from pybeansack.datamodels import *
from collectors import rssfeed, ychackernews
import coffeemaker
from coffeemaker.chains import *

llm = ChatGroq(api_key=os.getenv('GROQ_API_KEY'), model="llama3-8b-8192", temperature=0.1, verbose=False, streaming=False)
embedder = BeansackEmbeddings(".models/"+os.getenv("EMBEDDER_PATH"), 4096)
beansack = Beansack(os.getenv('DB_CONNECTION_STRING'), embedder)

def write_datamodels(items, file_name: str = None):
    if items:
        with open(f".test/{file_name or ic(items[0].source)}.json", 'w') as file:
            json.dump([bean.model_dump(exclude_unset=True, exclude_none=True) for bean in items], file)
            
def write_text(text, file_name):
    with open(f"test/{file_name}", 'w') as file:
        file.write(text)

def test_nlp():
    beans = rssfeed.collect_from("https://www.ghacks.net/feed/")[:5] 
    summarizer = Summarizer(llm, 3072)
    digestor = DigestExtractor(llm, 3072)

    for bean in beans:
        bean.summary = summarizer.run(bean.text)
        digest = digestor.run(bean.text)
        bean.highlights, bean.tags = digest.highlights, digest.keyphrases

    # write_datamodels(_rectify_beans(beans), "EXTRACTED-BEANS")
    write_datamodels(beans, "EXTRACTED-BEANS")

def test_collection():
    sources = [
        # "https://dev.to/feed",
        "https://techxplore.com/rss-feed/"
        "https://spacenews.com/feed/",
        "https://crypto.news/feed/"
    ]
    rssfeed.collect(sources=sources, store_func=write_datamodels)
    # [rssfeed.collect_from(src) for src in sources]
    # ychackernews.collect(store_func=write_datamodels)

def test_search():
    write_datamodels(ic(beansack.query_unique_beans(filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "QUERY_BEANS")
    write_datamodels(ic(beansack.text_search_beans(query="kamala harris election", filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "TEXT_SEARCH")
    write_datamodels(ic(beansack.vector_search_beans(query="kamala harris election", filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID: 0})), "VECTOR_SEARCH")

### TEST CALLS
# test_writing()
# test_nlp()
# test_collection_local()
# test_collection_live()
# test_rectify_beansack()
test_search()