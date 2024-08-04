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
from collectors import rssfeed, ychackernews, individual
from coffeemaker import orchestrator as orch
from coffeemaker.chains import *

llm = ChatGroq(api_key=os.getenv('GROQ_API_KEY'), model="llama3-8b-8192", temperature=0.1, verbose=False, streaming=False)
# embedder = BeansackEmbeddings(".models/"+os.getenv("EMBEDDER_PATH"), 4096)
# beansack = Beansack(os.getenv('DB_CONNECTION_STRING'), embedder)
orch.initialize(
    os.getenv("DB_CONNECTION_STRING"), 
    "/workspaces/coffeemaker-2/pycoffeemaker", 
    os.getenv("EMBEDDER_FILE"),
    os.getenv("GROQ_API_KEY"),    
    float(os.getenv('CLUSTER_EPS')),
    float(os.getenv('CATEGORY_EPS')))

def write_datamodels(items, file_name: str = None):
    if items:
        with open(f".test/{file_name or ic(items[0].source)}.json", 'w') as file:
            json.dump([bean.model_dump(exclude_unset=True, exclude_none=True) for bean in items], file)
            
def write_text(text, file_name):
    with open(f"test/{file_name}", 'w') as file:
        file.write(text)

def test_chains():
    sources = [
        # "https://dev.to/feed",
        "https://www.ghacks.net/feed/",
        "https://gearpatrol.com/feed/"
    ]
    rssfeed.collect(sources=sources, store_func=lambda beans: write_datamodels(orch._augment(beans), "TEST-CHAIN-"+beans[0].source))
   

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
    write_datamodels(ic(orch.remotesack.query_unique_beans(filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "QUERY_BEANS")
    write_datamodels(ic(orch.remotesack.text_search_beans(query="kamala harris election", filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "TEXT_SEARCH")
    write_datamodels(ic(orch.remotesack.vector_search_beans(query="kamala harris election", filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID: 0})), "VECTOR_SEARCH")

### TEST CALLS
# test_writing()
test_chains()
# test_collection_local()
# test_collection_live()
# test_rectify_beansack()
# test_search()
