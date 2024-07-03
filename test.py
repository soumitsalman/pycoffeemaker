## MAIN FUNC ##
from icecream import ic
import os
from dotenv import load_dotenv
import json
from datetime import datetime as dt
    
load_dotenv()
llm_api_key = os.getenv('GROQ_API_KEY')
db_conn = os.getenv('DB_CONNECTION_STRING')
embedder_path = "models/nomic.gguf"
feed_source = "newscollector/feedsources.txt"

from shared import utils
logger = utils.create_logger("tester")

def write_datamodels(items, file_name: str = None):
    if items:
        with open(f"test/{file_name or items[0].source}.json", 'w') as file:
            json.dump([bean.model_dump(exclude_unset=True, exclude_none=True) for bean in items], file)
            
def write_text(text, file_name):
    with open(f"test/{file_name}", 'w') as file:
        file.write(text)

from beanops.beansack import *
from beanops.datamodels import *
from newscollector import rssfeed, ychackernews

def test_nlp():
    beansack = Beansack(db_conn, llm_api_key, embedder_path)
    beans = rssfeed.collect_from("https://feeds.feedburner.com/fastcompany/headlines")    

    def _rectify_beans(beans: list[Bean]):
        summarizer = Summarizer(llm_api_key)
        embedder = LocalEmbedder(embedder_path)        
        for bean in beans:
            bean.summary = summarizer.summarize(bean.text)       
            bean.embedding = embedder.embed_documents(bean.digest())     
        return beans

    write_datamodels(_rectify_beans(beans), "EXTRACTED-BEANS")
    write_datamodels(beansack.extract_nuggets(beans, 20241806), "EXTRACTED-NUGGETS")

def test_collection_local():
    # rssfeed.collect(store_func=write_datamodels)
    ychackernews.collect(store_func=write_datamodels)

def test_collection_live():
    beansack = Beansack(db_conn, llm_api_key, embedder_path)
    rssfeed.collect(store_func=beansack.store)
    # ychackernews.collect(store_func=beansack.store)
    
def test_rectify_beansack():
    beansack = Beansack(db_conn, llm_api_key, embedder_path)
    beansack.rectify_beansack(2, False, True)

def test_retrieval():
    show_bean = lambda beans: "\n".join(f"[{dt.fromtimestamp(bean.updated).strftime('%y-%m-%d')}] {bean.title}" for bean in beans)

    beansack = Beansack(db_conn, llm_api_key, embedder_path)
    nuggets = beansack.get_nuggets(sort_by=TRENDING_AND_LATEST, limit=5)
    [print(nugget.keyphrase+"\n", show_bean(beans)) for nugget, beans in beansack.get_beans_by_nuggets(filter=timewindow_filter(3), nuggets=nuggets, limit=5)]
    [print(nugget.keyphrase+"\n", show_bean(beans)) for nugget, beans in beansack.get_beans_by_nuggets(filter=timewindow_filter(1), nugget_ids=[nug.id for nug in nuggets], limit=5)]


from espresso import console

def test_writing():
    session = console.InteractSession(Beansack(db_conn, llm_api_key, embedder_path), llm_api_key)
    try:
        for user_input in ["generative ai", "Donald Trump"]:
            # user_input = input("Enter something: ")
            if user_input.lower() == "exit":
                print("Exiting...")
                break
            else:
                resp = console.write(session, user_input)
                write_text(resp, f"{user_input}.md")
                
    except KeyboardInterrupt:
        print("\nExiting...")

### TEST CALLS
# test_writing()
# test_nlp()
# test_collection_local()
# test_collection_live()
test_rectify_beansack()
# test_retrieval()