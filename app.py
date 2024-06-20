## MAIN FUNC ##
from icecream import ic
import os
from dotenv import load_dotenv
from shared import utils

# before doing anything else
# 1. assign paths for all the files that gets accessed as part of the script
# 2. load environment variables
# 3. set log location
if __name__ == "__main__":
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = f"{curr_dir}/.env"
    embedder_model_path = f"{curr_dir}/models/nomic.gguf"
    feed_sources_path = f"{curr_dir}/newscollector/feedsources.txt"
    logger_path = f"{curr_dir}/app.log"
    
    load_dotenv(env_path)
    
    instance_mode = os.getenv("INSTANCE_MODE")
    llm_api_key = os.getenv('GROQ_API_KEY')
    db_conn = os.getenv('DB_CONNECTION_STRING')

    utils.set_logger_path(logger_path)  
    logger = utils.create_logger(instance_mode)

from newscollector import rssfeed, ychackernews
from beanops.beansack import Beansack

def start_collector():
    beansack = Beansack(db_conn, llm_api_key, embedder_model_path)
    # collect news articles and then rectify
    logger.info("Starting collection from rss feeds.")
    rssfeed.collect(sources=feed_sources_path, store_func=beansack.store)
    logger.info("Starting collection from YC hackernews.")
    ychackernews.collect(store_func=beansack.store)
    # TODO: add collection from reddit
    # TODO: add collection from nextdoor
    # TODO: add collection from linkedin
    logger.info("Starting large rectification.")
    beansack.rectify_beansack(3, True, True)


import interactives.console as console

def start_chat():
    console.run_console(
        console.InteractSession(
            Beansack(conn_str=db_conn, embedder_model_path=embedder_model_path),
            os.getenv("DEEPINFRA_API_KEY")
        ))
    
if __name__ == "__main__":
    if instance_mode == "CHAT":
        start_chat()        
    else:
        start_collector()
        
