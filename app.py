## MAIN FUNC ##
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
    utils.set_logger_path(logger_path)

from newscollector.rssfeeder import collect
from beansack.beanops import Beansack

def start_collector(embedder_model_path, feed_sources_path):
    logger = utils.create_logger("indexer")
    beansack = Beansack(os.getenv('DB_CONNECTION_STRING'), os.getenv('LLMSERVICE_API_KEY'), embedder_model_path)
    
    # collect news articles and then rectify
    logger.info("Starting collection from rss feeds.")
    collect(sources=feed_sources_path, store_func=beansack.store)
    # TODO: add collection from reddit
    # TODO: add collection from nextdoor
    # TODO: add collection from linkedin
    logger.info("Starting large rectification.")
    beansack.rectify(7, True, True)


if __name__ == "__main__":
    start_collector(embedder_model_path, feed_sources_path)
