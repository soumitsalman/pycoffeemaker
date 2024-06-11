## MAIN FUNC ##
from dotenv import load_dotenv
load_dotenv()

import os
from icecream import ic
from newscollector.rssfeeder import collect
from shared.utils import create_logger
from beansack.beanops import Beansack

logger = create_logger("indexer")

beansack = Beansack(os.getenv('DB_CONNECTION_STRING'), os.getenv('LLMSERVICE_API_KEY'))

test_feeds = [
    "https://www.marktechpost.com/feed/"
]
# collect(sources_file="rssfeeds.txt", store_func=beansack.add)
beansack.rectify(10, True, True)



# beansack.rectify(10)
# try:
#     beansack.rectify(10)
# except Exception as err:
#     logger.warning(f"{err}")
#     ic(err)

# print(summarize("In this example, summarizer_chain would be an instance of a summarization model or object, and summarizer_chain.summarize(text_to_summarize) is a method call to generate a summary for the given input text. You can then use the summary variable to do whatever you need with the generated summary, such as printing it or saving it to a file."))
