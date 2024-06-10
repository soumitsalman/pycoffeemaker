## BEAN SACK DB OPERATIONS ##
from functools import reduce
import json
import time

from retry import retry
from embedder import LocalNomic
from utils import create_logger
from datamodels import Bean, Noise, Nugget, CHANNEL
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from langchain_groq import ChatGroq
from langchain.chains.summarize import load_summarize_chain
from langchain.schema import Document
from bson import InvalidBSON

# names of db and collections
BEANSACK = "beansack"
BEANS = "beans"
NUGGETS = "concepts"
NOISES = "noises"

# names of important fields of collections
URL="url"
KIND = "kind"
TEXT = "text"
EMBEDDING = "embedding"
SUMMARY = "summary"
UPDATED = "updated"
KEYPHRASE = "keyphrase"
DESCRIPTION = "description"
TRENDSCORE = "trend_score"
URLS = "urls"
MAPPED_URL = "mapped_url"

BATCH_SIZE = 20
DEFAULT_MIN_SEARCH_SCORE = 0.7
DEFAULT_LIMIT = 50

logger = create_logger("beansack")

class Beansack:
    def __init__(self, conn_str: str, llm_api_key: str):        
        client = MongoClient(conn_str)        
        self.beanstore: Collection = client[BEANSACK][BEANS]
        self.nuggetstore: Collection = client[BEANSACK][NUGGETS]
        self.noisestore: Collection = client[BEANSACK][NOISES]
        self.embedder = LocalNomic()
        self.summarizer = load_summarize_chain(ChatGroq(api_key=llm_api_key, model="llama3-8b-8192"), chain_type="stuff", verbose=False)

    def rectify(self, last_ndays:int):
        # get all the beans to rectify        
        beans = _deserialize_beans(self.beanstore.find(
            {
                "$or": [
                    { EMBEDDING: { "$exists": False } },
                    { SUMMARY: { "$exists": False } }
                ],
                UPDATED: { "$gte": _get_time(last_ndays) }
            }
        ).sort({UPDATED: -1}))  
        self.rectify_beans(beans)

        # get all the beans to rectify        
        nuggets = _deserialize_nuggets(self.nuggetstore.find(
            {
                EMBEDDING: { "$exists": False },
                UPDATED: { "$gte": _get_time(last_ndays) }
            }
        ).sort({UPDATED: -1}))
        nuggets = self.rectify_nuggets(nuggets)       
        
        self.rectify_mappings(nuggets) 

    def rectify_beans(self, beans: list[Bean]):
        logger.info(f"{len(beans)} beans will go through rectification")
        def _generate(bean: Bean):
            to_set = {}
            try:
                if not bean.embedding:
                    bean.embedding = to_set[EMBEDDING] = self.embedder.embed_documents(bean.digest())
                if not bean.summary:
                    bean.summary = to_set[SUMMARY] = self.summarize(bean.text)                
            except Exception as err:
                logger.warning(f"{err}")
                ic(err)
                pass # do nothing, to set will be empty
            return UpdateOne({URL: bean.url}, {"$set": to_set}), bean
        
        updates, beans = zip(*[_generate(bean) for bean in beans])
        _update_collection(self.beanstore, list(updates))
        return beans

    def rectify_nuggets(self, nuggets: list[Nugget]):
        logger.info(f"{len(nuggets)} nuggets will go through rectification")
        def _generate(nugget: Nugget):
            to_set = {}
            try:
                if not nugget.embedding:                    
                    nugget.embedding = to_set[EMBEDDING] = self.embedder.embed_queries(nugget.description) # embedding as a search query towards the documents      
            except Exception as err:
                logger.warning(f"{err}")
                ic(err)
            return UpdateOne({KEYPHRASE: nugget.keyphrase, DESCRIPTION: nugget.description}, {"$set": to_set}), nugget
        
        updates, nuggets = zip(*[_generate(nugget) for nugget in nuggets])
        _update_collection(self.nuggetstore, list(updates))
        return nuggets

    def rectify_mappings(self, nuggets: list[Nugget]):
        search = lambda embedding: [bean.url for bean in self.vector_search_beans(
            embedding = embedding, 
            filter = {KIND: {"$ne": CHANNEL }}, 
            min_score = DEFAULT_MIN_SEARCH_SCORE,
            limit = DEFAULT_LIMIT, 
            projection = {URL: 1})]
        
        update_one = lambda nugget, urls: UpdateOne(
                {KEYPHRASE: nugget.keyphrase, DESCRIPTION: nugget.description, UPDATED: nugget.updated}, 
                {"$set": { TRENDSCORE: self.calculate_trend_score(urls), URLS: urls}})
        
        _update_collection(self.nuggetstore, [update_one(nugget, search(nugget.embedding)) for nugget in nuggets if nugget.embedding])
        
    def vector_search_beans(self, 
            embedding: list[float], 
            min_score = DEFAULT_MIN_SEARCH_SCORE, 
            filter = None, 
            limit = DEFAULT_LIMIT, 
            sort_by = None, 
            projection = None
        ) -> list[Bean]:
        pipline = [
            {
                "$search": {
                    "cosmosSearch": {
                        "vector": embedding,
                        "path":   EMBEDDING,
                        "k":      limit,
                    },
                    "returnStoredSource": True
                }
            },
            {
                "$addFields": { "search_score": {"$meta": "searchScore"} }
            },
            {
                "$match": { "search_score": {"$gte": min_score} }
            }
        ]       
        if filter:
            pipline[0]["$search"]["cosmosSearch"]["filter"] = filter
        if sort_by:
            pipline.append({"$sort": sort_by})
        if projection:
            pipline.append({"$project": projection})

        return _deserialize_beans(self.beanstore.aggregate(pipeline=pipline))
    
    def get_latest_noisestats(self, urls: list[str]) -> list[Noise]:
        pipeline = [
            {
                "$match": { "mapped_url": {"$in": urls} }
            },
            {
                "$sort": {"updated": -1}
            },
            {
                "$group": {
                    "_id": {
                        "mapped_url": "$mapped_url",
                        "source":     "$source",
                        "channel":    "$channel",
                    },
                    "updated":       {"$first": "$updated"},
                    "mapped_url":    {"$first": "$mapped_url"},
                    "channel":       {"$first": "$channel"},
                    "container_url": {"$first": "$container_url"},
                    "likes":         {"$first": "$likes"},
                    "comments":      {"$first": "$comments"}
                }
            },
            {
                "$group": {
                    "_id":           "$mapped_url",
                    "updated":       {"$first": "$updated"},
                    "mapped_url":    {"$first": "$mapped_url"},
                    "channel":       {"$first": "$channel"},
                    "container_url": {"$first": "$container_url"},
                    "likes":         {"$sum": "$likes"},
                    "comments":      {"$sum": "$comments"}
                }
            },
            {
                "$project": {
                    "mapped_url":    1,
                    "channel":       1,
                    "container_url": 1,
                    "likes":         1,
                    "comments":      1,
                    "score": {
                        "$add": [                            
                            {"$multiply": ["$comments", 3]},
                            "$likes"
                        ]
                    }
                }
            }
        ]
        return _deserialize_noises(self.noisestore.aggregate(pipeline))

    # current arbitrary calculation score: 10 x number_of_unique_articles_or_posts + 3*num_comments + likes     
    def calculate_trend_score(self, urls: list[str]) -> int:
        noises = self.get_latest_noisestats(urls)       
        return reduce(lambda a, b: a + b, [n.score for n in noises if n.score], len(noises)*10) 

    @retry(tries=5, jitter=5, delay=10)
    def summarize(self, text: str) -> str:
        return self.summarizer.invoke({"input_documents": [Document(text)]})['output_text']

## local utilities for pymongo
def _deserialize_beans(cursor) -> list[Bean]:
    try:
        return [Bean(**item) for item in cursor]
    except InvalidBSON as err:
        logger.warning(f"{err}")
        return []

def _deserialize_nuggets(cursor) -> list[Nugget]:
    try:
        return [Nugget(**item) for item in cursor]
    except InvalidBSON as err:
        logger.warning(f"{err}")
        return []

def _deserialize_noises(cursor) -> list[Noise]:
    try:
        return [Noise(**item) for item in cursor]
    except InvalidBSON as err:
        logger.warning(f"{err}")
        return []

def _update_collection(collection: Collection, updates: list[UpdateOne]):
    BATCH_SIZE = 200 # otherwise ghetto mongo throws a fit
    modified_count = reduce(lambda a, b: a+b, [ic(collection.bulk_write(updates[i:i+BATCH_SIZE])).modified_count for i in range(0, len(updates), BATCH_SIZE)], 0)
    logger.info(f"{modified_count} {collection.name} updated")

def _get_time(last_ndays: int):
    return int((datetime.now() - timedelta(days=last_ndays)).timestamp())

## RSS Reader
import feedparser
from langchain_community.document_loaders.html import UnstructuredHTMLLoader
from datamodels import Bean, ARTICLE
from datetime import datetime

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59"
T_TAGS = "tags"
T_DESCRIPTION = "summary"
T_PUBLISHED='published_parsed'

def collect_news(url):
    feed = ic(feedparser.parse(url, agent=USER_AGENT))
    
    source = feed.feed['title']
    updated = int(datetime.now().timestamp())
    make_bean = lambda entry: Bean(
        url=entry['link'],
        updated = updated,
        source = source,
        title=entry['title'],
        kind = ARTICLE,
        text=parse_description(entry),
        author=entry['author'],
        created=int(time.mktime(entry[T_PUBLISHED])),
        keywords=[tag.term for tag in entry.get(T_TAGS, [])]
    )    
    return [make_bean(entry) for entry in feed.entries]

def sanitation_check(beans: list[Bean]):    
    for bean in beans:
        res = []
        if not bean.text:
            res.append("text")
        if not bean.created:
            res.append("created")            
        if not bean.author:
            res.append("author")            
        if not bean.keywords:
            res.append("keywords")
        if res:
            ic(bean.url, "lacks", res)



MIN_PULL_LIMIT = 1000
def parse_description(entry):
    # the main body usually lives in <description> or <content:encoded>
    # load the largest one, then check if this is above min_pull_limit. 
    # if not, load_html
    html = parse_tags(entry, [T_DESCRIPTION])

    return html
        
# from the list of potential tags take the one with the largest content
def parse_tags(entry, tags: list[str]):    
    res = None
    for tag in tags:
        if tag in entry:
            if not res or (len(res) < len(entry[tag])):
                res = entry[tag]
    return res        


## MAIN FUNC ##
from dotenv import load_dotenv
load_dotenv()

import os
from icecream import ic
from datetime import datetime, timedelta

logger = create_logger("indexer")

rss_sources = [
    # "https://www.darkreading.com/rss.xml",
    # "https://www.phoronix.com/rss.php",
    "https://dev.to/feed"
]

rss_counter = 1
for source in rss_sources:
    beans = collect_news(source)
    sanitation_check(beans)
    with open(f"rss_{rss_counter}.json", 'w') as file:        
        json.dump([bean.model_dump_json(indent = 2) for bean in beans], file, indent=2)


# beansack = Beansack(os.getenv('DB_CONNECTION_STRING'), os.getenv('LLMSERVICE_API_KEY'))
# beansack.rectify(10)
# try:
#     beansack.rectify(10)
# except Exception as err:
#     logger.warning(f"{err}")
#     ic(err)

# print(summarize("In this example, summarizer_chain would be an instance of a summarization model or object, and summarizer_chain.summarize(text_to_summarize) is a method call to generate a summary for the given input text. You can then use the summary variable to do whatever you need with the generated summary, such as printing it or saving it to a file."))
