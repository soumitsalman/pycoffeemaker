############################
## BEANSACK DB OPERATIONS ##
############################

from datetime import datetime, timedelta
from functools import reduce
from beansack.nlp import *
from beansack.datamodels import *
from shared.utils import create_logger
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from bson import InvalidBSON

# names of db and collections
BEANSACK = "beansack"
BEANS = "beans"
NUGGETS = "concepts"
NOISES = "noises"

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
        self.llm_api_key = llm_api_key
        

    def add(self, beans: list[Bean]):        
        # filter out the old ones        
        exists = [item[K_URL] for item in self.beanstore.find({K_URL: {"$in": [bean.url for bean in beans]}}, {K_URL: 1})]
        beans = [bean for bean in beans if (bean.url not in exists) and bean.text]
        current_time = int(datetime.now().timestamp())

        ############
        # TODO: remove the media noises into separate item
        ############

        # process the articles, posts, comments differently from the channels
        chatters = [bean for bean in beans if bean.kind != CHANNEL ] 
        for bean in chatters:
            bean.updated = current_time
            bean.text = truncate(bean.text)

        if chatters:
            res = self.beanstore.insert_many([item.model_dump(exclude_unset=True, exclude_none=True) for item in chatters])
            logger.info("%d beans inserted", len(res.inserted_ids))            
            self.rectify_beans(chatters)   
            nuggets = self.extract_nuggets(chatters, current_time)
            if nuggets:         
                self.rectify_nuggets(nuggets)
                self.rectify_mappings(nuggets)

        #############        
        # TODO: process channels differently
        #############
        # channels = [bean for bean in beans if bean.kind == CHANNEL ] 

    def extract_nuggets(self, beans: list[Bean], batch_time: int) -> list[Nugget]:
        nuggetor = NuggetExtractor(self.llm_api_key)
        try:
            texts = [bean.text for bean in beans]
            nuggets = [Nugget(keyphrase=n[K_KEYPHRASE], event=n[K_EVENT], description=n[K_DESCRIPTION]) for n in nuggetor.extract(texts)]
            logger.info("%d nuggets extracted", len(nuggets) if nuggets else 0)    
            # add updated value to keep track of collection time and batch number
            for n in nuggets:
                n.updated = batch_time
            res = self.nuggetstore.insert_many([item.model_dump(exclude_unset=True, exclude_none=True) for item in nuggets])
            logger.info("%d nuggets inserted", len(res.inserted_ids))
            return nuggets
        except Exception as err:
            logger.warning("Nugget extraction failed %s", str(err))

    def rectify(self, last_ndays:int):
        # get all the beans to rectify        
        beans = _deserialize_beans(self.beanstore.find(
            {
                "$or": [
                    { K_EMBEDDING: { "$exists": False } },
                    { K_SUMMARY: { "$exists": False } }
                ],
                K_UPDATED: { "$gte": _get_time(last_ndays) }
            }
        ).sort({K_UPDATED: -1}))  
        self.rectify_beans(beans)

        # get all the beans to rectify        
        nuggets = _deserialize_nuggets(self.nuggetstore.find(
            {
                K_EMBEDDING: { "$exists": False },
                K_UPDATED: { "$gte": _get_time(last_ndays) }
            }
        ).sort({K_UPDATED: -1}))
        nuggets = self.rectify_nuggets(nuggets)       
        
        self.rectify_mappings(nuggets) 

    def rectify_beans(self, beans: list[Bean]):
        logger.info(f"{len(beans)} beans will go through rectification")
        summarizer = Summarizer(self.llm_api_key)
        embedder = LocalNomic()
        
        def _generate(bean: Bean):
            to_set = {}
            try:
                if not bean.embedding:
                    bean.embedding = to_set[K_EMBEDDING] = embedder.embed_documents(bean.digest())
                if not bean.summary:
                    bean.summary = to_set[K_SUMMARY] = summarizer.summarize(bean.text)
            except Exception as err:
                logger.warning(f"{err}")
                pass # do nothing, to set will be empty
            return UpdateOne({K_URL: bean.url}, {"$set": to_set}), bean
        
        updates, beans = zip(*[_generate(bean) for bean in beans])
        _update_collection(self.beanstore, list(updates))
        return beans

    def rectify_nuggets(self, nuggets: list[Nugget]):
        logger.info(f"{len(nuggets)} nuggets will go through rectification")
        embedder = LocalNomic()

        def _generate(nugget: Nugget):
            to_set = {}
            try:
                if not nugget.embedding:                    
                    nugget.embedding = to_set[K_EMBEDDING] = embedder.embed_queries(nugget.description) # embedding as a search query towards the documents      
            except Exception as err:
                logger.warning(f"{err}")
            return UpdateOne({K_KEYPHRASE: nugget.keyphrase, K_DESCRIPTION: nugget.description}, {"$set": to_set}), nugget
        
        updates, nuggets = zip(*[_generate(nugget) for nugget in nuggets])
        _update_collection(self.nuggetstore, list(updates))
        return nuggets

    def rectify_mappings(self, nuggets: list[Nugget]):
        logger.info(f"{len(nuggets)} nuggets will go through remapping")
        search = lambda embedding: [bean.url for bean in self.vector_search_beans(
            embedding = embedding, 
            filter = {K_KIND: {"$ne": CHANNEL }}, 
            min_score = DEFAULT_MIN_SEARCH_SCORE,
            limit = DEFAULT_LIMIT, 
            projection = {K_URL: 1})]
        
        update_one = lambda nugget, urls: UpdateOne(
                {K_KEYPHRASE: nugget.keyphrase, K_DESCRIPTION: nugget.description, K_UPDATED: nugget.updated}, 
                {"$set": { K_TRENDSCORE: self.calculate_trend_score(urls), K_URLS: urls}})
        
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
                        "path":   K_EMBEDDING,
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
        return reduce(lambda a, b: a + b, [n.score for n in noises if n.score], len(urls)*10) 

    

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
    modified_count = reduce(lambda a, b: a+b, [collection.bulk_write(updates[i:i+BATCH_SIZE]).modified_count for i in range(0, len(updates), BATCH_SIZE)], 0)
    logger.info(f"{modified_count} {collection.name} updated")

def _get_time(last_ndays: int):
    return int((datetime.now() - timedelta(days=last_ndays)).timestamp())
