############################
## BEANSACK DB OPERATIONS ##
############################

from datetime import datetime, timedelta
from functools import reduce
from nlp.chains import *
from nlp.embedding import *
from .datamodels import *
from shared.utils import create_logger
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from bson import InvalidBSON
from sklearn.metrics.pairwise import pairwise_distances
from scipy.cluster.hierarchy import linkage, fcluster
import numpy as np

# names of db and collections
BEANSACK = "beansack"
BEANS = "beans"
NUGGETS = "concepts"
NOISES = "noises"
SOURCES = "sources"

DEFAULT_MIN_SEARCH_SCORE = 0.7
DEFAULT_LIMIT = 50

LATEST = {K_UPDATED: -1}
TRENDING = {K_TRENDSCORE: -1}

CLEANUP_WINDOW = 30

logger = create_logger("beansack")

class Beansack:
    def __init__(self, conn_str: str, llm_api_key: str, embedder_model_path: str = None):        
        client = MongoClient(conn_str)        
        self.beanstore: Collection = client[BEANSACK][BEANS]
        self.nuggetstore: Collection = client[BEANSACK][NUGGETS]
        self.noisestore: Collection = client[BEANSACK][NOISES]        
        self.sourcestore: Collection = client[BEANSACK][SOURCES]  
        self.llm_api_key = llm_api_key
        self.embedder_model_path = embedder_model_path
        
    # stores beans and post-processes them for generating embeddings, summary and nuggets
    def store(self, beans: list[Bean]):        
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
            nuggets = [Nugget(keyphrase=n.get(K_KEYPHRASE), event=n.get(K_EVENT), description=n.get(K_DESCRIPTION)) for n in nuggetor.extract(texts)]
            # add updated value to keep track of collection time and batch number
            for n in nuggets:
                n.updated = batch_time
            res = self.nuggetstore.insert_many([item.model_dump(exclude_unset=True, exclude_none=True) for item in nuggets])
            logger.info("%d nuggets inserted", len(res.inserted_ids))
            return nuggets
        except Exception as err:
            logger.warning("Nugget extraction failed %s", str(err))

    def rectify_beansack(self, last_ndays:int, cleanup:bool, remap_all: bool):        
        if cleanup:
            time_filter = {K_UPDATED: { "$lte": _get_time(CLEANUP_WINDOW) }}
            # clean up beanstore (but retain the channels)
            res = self.beanstore.delete_many(time_filter)
            logger.info("%d old beans deleted", res.deleted_count)
            # clean up nuggets
            res = self.nuggetstore.delete_many(time_filter)
            logger.info("%d old nuggets deleted", res.deleted_count)
            # clean up beanstore
            res = self.beanstore.delete_many(time_filter)
            logger.info("%d old noises deleted", res.deleted_count)

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

        # get all the nuggets to rectify        
        nuggets = _deserialize_nuggets(self.nuggetstore.find(
            {
                K_EMBEDDING: { "$exists": False },
                K_UPDATED: { "$gte": _get_time(last_ndays) }
            }
        ).sort({K_UPDATED: -1}))
        nuggets = self.rectify_nuggets(nuggets)       
        
        if remap_all:
            # pull in all nuggets or else keep the ones in the rectify window
            nuggets = _deserialize_nuggets(self.nuggetstore.find({ K_EMBEDDING: { "$exists": True }}))

        # rectify the mappings
        self.rectify_mappings(nuggets) 

    def rectify_beans(self, beans: list[Bean]):
        summarizer = Summarizer(self.llm_api_key)
        embedder = LocalNomic(self.embedder_model_path)
        
        def _generate(bean: Bean):
            to_set = {}
            try:
                if not bean.embedding:
                    bean.embedding = to_set[K_EMBEDDING] = embedder.embed_documents(bean.digest())
                if not bean.summary:
                    bean.summary = to_set[K_SUMMARY] = summarizer.summarize(bean.text)
            except Exception as err:
                logger.warning(f"bean rectification error: {err}")
                pass # do nothing, to set will be empty
            return UpdateOne({K_URL: bean.url}, {"$set": to_set}), bean
        
        updates, beans = zip(*[_generate(bean) for bean in beans])
        update_count = _update_collection(self.beanstore, list(updates))
        logger.info(f"{update_count} beans rectified")
        return beans

    def rectify_nuggets(self, nuggets: list[Nugget]):
        embedder = LocalNomic(self.embedder_model_path)

        def _generate(nugget: Nugget):
            to_set = {}
            try:
                if not nugget.embedding:                    
                    nugget.embedding = to_set[K_EMBEDDING] = embedder.embed_queries(nugget.digest()) # embedding as a search query towards the documents      
            except Exception as err:
                logger.warning(f"nugget rectification error: {err}")
            return UpdateOne({K_KEYPHRASE: nugget.keyphrase, K_DESCRIPTION: nugget.description}, {"$set": to_set}), nugget
        
        updates, nuggets = zip(*[_generate(nugget) for nugget in nuggets])
        update_count =_update_collection(self.nuggetstore, list(updates))
        logger.info(f"{update_count} nuggets rectified")
        return nuggets

    def rectify_mappings(self, nuggets: list[Nugget]):        
        search = lambda embedding: [bean.url for bean in self.search_beans(
            embedding = embedding, 
            filter = {K_KIND: {"$ne": CHANNEL }}, 
            min_score = DEFAULT_MIN_SEARCH_SCORE,
            limit = DEFAULT_LIMIT, 
            projection = {K_URL: 1})]        
        update_one = lambda nugget, urls: UpdateOne(
                {K_KEYPHRASE: nugget.keyphrase, K_DESCRIPTION: nugget.description, K_UPDATED: nugget.updated}, 
                {"$set": { K_TRENDSCORE: self._calculate_trend_score(urls), K_URLS: urls}})
        
        update_count = _update_collection(self.nuggetstore, [update_one(nugget, search(nugget.embedding)) for nugget in nuggets if nugget.embedding])
        logger.info(f"{update_count} nuggets remapped")

    def get_beans(self,
            filter, 
            limit = None, 
            sort_by = None, 
            projection = None
        ) -> list[Bean]:
        cursor = self.beanstore.find(filter = filter, projection = projection)
        if sort_by:
            cursor = cursor.sort(sort_by)
        if limit:
            cursor = cursor.limit(limit)
        return _deserialize_beans(cursor)

    def get_nuggets(self,
            filter, 
            limit = None, 
            sort_by = None, 
            projection = None
        ) -> list[Nugget]:
        cursor = self.nuggetstore.find(filter = filter, projection = projection)
        if sort_by:
            cursor = cursor.sort(sort_by)
        if limit:
            cursor = cursor.limit(limit)
        return _deserialize_nuggets(cursor)

    def search_beans(self, 
            query: str = None,
            embedding: list[float] = None, 
            min_score = DEFAULT_MIN_SEARCH_SCORE, 
            filter = None, 
            limit = DEFAULT_LIMIT, 
            sort_by = None, 
            projection = None
        ) -> list[Bean]:
        pipline = self._vector_search_pipeline(query, embedding, min_score, filter, limit, sort_by, projection)
        return _deserialize_beans(self.beanstore.aggregate(pipeline=pipline))

    def search_nuggets(self, 
            query: str = None,
            embedding: list[float] = None, 
            min_score = DEFAULT_MIN_SEARCH_SCORE, 
            filter = None, 
            limit = DEFAULT_LIMIT, 
            sort_by = None, 
            projection = None
        ) -> list[Nugget]:
        pipline = self._vector_search_pipeline(query, embedding, min_score, filter, limit, sort_by, projection)
        return _deserialize_nuggets(self.nuggetstore.aggregate(pipeline=pipline))
    
    def search_beans_with_nuggets(self, 
            query: str = None,
            embedding: list[float] = None, 
            min_score = DEFAULT_MIN_SEARCH_SCORE, 
            limit = DEFAULT_LIMIT,
            filter = None,              
            projection = None
        ) -> list[dict]:
        beans = self.search_beans(
            query=query, 
            embedding=embedding, 
            min_score=min_score,
            filter=filter,
            limit=limit, 
            sort_by=LATEST, 
            projection=projection)

        url_filter = lambda bean: { K_URLS: {"$in": [bean.url] }}
        bwn = lambda bean: {"bean": bean, "nuggets": self.get_nuggets(filter = url_filter(bean), sort_by={**LATEST,**TRENDING}, projection={K_KEYPHRASE: 1, K_EVENT: 1, K_DESCRIPTION: 1, K_URLS: 1})}       
        return [bwn(bean) for bean in beans]

    def search_nuggets_with_beans(self, 
            query: str = None,
            embedding: list[float] = None, 
            min_score = DEFAULT_MIN_SEARCH_SCORE, 
            limit = DEFAULT_LIMIT,
            filter = None
        ) -> list:
        # search for the nuggets with that query/embedding
        nuggets = self.search_nuggets(
            query=query,
            embedding=embedding,
            min_score=min_score,
            filter=filter, # filter does not apply to nuggets
            limit=limit, 
            sort_by={**LATEST,**TRENDING}
        )   
        if nuggets:
            CLUSTER_DISTANCE = 18  # 18 seems to work. I HAVE NO IDEA WHAT THIS MEANS
            linkage_matrix = linkage(pairwise_distances([nugget.embedding for nugget in nuggets], metric='euclidean'), method='single')
            clusters = fcluster(linkage_matrix, t=CLUSTER_DISTANCE, criterion='distance')
            groups = {}
            for nugget, label in zip(nuggets, clusters):
                if label in groups:
                    groups[label].urls.append(nugget.urls)
                else:
                    groups[label] = nugget
            
            bean_filter = lambda nug: {**{K_URL: {"$in": nug.urls }}, **(filter or {})}
            getbeans = lambda nug: self.get_beans(filter = bean_filter(nug), limit=limit, sort_by=LATEST, projection={K_EMBEDDING: 0})
            # then get the beans for each of those nuggets
            return [(nug, getbeans(nug)) for nug in groups.values()]        

    def _vector_search_pipeline(self, text, embedding, min_score, filter, limit, sort_by, projection):
        pipline = [
            {
                "$search": {
                    "cosmosSearch": {
                        "vector": embedding or LocalNomic(self.embedder_model_path).embed_query(text),
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
        return pipline
    
    def _get_latest_noisestats(self, urls: list[str]) -> list[Noise]:
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
    def _calculate_trend_score(self, urls: list[str]) -> int:
        noises = self._get_latest_noisestats(urls)       
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
    # logger.info(f"{modified_count} {collection.name} updated")
    return modified_count


def _get_time(last_ndays: int):
    return int((datetime.now() - timedelta(days=last_ndays)).timestamp())
