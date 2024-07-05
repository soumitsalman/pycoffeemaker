############################
## BEANSACK DB OPERATIONS ##
############################

from datetime import datetime, timedelta
from functools import reduce
from itertools import groupby
import operator
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

DEFAULT_SEARCH_SCORE = 0.68
DEFAULT_MAPPING_SCORE = 0.75
DEFAULT_LIMIT = 100

LATEST = {K_UPDATED: -1}
TRENDING = {K_TRENDSCORE: -1}
TRENDING_AND_LATEST = {K_TRENDSCORE: -1, K_UPDATED: -1}

CLEANUP_WINDOW = 30

logger = create_logger("beansack")

class Beansack:
    def __init__(self, conn_str: str, llm_api_key: str = None, embedder_model_path: str = None):        
        client = MongoClient(conn_str)        
        self.beanstore: Collection = client[BEANSACK][BEANS]
        self.nuggetstore: Collection = client[BEANSACK][NUGGETS]
        self.noisestore: Collection = client[BEANSACK][NOISES]        
        self.sourcestore: Collection = client[BEANSACK][SOURCES]  
        if llm_api_key:
            self.nuggetor = NuggetExtractor(llm_api_key)
            self.summarizer = Summarizer(llm_api_key)        
        self.embedder = LocalEmbedder(embedder_model_path) if embedder_model_path else LocalEmbedder()

    ##########################
    ## STORING AND INDEXING ##
    ##########################
        
    # stores beans and post-processes them for generating embeddings, summary and nuggets
    def store(self, beans: list[Bean]):        
        # filter out the old ones        
        exists = [item[K_URL] for item in self.beanstore.find({K_URL: {"$in": [bean.url for bean in beans]}}, {K_URL: 1})]
        beans = [bean for bean in beans if (bean.url not in exists) and bean.text]
        current_time = int(datetime.now().timestamp())
        
        # removing the media noises into separate item        
        noises = []
        for bean in beans:
            if bean.noise:
                bean.noise.url = bean.url                
                bean.noise.updated = current_time
                bean.noise.text = truncate(bean.noise.text) if bean.noise.text else None
                bean.noise.score = (bean.noise.comments or 0)*3 + (bean.noise.likes or 0)
                noises.append(bean.noise)
                bean.noise = None    
        if noises:
            self.noisestore.insert_many([item.model_dump(exclude_unset=True, exclude_none=True) for item in noises])

        # process items with text body. if the body is empty just scrap it
        # process the articles, posts, comments differently from the channels
        chatters = [bean for bean in beans if bean.kind != CHANNEL ] 
        for bean in chatters:
            bean.updated = current_time
            bean.text = truncate(bean.text)

        if chatters:
            # insert the beans
            res = self.beanstore.insert_many([item.model_dump(exclude_unset=True, exclude_none=True) for item in chatters])
            logger.info("%d beans inserted", len(res.inserted_ids))    
            self.rectify_beans(chatters)        
               
            # extract and insert nuggets
            nuggets = self.extract_nuggets(chatters, current_time)  
            if nuggets:   
                res = self.nuggetstore.insert_many([item.model_dump(exclude_unset=True, exclude_none=True, by_alias=True) for item in nuggets])
                logger.info("%d nuggets inserted", len(res.inserted_ids))    
                self.rectify_nuggets(nuggets)     
            
            # now relink
            self._dedup_and_relink(chatters, nuggets)

        #############        
        # TODO: process channels differently
        #############
        # channels = [bean for bean in beans if bean.kind == CHANNEL ] 

    def extract_nuggets(self, beans: list[Bean], batch_time: int) -> list[Nugget]:         
        try:
            texts = [bean.text for bean in beans]
            nuggets = [Nugget(keyphrase=n.get(K_KEYPHRASE), event=n.get(K_EVENT), description=n.get(K_DESCRIPTION)) for n in self.nuggetor.extract(texts) if n]
            # add updated value to keep track of collection time and batch number
            for i, n in enumerate(nuggets):
                n.updated = batch_time
                n.id = f"{batch_time}-{i}"            
            return nuggets
        except Exception as err:
            logger.warning("Nugget extraction failed.")

    def rectify_beansack(self, last_ndays:int, cleanup:bool, rerank_all: bool):        
        if cleanup:
            time_filter = {K_UPDATED: { "$lte": get_timevalue(CLEANUP_WINDOW) }}
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
                K_UPDATED: { "$gte": get_timevalue(last_ndays) }
            }
        ))  
        self.rectify_beans(beans)

        # get all the nuggets to rectify        
        nuggets = _deserialize_nuggets(self.nuggetstore.find(
            {
                K_EMBEDDING: { "$exists": False },
                K_UPDATED: { "$gte": get_timevalue(last_ndays) }
            }
        ))
        nuggets = self.rectify_nuggets(nuggets)   
        
        if rerank_all:
            # pull in all nuggets or else keep the ones in the rectify window
            beans = _deserialize_beans(self.beanstore.find(filter = { K_EMBEDDING: { "$exists": True }}, projection = { K_URL: 1 }))
            nuggets = _deserialize_nuggets(self.nuggetstore.find({ K_EMBEDDING: { "$exists": True }}).sort(LATEST))
       
        # rectify the mappings
        self._dedup_and_relink(beans, nuggets) 

    def rectify_beans(self, beans: list[Bean]):
        def _generate(bean: Bean):
            to_set = {}
            try:
                if not bean.embedding:
                    bean.embedding = to_set[K_EMBEDDING] = self.embedder.embed_documents(bean.digest())
                if not bean.summary:
                    bean.summary = to_set[K_SUMMARY] = self.summarizer.summarize(bean.text)
            except Exception as err:
                logger.warning(f"bean rectification error: {err}")
                pass # do nothing, to set will be empty
            return UpdateOne({K_URL: bean.url}, {"$set": to_set}), bean
        
        if beans:
            updates, beans = zip(*[_generate(bean) for bean in beans])
            update_count = _update_collection(self.beanstore, list(updates))
            logger.info(f"{update_count} beans rectified")
        return beans

    def rectify_nuggets(self, nuggets: list[Nugget]):
        def _generate(nugget: Nugget):
            to_set = {}
            try:
                if not nugget.embedding:                    
                    nugget.embedding = to_set[K_EMBEDDING] = self.embedder.embed_queries(nugget.digest()) # embedding as a search query towards the documents      
            except Exception as err:
                logger.warning(f"nugget rectification error: {err}")
            return UpdateOne({K_ID: nugget.id}, {"$set": to_set}), nugget
        
        if nuggets:
            updates, nuggets = zip(*[_generate(nugget) for nugget in nuggets])
            update_count =_update_collection(self.nuggetstore, list(updates))
            logger.info(f"{update_count} nuggets rectified")
        return nuggets
    
    def _deduplicate_nuggets(self, nuggets: list[Nugget]):        
        # using .92 for similarity score calculation
        # and pulling in 10000 as a rough limit
        search = lambda nug: list(
            self.nuggetstore.aggregate(
                self._vector_search_pipeline(text=None, embedding=nug.embedding, min_score=0.92, filter = None, limit=10000, sort_by=LATEST, projection={K_ID:1})
            )
        )
        to_delete = (search(nug)[1:] for nug in nuggets)
        to_delete = [item[K_ID] for item in chain(*to_delete)]
        if to_delete:
            logger.info("%d duplicate nuggets deleted", self.nuggetstore.delete_many({K_ID: {"$in": to_delete}}).deleted_count)
        # send the remaining nuggets
        return self.get_nuggets(filter={K_ID: {"$in": [n.id for n in nuggets]}}) 

    def _dedup_and_relink(self, beans: list[Bean], nuggets: list[Nugget]):    
        # BUG: currently trend_score is calculated for the entire database. That should NOT be the case. There should be a different trend score per time segment
        # rectify beans trend score
        if beans:
            noises = {noise.url: noise for noise in self._get_latest_noisestats([bean.url for bean in beans])}
            update_one = lambda bean: UpdateOne({K_URL: bean.url}, {"$set": { K_TRENDSCORE: noises[bean.url].score}})
            update_count = _update_collection(self.beanstore, [update_one(bean) for bean in beans if bean.url in noises])
            logger.info(f"{update_count} beans updated with trendscore")

        # deduplicate the nuggets
        if nuggets:
            nuggets = self._deduplicate_nuggets(nuggets)

        # calculate and relink the unique nuggets
        if nuggets:
            # rectify nuggets trend score and mapping 
            # beans are already rectified with trendscore. so just leverage that  
            getbeans = lambda urls: self.beanstore.find(filter = {K_URL: {"$in": urls}}, projection={K_TRENDSCORE:1})
            nugget_trend_score = lambda urls: reduce(operator.add, [(bean[K_TRENDSCORE] or 0) for bean in getbeans(urls) if K_TRENDSCORE in bean], len(urls)*10)   
            search = lambda embedding: [bean.url for bean in self.search_beans(embedding = embedding, min_score = DEFAULT_MAPPING_SCORE, limit = 100, projection = {K_URL: 1})]

            update_one = lambda nugget, urls: UpdateOne(
                    {K_ID: nugget.id}, 
                    {"$set": { K_TRENDSCORE: nugget_trend_score(urls), K_URLS: urls}})
            
            update_count = _update_collection(self.nuggetstore, [update_one(nugget, search(nugget.embedding)) for nugget in nuggets if nugget.embedding])
            logger.info(f"{update_count} nuggets remapped")

    ####################
    ## GET AND SEARCH ##
    ####################

    def get_beans(self,
            filter, 
            limit = 0, 
            sort_by = None, 
            projection = None
        ) -> list[Bean]:
        cursor = self.beanstore.find(filter = filter, projection = projection, sort=sort_by, limit=limit)
        # if sort_by:
        #     cursor = cursor.sort(sort_by)
        # if limit:
        #     cursor = cursor.limit(limit)
        return _deserialize_beans(cursor)

    def get_nuggets(self,
            filter = None, 
            limit = 0, 
            sort_by = None,
            projection = None
        ) -> list[Nugget]:        
        # append the embeddings filter so that it takes into account only the indexed items
        filter = {
            **{K_EMBEDDING: {"$exists": True}}, 
            **(filter or {})
        }        
        cursor = self.nuggetstore.find(filter=filter, projection=projection, sort=sort_by, limit=limit)
        # if sort_by:
        #     cursor = cursor.sort(sort_by)
        # if limit:
        #     cursor = cursor.limit(limit)
        return _deserialize_nuggets(cursor)
    
    def get_beans_by_nuggets(self, 
            nugget_ids = None,
            nuggets = None,
            filter = None,
            limit = DEFAULT_LIMIT            
        ) -> list:
        # search for the nuggets with that query/embedding
        if not nuggets:
            nuggets = _deserialize_nuggets(self.nuggetstore.find(
                {
                    K_ID: {"$in": nugget_ids if isinstance(nugget_ids, list) else [nugget_ids]},
                    K_URLS: {"$exists": True}
                }, 
                {K_EMBEDDING: 0}))
        elif isinstance(nuggets, Nugget):
            nuggets = [nuggets]

        bean_filter = lambda nug: {**{K_URL: {"$in": nug.urls }}, **(filter or {})}
        getbeans = lambda nug: self.get_beans(filter = bean_filter(nug), limit=limit, sort_by=LATEST, projection={K_EMBEDDING: 0})
        # then get the beans for each of those nuggets
        if nuggets:
            return [(nug, getbeans(nug)) for nug in nuggets] 

    def search_beans(self, 
            query: str = None,
            embedding: list[float] = None, 
            min_score = DEFAULT_SEARCH_SCORE, 
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
            min_score = DEFAULT_SEARCH_SCORE, 
            filter = None, 
            limit = DEFAULT_LIMIT, 
            sort_by = None, 
            projection = None
        ) -> list[Nugget]:
        pipline = self._vector_search_pipeline(query, embedding, min_score, filter, limit, sort_by, projection)
        return _deserialize_nuggets(self.nuggetstore.aggregate(pipeline=pipline))
    
    def trending_beans(self, 
            query: str = None,
            embedding: list[float] = None, 
            min_score = DEFAULT_SEARCH_SCORE, 
            filter = None, 
            limit = DEFAULT_LIMIT, 
            projection = None
        ) -> list[Bean]:        
        # TODO: calculate the trend score from the social media
        if not (query or embedding):
            # if query or embedding is not given, then this is scalar query
            return self.get_beans(filter=filter, limit=limit, sort_by=TRENDING_AND_LATEST, projection=projection)
        else:
            # else run the vector query
            return self.search_beans(query=query, embedding=embedding, min_score=min_score, filter=filter, limit=limit, sort_by=TRENDING_AND_LATEST, projection=projection)

    def trending_nuggets(self, 
            query: str = None,
            embedding: list[float] = None, 
            min_score = DEFAULT_SEARCH_SCORE,             
            filter = None,
            limit = DEFAULT_LIMIT,
            projection = None            
        ) -> list:
        # HACK: limit value multiplication is a to make sure that we retrieve enough elements to dedupe this
        if not (query or embedding):
            # if query or embedding is not given, then this is scalar query            
            result = self.get_nuggets(filter = filter, limit = limit, sort_by=TRENDING_AND_LATEST)
        else:
            # else run the vector query
            result = self.search_nuggets(query=query, embedding=embedding, min_score=min_score, filter=filter, limit=limit, sort_by=TRENDING_AND_LATEST, projection=projection)
        return result
    
    def search_nuggets_with_beans(self, 
            query: str = None,
            embedding: list[float] = None, 
            min_score = DEFAULT_SEARCH_SCORE, 
            limit = DEFAULT_LIMIT,
            filter = None
        ) -> list:
        # search for the nuggets with that query/embedding
        nuggets = self.search_nuggets(
            query=query,
            embedding=embedding,
            min_score=min_score,
            filter=filter, 
            limit=limit*10, # HACK: to make sure that we retrieve enough elements to dedupe this
            sort_by=TRENDING_AND_LATEST
        )
        return self.get_beans_by_nuggets(nuggets=nuggets, filter=filter, limit=limit)

    def get_sources(self):
        return self.beanstore.distinct('source')

    def _vector_search_pipeline(self, text, embedding, min_score, filter, limit, sort_by, projection):
        pipline = [
            {
                "$search": {
                    "cosmosSearch": {
                        "vector": embedding or self.embedder.embed_query(text),
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
                "$match": { "url": {"$in": urls} }
            },
            {
                "$sort": {"updated": -1}
            },
            {
                "$group": {
                    "_id": {
                        "url": "$url",
                        "container_url": "$container_url"
                    },
                    "updated":       {"$first": "$updated"},
                    "url":           {"$first": "$url"},
                    "channel":       {"$first": "$channel"},
                    "container_url": {"$first": "$container_url"},
                    "likes":         {"$first": "$likes"},
                    "comments":      {"$first": "$comments"},
                    "score":         {"$first": "$score"}
                }
            },
            {
                "$group": {
                    "_id":           "$url",
                    "url":           {"$first": "$url"},
                    "channel":       {"$first": "$channel"},
                    "container_url": {"$first": "$container_url"},
                    "likes":         {"$sum": "$likes"},
                    "comments":      {"$sum": "$comments"},
                    "score":         {"$sum": "$score"}
                }
            }
        ]
        return _deserialize_noises(self.noisestore.aggregate(pipeline))

    # current arbitrary calculation score: 10 x number_of_unique_articles_or_posts + 3*num_comments + likes     
    def _calculate_trend_score(self, urls: list[str]) -> int:
        noises = self._get_latest_noisestats(urls)       
        return reduce(lambda a, b: a + b, [n.score for n in noises if n.score], len(urls)*10) 

# def _deduplicate_nuggets(nuggets: list[Nugget]):
#     # TODO: this line is to make sure that we don't take into account things that have not yet been indexed
#     nuggets = [nugget for nugget in nuggets if nugget.embedding]
#     if nuggets and len(nuggets)>1:
#         # do the comparison ONLY if there are 2 or more items
#         CLUSTER_DISTANCE = 18  # 18 seems to work. I HAVE NO IDEA WHAT THIS MEANS
#         linkage_matrix = linkage(pairwise_distances([nugget.embedding for nugget in nuggets], metric='euclidean'), method='single')
#         clusters = fcluster(linkage_matrix, t=CLUSTER_DISTANCE, criterion='distance')
#         groups = {}
#         for nugget, label in zip(nuggets, clusters):
#             if label in groups:
#                 groups[label].urls.append(nugget.urls)
#             else:
#                 groups[label] = nugget
#         return list(groups.values())
#     else:
#         return nuggets

# def _deduplicate_nuggets(nuggets: list[Nugget]):
#     # TODO: this line is to make sure that we don't take into account things that have not yet been indexed
#     nuggets = [nugget for nugget in nuggets if nugget.embedding]
#     if nuggets and len(nuggets)>1:
#         # do the comparison ONLY if there are 2 or more items
#         CLUSTER_DISTANCE = 18  # 18 seems to work. I HAVE NO IDEA WHAT THIS MEANS
#         linkage_matrix = linkage(pairwise_distances([nugget.embedding for nugget in nuggets], metric='euclidean'), method='single')
#         clusters = fcluster(linkage_matrix, t=CLUSTER_DISTANCE, criterion='distance')
#         by_label = lambda index: clusters[index]
#         return {label: [nuggets[i] for i in index_list] for label, index_list in groupby(range(len(nuggets)), by_label)}        
#     else:
#         return {0: [nuggets]}
    
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

def timewindow_filter(last_ndays: int):
    return {K_UPDATED: {"$gte": get_timevalue(last_ndays)}}

def get_timevalue(last_ndays: int):
    return int((datetime.now() - timedelta(days=last_ndays)).timestamp())


