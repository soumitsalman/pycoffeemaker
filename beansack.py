############################
## BEANSACK DB OPERATIONS ##
############################

from datetime import datetime, timedelta
from functools import reduce
import operator
from .embedding import *
from .datamodels import *
from .utils import create_logger
from bson import InvalidBSON
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection as MongoColl
import chromadb
from chromadb import Collection as ChromaColl
from icecream import ic


# names of db and collections
BEANSACK = "beansack"
BEANS = "beans"
HIGHLIGHTS = "highlights"
CHATTERS = "chatters"
SOURCES = "sources"

DEFAULT_SEARCH_SCORE = 0.73
DEFAULT_MAPPING_SCORE = 0.72
DEFAULT_LIMIT = 100

LATEST = {K_UPDATED: -1}
TRENDING = {K_TRENDSCORE: -1}
TRENDING_AND_LATEST = {K_TRENDSCORE: -1, K_UPDATED: -1}
LATEST_AND_TRENDING = {K_UPDATED: -1, K_TRENDSCORE: -1}

logger = create_logger("beansack")

class Beansack:
    def __init__(self, conn_str: str, embedder: BeansackEmbeddings = None):        
        client = MongoClient(conn_str)        
        self.beanstore: MongoColl = client[BEANSACK][BEANS]
        self.highlightstore: MongoColl = client[BEANSACK][HIGHLIGHTS]
        self.chatterstore: MongoColl = client[BEANSACK][CHATTERS]        
        self.sourcestore: MongoColl = client[BEANSACK][SOURCES]  
        self.embedder: BeansackEmbeddings = embedder

    ##########################
    ## STORING AND INDEXING ##
    ##########################
    def store_beans(self, beans: list[Bean]) -> int:    
        create_upsert = lambda bean: UpdateOne(
            filter={K_URL: bean.url},
            update={"$set": bean.model_dump(exclude_unset=True, exclude_none=True, by_alias=True)},
            upsert=True)
        
        if beans:
            beans = self._rectify_as_needed(beans) 
            res = self.beanstore.bulk_write([create_upsert(bean) for bean in beans], ordered=False)            
            return res.upserted_count
        return 0

    def filter_unstored_beans(self, beans: list[Bean]):
        exists = [item[K_URL] for item in self.beanstore.find({K_URL: {"$in": [bean.url for bean in beans]}}, {K_URL: 1})]
        beans = [bean for bean in beans if (bean.url not in exists)]
        return beans
        
    # this function checks for embeddings, updated time and any other rectification needed before inserting
    def _rectify_as_needed(self, items: list[Bean|Highlight]):
        # for each item if there is no embedding and create one from the text.
        batch_time = int(datetime.now().timestamp())
        for item in items:
            if not item.embedding:
                item.embedding = self.embedder.embed(item.digest())
            if not item.updated:
                item.updated = batch_time
        return items

    def store_chatters(self, chatters: list[Chatter]):
        if chatters:
            res = self.chatterstore.insert_many([item.model_dump(exclude_unset=True, exclude_none=True, by_alias=True) for item in chatters])
            return len(res.inserted_ids or [])
        
    def update_beans(self, urls: list[str], updates: dict|list[dict]) -> int:
        # if update is a single dict then it will apply to all beans with the specified urls
        # or else update is a list of equal length, and we will do a bulk_write of update one
        if urls:
            if isinstance(updates, dict):
                return self.beanstore.update_many(filter={K_URL: {"$in": urls}}, update={"$set": updates}).matched_count
            elif isinstance(updates, list) and ic((len(urls) == len(updates))):
                return self.beanstore.bulk_write(list(map(lambda url, update: UpdateOne({K_URL: url}, {"$set": update}), urls, updates))).modified_count
        
    def delete_old(self, window: int):
        time_filter = {K_UPDATED: { "$lte": get_timevalue(window) }}
        res = self.beanstore.delete_many(time_filter)
        logger.info("%d old beans deleted", res.deleted_count)
        res = self.chatterstore.delete_many(time_filter)
        logger.info("%d old chatters deleted", res.deleted_count)
        res = self.highlightstore.delete_many(time_filter)
        logger.info("%d old highlights deleted", res.deleted_count)

    ####################
    ## GET AND SEARCH ##
    ####################

    def get_beans(self, filter, skip = 0, limit = 0, sort_by = None, projection = None) -> list[Bean]:
        cursor = self.beanstore.find(filter = filter, projection = projection, sort=sort_by, skip = skip, limit=limit)
        return _deserialize_beans(cursor)

    def vector_search_beans(self, 
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
    
    def count_vector_search_beans(self, query: str = None, embedding: list[float] = None, min_score = DEFAULT_SEARCH_SCORE, filter: dict = None, limit = DEFAULT_LIMIT) -> int:
        pipeline = self._count_vector_search_pipeline(query, embedding, min_score, filter, limit)
        result = list(self.beanstore.aggregate(pipeline))
        return result[0]['total_count'] if result else 0
    
    def text_search_beans(self, query: str, filter = None, sort_by = None, skip=0, limit = DEFAULT_LIMIT, projection=None):
        return _deserialize_beans(
            self.beanstore.aggregate(
                self._text_search_pipeline(query, filter=filter, sort_by=sort_by, skip=skip, limit=limit, projection=projection, for_count=False)))
    
    def count_text_search_beans(self, query: str, filter = None, limit = DEFAULT_LIMIT):
        result = self.beanstore.aggregate(self._text_search_pipeline(query, filter=filter, sort_by=None, skip=0, limit=limit, projection=None, for_count=True))
        return next(iter(result))['total_count'] if result else 0
    
    def query_unique_beans(self, filter, sort_by = None, skip = 0, limit = DEFAULT_LIMIT, projection = None):
        pipeline = self._unique_beans_pipeline(filter, sort_by=sort_by, skip=skip, limit=limit, projection=projection, for_count=False)
        return _deserialize_beans(self.beanstore.aggregate(pipeline))
    
    def count_unique_beans(self, filter, limit = DEFAULT_LIMIT):
        pipeline = self._unique_beans_pipeline(filter, sort_by=None, skip=None, limit=limit, projection=None, for_count=True)
        result = self.beanstore.aggregate(pipeline)
        return next(iter(result))['total_count'] if result else 0
  
    def count_related_beans(self, urls: list[str]) -> dict:
        pipeline = [
            {
                "$match": {"cluster_id": {"$ne": None}}
            },
            {
                "$group": {
                    "_id": {
                        "cluster_id": "$cluster_id"
                    },
                    "cluster_id": {"$first": "$cluster_id"},            
                    "urls": {"$addToSet": "$url"},
                    "count": {"$sum": 1}
                }        
            },
            {
                "$match": {"urls": {"$in": urls}}
            },
            {
                "$unwind": "$urls"
            },
            {
                "$project": {
                    "_id": 0,
                    "cluster_id": "$cluster_id",
                    "url": "$urls",
                    "cluster_size": "$count"
                }
            }
        ]
        
        return {item.url: item['cluster_size'] for item in self.beanstore.aggregate(pipeline)}
    
    def _unique_beans_pipeline(self, filter, sort_by, skip, limit, projection, for_count):
        pipeline = [{"$match": filter}]
        if not for_count and sort_by:
            pipeline.append({"$sort": sort_by})
        
        pipeline.append({
            "$group": {
                "_id": "$cluster_id",
                "cluster_id": {"$first": "$cluster_id"},
                K_URL: {"$first": "$url"},
                K_TITLE: {"$first": "$title"},
                K_SUMMARY: {"$first": "$summary"},
                K_HIGHLIGHTS: {"$first": "$highlights"},
                K_TAGS: {"$first": "$tags"},
                K_SOURCE: {"$first": "$source"},
                K_UPDATED: {"$first": "$updated"},
                K_CREATED: {"$first": "$created"},
                K_LIKES: {"$first": "$likes"},
                K_COMMENTS: {"$first": "$comments"},
                K_AUTHOR: {"$first": "$author"},
                K_KIND: {"$first": "$kind"},
                K_IMAGEURL: {"$first": "$image_url"}
            }
        })

        if skip:
            pipeline.append({"$skip": skip})
        if limit:
            pipeline.append({"$limit": limit})
        if for_count:
            pipeline.append({ "$count": "total_count"})
        if not for_count and projection:
            pipeline.append({"$project": projection})
        return pipeline

    def _text_search_pipeline(self, text: str, filter, sort_by, skip, limit, projection, for_count):
        match = {"$text": {"$search": text}}
        if filter:
            match.update(filter)

        pipeline = [
            {   "$match": match },            
            {   "$addFields":  {"search_score": {"$meta": "textScore"}} },
            {   "$match": { "search_score": {"$gte": len(text.split(sep=" ,.;:`'\"\n\t\r\f"))}} }  # this is hueristic to count the number of word match
        ]        

        if not for_count:
            sort = {"search_score": -1}
            if sort_by:
                sort.update(sort_by)
            pipeline.append({"$sort": sort})

        if skip:
            pipeline.append({"$skip": skip})
        if limit:
            pipeline.append({"$limit": limit})
        if for_count:
            pipeline.append({ "$count": "total_count"})
        if not for_count and projection:
            pipeline.append({"$project": projection})
        return pipeline
   
    def _vector_search_pipeline(self, text, embedding, min_score, filter, limit, sort_by, projection):
        pipeline = [
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
            pipeline[0]["$search"]["cosmosSearch"]["filter"] = filter
        if sort_by:
            pipeline.append({"$sort": sort_by})
        if projection:
            pipeline.append({"$project": projection})
        return pipeline
    
    def _count_vector_search_pipeline(self, text, embedding, min_score, filter, limit):
        pipline = self._vector_search_pipeline(text, embedding, min_score, filter, limit, None, None)
        pipline.append({ "$count": "total_count"})
        return pipline
        
    def get_latest_chatter_stats(self, urls: list[str]) -> list[Chatter]:
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
                    "likes":         {"$first": "$likes"},
                    "comments":      {"$first": "$comments"}                
                }
            },
            {
                "$group": {
                    "_id":           "$url",
                    "url":           {"$first": "$url"},                    
                    "likes":         {"$sum": "$likes"},
                    "comments":      {"$sum": "$comments"}
                }
            }
        ]
        return _deserialize_chatters(self.chatterstore.aggregate(pipeline))


    

## local utilities for pymongo
def _deserialize_beans(cursor) -> list[Bean]:
    try:
        return [Bean(**item) for item in cursor]
    except InvalidBSON as err:
        logger.warning(f"{err}")
        return []

def _deserialize_chatters(cursor) -> list[Chatter]:
    try:
        return [Chatter(**item) for item in cursor]
    except InvalidBSON as err:
        logger.warning(f"{err}")
        return []
    
# def _deserialize_highlights(cursor) -> list[Highlight]:
#     try:
#         return [Highlight(**item) for item in cursor]
#     except InvalidBSON as err:
#         logger.warning(f"{err}")
#         return []

# either the updates will have to be present OR
# filters and values have to be present
def _bulk_update(collection: MongoColl, updates: list[UpdateOne] = None):
    BATCH_SIZE = 200 # otherwise ghetto mongo throws a fit
    modified_count = reduce(operator.add, [collection.bulk_write(updates[i:i+BATCH_SIZE]).modified_count for i in range(0, len(updates), BATCH_SIZE)], 0)
    return modified_count

def timewindow_filter(last_ndays: int):
    return {K_UPDATED: {"$gte": get_timevalue(last_ndays)}}

def get_timevalue(last_ndays: int):
    return int((datetime.now() - timedelta(days=last_ndays)).timestamp())

class Localsack: 
    beanstore: ChromaColl
    # categorystore: ChromaColl

    def __init__(self, path, embedder: BeansackEmbeddings = None):
        db = chromadb.PersistentClient(path=path)
        self.beanstore: ChromaColl  = db.get_or_create_collection("beans", embedding_function=embedder)        
    
    def store_beans(self, beans: list[Bean]) -> int:
        self.beanstore.add(
            ids=[bean.url for bean in beans],
            embeddings=self.beanstore._embed([bean.text for bean in beans]),
            metadatas=[{K_UPDATED: bean.updated} for bean in beans]
        ) 
        return len(beans)  
    
    def filter_unstored_beans(self, beans: list[Bean]) -> list[Bean]:        
        existing_beans = self.beanstore.get(ids=list({bean.url for bean in beans}), include=[])['ids']
        return list(filter(lambda bean: bean.url not in existing_beans, beans))
    
    def count_related_beans(self, urls: list[str]) -> dict:
        beans_result = self.beanstore.get(ids=urls, include=['metadatas'])
        get_cluster_size = lambda metadata: len(self.beanstore.get(where={K_CLUSTER_ID: metadata[K_CLUSTER_ID]}, include=[])["ids"])
        return {beans_result['ids'][i]: get_cluster_size(beans_result['metadatas'][i]) for i in range(len(beans_result['ids']))}
    
    def delete_old(self, window: int):
        self.beanstore.delete(where=timewindow_filter(window))

    def update_beans(self, urls: list[str], updates: list[dict]):
        if urls and updates:
            self.beanstore.update(ids = urls, metadatas=updates)
        

    


#####################################################
### DEPRECATED OLD STORING AND INDEXING FUNCTIONS ###
#####################################################
        # if llm:
        #     self.nuggetor = NuggetExtractor(llm)
        #     self.summarizer = Summarizer(llm)   

        # filter out the old ones        
        # exists = [item[K_URL] for item in self.beanstore.find({K_URL: {"$in": [bean.url for bean in beans]}}, {K_URL: 1})]
        # beans = [bean for bean in beans if (bean.url not in exists) and bean.text]
        # check for existing update time. If not assign one
        
        # noises = [to_noise(bean) for bean in beans if (bean.likes or bean.comments)]
        # if noises:
        #     self.noisestore.insert_many([item.model_dump(exclude_unset=True, exclude_none=True) for item in noises])

        # process the articles, posts, comments differently from the channels
        # each bean has to have either embedding or text or else don't try to save it
            
            # self.rectify_beans(chatters)        
               
    #         # extract and insert nuggets
    #         nuggets = self.extract_nuggets(chatters, current_time)  
    #         if nuggets:   
    #             res = self.nuggetstore.insert_many([item.model_dump(exclude_unset=True, exclude_none=True, by_alias=True) for item in nuggets])
    #             logger.info("%d nuggets inserted", len(res.inserted_ids))    
    #             nuggets = self.rectify_nuggets(nuggets) 
    #             # delete the old duplicate ones the nuggets
    #             total = len(nuggets)          
    #             nuggets = self._deduplicate_nuggets(nuggets)
    #             logger.info("%d duplicate nuggets deleted", total - len(nuggets))    
            
    #         # now relink
    #         self._rerank(chatters, nuggets)

    # # current arbitrary calculation score: 10 x number_of_unique_articles_or_posts + 3*num_comments + likes     
    # def _calculate_trend_score(self, urls: list[str]) -> int:
    #     noises = self.get_latest_chatter_stats(urls)       
    #     return reduce(lambda a, b: a + b, [n.score for n in noises if n.score], len(urls)*10) 

    # def search_highlights(self, 
    #         query: str = None,
    #         embedding: list[float] = None, 
    #         min_score = DEFAULT_SEARCH_SCORE, 
    #         filter = None, 
    #         limit = DEFAULT_LIMIT, 
    #         sort_by = None, 
    #         projection = None
    #     ) -> list[Highlight]:
    #     pipline = self._vector_search_pipeline(query, embedding, min_score, filter, limit, sort_by, projection)
    #     return _deserialize_highlights(self.highlightstore.aggregate(pipeline=pipline))
    
    # def count_search_highlights(self, query: str = None, embedding: list[float] = None, min_score = DEFAULT_SEARCH_SCORE, filter: dict = None, limit = DEFAULT_LIMIT) -> int:
    #     pipeline = self._count_vector_search_pipeline(query, embedding, min_score, filter, limit)
    #     result = list(self.highlightstore.aggregate(pipeline))
    #     return result[0]['total_count'] if result else 0

        # def get_highlights(self, filter, skip = 0, limit = 0,  sort_by = None, projection = None) -> list[Highlight]:        
    #     # append the embeddings filter so that it takes into account only the indexed items
    #     filter = {
    #         **{K_EMBEDDING: {"$exists": True}}, 
    #         **(filter or {})
    #     }        
    #     cursor = self.highlightstore.find(filter=filter, projection=projection, sort=sort_by, skip=skip, limit=limit)
    #     return _deserialize_highlights(cursor)
    
    # def get_beans_by_highlights(self, highlight: str|Highlight, filter = None, limit = DEFAULT_LIMIT, projection=None) -> list:
    #     if isinstance(highlight, Highlight):
    #         urls = highlight.urls
    #     else:
    #         item = self.highlightstore.find_one({K_ID: highlight, K_URLS: {"$exists": True}}, {K_URLS: 1})
    #         urls = item[K_URLS] if item else None        
    #     return self.get_beans(filter = {**{K_URL: {"$in": urls }}, **(filter or {})}, sort_by=LATEST, limit=limit, projection=projection) if urls else None
    
    # def count_beans_by_highlights(self, highlight: str|Highlight, filter: dict = None, limit: int = DEFAULT_LIMIT) -> list:
    #     if isinstance(highlight, Highlight):
    #         urls = highlight.urls
    #     else:
    #         item = self.highlightstore.find_one({K_ID: highlight, K_URLS: {"$exists": True}}, {K_URLS: 1})
    #         urls = item[K_URLS] if item else None
    #     return self.beanstore.count_documents(filter = {**{K_URL: {"$in": urls }}, **(filter or {})}, limit=limit) if urls else 0
    
    

    # def trending_highlights(self, 
    #         query: str = None,
    #         embedding: list[float] = None, 
    #         min_score = DEFAULT_SEARCH_SCORE,             
    #         filter = None,
    #         limit = DEFAULT_LIMIT,
    #         projection = None            
    #     ) -> list:
    #     if not (query or embedding):
    #         # if query or embedding is not given, then this is scalar query            
    #         result = self.get_highlights(filter = filter, limit = limit, sort_by=LATEST_AND_TRENDING)
    #     else:
    #         # else run the vector query
    #         result = self.search_highlights(query=query, embedding=embedding, min_score=min_score, filter=filter, limit=limit, sort_by=LATEST_AND_TRENDING, projection=projection)            
    #     return result
    
    # def count_trending_highlights(self, query: str = None, embedding: list[float] = None, min_score = DEFAULT_SEARCH_SCORE, filter = None, limit = DEFAULT_LIMIT) -> int:        
    #     return self.highlightstore.count_documents(filter=filter, limit=limit) \
    #         if not (query or embedding) \
    #         else self.count_search_highlights(query=query, embedding=embedding, min_score=min_score, filter=filter, limit=limit)


    # def store_noises(self, chatters: list[Chatter]):
    #     if beans:
    #         chatters = {noise.url: noise for noise in self._get_latest_noisestats([bean.url for bean in beans])}
    #         update_one = lambda bean: UpdateOne(
    #             {K_URL: bean.url}, 
    #             {
    #                 "$set": { 
    #                     K_LIKES: chatters[bean.url].likes,
    #                     K_COMMENTS: chatters[bean.url].comments,
    #                     K_TRENDSCORE: chatters[bean.url].score
    #                 }
    #             }
    #         )
    #         update_count = _bulk_update(self.beanstore, [update_one(bean) for bean in beans if bean.url in chatters])
    #         logger.info(f"{update_count} beans reranked")     

    # def _rerank(self, beans: list[Bean], highlights: list[Highlight]):    
    #     # BUG: currently trend_score is calculated for the entire database. That should NOT be the case. There should be a different trend score per time segment
    #     # rectify beans trend score
    #     if beans:
    #         noises = {noise.url: noise for noise in self.get_latest_chatter_stats([bean.url for bean in beans])}
    #         update_one = lambda bean: UpdateOne(
    #             {K_URL: bean.url}, 
    #             {
    #                 "$set": { 
    #                     K_LIKES: noises[bean.url].likes,
    #                     K_COMMENTS: noises[bean.url].comments,
    #                     K_TRENDSCORE: noises[bean.url].score
    #                 }
    #             }
    #         )
    #         update_count = _bulk_update(self.beanstore, [update_one(bean) for bean in beans if bean.url in noises])
    #         logger.info(f"{update_count} beans reranked")        

    #     # calculate and relink the unique nuggets
    #     if highlights:

    #         # beans are already rectified with trendscore. so just leverage that  
    #         getbeans = lambda urls: self.beanstore.find(filter = {K_URL: {"$in": urls}}, projection={K_TRENDSCORE:1})
    #         trend_score = lambda urls: reduce(operator.add, [(bean[K_TRENDSCORE] or 0) for bean in getbeans(urls) if K_TRENDSCORE in bean], len(urls)*10)   
    #         search = lambda embedding: [bean.url for bean in self.search_beans(embedding = embedding, min_score = DEFAULT_MAPPING_SCORE, limit = 500, projection = {K_URL: 1, K_UPDATED: 1})]
    #         update_one = lambda highlight, urls: UpdateOne(
    #                     {K_ID: highlight.id}, 
    #                     {"$set": { K_TRENDSCORE: trend_score(urls), K_URLS: urls}})
    #         # rectify nuggets trend score and mapping 
    #         # each nugget should have at least one mapped url that is of the same batch as itself.
    #         # if it cannot match the content, then delete that the nugget and proceed with the result  
    #         # TODO: remove this one, it is a redundant check          
    #         matches_same_batch = lambda highlight: self.count_search_beans(embedding=highlight.embedding, min_score=DEFAULT_MAPPING_SCORE, filter={K_UPDATED: highlight.updated}, limit = 1)

    #         to_update, to_delete = [], []
    #         for highlight in highlights:
    #             if matches_same_batch(highlight):
    #                 to_update.append(update_one(highlight, search(highlight.embedding)))
    #             else:
    #                 to_delete.append(highlight.id)

    #         total = _bulk_update(self.highlightstore, update=to_update)
    #         logger.info("%d nuggets reranked", total)
    #         if to_delete:
    #             deleted = self.highlightstore.delete_many({K_ID: {"$in": to_delete}}).deleted_count
    #             logger.info("%d straggler nuggets deleted", deleted)


    # # stores beans and post-processes them for generating embeddings, summary and nuggets
    # def _deprecated_store(self, beans: list[Bean]):        
    #     # filter out the old ones        
    #     exists = [item[K_URL] for item in self.beanstore.find({K_URL: {"$in": [bean.url for bean in beans]}}, {K_URL: 1})]
    #     beans = [bean for bean in beans if (bean.url not in exists) and bean.text]
    #     current_time = int(datetime.now().timestamp())
        
    #     # removing the media noises into separate item        
    #     noises = []
    #     for bean in beans:
    #         if bean.noise:
    #             bean.noise.url = bean.url                
    #             bean.noise.updated = current_time
    #             bean.noise.text = _truncate(bean.noise.text) if bean.noise.text else None
    #             bean.noise.score = (bean.noise.comments or 0)*3 + (bean.noise.likes or 0)
    #             noises.append(bean.noise)
    #             bean.noise = None    
    #     if noises:
    #         self.noisestore.insert_many([item.model_dump(exclude_unset=True, exclude_none=True) for item in noises])

    #     # process items with text body. if the body is empty just scrap it
    #     # process the articles, posts, comments differently from the channels
    #     chatters = [bean for bean in beans if bean.kind != CHANNEL ] 
    #     for bean in chatters:
    #         bean.updated = current_time
    #         bean.text = _truncate(bean.text)

    #     if chatters:
    #         # insert the beans
    #         res = self.beanstore.insert_many([item.model_dump(exclude_unset=True, exclude_none=True) for item in chatters])
    #         logger.info("%d beans inserted", len(res.inserted_ids))    
    #         self.rectify_beans(chatters)        
               
    #         # extract and insert nuggets
    #         nuggets = self.extract_nuggets(chatters, current_time)  
    #         if nuggets:   
    #             res = self.nuggetstore.insert_many([item.model_dump(exclude_unset=True, exclude_none=True, by_alias=True) for item in nuggets])
    #             logger.info("%d nuggets inserted", len(res.inserted_ids))    
    #             nuggets = self.rectify_nuggets(nuggets) 
    #             # delete the old duplicate ones the nuggets
    #             total = len(nuggets)          
    #             nuggets = self._deduplicate_nuggets(nuggets)
    #             logger.info("%d duplicate nuggets deleted", total - len(nuggets))    
            
    #         # now relink
    #         self._rerank(chatters, nuggets)

    #     #############        
    #     # TODO: process channels differently
    #     #############
    #     # channels = [bean for bean in beans if bean.kind == CHANNEL ] 

    # def extract_nuggets(self, beans: list[Bean], batch_time: int) -> list[Nugget]:         
    #     try:
    #         texts = [bean.text for bean in beans]
    #         is_valid_nugget = lambda nug: nug and (K_DESCRIPTION in nug) and (K_KEYPHRASE in nug) and (len(nug[K_DESCRIPTION]) > len(nug[K_KEYPHRASE]))
    #         nuggets = [Nugget(id=f"{batch_time}-{i}", keyphrase=n[K_KEYPHRASE], event=n.get(K_EVENT), description=n[K_DESCRIPTION], updated=batch_time) for i, n in enumerate(self.nuggetor.extract(texts)) if is_valid_nugget(n)]           
    #         return nuggets
    #     except Exception as err:
    #         logger.warning("Nugget extraction failed.")
    #         ic(err)

    # def rectify_beansack(self, last_ndays:int, cleanup:bool, rerank: bool):        
    #     if cleanup:
    #         time_filter = {K_UPDATED: { "$lte": get_timevalue(CLEANUP_WINDOW) }}
    #         # clean up beanstore (but retain the channels)
    #         res = self.beanstore.delete_many(time_filter)
    #         logger.info("%d old beans deleted", res.deleted_count)
    #         # clean up nuggets
    #         res = self.nuggetstore.delete_many(time_filter)
    #         logger.info("%d old nuggets deleted", res.deleted_count)
    #         # clean up beanstore
    #         res = self.beanstore.delete_many(time_filter)
    #         logger.info("%d old noises deleted", res.deleted_count)

    #     # get all the beans to rectify        
    #     beans = _deserialize_beans(self.beanstore.find(
    #         {
    #             "$or": [
    #                 { K_EMBEDDING: { "$exists": False } },
    #                 { K_SUMMARY: { "$exists": False } }
    #             ],
    #             K_UPDATED: { "$gte": get_timevalue(last_ndays) }
    #         }
    #     ))  
    #     self.rectify_beans(beans)

    #     # get all the nuggets to rectify        
    #     nuggets = _deserialize_nuggets(self.nuggetstore.find(
    #         {
    #             K_EMBEDDING: { "$exists": False },
    #             K_UPDATED: { "$gte": get_timevalue(last_ndays) }
    #         }
    #     ))
    #     nuggets = self.rectify_nuggets(nuggets)   
        
    #     if rerank:
    #         # pull in all nuggets or else keep the ones in the rectify window
    #         filter = {
    #             K_UPDATED: { "$gte": get_timevalue(last_ndays) },
    #             K_EMBEDDING: { "$exists": True }
    #         }
    #         beans = _deserialize_beans(self.beanstore.find(filter = filter, projection = { K_URL: 1 }))
    #         nuggets = _deserialize_nuggets(self.nuggetstore.find(filter=filter, sort=LATEST))
       
    #     # rectify the mappings
    #     self._rerank(beans, nuggets) 

    # def rectify_beans(self, beans: list[Bean]):
    #     def _generate(bean: Bean):
    #         to_set = {}
    #         try:
    #             if not bean.embedding:
    #                 bean.embedding = to_set[K_EMBEDDING] = self.embedder.embed_documents(bean.digest())
    #             if not bean.summary:
    #                 summary = self.summarizer.summarize(bean.text)
    #                 # this is heuristic to weed out bad summaries
    #                 bean.summary = to_set[K_SUMMARY] = summary if len(summary) > len(bean.title) else f"{bean.text[:500]}..."
    #         except Exception as err:
    #             logger.warning(f"bean rectification error: {err}")
    #             pass # do nothing, to set will be empty
    #         return UpdateOne({K_URL: bean.url}, {"$set": to_set}), bean
        
    #     if beans:
    #         updates, beans = zip(*[_generate(bean) for bean in beans])
    #         update_count = _update_collection(self.beanstore, list(updates))
    #         logger.info(f"{update_count} beans rectified")
    #     return beans

    # def rectify_nuggets(self, nuggets: list[Nugget]):
    #     def _generate(nugget: Nugget):
    #         to_set = {}
    #         try:
    #             if not nugget.embedding:                    
    #                 nugget.embedding = to_set[K_EMBEDDING] = self.embedder.embed_query(nugget.digest()) # embedding as a search query towards the documents      
    #         except Exception as err:
    #             logger.warning(f"nugget rectification error: {err}")
    #         return UpdateOne({K_ID: nugget.id}, {"$set": to_set}), nugget
        
    #     if nuggets:
    #         updates, nuggets = zip(*[_generate(nugget) for nugget in nuggets])
    #         update_count =_update_collection(self.nuggetstore, list(updates))
    #         logger.info(f"{update_count} nuggets rectified")
    #         return [nugget for nugget in nuggets if nugget.embedding]
    #     else:
    #         return nuggets
    
    # def _deduplicate_nuggets(self, nuggets: list[Nugget]):        
    #     # using .92 for similarity score calculation
    #     # and pulling in 10000 as a rough limit
    #     total = 0
    #     for nug in nuggets:
    #         to_delete = list(self.nuggetstore.aggregate(
    #             self._vector_search_pipeline(text=None, embedding=nug.embedding, min_score=0.92, filter = None, limit=10000, sort_by=LATEST, projection={K_ID:1})
    #         ))[1:]
    #         to_delete = [item[K_ID] for item in to_delete]
    #         total += self.nuggetstore.delete_many({K_ID: {"$in": to_delete}}).deleted_count
                
    #     return self.get_nuggets(filter={K_ID: {"$in": [n.id for n in nuggets]}}) 