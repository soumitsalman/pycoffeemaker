############################
## BEANSACK DB OPERATIONS ##
############################

from datetime import datetime, timedelta
from functools import reduce
import operator
from .chains import *
from .embedding import *
from .datamodels import *
from .utils import create_logger
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from bson import InvalidBSON


# names of db and collections
BEANSACK = "beansack"
BEANS = "beans"
NUGGETS = "concepts"
NOISES = "noises"
SOURCES = "sources"

DEFAULT_SEARCH_SCORE = 0.68
DEFAULT_MAPPING_SCORE = 0.72
DEFAULT_LIMIT = 100

LATEST = {K_UPDATED: -1}
TRENDING = {K_TRENDSCORE: -1}
TRENDING_AND_LATEST = {K_TRENDSCORE: -1, K_UPDATED: -1}

CLEANUP_WINDOW = 30
MAX_TEXT_LENGTH = 2000

logger = create_logger("beansack")

class Beansack:
    def __init__(self, conn_str: str, embedder: Embeddings = None, llm = None):        
        client = MongoClient(conn_str)        
        self.beanstore: Collection = client[BEANSACK][BEANS]
        self.nuggetstore: Collection = client[BEANSACK][NUGGETS]
        self.noisestore: Collection = client[BEANSACK][NOISES]        
        self.sourcestore: Collection = client[BEANSACK][SOURCES]  
        self.embedder: Embeddings = embedder
        if llm:
            self.nuggetor = NuggetExtractor(llm)
            self.summarizer = Summarizer(llm)   

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
                bean.noise.text = _truncate(bean.noise.text) if bean.noise.text else None
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
            bean.text = _truncate(bean.text)

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
                nuggets = self.rectify_nuggets(nuggets) 
                # delete the old duplicate ones the nuggets
                total = len(nuggets)          
                nuggets = self._deduplicate_nuggets(nuggets)
                logger.info("%d duplicate nuggets deleted", total - len(nuggets))    
            
            # now relink
            self._rerank(chatters, nuggets)

        #############        
        # TODO: process channels differently
        #############
        # channels = [bean for bean in beans if bean.kind == CHANNEL ] 

    def extract_nuggets(self, beans: list[Bean], batch_time: int) -> list[Nugget]:         
        try:
            texts = [bean.text for bean in beans]
            is_valid_nugget = lambda nug: nug and (K_DESCRIPTION in nug) and (K_KEYPHRASE in nug) and (len(nug[K_DESCRIPTION]) > len(nug[K_KEYPHRASE]))
            nuggets = [Nugget(id=f"{batch_time}-{i}", keyphrase=n[K_KEYPHRASE], event=n.get(K_EVENT), description=n[K_DESCRIPTION], updated=batch_time) for i, n in enumerate(self.nuggetor.extract(texts)) if is_valid_nugget(n)]           
            return nuggets
        except Exception as err:
            logger.warning("Nugget extraction failed.")
            ic(err)

    def rectify_beansack(self, last_ndays:int, cleanup:bool, rerank: bool):        
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
        
        if rerank:
            # pull in all nuggets or else keep the ones in the rectify window
            filter = {
                K_UPDATED: { "$gte": get_timevalue(last_ndays) },
                K_EMBEDDING: { "$exists": True }
            }
            beans = _deserialize_beans(self.beanstore.find(filter = filter, projection = { K_URL: 1 }))
            nuggets = _deserialize_nuggets(self.nuggetstore.find(filter=filter, sort=LATEST))
       
        # rectify the mappings
        self._rerank(beans, nuggets) 

    def rectify_beans(self, beans: list[Bean]):
        def _generate(bean: Bean):
            to_set = {}
            try:
                if not bean.embedding:
                    bean.embedding = to_set[K_EMBEDDING] = self.embedder.embed_documents(bean.digest())
                if not bean.summary:
                    summary = self.summarizer.summarize(bean.text)
                    # this is heuristic to weed out bad summaries
                    bean.summary = to_set[K_SUMMARY] = summary if len(summary) > len(bean.title) else f"{bean.text[:500]}..."
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
                    nugget.embedding = to_set[K_EMBEDDING] = self.embedder.embed_query(nugget.digest()) # embedding as a search query towards the documents      
            except Exception as err:
                logger.warning(f"nugget rectification error: {err}")
            return UpdateOne({K_ID: nugget.id}, {"$set": to_set}), nugget
        
        if nuggets:
            updates, nuggets = zip(*[_generate(nugget) for nugget in nuggets])
            update_count =_update_collection(self.nuggetstore, list(updates))
            logger.info(f"{update_count} nuggets rectified")
            return [nugget for nugget in nuggets if nugget.embedding]
        else:
            return nuggets
    
    def _deduplicate_nuggets(self, nuggets: list[Nugget]):        
        # using .92 for similarity score calculation
        # and pulling in 10000 as a rough limit
        total = 0
        for nug in nuggets:
            to_delete = list(self.nuggetstore.aggregate(
                self._vector_search_pipeline(text=None, embedding=nug.embedding, min_score=0.92, filter = None, limit=10000, sort_by=LATEST, projection={K_ID:1})
            ))[1:]
            to_delete = [item[K_ID] for item in to_delete]
            total += self.nuggetstore.delete_many({K_ID: {"$in": to_delete}}).deleted_count
                
        return self.get_nuggets(filter={K_ID: {"$in": [n.id for n in nuggets]}}) 

    def _rerank(self, beans: list[Bean], nuggets: list[Nugget]):    
        # BUG: currently trend_score is calculated for the entire database. That should NOT be the case. There should be a different trend score per time segment
        # rectify beans trend score
        if beans:
            noises = {noise.url: noise for noise in self._get_latest_noisestats([bean.url for bean in beans])}
            update_one = lambda bean: UpdateOne(
                {K_URL: bean.url}, 
                {
                    "$set": { 
                        K_TOTALLIKES: noises[bean.url].likes,
                        K_TOTALCOMMENTS: noises[bean.url].comments,
                        K_TRENDSCORE: noises[bean.url].score
                    }
                }
            )
            update_count = _update_collection(self.beanstore, [update_one(bean) for bean in beans if bean.url in noises])
            logger.info(f"{update_count} beans reranked")        

        # calculate and relink the unique nuggets
        if nuggets:

            # beans are already rectified with trendscore. so just leverage that  
            getbeans = lambda urls: self.beanstore.find(filter = {K_URL: {"$in": urls}}, projection={K_TRENDSCORE:1})
            nugget_trend_score = lambda urls: reduce(operator.add, [(bean[K_TRENDSCORE] or 0) for bean in getbeans(urls) if K_TRENDSCORE in bean], len(urls)*10)   
            search = lambda embedding: [bean.url for bean in self.search_beans(embedding = embedding, min_score = DEFAULT_MAPPING_SCORE, limit = 500, projection = {K_URL: 1})]
            update_one = lambda nugget, urls: UpdateOne(
                        {K_ID: nugget.id}, 
                        {"$set": { K_TRENDSCORE: nugget_trend_score(urls), K_URLS: urls}})
            # rectify nuggets trend score and mapping 
            # each nugget should have at least one mapped url that is of the same batch as itself.
            # if it cannot match the content, then delete that the nugget and proceed with the result            
            matches_same_batch = lambda nugget: self.count_search_beans(embedding=nugget.embedding, min_score=DEFAULT_MAPPING_SCORE, filter={K_UPDATED: nugget.updated}, limit = 1)

            to_update, to_delete = [], []
            for nugget in nuggets:
                if matches_same_batch(nugget):
                    to_update.append(update_one(nugget, search(nugget.embedding)))
                else:
                    to_delete.append(nugget.id)

            total = _update_collection(self.nuggetstore, to_update)
            logger.info("%d nuggets reranked", total)
            if to_delete:
                deleted = self.nuggetstore.delete_many({K_ID: {"$in": to_delete}}).deleted_count
                logger.info("%d straggler nuggets deleted", deleted)

    
    ####################
    ## GET AND SEARCH ##
    ####################

    def get_beans(self, filter, skip = 0, limit = 0, sort_by = None, projection = None) -> list[Bean]:
        cursor = self.beanstore.find(filter = filter, projection = projection, sort=sort_by, skip = skip, limit=limit)
        return _deserialize_beans(cursor)

    def get_nuggets(self, filter, skip = 0, limit = 0,  sort_by = None, projection = None) -> list[Nugget]:        
        # append the embeddings filter so that it takes into account only the indexed items
        filter = {
            **{K_EMBEDDING: {"$exists": True}}, 
            **(filter or {})
        }        
        cursor = self.nuggetstore.find(filter=filter, projection=projection, sort=sort_by, skip=skip, limit=limit)
        return _deserialize_nuggets(cursor)
    
    def get_beans_by_nugget(self, 
            nugget_id = None,
            nugget: Nugget = None,
            filter = None,
            limit = DEFAULT_LIMIT,
            projection=None          
        ) -> list:
        urls = nugget.urls if nugget else self.nuggetstore.find_one({K_ID: nugget_id, K_URLS: {"$exists": True}}, {K_URLS: 1})[K_URLS]
        bean_filter = {**{K_URL: {"$in": urls }}, **(filter or {})}
        return self.get_beans(filter = bean_filter, limit=limit, sort_by=LATEST, projection=projection)
    
    def count_beans_by_nugget(self, 
            nugget_id = None,
            nugget: Nugget = None,
            filter = None,
            limit = DEFAULT_LIMIT            
        ) -> list:
        urls = nugget.urls if nugget else self.nuggetstore.find_one({K_ID: nugget_id, K_URLS: {"$exists": True}}, {K_URLS: 1})[K_URLS]
        bean_filter = {**{K_URL: {"$in": urls }}, **(filter or {})}
        return self.beanstore.count_documents(filter = bean_filter, limit=limit)

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
    
    def count_search_beans(self, query: str = None, embedding: list[float] = None, min_score = DEFAULT_SEARCH_SCORE, filter: dict = None, limit = DEFAULT_LIMIT) -> int:
        pipeline = self._count_vector_search_pipeline(query, embedding, min_score, filter, limit)
        result = list(self.beanstore.aggregate(pipeline))
        return result[0]['total_count'] if result else 0

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
    
    def count_search_nuggets(self, query: str = None, embedding: list[float] = None, min_score = DEFAULT_SEARCH_SCORE, filter: dict = None, limit = DEFAULT_LIMIT) -> int:
        pipeline = self._count_vector_search_pipeline(query, embedding, min_score, filter, limit)
        result = list(self.nuggetstore.aggregate(pipeline))
        return result[0]['total_count'] if result else 0
    
    def trending_beans(self, 
            query: str = None,
            embedding: list[float] = None, 
            min_score = DEFAULT_SEARCH_SCORE, 
            filter = None, 
            limit = DEFAULT_LIMIT, 
            projection = None
        ) -> list[Bean]:        
        if not (query or embedding):
            # if query or embedding is not given, then this is scalar query
            return self.get_beans(filter=filter, limit=limit, sort_by=TRENDING_AND_LATEST, projection=projection)
        else:
            # else run the vector query
            return self.search_beans(query=query, embedding=embedding, min_score=min_score, filter=filter, limit=limit, sort_by=TRENDING_AND_LATEST, projection=projection)
        
    def count_trending_beans(self, query: str = None, embedding: list[float] = None, min_score = DEFAULT_SEARCH_SCORE, filter = None, limit = DEFAULT_LIMIT) -> int:        
        return self.beanstore.count_documents(filter=filter, limit=limit) \
            if not (query or embedding) \
            else self.count_search_beans(query=query, embedding=embedding, min_score=min_score, filter=filter, limit=limit)

    def trending_nuggets(self, 
            query: str = None,
            embedding: list[float] = None, 
            min_score = DEFAULT_SEARCH_SCORE,             
            filter = None,
            limit = DEFAULT_LIMIT,
            projection = None            
        ) -> list:
        if not (query or embedding):
            # if query or embedding is not given, then this is scalar query            
            result = self.get_nuggets(filter = filter, limit = limit, sort_by=TRENDING_AND_LATEST)
        else:
            # else run the vector query
            result = self.search_nuggets(query=query, embedding=embedding, min_score=min_score, filter=filter, limit=limit, sort_by=TRENDING_AND_LATEST, projection=projection)            
        return result
    
    def count_trending_nuggets(self, query: str = None, embedding: list[float] = None, min_score = DEFAULT_SEARCH_SCORE, filter = None, limit = DEFAULT_LIMIT) -> int:        
        return self.nuggetstore.count_documents(filter=filter, limit=limit) \
            if not (query or embedding) \
            else self.count_search_nuggets(query=query, embedding=embedding, min_score=min_score, filter=filter, limit=limit)
    
    # def search_nuggets_with_beans(self, 
    #         query: str = None,
    #         embedding: list[float] = None, 
    #         min_score = DEFAULT_SEARCH_SCORE, 
    #         limit = DEFAULT_LIMIT,
    #         filter = None
    #     ) -> list:
    #     # search for the nuggets with that query/embedding
    #     nuggets = self.search_nuggets(
    #         query=query,
    #         embedding=embedding,
    #         min_score=min_score,
    #         filter=filter, 
    #         limit=limit*10, # HACK: to make sure that we retrieve enough elements to dedupe this
    #         sort_by=TRENDING_AND_LATEST
    #     )
    #     return self.get_beans_by_nuggets(nuggets=nuggets, filter=filter, limit=limit)

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
    
    def _count_vector_search_pipeline(self, text, embedding, min_score, filter, limit):
        pipline = self._vector_search_pipeline(text, embedding, min_score, filter, limit, None, None)
        pipline.append({ "$count": "total_count"})
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

_encoding = tiktoken.get_encoding("cl100k_base")
# truncates from both end and keeps the middle
def _truncate(input: str) -> str:
    tokens = _encoding.encode(input)
    token_count = len(tokens)
    if token_count > MAX_TEXT_LENGTH:
        mid = token_count//2
        tokens = tokens[mid - (MAX_TEXT_LENGTH//2): mid + (MAX_TEXT_LENGTH//2)]
    return _encoding.decode(tokens)   

def timewindow_filter(last_ndays: int):
    return {K_UPDATED: {"$gte": get_timevalue(last_ndays)}}

def get_timevalue(last_ndays: int):
    return int((datetime.now() - timedelta(days=last_ndays)).timestamp())
