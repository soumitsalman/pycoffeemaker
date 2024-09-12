use("beansack")

db.runCommand(
    {
      "createIndexes": "beans",
      "indexes": [
        {
          "name": "beans_vector_search",
          "key": 
          {
            "embedding": "cosmosSearch"
          },
          "cosmosSearchOptions": 
          {
            "kind": "vector-ivf",
            "numLists": 10,
            "similarity": "COS",
            "dimensions": 1024
          }
        }
      ]
    }
);

db.beans.createIndex(
  {
      url: 1
  },
  {
      name: "beans_url"
  }
);

db.beans.createIndex(
  {
      tags: 1
  },
  {
      name: "beans_tags"
  }
);

db.beans.createIndex(
  {
      categories: 1,
      cluster_id: 1,
      kind: 1,
      created: -1,
      trend_score: -1
  },
  {
      name: "beans_categories_kind_newest_and_trending"
  }
);

db.beans.createIndex(
  {
      categories: 1,
      cluster_id: 1,
      kind: 1,
      trend_score: -1,
      updated: -1,
      
  },
  {
      name: "beans_categories_kind_trending_and_latest"
  }
);

db.beans.createIndex(
  {
      url: 1,
      cluster_id: 1,
      
  },
  {
      name: "beans_url_and_cluster"
  }
);

db.beans.createIndex(
  {
      categories: "text",
      tags: "text",
      title: "text",
      summary: "text",
      highlights: "text"
  },
  {
      name: "beans_text_search"
  }
);


DELETE_TIME = 2592000 // 30 DAYS * 24 HOURS * 60 MINS * 60 SECONDS
db.beans.createIndex(
    {"_ts":1}, 
    {
        name: "delete-stale-beans",
        expireAfterSeconds: DELETE_TIME
    }
);
db.chatters.createIndex(
    {"_ts":1}, 
    {
        name: "delete-stale-chatters",
        expireAfterSeconds: DELETE_TIME
    }
);

db.chatters.createIndex(
  {
      url: 1,
      source: 1,
      
  },
  {
      name: "chatters_url_and_source"
  }
);