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
      tags: "text",
      title: "text"
  },
  {
      name: "beans_text_search"
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

db.runCommand(
    {
      "createIndexes": "baristas",
      "indexes": [
        {
          "name": "categories_vector_search",
          "key": 
          {
            "embedding": "cosmosSearch"
          },
          "cosmosSearchOptions": 
          {
            "kind": "vector-ivf",
            "numLists": 1,
            "similarity": "COS",
            "dimensions": 1024
          }
        }
      ]
    }
);

db.categories.createIndex(
    {
        text: 1,
        source: 1
    },
    {
        name: "categories_scalar"
    }
)