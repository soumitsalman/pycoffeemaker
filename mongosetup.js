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
      categories: 1
  },
  {
      name: "beans_categories_search"
  }
);

db.beans.createIndex(
  {
      tags: 1
  },
  {
      name: "beans_tags_search"
  }
);

db.beans.createIndex(
  {
      trend_score: -1,
      updated: -1
  },
  {
      name: "beans_trend_score_and_latest"
  }
);