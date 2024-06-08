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
            "dimensions": 768
          }
        }
      ]
    }
);