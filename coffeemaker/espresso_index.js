use('espresso')

db.runCommand(
    {
      "createIndexes": "categories",
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