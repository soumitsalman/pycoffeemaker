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
      cluster_id: 1
  },
  {
      name: "beans_cluster_id"
  }
);

db.beans.createIndex(
  {
      categories: 1
  },
  {
      name: "beans_categories"
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
      kind: 1
  },
  {
      name: "beans_kind"
  }
);

db.beans.createIndex(
  {
      created: -1
  },
  {
      name: "beans_newest"
  }
);

db.beans.createIndex(
  {
      updated: -1
  },
  {
      name: "beans_latest"
  }
);

db.beans.createIndex(
  {
      trend_score: -1
  },
  {
      name: "beans_trending"
  }
);

db.beans.createIndex(
  {
      created: -1,
      trend_score: -1
  },
  {
      name: "beans_newest_and_trending"
  }
);

db.beans.createIndex(
  {
      latest: -1,
      trend_score: -1
  },
  {
      name: "beans_latest_and_trending"
  }
);

db.beans.createIndex(
  {
      categories: 1,
      kind: 1,
      trend_score: -1
  },
  {
      name: "beans_categories_kind_and_trending"
  }
);

db.beans.createIndex(
  {
      categories: 1,
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
