use("beansackV2")

// vector indexes for beans and baristas
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
          "numLists": 1,
          "similarity": "COS",
          "dimensions": 384
        }
      }
    ]
  }
);

db.runCommand(
  {
    "createIndexes": "baristas",
    "indexes": [
      {
        "name": "baristas_vector_search",
        "key": 
        {
          "embedding": "cosmosSearch"
        },
        "cosmosSearchOptions": 
        {
          "kind": "vector-ivf",
          "numLists": 1,
          "similarity": "COS",
          "dimensions": 384
        }
      }
    ]
  }
);

// text indexes for beans and baristas
db.baristas.createIndex(
  {
      tags: "text",
      title: "text",
      description: "text",
      sources: "text",
  },
  {
      name: "baristas_text_search"
  }
);

db.beans.createIndex(
  {
      tags: "text",
      title: "text",
      summary: "text"
  },
  {
      name: "beans_text_search"
  }
);


// needed for related beans
db.beans.createIndex(
  {
      url: 1
  },
  {
      name: "beans_url"
  }
);

// needed for sorting
db.beans.createIndex(
  {
      created: -1,
      trend_score: -1
  },
  {
      name: "beans_created_and_trending"
  }
);

// needed for sorting
db.beans.createIndex(
  {
      updated: -1,
      trend_score: -1
  },
  {
      name: "beans_updated_and_trending"
  }
);

// needed for tags search
db.beans.createIndex(
  {
      tags: 1,
      kind: 1,
      updated: -1
  },
  {
      name: "beans_tags_and_kind_and_updated"
  }
);

// needed for tags search
db.beans.createIndex(
  {
      tags: 1,
      kind: 1,
      created: -1
  },
  {
      name: "beans_tags_and_kind_and_created"
  }
);

// needed for group by cluster
db.beans.createIndex(
  {
      cluster_id: 1,
      kind: 1,        
      created: -1,  
      trend_score: -1
  },
  {
      name: "beans_cluster_kind_created_and_trending"
  }
);

// needed for group by cluster
db.beans.createIndex(
  {
      cluster_id: 1,
      kind: 1,        
      updated: -1,  
      trend_score: -1
  },
  {
      name: "beans_cluster_kind_updated_and_trending"
  }
);

// needed for related beans
db.beans.createIndex(
  {
      cluster_id: 1
  },
  {
      name: "beans_cluster_id"
  }
);
