# Graph Report - .  (2026-06-01)

## Corpus Check
- 78 files · ~78,177 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1322 nodes · 2232 edges · 80 communities (55 shown, 25 thin omitted)
- Extraction: 92% EXTRACTED · 8% INFERRED · 0% AMBIGUOUS · INFERRED: 170 edges (avg confidence: 0.76)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_API Collectors|API Collectors]]
- [[_COMMUNITY_Lance Cupboard|Lance Cupboard]]
- [[_COMMUNITY_Digest Models|Digest Models]]
- [[_COMMUNITY_Image Micro-Agents|Image Micro-Agents]]
- [[_COMMUNITY_DuckDB SQL Utils|DuckDB SQL Utils]]
- [[_COMMUNITY_Digestor Agents|Digestor Agents]]
- [[_COMMUNITY_Lance Beansack|Lance Beansack]]
- [[_COMMUNITY_State Cache Backends|State Cache Backends]]
- [[_COMMUNITY_Factory Migration|Factory Migration]]
- [[_COMMUNITY_GLiNER Extractors|GLiNER Extractors]]
- [[_COMMUNITY_DuckSack Storage|DuckSack Storage]]
- [[_COMMUNITY_DuckDB CDN Layer|DuckDB CDN Layer]]
- [[_COMMUNITY_Async State Cache|Async State Cache]]
- [[_COMMUNITY_Embedding Models|Embedding Models]]
- [[_COMMUNITY_PG State Cache|PG State Cache]]
- [[_COMMUNITY_Worker Pipeline|Worker Pipeline]]
- [[_COMMUNITY_MongoDB Storage|MongoDB Storage]]
- [[_COMMUNITY_Surreal State Machine|Surreal State Machine]]
- [[_COMMUNITY_Async Scrapers Tests|Async Scrapers Tests]]
- [[_COMMUNITY_Collector Orchestrator|Collector Orchestrator]]
- [[_COMMUNITY_Async CDN Store|Async CDN Store]]
- [[_COMMUNITY_Test Data Generators|Test Data Generators]]
- [[_COMMUNITY_Cupboard Backends|Cupboard Backends]]
- [[_COMMUNITY_State Cache Base|State Cache Base]]
- [[_COMMUNITY_Classifier Topics|Classifier Topics]]
- [[_COMMUNITY_PG Cache SQL|PG Cache SQL]]
- [[_COMMUNITY_Micro-Agent Runtime|Micro-Agent Runtime]]
- [[_COMMUNITY_Entity Extractor|Entity Extractor]]
- [[_COMMUNITY_RSS Docker IO|RSS Docker IO]]
- [[_COMMUNITY_Embedder Base|Embedder Base]]
- [[_COMMUNITY_Mongo Publishers|Mongo Publishers]]
- [[_COMMUNITY_Bean Domain Models|Bean Domain Models]]
- [[_COMMUNITY_Simple Vector DB|Simple Vector DB]]
- [[_COMMUNITY_Sip Source Models|Sip Source Models]]
- [[_COMMUNITY_Rectify Migrations|Rectify Migrations]]
- [[_COMMUNITY_Grafeo Cupboard|Grafeo Cupboard]]
- [[_COMMUNITY_Firebird State Cache|Firebird State Cache]]
- [[_COMMUNITY_Bean Query Pipelines|Bean Query Pipelines]]
- [[_COMMUNITY_Warehouse Tests|Warehouse Tests]]
- [[_COMMUNITY_Infinity Embeddings|Infinity Embeddings]]
- [[_COMMUNITY_Knowledge Graph Edges|Knowledge Graph Edges]]
- [[_COMMUNITY_Classification Taxonomy|Classification Taxonomy]]
- [[_COMMUNITY_Test File Utils|Test File Utils]]
- [[_COMMUNITY_Consolidator Worker|Consolidator Worker]]
- [[_COMMUNITY_Coffeemaker Runtime|Coffeemaker Runtime]]
- [[_COMMUNITY_Chatter Engagement|Chatter Engagement]]
- [[_COMMUNITY_Local Tokenizer|Local Tokenizer]]
- [[_COMMUNITY_Web Page Scraper|Web Page Scraper]]
- [[_COMMUNITY_Classification Cache|Classification Cache]]
- [[_COMMUNITY_Factory Setup|Factory Setup]]
- [[_COMMUNITY_Content Quality Metrics|Content Quality Metrics]]
- [[_COMMUNITY_Docker Compose Services|Docker Compose Services]]
- [[_COMMUNITY_Processing State Machine|Processing State Machine]]
- [[_COMMUNITY_Vector Search|Vector Search]]
- [[_COMMUNITY_Feed Source Config|Feed Source Config]]
- [[_COMMUNITY_Fire Classification Cache|Fire Classification Cache]]
- [[_COMMUNITY_Digestor Worker|Digestor Worker]]
- [[_COMMUNITY_Text Cleanup Utils|Text Cleanup Utils]]
- [[_COMMUNITY_Machine Ops Runner|Machine Ops Runner]]
- [[_COMMUNITY_User Accounts|User Accounts]]
- [[_COMMUNITY_Bean Persistence|Bean Persistence]]
- [[_COMMUNITY_Test Module Entry|Test Module Entry]]
- [[_COMMUNITY_SQL Table Constants|SQL Table Constants]]
- [[_COMMUNITY_Bean Field Keys|Bean Field Keys]]
- [[_COMMUNITY_NLP Package|NLP Package]]
- [[_COMMUNITY_Bean Merge CDN|Bean Merge CDN]]
- [[_COMMUNITY_Storage Field Mapping|Storage Field Mapping]]
- [[_COMMUNITY_Collector Entry Point|Collector Entry Point]]
- [[_COMMUNITY_Classification DataFrame|Classification DataFrame]]
- [[_COMMUNITY_DB Transactions|DB Transactions]]
- [[_COMMUNITY_Chatter Storage|Chatter Storage]]
- [[_COMMUNITY_Pybeansack Package|Pybeansack Package]]
- [[_COMMUNITY_DB Type Config|DB Type Config]]
- [[_COMMUNITY_User Model|User Model]]
- [[_COMMUNITY_Scraper Page Model|Scraper Page Model]]

## God Nodes (most connected - your core abstractions)
1. `MongoDB` - 54 edges
2. `DuckDB` - 39 edges
3. `DuckSack` - 38 edges
4. `PGSack` - 33 edges
5. `LanceSack` - 28 edges
6. `EmbedderBase` - 18 edges
7. `Digest` - 17 edges
8. `Collector` - 15 edges
9. `_build_rss_item()` - 15 edges
10. `Cupboard` - 14 edges

## Surprising Connections (you probably didn't know these)
- `localindexer (legacy INDEXER)` --semantically_similar_to--> `EMBEDDER worker mode`  [AMBIGUOUS] [semantically similar]
  docker-compose.yaml → README.md
- `SAME_AS graph edge` --semantically_similar_to--> `Bean`  [AMBIGUOUS] [semantically similar]
  history.txt → README.md
- `PUBLISHED graph edge` --semantically_similar_to--> `Publisher`  [AMBIGUOUS] [semantically similar]
  history.txt → README.md
- `migrate_users()` --calls--> `Collector`  [INFERRED]
  factory/rectify.py → workers/collectororch.py
- `migrate_classification_cache()` --calls--> `SimpleVectorDB`  [INFERRED]
  factory/rectify.py → pybeansack/simplevectordb.py

## Hyperedges (group relationships)
- **Coffeemaker MODE Worker Orchestration** — run_coffeemaker_runner, run_processing_mode_scheduler, collector_orchestrator, embedder_worker, extractor_worker, digestor_worker, classifier_worker, consolidator_worker, beansack_porter, cupboard_porter [EXTRACTED 1.00]
- **Pluggable State Cache Backends** — state_cache_base_contract, pg_state_cache, pg_async_state_cache, firebird_state_cache, sqlite_turso_state_cache, surreal_state_machine, pgcls_combined_cache [INFERRED 0.85]
- **Bean State Progression Through Pipeline** — utils_processing_state_machine, collector_orchestrator, embedder_worker, classifier_worker, extractor_worker, digestor_worker, consolidator_worker, beansack_porter, cupboard_porter [INFERRED 0.80]
- **Beansack storage backend implementations** — pgsack_PGSack, lancesack_LanceSack, duckdbsack_DuckDB, ducklakesack_DuckSack, mongosack_MongoDB [EXTRACTED 1.00]
- **Article and engagement domain models** — models_Bean, models_Chatter, models_Publisher, models_TrendingBean, models_AggregatedBean [EXTRACTED 1.00]
- **SQL trending and aggregation query stack** — pgsack_sql_trend_aggregates, pgsack_sql_trending_view, pgsack_sql_aggregated_view, pgsack_PGSack [INFERRED 0.85]
- **Cupboard vector storage backends** — pgcupboard_Cupboard, surrealcupboard_Cupboard, grafeocupboard_Cupboard, lancecupboard_LanceDBCupboard, models_Sip, models_Source [INFERRED 0.85]
- **NLP embed and digest stack** — embedders_create_embedder, embedders_EmbedderBase, agents_create_micro_agent, agents_MicroAgentBase, agents_EntityExtractor, nlp_Digest [INFERRED 0.88]
- **Web content ingestion collectors** — apicollectors_APICollector, apicollectors_APICollectorAsync, scrapers_AsyncWebScraper, scrapers_WebCrawler, utils_cleanup_item, datacollectors_field_constants [INFERRED 0.90]
- **Coffeemaker collect-analyze-port pipeline** — workers_collectororch, workers_analyzerorch, workers_porterorch, workercache_statmachine [EXTRACTED 1.00]
- **Project Cafecito core data units** — bean_data_unit, chatter_data_unit, publisher_data_unit, sip_data_unit, composite_data_unit [EXTRACTED 1.00]
- **Docker Compose local dev infrastructure** — compose_pgcache, compose_localmongo, compose_azurite, compose_localcrawler [EXTRACTED 1.00]

## Communities (80 total, 25 thin omitted)

### Community 0 - "API Collectors"
Cohesion: 0.05
Nodes (48): APICollector, APICollectorAsync, _batch_run(), _build_hackernews_item(), _build_reddit_item(), _build_rss_item(), _extract_author_email(), _extract_body() (+40 more)

### Community 1 - "Lance Cupboard"
Cohesion: 0.05
Nodes (22): Config, LanceDBCupboard, # NOTE: this is deprecated. IGNORE, # NOTE: something wrong with the vector index creation, Generated article stored in cupboard, _Sip, _where(), Sip (+14 more)

### Community 2 - "Digest Models"
Cohesion: 0.05
Nodes (45): BaseModel, Digest, Metadata, parse_compressed(), parse_json(), parse_markdown(), valid_names(), valid_regions() (+37 more)

### Community 3 - "Image Micro-Agents"
Cohesion: 0.07
Nodes (21): ABC, DiffuserImageGenerationAgent, image_agent_from_path(), LlamaCppTextGeneratorClient, LMAgentBase, LMClientBase, LocalTokenizer, ONNXText2TextClient (+13 more)

### Community 4 - "DuckDB SQL Utils"
Cohesion: 0.07
Nodes (25): _conflict_target(), create_db(), cursor(), _insert_multivalues_sql(), _limit(), PGSack, _primary_key_fields(), _query_composites() (+17 more)

### Community 5 - "Digestor Agents"
Cohesion: 0.07
Nodes (14): _build_tool_schema(), DigestorBase, from_path(), LocalTokenizer, NamedEntityExtractor, OpenAIDigestor, ORTDigestor, OVDigestor (+6 more)

### Community 6 - "Lance Beansack"
Cohesion: 0.06
Nodes (16): Beansack, LanceModel, _Bean, _Chatter, _connect(), create_db(), LanceSack, _Publisher (+8 more)

### Community 7 - "State Cache Backends"
Cohesion: 0.07
Nodes (23): ClassificationCacheBase, AsyncStateCache, batch_search(), ClassificationCache, _copy_insert_state_rows(), _copy_insert_state_rows_async(), _create_emb_tables_sql(), _create_multi_state_query_expr() (+15 more)

### Community 8 - "Factory Migration"
Cohesion: 0.08
Nodes (9): db_instance(), migrate(), _beans_to_df(), create_db(), DuckDB, _publishers_to_df(), # TODO: merge this with ducklakesack since the logic is mostly the same, just di, _select() (+1 more)

### Community 9 - "GLiNER Extractors"
Cohesion: 0.06
Nodes (46): EntityExtractor GLiNER, MicroAgentBase, RemoteMicroAgent OpenAI, TransformerMicroAgent, VLLMMicroAgent, create_micro_agent factory, parse_compressed digest parser, parse_markdown digest parser (+38 more)

### Community 10 - "DuckSack Storage"
Cohesion: 0.1
Nodes (8): _beans_to_df(), create_db(), DuckSack, execute(), _execute_df(), _publishers_to_df(), _select(), _where()

### Community 11 - "DuckDB CDN Layer"
Cohesion: 0.1
Nodes (36): AsyncCDNStore, CDNStore, Beansack, DuckDB, duckdbsack.create_db, DuckSack, ducklakesack.create_db, DuckLake warehouse attach (+28 more)

### Community 12 - "Async State Cache"
Cohesion: 0.11
Nodes (11): AsyncStateCache, _cleanup_sql(), _create_id_exists_expr(), _create_multi_state_query_expr(), _create_single_state_query_expr(), _create_state_query_expr(), _create_state_tables_sql(), _ensure_database() (+3 more)

### Community 13 - "Embedding Models"
Cohesion: 0.1
Nodes (9): create_embedder(), LlamaCppEmbeddings, ORTEmbeddings, OVEmbeddings, # NOTE: moving the import inside the function so that there is no need to instal, RemoteEmbeddings, VLLMEmbedder, test_orch_on_lancesack() (+1 more)

### Community 14 - "PG State Cache"
Cohesion: 0.13
Nodes (8): AsyncStateCache, _create_multi_state_query_expr(), create_rows(), _create_single_state_query_expr(), _create_state_query_expr(), _create_state_tables_sql(), _rectify_path(), StateCache

### Community 15 - "Worker Pipeline"
Cohesion: 0.16
Nodes (26): Bean Processing Pipeline, Beansack Porter, Classifier and Clusterer Worker, Collector Orchestrator, Consolidator Worker, Cupboard Porter, Digestor Worker, Embedder Worker (+18 more)

### Community 17 - "Surreal State Machine"
Cohesion: 0.15
Nodes (11): AsyncStateMachine, create_data_to_store(), create_exists_query_expr(), _create_multi_state_query_expr(), create_optimize_expr(), create_query_expr(), _create_single_state_query_expr(), create_table_expr() (+3 more)

### Community 18 - "Async Scrapers Tests"
Cohesion: 0.17
Nodes (19): APICollectorAsync, AsyncWebScraper, _analyzer_cls_cache(), _analyzer_test_cache(), create_test_data_file(), hydrate_test_db(), main(), save_models() (+11 more)

### Community 19 - "Collector Orchestrator"
Cohesion: 0.15
Nodes (7): Collector, parse_sources(), Store storable collection results and persist the rest for scraping., run(), validate_bean_item(), validate_chatter_item(), validate_source_item()

### Community 21 - "Async CDN Store"
Cohesion: 0.16
Nodes (10): AsyncCDNStore, CDNStore, _guess_type(), _public_url(), Uploads a single binary file.          Parameters:             path: should be i, Uploads multiple text items concurrently.          Parameters:             data:, Creates a public access URL based on template. Ex: https://{bucket}.t3.tigrisfil, Uploads a single text file.          Parameters:             path: should be in (+2 more)

### Community 22 - "Test Data Generators"
Cohesion: 0.16
Nodes (17): generate_fake_beans(), generate_fake_chatters(), generate_fake_digests(), generate_fake_embeddings(), generate_fake_mugs(), generate_fake_publishers(), generate_fake_sips(), Test warehouse maintenance tasks (+9 more)

### Community 23 - "Cupboard Backends"
Cohesion: 0.16
Nodes (5): Cupboard, Insert a batch while skipping records that already exist., Link inserted events to matching sources via `source['base_url']->PUBLISHED-> ev, Link matching events to inserted sources via `source->PUBLISHED->event['base_url, transaction()

### Community 24 - "State Cache Base"
Cohesion: 0.14
Nodes (6): AsyncStateCacheBase, StateCacheBase, AsyncStateCache, _create_state_tables_sql(), Initialize the ProcessingCache with a PostgreSQL connection string and table set, StateCache

### Community 25 - "Classifier Topics"
Cohesion: 0.16
Nodes (9): create_composer_topics_locally(), StateCacheBase, _bean_to_str(), Classifier, Embedder, Extractor, _group_to_str(), run() (+1 more)

### Community 26 - "PG Cache SQL"
Cohesion: 0.21
Nodes (16): decode_data(), _add_ts_and_ids_expr(), _copy_insert_state_rows(), _copy_insert_state_rows_async(), _create_multi_state_query_expr(), create_query_expr(), _create_single_state_query_expr(), deduplicate() (+8 more)

### Community 27 - "Micro-Agent Runtime"
Cohesion: 0.16
Nodes (6): MicroAgentBase, parse_compressed(), parse_markdown(), _run_single(), _strip_fences(), VLLMMicroAgent

### Community 29 - "RSS Docker IO"
Cohesion: 0.16
Nodes (12): Beansack, RSS feedparser field reference, DockerfileIO, entry.content, entry.title, run_porter(), Asynchronous unaware orchestrators, test_porter_orch() (+4 more)

### Community 30 - "Embedder Base"
Cohesion: 0.19
Nodes (4): EmbedderBase, Takes a list of strings/large documents as input and chunks them into smaller pi, Embeds a single string as a query. It prepends `query: ` to the input., This takes a string or an list of strings as an input.         This calls the em

### Community 31 - "Mongo Publishers"
Cohesion: 0.18
Nodes (9): _fix_publisher_ids(), _Publisher, # TODO: split out function for adding embeddings and gists. code commented out b, # TODO: add function for recompute (for clusters, categories, sentiments and tre, # TODO: add a recompute and cleanup function, # TODO: add delete for bookmarked bean, # NOTE: remove anything collected 7 days ago that did not get processed by analy, # TODO: this is a temporary fix. (+1 more)

### Community 32 - "Bean Domain Models"
Cohesion: 0.19
Nodes (10): AggregatedBean, Bean, Config, Publisher, Metadata of the website, publication or social medium from which an article or c, Metadata of an article such as a news or blog post., # TODO: add entities and region down the road, Bean with additional fields for tracking social media engagement and propagation (+2 more)

### Community 33 - "Simple Vector DB"
Cohesion: 0.19
Nodes (3): _prepare_to_store(), Opens connection to existing or new simple database on `db_path`.         Create, SimpleVectorDB

### Community 34 - "Sip Source Models"
Cohesion: 0.22
Nodes (12): Cupboard, Mug, pycupboard/requirements.txt, Sip, extract_sips_from_content(), extract_tldr_highlight(), import_espresso_rss(), parse_rss_to_mugs_and_sips() (+4 more)

### Community 35 - "Rectify Migrations"
Cohesion: 0.2
Nodes (11): cleanup_bean_tags(), hydrate_processing_cache(), migrate_classification_cache(), migrate_classification_cache_pg_to_fire(), migrate_users(), Migrate ClassificationCache from PostgreSQL to Firebird/zvec.      Args:, Hydrates local processing cache with beans and publishers from production/backup, Read a Parquet file, split it into chunks of `chunk_size` rows and     write eac (+3 more)

### Community 37 - "Firebird State Cache"
Cohesion: 0.2
Nodes (5): _create_rows(), _merge_state_sql(), encode_data(), _create_rows(), set()

### Community 38 - "Bean Query Pipelines"
Cohesion: 0.2
Nodes (4): _beans_query_pipeline(), _beans_text_search_pipeline(), _deserialize_beans(), _related_beans_pipeline()

### Community 39 - "Warehouse Tests"
Cohesion: 0.18
Nodes (11): Test storing Chatter data in warehouse, Test storing Source data in warehouse, Test storing BeanCore data in warehouse, Test storing BeanEmbedding data in warehouse, Test storing BeanGist data in warehouse, _run_test_func(), test_store_chatters(), test_store_cores() (+3 more)

### Community 40 - "Infinity Embeddings"
Cohesion: 0.2
Nodes (3): InfinityEmbeddings, TransformerEmbeddings, clear_gpu_cache()

### Community 41 - "Knowledge Graph Edges"
Cohesion: 0.2
Nodes (11): Bean, Chatter, Composite, PUBLISHED graph edge, SAME_AS graph edge, SurrealDB graph query log, Publisher, create_client (+3 more)

### Community 42 - "Classification Taxonomy"
Cohesion: 0.22
Nodes (11): Topic classification taxonomy, CLASSIFICATION_CACHE, factory/classifications.yaml, create_embedder, EntityExtractor, Sentiment labels, test_embedder_orch(), CLASSIFIER worker mode (+3 more)

### Community 43 - "Test File Utils"
Cohesion: 0.38
Nodes (9): load_json(), save_image(), save_json(), save_markdown(), test_digestor(), test_embedder(), test_extractor(), to_filename() (+1 more)

### Community 44 - "Consolidator Worker"
Cohesion: 0.33
Nodes (4): Consolidator, _group_items(), Consolidates events and data to create consolidated briefings and signals, Groups items based on L2 distance between embeddings.          Args:         ite

### Community 45 - "Coffeemaker Runtime"
Cohesion: 0.25
Nodes (9): Coffeemaker, DockerfileGPU, Espresso, create_micro_agent, Legacy digest prompts, Digest schema, Project Cafecito, Pycoffeemaker (Coffeemaker) (+1 more)

### Community 46 - "Chatter Engagement"
Cohesion: 0.25
Nodes (4): Chatter, Social media engagement stats of an article/bean (specified by `url`)., _deserialize_chatters(), Retrieves the latest social media status from different mediums.

### Community 50 - "Factory Setup"
Cohesion: 0.38
Nodes (5): create_classification_cache(), create_classification_embeddings(), create_classification_files(), create_processing_cache(), Seed cache with classification embeddings

### Community 51 - "Content Quality Metrics"
Cohesion: 0.52
Nodes (6): call_to_action_density(), compression_ratio(), narrative_density(), repeated_phrases(), should_reject_input(), surface_signature_trigger()

### Community 52 - "Docker Compose Services"
Cohesion: 0.29
Nodes (6): azurite service, localindexer (legacy INDEXER), localcrawler (crawl4ai), localmongo service, pgcache service, pybeansack/docker-compose.yml

### Community 53 - "Processing State Machine"
Cohesion: 0.33
Nodes (6): PROCESSING_CACHE, Bean processing states, Insert/delete over update, Fault-tolerant state machine, test_collector_orch(), Worker state cache (state machine)

### Community 55 - "Feed Source Config"
Cohesion: 0.47
Nodes (6): factory/feeds.yaml, Reddit subreddit sources, RSS feed sources, YCombinator Hacker News API sources, tests/sources-1.yaml, tests/sources-2.yaml

### Community 58 - "Text Cleanup Utils"
Cohesion: 0.6
Nodes (3): cleanup_markdown(), remove_after(), remove_before()

### Community 59 - "Machine Ops Runner"
Cohesion: 0.7
Nodes (4): run(), shutdown_az(), shutdown_td(), start_td()

## Ambiguous Edges - Review These
- `Factory Setup and Cache Seeding` → `zvec Classification Cache`  [AMBIGUOUS]
  factory/setup.py · relation: conceptually_related_to
- `Factory Setup and Cache Seeding` → `PostgreSQL State Cache`  [AMBIGUOUS]
  factory/setup.py · relation: conceptually_related_to
- `Grafeo Cupboard` → `Digest NLP Schema`  [AMBIGUOUS]
  pycupboard/grafeocupboard.py · relation: conceptually_related_to
- `EMBEDDER worker mode` → `localindexer (legacy INDEXER)`  [AMBIGUOUS]
  README.md · relation: semantically_similar_to
- `Bean` → `SAME_AS graph edge`  [AMBIGUOUS]
  history.txt · relation: semantically_similar_to
- `Publisher` → `PUBLISHED graph edge`  [AMBIGUOUS]
  history.txt · relation: semantically_similar_to

## Knowledge Gaps
- **176 isolated node(s):** `Seed cache with classification embeddings`, `Read a Parquet file, split it into chunks of `chunk_size` rows and     write eac`, `Migrate ClassificationCache from PostgreSQL to Firebird/zvec.      Args:`, `Hydrates local processing cache with beans and publishers from production/backup`, `Consolidates events and data to create consolidated briefings and signals` (+171 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **25 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `Factory Setup and Cache Seeding` and `zvec Classification Cache`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Factory Setup and Cache Seeding` and `PostgreSQL State Cache`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Grafeo Cupboard` and `Digest NLP Schema`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `EMBEDDER worker mode` and `localindexer (legacy INDEXER)`?**
  _Edge tagged AMBIGUOUS (relation: semantically_similar_to) - confidence is low._
- **What is the exact relationship between `Bean` and `SAME_AS graph edge`?**
  _Edge tagged AMBIGUOUS (relation: semantically_similar_to) - confidence is low._
- **What is the exact relationship between `Publisher` and `PUBLISHED graph edge`?**
  _Edge tagged AMBIGUOUS (relation: semantically_similar_to) - confidence is low._
- **Why does `run_porter()` connect `RSS Docker IO` to `Lance Cupboard`, `Rectify Migrations`?**
  _High betweenness centrality (0.099) - this node is a cross-community bridge._