# Graph Report - pycoffeemaker  (2026-06-09)

## Corpus Check
- 67 files · ~79,565 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1553 nodes · 2485 edges · 101 communities (78 shown, 23 thin omitted)
- Extraction: 93% EXTRACTED · 7% INFERRED · 0% AMBIGUOUS · INFERRED: 175 edges (avg confidence: 0.76)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `3e20f91d`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]
- [[_COMMUNITY_Community 8|Community 8]]
- [[_COMMUNITY_Community 9|Community 9]]
- [[_COMMUNITY_Community 10|Community 10]]
- [[_COMMUNITY_Community 11|Community 11]]
- [[_COMMUNITY_Community 12|Community 12]]
- [[_COMMUNITY_Community 13|Community 13]]
- [[_COMMUNITY_Community 14|Community 14]]
- [[_COMMUNITY_Community 15|Community 15]]
- [[_COMMUNITY_Community 16|Community 16]]
- [[_COMMUNITY_Community 17|Community 17]]
- [[_COMMUNITY_Community 18|Community 18]]
- [[_COMMUNITY_Community 19|Community 19]]
- [[_COMMUNITY_Community 20|Community 20]]
- [[_COMMUNITY_Community 21|Community 21]]
- [[_COMMUNITY_Community 22|Community 22]]
- [[_COMMUNITY_Community 23|Community 23]]
- [[_COMMUNITY_Community 24|Community 24]]
- [[_COMMUNITY_Community 25|Community 25]]
- [[_COMMUNITY_Community 26|Community 26]]
- [[_COMMUNITY_Community 27|Community 27]]
- [[_COMMUNITY_Community 28|Community 28]]
- [[_COMMUNITY_Community 29|Community 29]]
- [[_COMMUNITY_Community 30|Community 30]]
- [[_COMMUNITY_Community 31|Community 31]]
- [[_COMMUNITY_Community 32|Community 32]]
- [[_COMMUNITY_Community 33|Community 33]]
- [[_COMMUNITY_Community 34|Community 34]]
- [[_COMMUNITY_Community 35|Community 35]]
- [[_COMMUNITY_Community 36|Community 36]]
- [[_COMMUNITY_Community 37|Community 37]]
- [[_COMMUNITY_Community 38|Community 38]]
- [[_COMMUNITY_Community 39|Community 39]]
- [[_COMMUNITY_Community 40|Community 40]]
- [[_COMMUNITY_Community 41|Community 41]]
- [[_COMMUNITY_Community 42|Community 42]]
- [[_COMMUNITY_Community 43|Community 43]]
- [[_COMMUNITY_Community 44|Community 44]]
- [[_COMMUNITY_Community 45|Community 45]]
- [[_COMMUNITY_Community 46|Community 46]]
- [[_COMMUNITY_Community 47|Community 47]]
- [[_COMMUNITY_Community 48|Community 48]]
- [[_COMMUNITY_Community 49|Community 49]]
- [[_COMMUNITY_Community 50|Community 50]]
- [[_COMMUNITY_Community 51|Community 51]]
- [[_COMMUNITY_Community 52|Community 52]]
- [[_COMMUNITY_Community 53|Community 53]]
- [[_COMMUNITY_Community 54|Community 54]]
- [[_COMMUNITY_Community 55|Community 55]]
- [[_COMMUNITY_Community 56|Community 56]]
- [[_COMMUNITY_Community 57|Community 57]]
- [[_COMMUNITY_Community 58|Community 58]]
- [[_COMMUNITY_Community 59|Community 59]]
- [[_COMMUNITY_Community 60|Community 60]]
- [[_COMMUNITY_Community 61|Community 61]]
- [[_COMMUNITY_Community 62|Community 62]]
- [[_COMMUNITY_Community 63|Community 63]]
- [[_COMMUNITY_Community 64|Community 64]]
- [[_COMMUNITY_Community 65|Community 65]]
- [[_COMMUNITY_Community 66|Community 66]]
- [[_COMMUNITY_Community 67|Community 67]]
- [[_COMMUNITY_Community 68|Community 68]]
- [[_COMMUNITY_Community 69|Community 69]]
- [[_COMMUNITY_Community 70|Community 70]]
- [[_COMMUNITY_Community 71|Community 71]]
- [[_COMMUNITY_Community 72|Community 72]]
- [[_COMMUNITY_Community 73|Community 73]]
- [[_COMMUNITY_Community 74|Community 74]]
- [[_COMMUNITY_Community 75|Community 75]]
- [[_COMMUNITY_Community 76|Community 76]]
- [[_COMMUNITY_Community 77|Community 77]]
- [[_COMMUNITY_Community 78|Community 78]]
- [[_COMMUNITY_Community 79|Community 79]]
- [[_COMMUNITY_Community 80|Community 80]]
- [[_COMMUNITY_Community 81|Community 81]]
- [[_COMMUNITY_Community 82|Community 82]]
- [[_COMMUNITY_Community 83|Community 83]]
- [[_COMMUNITY_Community 84|Community 84]]
- [[_COMMUNITY_Community 85|Community 85]]
- [[_COMMUNITY_Community 86|Community 86]]
- [[_COMMUNITY_Community 87|Community 87]]
- [[_COMMUNITY_Community 89|Community 89]]
- [[_COMMUNITY_Community 90|Community 90]]
- [[_COMMUNITY_Community 91|Community 91]]
- [[_COMMUNITY_Community 95|Community 95]]
- [[_COMMUNITY_Community 96|Community 96]]
- [[_COMMUNITY_Community 97|Community 97]]
- [[_COMMUNITY_Community 98|Community 98]]
- [[_COMMUNITY_Community 99|Community 99]]
- [[_COMMUNITY_Community 100|Community 100]]

## God Nodes (most connected - your core abstractions)
1. `MongoDB` - 54 edges
2. `DuckDB` - 39 edges
3. `DuckSack` - 38 edges
4. `PGSack` - 33 edges
5. `LanceSack` - 28 edges
6. `Collector` - 20 edges
7. `EmbedderBase` - 18 edges
8. `Digest` - 17 edges
9. `AsyncWebScraper` - 16 edges
10. `_build_rss_item()` - 15 edges

## Surprising Connections (you probably didn't know these)
- `localindexer (legacy INDEXER)` --semantically_similar_to--> `EMBEDDER worker mode`  [AMBIGUOUS] [semantically similar]
  docker-compose.yaml → README.md
- `SAME_AS graph edge` --semantically_similar_to--> `Bean`  [AMBIGUOUS] [semantically similar]
  history.txt → README.md
- `PUBLISHED graph edge` --semantically_similar_to--> `Publisher`  [AMBIGUOUS] [semantically similar]
  history.txt → README.md
- `db_instance()` --calls--> `LanceDB`  [INFERRED]
  factory/migrate.py → pybeansack/README.md
- `db_instance()` --calls--> `DuckLake`  [INFERRED]
  factory/migrate.py → pybeansack/README.md

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

## Communities (101 total, 23 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.06
Nodes (22): Config, LanceDBCupboard, # NOTE: this is deprecated. IGNORE, # NOTE: something wrong with the vector index creation, Generated article stored in cupboard, _Sip, _where(), Sip (+14 more)

### Community 1 - "Community 1"
Cohesion: 0.05
Nodes (48): AINewsDigest, Briefing, cleanup_digest_fields(), cleanup_names(), CyberNewsDigest, Digest, EarningsReportSummary, FinancialCoreMetrics (+40 more)

### Community 2 - "Community 2"
Cohesion: 0.07
Nodes (20): ABC, DiffuserImageGenerationAgent, image_agent_from_path(), LlamaCppTextGeneratorClient, LMAgentBase, LMClientBase, LocalTokenizer, ONNXText2TextClient (+12 more)

### Community 3 - "Community 3"
Cohesion: 0.07
Nodes (25): _conflict_target(), create_db(), cursor(), _insert_multivalues_sql(), _limit(), PGSack, _primary_key_fields(), _query_composites() (+17 more)

### Community 4 - "Community 4"
Cohesion: 0.07
Nodes (14): _build_tool_schema(), DigestorBase, from_path(), LocalTokenizer, NamedEntityExtractor, OpenAIDigestor, ORTDigestor, OVDigestor (+6 more)

### Community 5 - "Community 5"
Cohesion: 0.06
Nodes (16): Beansack, LanceModel, _Bean, _Chatter, _connect(), create_db(), LanceSack, _Publisher (+8 more)

### Community 6 - "Community 6"
Cohesion: 0.07
Nodes (22): create_composer_topics_locally(), create_classification_cache(), create_classification_embeddings(), create_classification_files(), create_processing_cache(), Seed cache with classification embeddings, StateCacheBase, ClassificationCache (+14 more)

### Community 7 - "Community 7"
Cohesion: 0.07
Nodes (23): ClassificationCacheBase, AsyncStateCache, batch_search(), ClassificationCache, _copy_insert_state_rows(), _copy_insert_state_rows_async(), _create_emb_tables_sql(), _create_multi_state_query_expr() (+15 more)

### Community 8 - "Community 8"
Cohesion: 0.08
Nodes (7): _beans_to_df(), create_db(), DuckDB, _publishers_to_df(), # TODO: merge this with ducklakesack since the logic is mostly the same, just di, _select(), _where()

### Community 9 - "Community 9"
Cohesion: 0.1
Nodes (8): _beans_to_df(), create_db(), DuckSack, execute(), _execute_df(), _publishers_to_df(), _select(), _where()

### Community 10 - "Community 10"
Cohesion: 0.08
Nodes (11): create_micro_agent(), EntityExtractor, LocalTokenizer, MicroAgentBase, parse_compressed(), parse_markdown(), RemoteMicroAgent, _run_single() (+3 more)

### Community 11 - "Community 11"
Cohesion: 0.09
Nodes (13): AsyncStateCache, ClassificationCache, _cleanup_sql(), _create_id_exists_expr(), _create_multi_state_query_expr(), _create_single_state_query_expr(), _create_state_query_expr(), _create_state_tables_sql() (+5 more)

### Community 12 - "Community 12"
Cohesion: 0.1
Nodes (36): AsyncCDNStore, CDNStore, Beansack, DuckDB, duckdbsack.create_db, DuckSack, ducklakesack.create_db, DuckLake warehouse attach (+28 more)

### Community 13 - "Community 13"
Cohesion: 0.06
Nodes (31): Capabilities, Cloud GPU ops, code:block1 (pycoffeemaker/), code:bash (python -m venv .venv && source .venv/bin/activate), code:bash (# Collector), code:bash (docker build -f DockerfileGPU -t coffeemaker:gpu .), code:bash (docker run --gpus all --env-file .env \), code:bash (docker compose up pgcache localmongo localpostgres   # infra) (+23 more)

### Community 14 - "Community 14"
Cohesion: 0.07
Nodes (29): API summary, APPENDIX: Content Generation Models Evaluation, Article & Content Generation, Backend selection, Batching, code:block1 (nlp/), code:bash (pip install -r nlp/requirements.txt), code:python (from nlp import create_embedder) (+21 more)

### Community 15 - "Community 15"
Cohesion: 0.07
Nodes (28): `APICollector` / `APICollectorAsync`, `AsyncWebScraper`, Chatter fields (when present), Choosing a scraper, code:python ({), code:python ({), code:python ({), code:python ({) (+20 more)

### Community 16 - "Community 16"
Cohesion: 0.13
Nodes (8): AsyncStateCache, _create_multi_state_query_expr(), create_rows(), _create_single_state_query_expr(), _create_state_query_expr(), _create_state_tables_sql(), _rectify_path(), StateCache

### Community 17 - "Community 17"
Cohesion: 0.1
Nodes (9): create_embedder(), InfinityEmbeddings, LlamaCppEmbeddings, OVEmbeddings, # NOTE: moving the import inside the function so that there is no need to instal, RemoteEmbeddings, TransformerEmbeddings, test_orch_on_lancesack() (+1 more)

### Community 18 - "Community 18"
Cohesion: 0.16
Nodes (26): Bean Processing Pipeline, Beansack Porter, Classifier and Clusterer Worker, Collector Orchestrator, Consolidator Worker, Cupboard Porter, Digestor Worker, Embedder Worker (+18 more)

### Community 20 - "Community 20"
Cohesion: 0.14
Nodes (7): Collector, parse_sources(), Store storable collection results and persist the rest for scraping., Store storable collection results and persist the rest for scraping., validate_bean_item(), validate_chatter_item(), validate_source_item()

### Community 21 - "Community 21"
Cohesion: 0.15
Nodes (11): AsyncStateMachine, create_data_to_store(), create_exists_query_expr(), _create_multi_state_query_expr(), create_optimize_expr(), create_query_expr(), _create_single_state_query_expr(), create_table_expr() (+3 more)

### Community 22 - "Community 22"
Cohesion: 0.17
Nodes (19): APICollectorAsync, AsyncWebScraper, _analyzer_cls_cache(), _analyzer_test_cache(), create_test_data_file(), hydrate_test_db(), main(), save_models() (+11 more)

### Community 24 - "Community 24"
Cohesion: 0.16
Nodes (10): AsyncCDNStore, CDNStore, _guess_type(), _public_url(), Uploads a single binary file.          Parameters:             path: should be i, Uploads multiple text items concurrently.          Parameters:             data:, Creates a public access URL based on template. Ex: https://{bucket}.t3.tigrisfil, Uploads a single text file.          Parameters:             path: should be in (+2 more)

### Community 25 - "Community 25"
Cohesion: 0.16
Nodes (17): generate_fake_beans(), generate_fake_chatters(), generate_fake_digests(), generate_fake_embeddings(), generate_fake_mugs(), generate_fake_publishers(), generate_fake_sips(), Test warehouse maintenance tasks (+9 more)

### Community 26 - "Community 26"
Cohesion: 0.16
Nodes (5): Cupboard, Insert a batch while skipping records that already exist., Link inserted events to matching sources via `source['base_url']->PUBLISHED-> ev, Link matching events to inserted sources via `source->PUBLISHED->event['base_url, transaction()

### Community 27 - "Community 27"
Cohesion: 0.14
Nodes (6): AsyncStateCacheBase, StateCacheBase, AsyncStateCache, _create_state_tables_sql(), Initialize the ProcessingCache with a PostgreSQL connection string and table set, StateCache

### Community 28 - "Community 28"
Cohesion: 0.18
Nodes (8): Collects the body of the url as a markdown, Collects the bodies of the urls as markdowns, Collects the bodies of the beans as markdowns, Collects the body of the url as a markdown, Collects the bodies of the urls as markdowns, Collects the bodies of the beans as markdowns, WebCrawler, strip_html_tags()

### Community 29 - "Community 29"
Cohesion: 0.23
Nodes (15): _add_ts_and_ids_expr(), _copy_insert_state_rows(), _copy_insert_state_rows_async(), _create_multi_state_query_expr(), create_query_expr(), _create_single_state_query_expr(), deduplicate(), deserialize_data_rows() (+7 more)

### Community 30 - "Community 30"
Cohesion: 0.16
Nodes (16): MicroAgentBase, RemoteMicroAgent OpenAI, TransformerMicroAgent, VLLMMicroAgent, create_micro_agent factory, parse_compressed digest parser, parse_markdown digest parser, LMAgentBase LM clients (deprecated) (+8 more)

### Community 31 - "Community 31"
Cohesion: 0.16
Nodes (11): Prepare result for page scraping (bean and publisher)., Prepare result for page scraping (bean and publisher)., Scrape a single URL for both bean and publisher data., Scrape a single URL for both bean and publisher data., Scrape multiple URLs in parallel for bean and publisher data., Scrape multiple URLs in parallel for bean and publisher data., Augment existing beans with scraped data., Augment existing beans with scraped data. (+3 more)

### Community 32 - "Community 32"
Cohesion: 0.14
Nodes (14): Coffeemaker, RSS feedparser field reference, DockerfileGPU, DockerfileIO, Espresso, entry.content, entry.title, create_micro_agent (+6 more)

### Community 33 - "Community 33"
Cohesion: 0.16
Nodes (6): APICollectorAsync, AsyncWebScraper, Async context manager enter, Async context manager exit, excluded_url(), run()

### Community 34 - "Community 34"
Cohesion: 0.19
Nodes (4): EmbedderBase, Takes a list of strings/large documents as input and chunks them into smaller pi, Embeds a single string as a query. It prepends `query: ` to the input., This takes a string or an list of strings as an input.         This calls the em

### Community 35 - "Community 35"
Cohesion: 0.21
Nodes (9): Digest, Metadata, parse_compressed(), parse_json(), parse_markdown(), valid_names(), valid_regions(), valid_stock_tickers() (+1 more)

### Community 36 - "Community 36"
Cohesion: 0.16
Nodes (12): db_instance(), migrate(), code:python (db = create_client("lance", lancedb_storage="/path/to/lanced), code:python (db = create_client("mongodb", mongodb_uri="mongodb://localho), code:python (db = create_client("postgres", pg_connection_string="postgre), code:python (db = create_client("duck", duckdb_storage="/path/to/beansack), Database Backends, DuckDB (+4 more)

### Community 37 - "Community 37"
Cohesion: 0.18
Nodes (9): _fix_publisher_ids(), _Publisher, # TODO: split out function for adding embeddings and gists. code commented out b, # TODO: add function for recompute (for clusters, categories, sentiments and tre, # TODO: add a recompute and cleanup function, # TODO: add delete for bookmarked bean, # NOTE: remove anything collected 7 days ago that did not get processed by analy, # TODO: this is a temporary fix. (+1 more)

### Community 38 - "Community 38"
Cohesion: 0.19
Nodes (3): _prepare_to_store(), Opens connection to existing or new simple database on `db_path`.         Create, SimpleVectorDB

### Community 39 - "Community 39"
Cohesion: 0.19
Nodes (10): AggregatedBean, Bean, Config, Publisher, Metadata of the website, publication or social medium from which an article or c, Metadata of an article such as a news or blog post., # TODO: add entities and region down the road, Bean with additional fields for tracking social media engagement and propagation (+2 more)

### Community 40 - "Community 40"
Cohesion: 0.15
Nodes (3): ORTEmbeddings, VLLMEmbedder, clear_gpu_cache()

### Community 41 - "Community 41"
Cohesion: 0.23
Nodes (11): _build_hackernews_item(), _build_reddit_item(), Scrape multiple sites for publisher data, deduplicating by base_url., Scrape multiple sites for publisher data, deduplicating by base_url., Scrape a single publisher for publisher data., cleanup_item(), extract_base_url(), guess_article_type() (+3 more)

### Community 42 - "Community 42"
Cohesion: 0.15
Nodes (12): code:python (new_beans = [...]  # Your beans to store), code:python (beans_to_update = [...]  # Beans with new metadata), code:python (beans_with_embeddings = [...]  # Beans with new embedding ve), code:python (categories = db.distinct_categories(limit=100)), code:python (total_beans = db.count_rows("beans")), Common Patterns, Count Records, Deduplicate Before Storing (+4 more)

### Community 43 - "Community 43"
Cohesion: 0.2
Nodes (11): cleanup_bean_tags(), hydrate_processing_cache(), migrate_classification_cache(), migrate_classification_cache_pg_to_fire(), migrate_users(), Migrate ClassificationCache from PostgreSQL to Firebird/zvec.      Args:, Hydrates local processing cache with beans and publishers from production/backup, Read a Parquet file, split it into chunks of `chunk_size` rows and     write eac (+3 more)

### Community 45 - "Community 45"
Cohesion: 0.24
Nodes (4): APICollector, _batch_run(), _get_site_url(), _return_collected()

### Community 46 - "Community 46"
Cohesion: 0.17
Nodes (9): Scrape a single site for publisher data., Scrape a single site for publisher data., Scrape a site for publisher data., Augment existing publishers with scraped data., Scrape a site for publisher data., Augment existing publishers with scraped data., _scrape_favicon(), extract_domain() (+1 more)

### Community 47 - "Community 47"
Cohesion: 0.17
Nodes (11): Bean state flow, code:block1 (collected), Concepts, `deduplicate(object_type, state, items)`, `get(object_type, states, exclude_states=..., ids=None, window=..., limit=0, offset=0)`, How to use the API, `optimize(cleanup_older_than=...)`, Related docs (+3 more)

### Community 48 - "Community 48"
Cohesion: 0.2
Nodes (5): _create_rows(), _merge_state_sql(), encode_data(), _create_rows(), set()

### Community 49 - "Community 49"
Cohesion: 0.2
Nodes (4): _beans_query_pipeline(), _beans_text_search_pipeline(), _deserialize_beans(), _related_beans_pipeline()

### Community 50 - "Community 50"
Cohesion: 0.18
Nodes (11): Test storing Chatter data in warehouse, Test storing Source data in warehouse, Test storing BeanCore data in warehouse, Test storing BeanEmbedding data in warehouse, Test storing BeanGist data in warehouse, _run_test_func(), test_store_chatters(), test_store_cores() (+3 more)

### Community 51 - "Community 51"
Cohesion: 0.33
Nodes (9): _build_rss_item(), _extract_author_email(), _extract_body(), _extract_feed_metadata(), _extract_language(), _extract_link(), _extract_main_image(), _extract_tags() (+1 more)

### Community 52 - "Community 52"
Cohesion: 0.18
Nodes (11): code:python (from pybeansack import create_client), code:python (from pybeansack.models import Bean), code:python (from datetime import datetime, timedelta), code:python (from pybeansack.models import Chatter), code:python (from pybeansack.models import Publisher), Initialize a Database Client, Query Beans, Quick Start (+3 more)

### Community 53 - "Community 53"
Cohesion: 0.22
Nodes (11): Topic classification taxonomy, CLASSIFICATION_CACHE, factory/classifications.yaml, create_embedder, EntityExtractor, Sentiment labels, test_embedder_orch(), CLASSIFIER worker mode (+3 more)

### Community 54 - "Community 54"
Cohesion: 0.2
Nodes (11): Bean, Chatter, Composite, PUBLISHED graph edge, SAME_AS graph edge, SurrealDB graph query log, Publisher, create_client (+3 more)

### Community 55 - "Community 55"
Cohesion: 0.25
Nodes (10): Mug, Sip, extract_sips_from_content(), extract_tldr_highlight(), import_espresso_rss(), parse_rss_to_mugs_and_sips(), Main function to fetch RSS and save JSON., Extract the TLDR section from content. (+2 more)

### Community 56 - "Community 56"
Cohesion: 0.24
Nodes (5): Project-wide utilities (logging, dates)., log_runtime(), log_runtime_async(), structlog logfmt logging: file when LOG_DIR set, else stderr., _runtime_decorator()

### Community 57 - "Community 57"
Cohesion: 0.38
Nodes (9): load_json(), save_image(), save_json(), save_markdown(), test_digestor(), test_embedder(), test_extractor(), to_filename() (+1 more)

### Community 58 - "Community 58"
Cohesion: 0.29
Nodes (10): APICollector sync, APICollectorAsync, RSS feed item builder, Collector Bean Field Constants, datacollectors public exports, should_reject_input content filter, AsyncWebScraper readability, WebCrawler crawl4ai (+2 more)

### Community 59 - "Community 59"
Cohesion: 0.22
Nodes (6): _clean_markdown(), Remove any content before the first line starting with '# '., # TODO: add a check to remove "advertisement", Remove any content before the first line starting with '# '., # TODO: add a check to remove "advertisement", parse_int()

### Community 60 - "Community 60"
Cohesion: 0.2
Nodes (9): Cafecito naming, code:block1 (pycoffeemaker/), Coffeemaker, Data units, graphify, Other components, Repository layout, State machine (`workers/workercache/`) (+1 more)

### Community 61 - "Community 61"
Cohesion: 0.22
Nodes (8): PROCESSING_CACHE, Bean processing states, run_porter(), Insert/delete over update, Fault-tolerant state machine, test_collector_orch(), Worker state cache (state machine), Collector orchestrator

### Community 62 - "Community 62"
Cohesion: 0.25
Nodes (4): Chatter, Social media engagement stats of an article/bean (specified by `url`)., _deserialize_chatters(), Retrieves the latest social media status from different mediums.

### Community 63 - "Community 63"
Cohesion: 0.36
Nodes (8): LanceDB Cupboard (deprecated), Sip Pydantic Model, Source Pydantic Model, URL UUID5 ID Generator, PostgreSQL Async Cupboard, pgvector sips sources relations schema, Collect normalize embed store pipeline, Pluggable vector cupboard backends

### Community 65 - "Community 65"
Cohesion: 0.52
Nodes (6): call_to_action_density(), compression_ratio(), narrative_density(), repeated_phrases(), should_reject_input(), surface_signature_trigger()

### Community 66 - "Community 66"
Cohesion: 0.29
Nodes (7): code:block20 (pydantic), code:bash (# Set environment variables), code:bash (docker-compose up -d), Development, Docker Setup, Requirements, Running Tests

### Community 67 - "Community 67"
Cohesion: 0.29
Nodes (6): Architecture, code:bash (pip install pybeansack), Features, Installation, License, PYBEANSACK

### Community 68 - "Community 68"
Cohesion: 0.38
Nodes (7): Beansack, Cupboard, pycupboard/requirements.txt, Asynchronous unaware orchestrators, test_porter_orch(), PORTER worker mode, Porter orchestrator

### Community 69 - "Community 69"
Cohesion: 0.29
Nodes (6): azurite service, localindexer (legacy INDEXER), localcrawler (crawl4ai), localmongo service, pgcache service, pybeansack/docker-compose.yml

### Community 71 - "Community 71"
Cohesion: 0.33
Nodes (6): EntityExtractor GLiNER, EmbedderBase, Briefing Intelligence Model, Digest NLP Schema, Domain-Specific Digest Models, run_batch()

### Community 72 - "Community 72"
Cohesion: 0.4
Nodes (6): Grafeo Cupboard, GrafeoDB Backend, Events Signals Reports Graph Types, SurrealDB Async Cupboard, PUBLISHED source-event edge, SAME_AS event link edge

### Community 73 - "Community 73"
Cohesion: 0.33
Nodes (5): Article/Entry-Level Fields, code:python (feed = feedparser.parse(rss_url)), Example: Accessing Fields, Feed-Level Fields, RSS Feed Field Reference (feedparser)

### Community 74 - "Community 74"
Cohesion: 0.47
Nodes (6): factory/feeds.yaml, Reddit subreddit sources, RSS feed sources, YCombinator Hacker News API sources, tests/sources-1.yaml, tests/sources-2.yaml

### Community 76 - "Community 76"
Cohesion: 0.6
Nodes (3): cleanup_markdown(), remove_after(), remove_before()

### Community 77 - "Community 77"
Cohesion: 0.4
Nodes (5): Advanced Usage, Batch Processing, code:python (from pybeansack.mongosack import MongoDB), code:python (from concurrent.futures import ThreadPoolExecutor), Using Custom Database Functions

### Community 78 - "Community 78"
Cohesion: 0.4
Nodes (5): AggregatedBean, Bean (Article), Chatter (Social Media), Data Models, Publisher

### Community 79 - "Community 79"
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
- **305 isolated node(s):** `Seed cache with classification embeddings`, `Read a Parquet file, split it into chunks of `chunk_size` rows and     write eac`, `Migrate ClassificationCache from PostgreSQL to Firebird/zvec.      Args:`, `Hydrates local processing cache with beans and publishers from production/backup`, `Consolidates events and data to create consolidated briefings and signals` (+300 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **23 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

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
- **Why does `DuckDB` connect `Community 8` to `Community 43`, `Community 36`, `Community 5`?**
  _High betweenness centrality (0.112) - this node is a cross-community bridge._