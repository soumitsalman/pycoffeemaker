# State Machine
Coffeemaker workers store raw data in State Machine. It also pulls raw data from statemachine to determine what work needs to be done. Under the hood it is a giant data warehouse capable to store documents of different fields

Implementation note: the current backend is a local Turso-compatible SQLite file so the tables stay on disk, remain concurrency-friendly, and can be opened locally without changing the worker-facing API.

## Attributes
- MUST be fault tolerant and thus persist data across shutdown/reboot/system failure.
- MUST allow concurrent writes and reads.
- MUST retain at least a minimal set of information to indicate completion of work (ex: embedder worker from `analyzerorch.py` finished generating embedding for a set of `Bean`s uniquely identified by their `URL`s)
- MUST hold `dict[str, Any]` or BaseModel structured data where the type of the field value is not known ahead of runtime
- SHOULD live locally for faster IO for the workers.
- MAY NEED regular cleanup/pruning to avoid data bloating of local disk

## Structure
Statemachine has a one table per data type: bean for `Bean`, publisher for `Publisher` etc. The names are artibary and set at runtime as per the need of the application. For each table data is stored as follows
- state: str - the state value of the object ex: collected, embedded, scraped, classified etc.
- ts: datetime - the timestamp of when the state was achived
- data: dict[str, Any] - the data (fields,values) of the object
- id: Optional[str] - set if the data object has a unique identifier in its fields ex: `url` for Bean, `base_url` for Publisher, None for Chatter

Each worker stores the work performed in their own table. The unique identifier is the primary key/id equivalent for a data unit in that table. Presence of a url in the table indicates presense of the work done. Absence of the unique identifier in a table can potentially mean that work has not be done. 
Each worker can pull/read from 1 or more tables of other workers to determine the work it needs to do. Except: Collectors - collector workers collect data and simply store in the state machine.
The state machine prefers read, insert, delete OVER update. Updates are more expensive then bulk delete and insert. State machine avoids update to accommodate higher IO throughput and write concurrency. 

### Sample Read and Write Flow
| Worker | READs | STOREs |
| --- | --- | --- |
| collector_worker | READ data FROM rss_feed, reddit_api, hackernews_api | STORE contents, title, other_metadata INTO contents_table |
| embedder_worker |  READ content FROM contents_table | STORE embedding INTO embeddings_table |
| extractor_worker | READ content FROM contents_table | STORE people, places, organizations INTO entities_table |
| digestor_worker | READ content FROM contents_table | STORE key_points, data_points, highlights, gist INTO digests_table |
| cdn_worker | READ content FROM contents_table | STORE content_url INTO cdn_table |
| classifier_worker | READ embedding FROM embedded_table | STORE categories, sentiments, related INTO classifications_table |
| beansack_porter_worker | READ metadata, embedding, people, places, organizations, content_url FROM contents_table, embeddings_table, classifications_tabe, cdn_table | STORE bean INTO beansack_db |
| sips_porter_worker | READ title, other_metadata, embedding, people, places, organizations, content_url, key_points, data_points, gist FROM contents_table, embeddings_table, classifications_table, cdn_table, digests_table | STORE sips INTO sips_db |



