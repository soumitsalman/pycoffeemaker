# Coffeemaker
Coffeemaker is the main backend processing engine for Project Cafecito. Coffeemaker collects/scrape data from web and processes them to extract various relational metadata. All of these processed information then goes to different databases for different Project Cafecito services to pick up. Coffeemaker is a combination of 
- multiple data collectors
- multiple data processors & indexers
- a raw data warehouse where every worker dumps their output.  

## Coffeemaker Design
Coffeemaker uses different data collection libraries and language models for its operators

### Orchestrators/Workers/Operators
`coffeemaker/orchestrators/`
- `collectororch.py` collects news, blogs, posts, reports, pdfs, csvs etc. from different sources. Extracts common set of fields - title, content, metadata, comments, likes, shares. Scrapes pages and publishers. This is an IO heavy workload
- `analyzerorch.py` processes collected news to create vector embeddings, extract named entities, key events, data points, atomic facts, create summaries, digests, analyze topic categories, sentiments, clustering. This is a compute heavy workload and often requires GPU for LLM operations
- `porterorch.py` takes processed contents and ships fully crafted data to different databases for different services, ex: Beansack - for Cafecito Beans, Sips for Cafecito Espresso, Cupboard for Cafecito Cortado etc. This is an IO heavy workload.

Each workers/operators (called orchestrators) that work asynchronously to do their part and are remain unaware if another worker is running at the same time. The main application runner `run.py` schedules these workers as needed (sometime in parallel and sometimes sequentially).

### Data Units
`pybeansack/models.py`
Bean, Chatter, Publisher: molecular units of information produced by coffeemaker. The atomic fields of each data unit is produced by different workers within coffeemaker.

### State Machine
`coffeemaker/orchestrators/STATEMACHINE.md` | `coffeemaker/orchestrators/statemachine.py`
Coffeemaker must be fault tolerant and needs to persist worker states to avoid deduplication of already performed work. The processing states and raw data are saved in a state machine (a flexible persistant DB).

## Other Major Components
language models and natural language processing: `nlp/src/`
Beansack DB and data models: `pybeansack/`
Data collectors and scrapers: `coffeemaker/collectors/`

---
HARD REQUIREMENT: EXTREMELY CONCISE TOKEN EFFICIENT RESPONSES ONLY