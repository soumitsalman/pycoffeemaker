# Start Generation Here
FROM python:3.12-bookworm

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Set the working directory
WORKDIR /worker
COPY . .

RUN pip install --no-cache-dir torch==2.6.0+cpu --index-url https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir -r ./coffeemaker/pybeansack/requirements.txt
RUN pip install --no-cache-dir -r ./requirements-cpu.txt
# RUN crawl4ai-setup

ENV DB_LOCAL=.db
ENV COLLECTOR_SOURCES=/worker/coffeemaker/collectors/feeds.yaml
ENV COLLECTOR_OUT_QUEUES=indexing-queue,digesting-queue
ENV INDEXER_IN_QUEUE=indexing-queue
ENV DIGESTOR_IN_QUEUE=digesting-queue
ENV WORDS_THRESHOLD_FOR_SCRAPING=200
ENV WORDS_THRESHOLD_FOR_INDEXING=70
ENV WORDS_THRESHOLD_FOR_DIGESTING=150
ENV COLLECTOR_BATCH_SIZE=64
ENV INDEXER_BATCH_SIZE=32
ENV DIGESTOR_BATCH_SIZE=32
ENV EMBEDDER_PATH=/worker/models/GIST-small-Embedding-v0
ENV EMBEDDER_CONTEXT_LEN=512
ENV CLUSTER_EPS=0.09
ENV DIGESTOR_CONTEXT_LEN=8192

CMD ["python", "./run.py"]
