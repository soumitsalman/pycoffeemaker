# Start Generation Here
FROM python:3.12-bookworm

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    g++ \
    gcc \
    wget \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /worker
COPY . .

# RUN rm -r ./coffeemaker/pybeansack
# RUN git clone https://github.com/soumitsalman/pybeansack.git ./coffeemaker/pybeansack
RUN pip install --no-cache-dir -r ./coffeemaker/pybeansack/requirements.txt
RUN pip install --no-cache-dir -r ./requirements-cpu.txt
RUN mkdir .models .db

RUN crawl4ai-setup
RUN crawl4ai-doctor

RUN transformers-cli download --cache-dir ./.models avsolatorio/GIST-small-Embedding-v0
RUN wget https://huggingface.co/soumitsr/SmolLM2-135M-Instruct-article-digestor-gguf/resolve/main/unsloth.Q8_0.gguf -O ./.models/SmolLM2-135M-Instruct-article-digestor-q8.gguf

ENV DB_LOCAL=.db

ENV COLLECTOR_SOURCES=./coffeemaker/collectors/feeds.yaml

ENV COLLECTOR_OUT_QUEUES=indexing-queue,digesting-queue
ENV INDEXER_IN_QUEUE=indexing-queue
ENV DIGESTOR_IN_QUEUE=digesting-queue

ENV WORDS_THRESHOLD_FOR_SCRAPING=200
ENV WORDS_THRESHOLD_FOR_INDEXING=70
ENV WORDS_THRESHOLD_FOR_DIGESTING=150

ENV COLLECTOR_BATCH_SIZE=64
ENV INDEXER_BATCH_SIZE=32
ENV DIGESTOR_BATCH_SIZE=32

ENV HF_HOME=.models
ENV HF_HUB_CACHE=.models
ENV HF_ASSETS_CACHE=.models

ENV EMBEDDER_PATH=avsolatorio/GIST-small-Embedding-v0
ENV EMBEDDER_CONTEXT_LEN=512
ENV CLUSTER_EPS=0.09

ENV DIGESTOR_CONTEXT_LEN=8192

CMD ["python", "./app.py"]
