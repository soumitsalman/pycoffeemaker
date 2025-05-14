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
# RUN mkdir .logs

RUN pip install --no-cache-dir -r coffeemaker/pybeansack/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir .models .db

RUN crawl4ai-setup
RUN crawl4ai-doctor

RUN transformers-cli download --cache-dir /worker/.models avsolatorio/GIST-small-Embedding-v0
RUN wget https://huggingface.co/soumitsr/SmolLM2-135M-Instruct-article-digestor-gguf/resolve/main/unsloth.Q8_0.gguf -O .models/SmolLM2-135M-Instruct-article-digestor-q8.gguf

ENV HF_HOME=.models
ENV HF_HUB_CACHE=.models
ENV HF_ASSETS_CACHE=.models

ENV DB_LOCAL=.db

ENV COLLECTOR_SOURCES=/worker/coffeemaker/collectors/sources.yaml

ENV EMBEDDER_PATH=avsolatorio/GIST-small-Embedding-v0
ENV EMBEDDER_CONTEXT_LEN=512
ENV INDEXER_BATCH_SIZE=32
ENV CLUSTER_EPS=3.75

ENV DIGESTOR_BATCH_SIZE=32

CMD ["python", "/worker/app.py"]
