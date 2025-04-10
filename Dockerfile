# Start Generation Here
FROM ubuntu:20.04

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

#### environment set up instructions ####
# 1. install build-essential & wget
# 2. install python3 and pip
# 2. install azcopy
# 3. install chrome browser
# 5. install playwright and playwright-deps
# 6. install crawl4ai
# 6. create .models, .db and .logs directories
# 7. download the models in .models
# 8. download beansack.db from azure blob storage to .db


RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    g++ \
    gcc \
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app
COPY . .

RUN pip install -r requirements.txt
RUN mkdir .db .models .logs

RUN wget https://huggingface.co/soumitsr/GIST-small-Embedding-v0-Q8_0-GGUF/resolve/main/gist-small-embedding-v0-q8_0.gguf -O .models/gist-small-embedding-v0-q8_0.gguf
RUN wget https://huggingface.co/soumitsr/SmolLM2-135M-Instruct-article-digestor-gguf/resolve/main/unsloth.Q8_0.gguf -O .models/SmolLM2-135M-Instruct-article-digestor-q8.gguf

ENV LOCAL_DB_PATH .db
ENV DB_NAME beansackV2
ENV EMBEDDER_PATH llama-cpp:///app/.models/gist-small-embeddingv-0-q8.gguf
ENV LLM_PATH llama-cpp:///app/.models/SmolLM2-135M-Instruct-article-digestor-q8.gguf
ENV LLM_N_CTX 4096
ENV CLUSTER_EPS 3.6

RUN playwright install-deps
RUN playwright install

# End Generation Here
CMD ["python", "/app/app.py"]
