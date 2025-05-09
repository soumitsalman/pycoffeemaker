FROM python:3.12-bookworm

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    g++ \
    gcc \
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /worker
COPY . .

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r coffeemaker/pybeansack/requirements.txt
RUN crawl4ai-setup
RUN crawl4ai-doctor

RUN mkdir .db .models .logs
RUN wget https://huggingface.co/soumitsr/GIST-small-Embedding-v0-Q8_0-GGUF/resolve/main/gist-small-embedding-v0-q8_0.gguf -O .models/gist-small-embedding-v0-q8_0.gguf
RUN wget https://huggingface.co/soumitsr/SmolLM2-135M-Instruct-article-digestor-gguf/resolve/main/unsloth.Q8_0.gguf -O .models/SmolLM2-135M-Instruct-article-digestor-q8.gguf

ENV DB_LOCAL .db
ENV DB_NAME beansackV2
ENV EMBEDDER_PATH llama-cpp:///app/.models/gist-small-embeddingv-0-q8.gguf
ENV LLM_PATH llama-cpp:///app/.models/SmolLM2-135M-Instruct-article-digestor-q8.gguf
ENV LLM_N_CTX 4096
ENV CLUSTER_EPS 3.6

# End Generation Here
CMD ["python", "/worker/app.py"]
