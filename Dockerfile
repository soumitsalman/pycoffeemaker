# Build stage
FROM python:3.12-slim-bookworm AS builder

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV HF_HOME=/opt/models
ENV HF_HUB_CACHE=/opt/models
ENV SENTENCE_TRANSFORMERS_HOME=/opt/models

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set up virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy only requirements first to leverage Docker cache
COPY ./coffeemaker/pybeansack/requirements.txt ./pybeansack-requirements.txt
COPY ./coffeemaker/nlp/src/requirements.txt ./nlp-requirements.txt
COPY ./requirements.txt ./requirements.txt

# Install dependencies and download models
RUN pip install --no-cache-dir torch==2.6.0+cpu --index-url https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir intel_extension_for_pytorch
RUN pip install --no-cache-dir -r pybeansack-requirements.txt
RUN pip install --no-cache-dir -r nlp-requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p /opt/models
RUN huggingface-cli download --cache-dir /opt/models avsolatorio/GIST-small-Embedding-v0
RUN huggingface-cli download --cache-dir /opt/models soumitsr/led-base-article-digestor

# Final stage
FROM python:3.12-slim-bookworm

# Copy virtual environment and models from builder
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /opt/models /worker/.models

ENV PATH="/opt/venv/bin:$PATH"

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HF_HOME=/worker/.models \
    HF_HUB_CACHE=/worker/.models \
    SENTENCE_TRANSFORMERS_HOME=/worker/.models \
    WORDS_THRESHOLD_FOR_SCRAPING=150 \
    WORDS_THRESHOLD_FOR_INDEXING=150 \
    WORDS_THRESHOLD_FOR_DIGESTING=150 \
    EMBEDDER_PATH=avsolatorio/GIST-small-Embedding-v0 \
    EMBEDDER_CONTEXT_LEN=512 \
    MAX_RELATED_EPS=0.45 \
    DIGESTOR_PATH=soumitsr/led-base-article-digestor \
    DIGESTOR_CONTEXT_LEN=4096

# Set the working directory
WORKDIR /worker

# Copy only necessary files
COPY . .

CMD ["python", "./run.py"]