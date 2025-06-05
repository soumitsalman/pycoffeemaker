# Start Generation Here
FROM python:3.12-bookworm

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Set the working directory
WORKDIR /worker
COPY . .

RUN pip install --no-cache-dir torch==2.6.0+cpu --index-url https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir -r ./coffeemaker/pybeansack/requirements.txt
RUN pip install --no-cache-dir -r ./coffeemaker/nlp/src/requirements-min.txt
RUN pip install --no-cache-dir -r ./requirements.txt
RUN mkdir .models
RUN huggingface-cli download --cache-dir /worker/.models avsolatorio/GIST-small-Embedding-v0

ENV HF_HOME=/worker/.models
ENV HF_HUB_CACHE=/worker/.models
ENV SENTENCE_TRANSFORMERS_HOME=/worker/.models

ENV DB_LOCAL=.db
ENV WORDS_THRESHOLD_FOR_SCRAPING=150
ENV WORDS_THRESHOLD_FOR_INDEXING=150
ENV WORDS_THRESHOLD_FOR_DIGESTING=150

ENV EMBEDDER_PATH=avsolatorio/GIST-small-Embedding-v0
ENV EMBEDDER_CONTEXT_LEN=512
ENV MAX_RELATED_EPS=0.1
ENV DIGESTOR_CONTEXT_LEN=4096

CMD ["python", "./run.py"]
