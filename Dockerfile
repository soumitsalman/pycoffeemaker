# everything here is designed to run on cpu
FROM cnstark/pytorch:2.3.1-py3.10.15-ubuntu22.04 AS builder

WORKDIR /worker

COPY coffeemaker/pybeansack/requirements.txt pybeansack-requirements.txt
COPY requirements.txt requirements.txt

RUN pip install --prefix=/pythonlibs intel_extension_for_pytorch
RUN pip install --prefix=/pythonlibs -r pybeansack-requirements.txt
RUN pip install --prefix=/pythonlibs -r requirements.txt

FROM cnstark/pytorch:2.3.1-py3.10.15-ubuntu22.04

WORKDIR /worker

COPY --from=builder /pythonlibs /usr/local
COPY . .

ENV HF_HOME=/worker/.models \
    HF_HUB_CACHE=/worker/.models \
    HF_ASSETS_CACHE=/worker/.models \
    SENTENCE_TRANSFORMERS_HOME=/worker/.models \
    # Collector stuff
    WORDS_THRESHOLD_FOR_SCRAPING=150 \
    COLLECTOR_SOURCES=/worker/factory/feeds.yaml \
    # Indexer stuff
    WORDS_THRESHOLD_FOR_INDEXING=150 \
    EMBEDDER_PATH=openvino:///worker/.models/gist-small-embedding-v0-openvino \
    EMBEDDER_CONTEXT_LEN=512 \
    MAX_RELATED_EPS=0.45 \
    MAX_ANALYZE_NDAYS=7 \
    # Digestor stuff
    WORDS_THRESHOLD_FOR_DIGESTING=150 \    
    DIGESTOR_PATH=soumitsr/led-base-article-digestor \
    DIGESTOR_CONTEXT_LEN=4096 \
    # Composer stuff
    MIN_CLUSTER_SIZE=16 \
    MAX_CLUSTER_SIZE=128 \
    COMPOSER_TOPICS=/worker/factory/composer-topics.json

CMD ["python", "run.py"]