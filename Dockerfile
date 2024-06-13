FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    make \
    g++ \
    python3 \
    python3-pip \
    libboost-all-dev \
    libssl-dev \
    libffi-dev \
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /pycoffeemaker
COPY . .

RUN mkdir -p models
RUN wget -O models/nomic.gguf https://huggingface.co/nomic-ai/nomic-embed-text-v1.5-GGUF/resolve/main/nomic-embed-text-v1.5.Q8_0.gguf

RUN pip install -r requirements.txt

# Add a script or command to run by default (optional)
CMD ["python3", "app.py"]