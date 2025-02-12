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
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app
COPY . .

RUN pip install -r requirements.txt
RUN mkdir .db .models .logs
RUN playwright install-deps
RUN playwright install

# End Generation Here
CMD ["python", "/app/app.py"]
