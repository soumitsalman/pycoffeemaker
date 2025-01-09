# Start Generation Here
FROM ubuntu:20.04

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
RUN mkdir .db .models

# download models and beansack.db

# End Generation Here
CMD ["python", "/app/app.py"]