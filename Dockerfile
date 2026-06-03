# Use a base image of Python (Alpine version for a smaller container size and minimal dependencies)
FROM python:3.10-slim

# Install required build tools and libraries for native dependencies and librdkafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates bash git \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip to the latest version
RUN pip install --no-cache-dir --upgrade pip

# Full rebuild bust: pass CACHE_BUST=<timestamp> to re-run pip install AND code clone.
# Used by:  make build-producer-scache
ARG CACHE_BUST=1

# Install dependencies from the build context (submodule checkout on disk).
# This layer is cached when using scache-nolib; re-run only when using scache.
COPY producer/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Code-only bust: pass CODE_BUST=<timestamp> to re-run only the git clones, keeping pip cached.
# Used by:  make build-producer-scache-nolib
ARG CODE_BUST=1

WORKDIR /app

RUN git clone --branch sereBench https://github.com/DIETI-DISTA-IoT/Train_IoT_data_producer.git .
RUN git clone --branch sereBench https://github.com/DIETI-DISTA-IoT/of-core OpenFAIR/

# Set environment variables for Kafka connection
ENV KAFKA_BROKER="kafka:9092"
ENV VEHICLE_NAME="e700_4801"
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 5000

CMD ["python", "produce.py"]
