# Use a base image of Python (Alpine version for a smaller container size and minimal dependencies)
FROM python:3.10-slim

# Install required build tools and libraries for native dependencies and librdkafka
# This includes compilers and development libraries to allow Python packages with C/C++ extensions to compile properly
# Install system dependencies. Using Debian slim to get many Python wheels (incl. confluent-kafka) without compiling.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates bash git \
    && rm -rf /var/lib/apt/lists/*


# Set workdir and copy current project producer code
WORKDIR /app
COPY . /app

# Set environment variables for Kafka connection
# KAFKA_BROKER: Address of the Kafka broker
# TOPIC_NAME: Kafka topic to which the synthetic data will be published
ENV KAFKA_BROKER="kafka:9092"
ENV VEHICLE_NAME="e700_4801"
ENV PYTHONUNBUFFERED=1

# Upgrade pip to the latest version
RUN pip install --no-cache-dir --upgrade pip

# Install the dependencies for our Flask producer
RUN pip install --no-cache-dir -r requirements.txt

# Expose API ports
EXPOSE 5000

# Default command runs the Flask-enabled producer
CMD ["python", "produce.py"]
