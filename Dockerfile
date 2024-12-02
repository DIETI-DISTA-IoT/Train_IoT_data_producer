# Use a base image of Python (Alpine version for a smaller container size and minimal dependencies)
FROM python:3.10-alpine


# Set the working directory inside the container
# This is where all the files will be copied, and the commands will be executed
WORKDIR /synthetic_data_sensor_generator_kafka_producer

# Set environment variables for Kafka connection
# KAFKA_BROKER: Address of the Kafka broker
# TOPIC_NAME: Kafka topic to which the synthetic data will be published
ENV KAFKA_BROKER="kafka:9092"
ENV VEHICLE_NAME="e700_4801"

# Install required build tools and libraries for native dependencies and librdkafka
# This includes compilers and development libraries to allow Python packages with C/C++ extensions to compile properly
RUN apk update && apk add --no-cache gcc g++ musl-dev linux-headers librdkafka librdkafka-dev libc-dev python3-dev bash git

# Copy the wait-for-it.sh script into the container and set executable permissions
# wait-for-it.sh is a script used to wait for a service (like Kafka) to be available before executing subsequent commands
COPY wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh

# Copy the trained generators for synthetic data
COPY copula_anomalie.pkl /synthetic_data_sensor_generator_kafka_producer/copula_anomalie.pkl
COPY copula_normali.pkl /synthetic_data_sensor_generator_kafka_producer/copula_normali.pkl

# Copy the requirements file
COPY requirements_real_producer.txt /synthetic_data_sensor_generator_kafka_producer/requirements_real_producer.txt

# Copy the data simulator script
COPY synthetic_data_generator.py /synthetic_data_sensor_generator_kafka_producer/synthetic_data_generator.py
RUN chmod +x /synthetic_data_sensor_generator_kafka_producer/synthetic_data_generator.py

# Upgrade pip to the latest version
RUN pip install --no-cache-dir --upgrade pip

# Install the dependencies specified in the requirements file and other required libraries
# The requirements file should contain the project's core dependencies
# Additionally, seaborn, matplotlib, scikit-learn, scipy, pandas, numpy, and copulas are installed for data processing and visualization
RUN pip install --no-cache-dir -r requirements_real_producer.txt
RUN pip install scipy pandas numpy copulas
# Command to start the data simulator script when the container is run
CMD ["python", "synthetic_data_generator.py"]
