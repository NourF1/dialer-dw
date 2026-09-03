FROM apache/airflow:2.8.1-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Copy both dependency specification files into the image
COPY requirements.txt /requirements.txt
COPY requirements-dev.txt /requirements-dev.txt

# Install production dependencies followed by development/testing dependencies
RUN pip install --no-cache-dir -r /requirements.txt \
    && pip install --no-cache-dir -r /requirements-dev.txt
