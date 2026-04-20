# Start from the exact image you are already using
FROM apache/airflow:3.1.8

# Switch to the root user so we can install system packages
USER root

# Install Java (OpenJDK 17 is standard for Debian Bookworm/Spark 3.5)
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set the Java Home environment variable so PySpark can find it
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Switch back to the safe 'airflow' user
USER airflow

# Install required packages
RUN pip install --no-cache-dir \
    uv \
    kaggle \
    google-cloud-storage \
    pyspark==3.5.1 \
    streamlit \
    google-cloud-bigquery
