FROM apache/airflow:2.8.1-python3.11

USER root

# Java + curl
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Java env
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Spark env (ha PySpark-ot használsz)
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# requirements
COPY requirements.txt .

USER airflow

# pip upgrade + install
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt