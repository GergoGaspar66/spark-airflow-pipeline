FROM apache/airflow:2.8.1-python3.11

USER root

# 1. Java (JRE) és CURL telepítése egy lépésben
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. JAVA_HOME környezeti változó beállítása
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 3. A requirements.txt bemásolása még ROOT-ként (így van hozzá joga)
COPY requirements.txt .

# 4. Váltás az Airflow felhasználóra a csomagok biztonságos telepítéséhez
USER airflow

# 5. Az összes csomag telepítése egy lépésben a requirements.txt-ből
RUN pip install --no-cache-dir -r requirements.txt
