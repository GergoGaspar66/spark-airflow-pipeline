# scripts/bronze_etl.py
import os
import glob
import shutil
import logging

# Standard Airflow/Python logger beállítása
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_bronze():
    logger.info("--- BRONZE LAYER PROCESSING STARTED ---")

    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    # 1. Spark Session konfigurálása
    builder = SparkSession.builder \
        .appName("Bronze_Layer_ETL") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.bindAddress", "127.0.0.1")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    source_dir = "/opt/airflow/raw"
    bronze_dir = "/opt/airflow/data/bronze"
    processed_dir = "/opt/airflow/processed/bronze"

    # 2. CSV fájlok keresése
    csv_files = glob.glob(os.path.join(source_dir, "*.csv"))

    if not csv_files:
        logger.warning(
            "Nem talalhato feldolgozando CSV fajl. Bronze reteg atugorva.")
        spark.stop()
        return

    logger.info(f"Talalt CSV fajlok szama: {len(csv_files)}")

    # 3. CSV fájlok beolvasása és összefűzése Delta táblába
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(csv_files)

    df_raw.write \
        .format("delta") \
        .mode("append") \
        .save(bronze_dir)

    logger.info(
        "Az adatok sikeresen elmentve Delta formatumban a Bronze retegbe.")
    spark.stop()

    # 4. A feldolgozott nyers CSV fájlok átmozgatása
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        destination = os.path.join(processed_dir, file_name)

        if os.path.exists(destination):
            os.remove(destination)

        shutil.move(file_path, destination)
        logger.info(f"Fajl sikeresen archivalva ide: {destination}")

    logger.info("--- BRONZE LAYER PROCESSING FINISHED ---")


if __name__ == "__main__":
    run_bronze()
