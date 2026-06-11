import os
import shutil
import glob
from prefect import flow, task
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Pont-alapú importálás a scripts mappából
from scripts.bronze_etl import run_bronze
from scripts.silver_etl import run_silver
from scripts.gold_etl import run_gold


def get_spark_session():
    builder = SparkSession.builder \
        .appName("Medallion_Delta_Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.sql.ui.enabled", "false") \
        .config("spark.cleaner.referenceTracking", "false") \
        .config("spark.cleaner.referenceTracking.blocking", "false") \
        .config("spark.network.crypto.enabled", "false") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.master", "local[*]") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")

    return configure_spark_with_delta_pip(builder).getOrCreate()


@task(name="Bronze-Fázis-Feladat")
def bronze_task(spark):
    run_bronze(spark)


@task(name="Silver-Fázis-Feladat")
def silver_task(spark):
    run_silver(spark)


@task(name="Gold-Fázis-Feladat")
def gold_task(spark):
    run_gold(spark)


@flow(name="Modular-Delta-Medallion-Pipeline")
def main_orchestrator():
    # Ha nincs új fájl, békén hagyjuk a meglévő Git-ből letöltött data/bronze mappát, így a Silver sem fog elhasalni.
    new_csv_files = glob.glob("raw/*.csv")
    
    if new_csv_files and os.path.exists("data"):
        print("=== Új adatok érkeztek. Régi adatszerkezet eltávolítása a tiszta particionáláshoz ===")
        shutil.rmtree("data")
    elif not new_csv_files:
        print("=== Nincsenek új nyers fájlok, megtartjuk a meglévő data könyvtárat. ===")

    spark = get_spark_session()

    try:
        # A fázisok futtatása szigorú Prefect függőséggel (submit és wait_for használata ajánlott)
        bronze_future = bronze_task.submit(spark)
        silver_future = silver_task.submit(spark, wait_for=[bronze_future])
        gold_task.submit(spark, wait_for=[silver_future])
    finally:
        print("=== Spark Session biztonságos leállítása ===")
        spark.stop()


if __name__ == "__main__":
    main_orchestrator()
