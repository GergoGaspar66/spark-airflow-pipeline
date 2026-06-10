import os
import shutil
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
    # JAVÍTÁS: Mielőtt elindul a Spark, fizikailag letöröljük a régi, nem particionált adatokat.
    # Ez garantálja, hogy a Delta Lake tiszta lappal indul, és engedi a dátum alapú könyvtárakat!
    if os.path.exists("data"):
        print("=== Régi adatszerkezet eltávolítása a tiszta particionáláshoz ===")
        shutil.rmtree("data")

    spark = get_spark_session()

    try:
        # A fázisok futtatása sorrendben (Az overwriteSchema NÉLKÜLI scriptekkel)
        bronze_task(spark)
        silver_task(spark)
        gold_task(spark)
    finally:
        print("=== Spark Session biztonságos leállítása ===")
        spark.stop()


if __name__ == "__main__":
    main_orchestrator()
