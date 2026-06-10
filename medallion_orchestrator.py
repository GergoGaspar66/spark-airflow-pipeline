from gold_stage import run_gold
from silver_etl import run_silver
from bronze_etl import run_bronze
import sys
from prefect import flow, task
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Biztosítjuk, hogy a Python lássa a scripts mappát az importáláshoz
sys.path.append("scripts")


def get_spark_session():
    # Spark konfigurálása a Delta Lake (.jar) motor automatikus letöltéséhez
    builder = SparkSession.builder \
        .appName("Medallion_Delta_Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.driver.bindAddress", "127.0.0.1")

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
    # 1. Elindítjuk a közös Spark Session-t
    spark = get_spark_session()

    # 2. Végrehajtjuk a fázisokat sorrendben
    bronze_task(spark)
    silver_task(spark)
    gold_task(spark)

    # 3. A legvégén biztonságosan leállítjuk a Sparkot
    spark.stop()


if __name__ == "__main__":
    main_orchestrator()
