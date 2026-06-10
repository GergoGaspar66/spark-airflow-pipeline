import os
from prefect import get_run_logger


def run_silver(spark):
    logger = get_run_logger()
    logger.info("--- SILVER LAYER PROCESSING STARTED ---")

    bronze_dir = "data/bronze"
    silver_dir = "data/silver"

    if not os.path.exists(bronze_dir):
        logger.warning(
            "A Bronze Delta tabla meg nem jott letre. Silver reteg atugorva.")
        return

    # Bronze Delta adatok beolvasása
    df_bronze = spark.read.format("delta").load(bronze_dir)

    # Adattisztítási logika
    df_clean = df_bronze.dropDuplicates().dropna(how="all")

    # Mentés a Silver Delta táblába
    df_clean.write \
        .format("delta") \
        .mode("overwrite") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(silver_dir)

    logger.info(f"Adatok sikeresen megtisztitva es elmentve ide: {silver_dir}")
    logger.info("--- SILVER LAYER PROCESSING FINISHED ---")
