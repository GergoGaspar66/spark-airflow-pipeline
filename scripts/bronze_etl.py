import os
import glob
import shutil
from prefect import get_run_logger
from pyspark.sql.functions import to_date, current_date


def run_bronze(spark):
    logger = get_run_logger()
    logger.info("--- BRONZE LAYER PROCESSING STARTED ---")

    source_dir = "raw"
    bronze_dir = "data/bronze"
    processed_dir = "processed/bronze"

    csv_files = glob.glob(os.path.join(source_dir, "*.csv"))

    if not csv_files:
        logger.warning(
            "Nem talalhato feldolgozando CSV fajl. Bronze reteg atugorva.")
        return

    logger.info(f"Talalt CSV fajlok szama: {len(csv_files)}")

    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(csv_files)

    # Dinamikus dátumkezelés az Order Date oszlop alapján
    if "Order Date" in df_raw.columns:
        df_partitioned = df_raw.withColumn(
            "Feldolgozas_Datuma", to_date(df_raw["Order Date"]))
    else:
        logger.warning(
            "Az 'Order Date' oszlop nem talalhato, mai datum hasznalata.")
        df_partitioned = df_raw.withColumn(
            "Feldolgozas_Datuma", current_date())

    # Particionált mentés és felülírás
    df_partitioned.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("Feldolgozas_Datuma") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(bronze_dir)

    logger.info(f"Az adatok sikeresen elmentve particionalva: {bronze_dir}")

    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        destination = os.path.join(processed_dir, file_name)
        if os.path.exists(destination):
            os.remove(destination)
        shutil.move(file_path, destination)
        logger.info(f"Fajl archivalva: {destination}")

    logger.info("--- BRONZE LAYER PROCESSING FINISHED ---")
