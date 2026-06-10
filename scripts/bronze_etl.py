import os
import glob
import shutil
from prefect import get_run_logger


def run_bronze(spark):
    logger = get_run_logger()
    logger.info("--- BRONZE LAYER PROCESSING STARTED ---")

    source_dir = "raw"
    bronze_dir = "data/bronze"
    processed_dir = "processed/bronze"

    # CSV fájlok keresése a projekt raw mappájában
    csv_files = glob.glob(os.path.join(source_dir, "*.csv"))

    if not csv_files:
        logger.warning(
            "Nem talalhato feldolgozando CSV fajl. Bronze reteg atugorva.")
        return

    logger.info(f"Talalt CSV fajlok szama: {len(csv_files)}")

    # CSV fájlok beolvasása (Garantáltan megőrzi a ,, karaktereket is)
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(csv_files)

    # Mentés tranzakciós Delta táblaként
    df_raw.write \
        .format("delta") \
        .mode("append") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(bronze_dir)

    logger.info(
        f"Az adatok sikeresen elmentve Delta formatumban: {bronze_dir}")

    # Feldolgozott forrásfájlok átmozgatása az archivumba
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        destination = os.path.join(processed_dir, file_name)

        if os.path.exists(destination):
            os.remove(destination)

        shutil.move(file_path, destination)
        logger.info(f"Fajl sikeresen archivalva ide: {destination}")

    logger.info("--- BRONZE LAYER PROCESSING FINISHED ---")
