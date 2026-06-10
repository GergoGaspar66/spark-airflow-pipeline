import os
import glob
import shutil
import logging
from datetime import datetime
from prefect import get_run_logger
from pyspark.sql.functions import to_date, current_date


def run_bronze(spark):
    # 1. Prefect és helyi fájl logolás előkészítése
    prefect_logger = get_run_logger()

    # Létrehozzuk a logs mappát, ha még nem létezik
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # Egyedi logfájl név dátummal (pl. logs/bronze_20260610.log)
    log_file = os.path.join(
        log_dir, f"bronze_{datetime.now().strftime('%Y%m%d')}.log")

    # FileHandler beállítása a fizikai mentéshez
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Létrehozunk egy belső loggert, ami a fájlba is ír
    file_logger = logging.getLogger("BronzeFileLogger")
    file_logger.setLevel(logging.INFO)
    # Biztosítjuk, hogy ne adjunk hozzá kétszer handlert, ha többször fut le
    if not file_logger.handlers:
        file_logger.addHandler(file_handler)

    # Segédfüggvény, ami egyszerre ír a Prefect felületére ÉS a helyi logfájlba
    def log_info(msg):
        prefect_logger.info(msg)
        file_logger.info(msg)

    def log_warning(msg):
        prefect_logger.warning(msg)
        file_logger.warning(msg)

    # 2. A feldolgozás indítása
    log_info("--- BRONZE LAYER PROCESSING STARTED ---")
    log_info(f"Logfajl mentese ide: {log_file}")

    source_dir = "raw"
    bronze_dir = "data/bronze"
    processed_dir = "processed/bronze"

    csv_files = glob.glob(os.path.join(source_dir, "*.csv"))

    if not csv_files:
        log_warning(
            "Nem talalhato feldolgozando CSV fajl. Bronze reteg atugorva.")
        return

    log_info(f"Talalt CSV fajlok szama: {len(csv_files)}")
    log_info(
        f"Feldolgozando fajlok listaja: {', '.join([os.path.basename(f) for f in csv_files])}")

    # CSV beolvasása
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(csv_files)

    # === BASIC INFO GYŰJTÉSE ÉS LOGOLÁSA ===
    sorok_szama = df_raw.count()
    oszlopok = df_raw.columns
    log_info(f"BASIC INFO - Beolvasott nyers sorok szama: {sorok_szama}")
    log_info(f"BASIC INFO - Talalt oszlopok szama: {len(oszlopok)}")
    log_info(f"BASIC INFO - Oszlopok nevei: {', '.join(oszlopok)}")

    # Dinamikus dátumkezelés az Order Date oszlop alapján
    if "Order Date" in df_raw.columns:
        df_partitioned = df_raw.withColumn(
            "Feldolgozas_Datuma", to_date(df_raw["Order Date"]))
    else:
        log_warning(
            "Az 'Order Date' oszlop nem talalhato, mai datum hasznalata.")
        df_partitioned = df_raw.withColumn(
            "Feldolgozas_Datuma", current_date())

    # Particionált mentés, felülírás és séma-transzformáció engedélyezése
    df_partitioned.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("Feldolgozas_Datuma") \
        .option("overwriteSchema", "true") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(bronze_dir)

    log_info(
        f"Az adatok sikeresen elmentve particionalva a Delta tablaba: {bronze_dir}")

    # Archiválás
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        destination = os.path.join(processed_dir, file_name)
        if os.path.exists(destination):
            os.remove(destination)
        shutil.move(file_path, destination)
        log_info(f"Fajl sikeresen archivalva ide: {destination}")

    log_info("--- BRONZE LAYER PROCESSING FINISHED ---")

    # Bezárjuk a fájlkezelőt, hogy a Git el tudja érni a fájlt
    file_handler.close()
    file_logger.removeHandler(file_handler)
