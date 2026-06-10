import os
from prefect import get_run_logger


def run_gold(spark):
    logger = get_run_logger()
    logger.info("--- GOLD LAYER PROCESSING STARTED ---")

    silver_dir = "data/silver"
    gold_dir = "data/gold"

    if not os.path.exists(silver_dir):
        logger.warning(
            "A Silver Delta tabla meg nem jott letre. Gold reteg atugorva.")
        return

    # Silver Delta adatok beolvasása
    df_silver = spark.read.format("delta").load(silver_dir)

    # Üzleti aggregáció (Példa csoportosítás, a saját logikádra formálhatod)
    df_gold = df_silver.groupBy().count()

    # Mentés a döntéstelőkészítő Gold Delta táblába
    df_gold.write \
        .format("delta") \
        .mode("overwrite") \
        .save(gold_dir)

    logger.info(f"Uzleti riport sikeresen elmentve ide: {gold_dir}")
    logger.info("--- GOLD LAYER PROCESSING FINISHED ---")
