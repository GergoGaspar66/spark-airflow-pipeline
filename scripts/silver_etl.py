import os
import glob
import shutil
import logging
from datetime import datetime
from prefect import get_run_logger
from delta.tables import DeltaTable
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType



def run_silver(spark):
    # 1. Prefect és helyi fájl logolás előkészítése
    prefect_logger = get_run_logger()

    # Létrehozzuk a logs mappát, ha még nem létezik
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # Egyedi logfájl név dátummal (pl. logs/silver_20260611.log)
    log_file = os.path.join(
        log_dir, f"silver_{datetime.now().strftime('%Y%m%d')}.log")

    # FileHandler beállítása a fizikai mentéshez
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Létrehozunk egy belső loggert, ami a fájlba is ír
    file_logger = logging.getLogger("SilverFileLogger")
    file_logger.setLevel(logging.INFO)
    # Biztosítjuk, hogy ne adjunk hozzá kétszer handlert, ha többször fut le
    if not file_logger.handlers:
        file_logger.addHandler(file_handler)

    # Segédfüggvények, amik egyszerre írnak a Prefect felületére ÉS a helyi logfájlba
    def log_info(msg):
        prefect_logger.info(msg)
        file_logger.info(msg)

    def log_warning(msg):
        prefect_logger.warning(msg)
        file_logger.warning(msg)

    def log_error(msg):
        prefect_logger.error(msg)
        file_logger.error(msg)

    # 2. A feldolgozás indítása
    log_info("--- SILVER LAYER PROCESSING STARTED ---")
    log_info(f"Logfajl mentese ide: {log_file}")

    # Lekérjük a futó script (silver_etl.py) abszolút fizikai helyét a runneren
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Mivel a script a 'scripts' mappában van, a szülőmappa lesz a projekt fő gyökere
    project_root = os.path.dirname(current_script_dir)

    # Biztonságos, abszolút útvonalak létrehozása a Spark számára
    bronze_dir = os.path.join(project_root, "data", "bronze")
    silver_base_dir = os.path.join(project_root, "data", "silver")

    # Debug logok, hogy a Prefect UI-on pontosan lásd a fizikai mappát hiba esetén
    log_info(f"DEBUG - Keresett abszolut Bronze utvonal: {bronze_dir}")
    log_info(f"DEBUG - Letezik a mappa a runneren? {os.path.exists(bronze_dir)}")

    try:
        # 1. ELLENŐRZÉS: Létezik-e a mappa, és van-e benne egyáltalán adat?
        # Ha a mappa nem létezik, vagy teljesen üres (nincs benne _delta_log), akkor átugorjuk a Silver fázist
        if not os.path.exists(bronze_dir) or not os.listdir(bronze_dir):
            log_warning(f"A Bronze mappa hianyzik vagy teljesen ures: {bronze_dir}. A Silver feldolgozas atugorva.")
            return

        # Biztonsági ellenőrzés: Megnézzük, hogy a Delta napló ott van-e a mappában
        delta_log_path = os.path.join(bronze_dir, "_delta_log")
        if not os.path.exists(delta_log_path):
            log_warning(f"A mappa letezik, de nem ervenyes Delta tabla (hianyzik a _delta_log): {bronze_dir}. Silver atugorva.")
            return

        # 2. BIZTONSÁGOS BEOLVASÁS: Ha a fenti ellenőrzések sikeresek, csak akkor olvassuk be
        log_info(f"Bronze Delta tabla beolvasasa innen: {bronze_dir}")
        df_bronze = spark.read.format("delta").load(bronze_dir)

        sorok_szama_nyers = df_bronze.count()
        if sorok_szama_nyers == 0:
            log_warning("A Bronze tabla ures. Silver reteg feldolgozasa leallitva.")
            return

        # === BASIC INFO GYŰJTÉSE ÉS LOGOLÁSA ===
        log_info(f"BASIC INFO - Beolvasott nyers sorok szama: {sorok_szama_nyers}")

        # === TRANSZFORMÁCIÓ 1: Oszlopnevek tisztítása ===
        log_info("Oszlopnevek szabvanyositasa (szokozok csereje alahuzasra)...")
        cleaned_columns = [
            F.col(f"`{c}`").alias(
                c.replace(" ", "_").replace("%", "Percent").replace("$", "Amount")
            ) for c in df_bronze.columns
        ]
        df_stage1 = df_bronze.select(cleaned_columns)

        # === TRANSZFORMÁCIÓ 2: Vektorizált adattisztítás és típuskonverzió ===
        log_info("Penzugyi mezok, datumok es mennyisegek tiszitasa vektorizaltan...")
        money_columns = [
            "Cost_Price", "Retail_Price", "Profit_Margin", 
            "Sub_Total", "Discount_Amount", "Order_Total", 
            "Shipping_Cost", "Total"
        ]

        df_typed = df_stage1.select([
            # 1. Pénzügyi adatok tisztítása és Double típusra kasztolása
            F.regexp_replace(F.col(c), r"[\$,]", "").cast(DoubleType()).alias(c) 
            if c in money_columns
            
            # 2. Százalékos érték átalakítása tizedes törtre (pl. "2%" -> 0.02)
            else (F.regexp_replace(F.col(c), "%", "").cast(DoubleType()) / 100.0).alias(c)
            if c == "Discount_Percent"
            
            # 3. Rendelési mennyiség Integer formátumra alakítása
            else F.col(c).cast(IntegerType()).alias(c)
            if c == "Order_Quantity"
            
            # 4. Dátum mezők Spark DateType formátumra konvertálása (dd-MM-yyyy)
            else F.to_date(F.col(c), "dd-MM-yyyy").alias(c)
            if c in ["Order_Date", "Ship_Date"]
            
            # 5. Minden más oszlop változatlan marad
            else F.col(c)
            for c in df_stage1.columns
        ])

        # === TRANSZFORMÁCIÓ 3: Deduplikáció és Minőségi Szűrések ===
        log_info("Deduplikacio inditasa...")
        df_deduplicated = df_typed.dropDuplicates()
        sorok_szama_dedup = df_deduplicated.count()

        log_info("Szigoru NULL es hibas adat szures a kritikus oszlopokra...")
        df_filtered = df_deduplicated.filter(
            (F.col("Order_No").isNotNull()) & 
            (F.col("Order_No") != "") & 
            (F.col("Order_No") != "__HIVE_DEFAULT_PARTITION__") &
            (F.col("Total").isNotNull()) &
            (F.col("Order_Total").isNotNull()) &
            (F.col("Order_Quantity").isNotNull()) &
            (F.col("Order_Date").isNotNull())  # A naptár és SCD2 miatt ez is kritikus
        )
        
        kiszurt_sorok = sorok_szama_dedup - df_filtered.count()
        if kiszurt_sorok > 0:
            log_warning(f"ADATMINOSEGI RIASZTAS - Kiszurtunk {kiszurt_sorok} sort hibas adatok miatt!")

        # === TRANSZFORMÁCIÓ 4: Leíró szöveges mezők NULL/Blank kezelése ===
        log_info("Opcionalis szoveges mezok blank/NA ertekeinek csereje 'Unknown' szora...")
        string_columns = [
            "Customer_Name", "Address", "City", "State", "Customer_Type", 
            "Account_Manager", "Order_Priority", "Product_Name", 
            "Product_Category", "Product_Container", "Ship_Mode"
        ]

        df_silver_flat = df_filtered.select([
            F.when(
                (F.col(c).isNull()) | 
                (F.trim(F.col(c)) == "") | 
                (F.lower(F.trim(F.col(c))).isin("na", "n/a", "null", "nan")), 
                "Unknown"
            ).otherwise(F.col(c)).alias(c)
            if c in string_columns
            else F.col(c)
            for c in df_filtered.columns
        ])

        # =========================================================================
        # === STAR SCHEMA KIALAKÍTÁSA & SCD2  (DELTA MERGE LOGIKA) ===
        # =========================================================================
        log_info("--- STAR SCHEMA ELŐKÉSZÍTÉSE SURROGATE KULCSOKKAL ÉS SCD2-VEL ---")

        # 1. DIMENZIÓ: dim_customers (SCD2)
        log_info("Customer dimenzio letrehozasa es SCD2 kezelese...")
        customer_dim_path = os.path.join(silver_base_dir, "dim_customers")
        
        # Az SCD2 miatt a Customer_Key-t most már CSAK a nevükből képezzük le
        df_incoming_cust = df_silver_flat.select(
            "Customer_Name", "Address", "City", "State", "Customer_Type", "Order_Date"
        ).dropDuplicates(["Customer_Name"]) \
         .withColumn("Customer_Key", F.md5(F.coalesce("Customer_Name", F.lit(""))))

        # Első inicializálás, ha még egyáltalán nem létezik a tábla
        if not os.path.exists(customer_dim_path):
            log_info("dim_customers nem letezik, elso inicializalas...")
            df_init_cust = df_incoming_cust.withColumn("Valid_From", F.col("Order_Date")) \
                                           .withColumn("Valid_Until", F.lit(None).cast("date"))
            
            df_init_cust.select("Customer_Key", "Customer_Name", "Address", "City", "State", "Customer_Type", "Valid_From", "Valid_Until") \
                        .write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
                        .option("delta.columnMapping.mode", "name") \
                        .option("delta.minReaderVersion", "2").option("delta.minWriterVersion", "5").save(customer_dim_path)
        else:
            log_info("dim_customers incremental SCD2 frissites...")
            target_table_cust = DeltaTable.forPath(spark, customer_dim_path)
            df_target_cust = target_table_cust.toDF()

            # Változások detektálása az aktív (Valid_Until IS NULL) sorokhoz képest
            df_cust_updates = df_incoming_cust.join(df_target_cust.filter(F.col("Valid_Until").isNull()), "Customer_Key", "inner") \
                .filter((df_incoming_cust["Address"] != df_target_cust["Address"]) | 
                        (df_incoming_cust["City"] != df_target_cust["City"]) | 
                        (df_incoming_cust["State"] != df_target_cust["State"]) | 
                        (df_incoming_cust["Customer_Type"] != df_target_cust["Customer_Type"])) \
                .select(df_incoming_cust["*"])

            df_staged_cust = df_cust_updates.select(F.lit(None).cast("string").alias("merge_key"), F.col("*")) \
                .union(df_incoming_cust.select(F.col("Customer_Key").alias("merge_key"), F.col("*")))

            # SCD2 Merge futtatása
            target_table_cust.alias("target").merge(df_staged_cust.alias("source"), "target.Customer_Key = source.merge_key AND target.Valid_Until IS NULL") \
                .whenMatchedUpdate(
                    condition="target.Address <> source.Address OR target.City <> source.City OR target.State <> source.State OR target.Customer_Type <> source.Customer_Type",
                    set={"Valid_Until": "source.Order_Date"}
                ).whenNotMatchedInsert(values={
                    "Customer_Key": "source.Customer_Key", "Customer_Name": "source.Customer_Name", "Address": "source.Address",
                    "City": "source.City", "State": "source.State", "Customer_Type": "source.Customer_Type",
                    "Valid_From": "source.Order_Date", "Valid_Until": F.lit(None).cast('date')
                }).execute()

        # 2. DIMENZIÓ: dim_products (SCD2)
        log_info("Product dimenzio letrehozasa es SCD2 kezelese...")
        product_dim_path = os.path.join(silver_base_dir, "dim_products")
        
        df_incoming_prod = df_silver_flat.select(
            "Product_Name", "Product_Category", "Product_Container", "Order_Date"
        ).dropDuplicates(["Product_Name"]) \
         .withColumn("Product_Key", F.md5(F.coalesce("Product_Name", F.lit(""))))

               # Első inicializálás, ha még egyáltalán nem létezik a tábla
        if not os.path.exists(product_dim_path):
            log_info("dim_products nem letezik, elso inicializalas...")
            df_init_prod = df_incoming_prod.withColumn("Valid_From", F.col("Order_Date")) \
                                           .withColumn("Valid_Until", F.lit(None).cast("date"))
            
            df_init_prod.select("Product_Key", "Product_Name", "Product_Category", "Product_Container", "Valid_From", "Valid_Until") \
                        .write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
                        .option("delta.columnMapping.mode", "name") \
                        .option("delta.minReaderVersion", "2").option("delta.minWriterVersion", "5").save(product_dim_path)
        else:
            log_info("dim_products incremental SCD2 frissites...")
            target_table_prod = DeltaTable.forPath(spark, product_dim_path)
            df_target_prod = target_table_prod.toDF()

            # Változások detektálása az aktív (Valid_Until IS NULL) sorokhoz képest
            df_prod_updates = df_incoming_prod.join(df_target_prod.filter(F.col("Valid_Until").isNull()), "Product_Key", "inner") \
                .filter((df_incoming_prod["Product_Category"] != df_target_prod["Product_Category"]) | 
                        (df_incoming_prod["Product_Container"] != df_target_prod["Product_Container"])) \
                .select(df_incoming_prod["*"])

            df_staged_prod = df_prod_updates.select(F.lit(None).cast("string").alias("merge_key"), F.col("*")) \
                .union(df_incoming_prod.select(F.col("Product_Key").alias("merge_key"), F.col("*")))

            # SCD2 Merge futtatása
            target_table_prod.alias("target").merge(df_staged_prod.alias("source"), "target.Product_Key = source.merge_key AND target.Valid_Until IS NULL") \
                .whenMatchedUpdate(
                    condition="target.Product_Category <> source.Product_Category OR target.Product_Container <> source.Product_Container",
                    set={"Valid_Until": "source.Order_Date"}
                ).whenNotMatchedInsert(values={
                    "Product_Key": "source.Product_Key", "Product_Name": "source.Product_Name",
                    "Product_Category": "source.Product_Category", "Product_Container": "source.Product_Container",
                    "Valid_From": "source.Order_Date", "Valid_Until": F.lit(None).cast('date')
                }).execute()

        # 4. DIMENZIÓ: dim_date (Dinamikus generálás az egyedi rendelési dátumokból)
        log_info("Dinamikus dim_date tabla letrehozasa...")
        date_dim_path = os.path.join(silver_base_dir, "dim_date")
        df_dim_date = df_silver_flat.select("Order_Date").dropDuplicates()
        df_dim_date_final = df_dim_date.select(
            F.col("Order_Date").alias("Date_Key"),
            F.year("Order_Date").alias("Year"),
            F.month("Order_Date").alias("Month"),
            F.dayofmonth("Order_Date").alias("Day"),
            F.quarter("Order_Date").alias("Quarter"),
            F.date_format("Order_Date", "E").alias("Day_Of_Week"),
            F.date_format("Order_Date", "MMMM").alias("Month_Name")
        )

        # 5. TÉNYTÁBLA: fact_sales (Surrogate kulcsok visszakötése Időgép / SCD2 alapon)
        log_info("Fact sales tabla osszeallitasa surrogate kulcsokkal (SCD2 alapu JOIN)...")
        
        # Újra beolvassuk a frissített dimenziókat, hogy a legfrissebb SCD2 állapotot lássuk
        df_current_customers = spark.read.format("delta").load(customer_dim_path)
        df_current_products = spark.read.format("delta").load(product_dim_path)

        # Az SCD2 visszakötés titka: az Order_Date-nek a Valid_From és a Valid_Until közé kell esnie.
        df_fact_sales = df_silver_flat \
            .join(df_current_customers, 
                  (df_silver_flat["Customer_Name"] == df_current_customers["Customer_Name"]) & 
                  (df_silver_flat["Order_Date"] >= df_current_customers["Valid_From"]) & 
                  (df_silver_flat["Order_Date"] <= F.coalesce(df_current_customers["Valid_Until"], F.to_date(F.lit("2099-12-31")))), "left") \
            .join(df_current_products, 
                  (df_silver_flat["Product_Name"] == df_current_products["Product_Name"]) & 
                  (df_silver_flat["Order_Date"] >= df_current_products["Valid_From"]) & 
                  (df_silver_flat["Order_Date"] <= F.coalesce(df_current_products["Valid_Until"], F.to_date(F.lit("2099-12-31")))), "left")
            
        # Csak a kulcsokat és a numerikus/logisztikai mérőszámokat tartjuk meg a ténytáblában
        df_fact_sales_final = df_fact_sales.select(
            "Order_No", "Customer_Key", "Product_Key", "Account_Manager",
            F.col("Order_Date").alias("Date_Key"), "Ship_Date", "Order_Priority", "Ship_Mode",
            "Cost_Price", "Retail_Price", "Profit_Margin", "Order_Quantity", "Sub_Total",
            "Discount_Percent", "Discount_Amount", "Order_Total", "Shipping_Cost", "Total", "Feldolgozas_Datuma"
        )

        # === ADATTÁBLÁK KIÍRÁSA DELTA LAKE FORMÁTUMBAN ===
        tables_to_save = {
            "dim_date": (df_dim_date_final, None),
            "fact_sales": (df_fact_sales_final, "Feldolgozas_Datuma")
        }

        for table_name, (df_table, partition_col) in tables_to_save.items():
            target_path = os.path.join(silver_base_dir, table_name)
            log_info(f"Mentes: {table_name} -> {target_path}")
            
            writer = df_table.write.format("delta").mode("overwrite").option("mergeSchema", "false") \
                .option("delta.columnMapping.mode", "name") \
                .option("delta.minReaderVersion", "2") \
                .option("delta.minWriterVersion", "5")
                
            if partition_col:
                writer.partitionBy(partition_col).save(target_path)
            else:
                writer.save(target_path)

        # === NOT NULL CONSTRAINTS AKTIVÁLÁSA AZ ELSŐDLEGES KULCSOKRA ===
        log_info("Konstrukcios NOT NULL kenyszerfeltetelek ellenorzese es aktivalasa...")
        spark.sql(f"ALTER TABLE delta.`{customer_dim_path}` ALTER COLUMN Customer_Key SET NOT NULL")
        spark.sql(f"ALTER TABLE delta.`{product_dim_path}` ALTER COLUMN Product_Key SET NOT NULL")
        spark.sql(f"ALTER TABLE delta.`{date_dim_path}` ALTER COLUMN Date_Key SET NOT NULL")
        spark.sql(f"ALTER TABLE delta.`{os.path.join(silver_base_dir, 'fact_sales')}` ALTER COLUMN Order_No SET NOT NULL")

        log_info("--- SILVER LAYER PROCESSING FINISHED ---")

         # === DELTA PERFORMANCE OPTIMALIZÁCIÓ ===
        log_info("Delta Lake performance optimalizáció indítása (OPTIMIZE & VACUUM)...")
        
        # Lefuttatjuk az optimalizálást a ténytáblára és az SCD2 dimenziókra
        for table_name in ["fact_sales", "dim_customers", "dim_products"]:
            table_path = os.path.join(silver_base_dir, table_name)
            
            # Apró fájlok összefésülése a gyorsabb olvasásért
            spark.sql(f"OPTIMIZE delta.`{table_path}`")
            
            # Régi tranzakciós szemét törlése (7 napos retention period megőrzésével)
            spark.sql(f"VACUUM delta.`{table_path}`")
            
        log_info("A Delta táblák optimalizálása sikeresen befejeződött!")

    except Exception as e:
        log_error(f"HIBA TORTENT a Silver reteg feldolgozasa kozben: {str(e)}")
        raise e

    finally:
        # Bezárjuk a fájlkezelőt, hogy a Git el tudja érni a fájlt
        file_handler.close()
        file_logger.removeHandler(file_handler)
