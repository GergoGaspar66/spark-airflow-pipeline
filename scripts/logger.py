import os
import logging
from datetime import datetime
from prefect import get_run_logger

def get_pipeline_loggers(layer_name: str):
    """
    Közös logkezelő modul Prefect és helyi fájl alapú naplózáshoz.
    Visszaadja a három segédfüggvényt: log_info, log_warning, log_error,
    valamint a file_handler-t és a file_logger-t a későbbi biztonságos lezáráshoz.
    """
    prefect_logger = get_run_logger()

    # Mappa kezelése
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # Dinamikus fájlnév a réteg neve alapján (pl. logs/silver_20260611.log)
    log_file = os.path.join(
        log_dir, f"{layer_name}_{datetime.now().strftime('%Y%m%d')}.log"
    )

    # FileHandler beállítása
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Egyedi belső logger létrehozása a réteg neve alapján (így nem akadnak össze)
    logger_name = f"{layer_name.capitalize()}FileLogger"
    file_logger = logging.getLogger(logger_name)
    file_logger.setLevel(logging.INFO)
    
    if not file_logger.handlers:
        file_logger.addHandler(file_handler)

    # A jól ismert segédfüggvények
    def log_info(msg):
        prefect_logger.info(msg)
        file_logger.info(msg)

    def log_warning(msg):
        prefect_logger.warning(msg)
        file_logger.warning(msg)

    def log_error(msg):
        prefect_logger.error(msg)
        file_logger.error(msg)

    # Visszaadjuk a függvényeket és a kezelőket is, hogy a finally ágban le lehessen zárni
    return log_info, log_warning, log_error, file_handler, file_logger
