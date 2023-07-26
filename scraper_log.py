import logging

# Configure the logging module
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scraper_log.txt", encoding="utf-8"),
        logging.StreamHandler()
    ]
)