import logging
import os
import time

# Create a 'log' directory if it doesn't exist
if not os.path.exists('logs'):
    os.mkdir('logs')

# Get the current time as a string in the format "YYYY-MM-DD_HH-MM-SS"
current_time = time.strftime("%Y-%m-%d_%H-%M-%S")

# Construct the log file path with the current time inside the 'log' directory
log_file_path = os.path.join('logs', f"scraper_log_{current_time}.txt")

# Configure the logging module
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
