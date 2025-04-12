import logging
import os
from datetime import datetime

def setup_logger(log_dir="logs", log_filename=None):
    os.makedirs(log_dir, exist_ok=True)
    if log_filename is None:
        log_filename = datetime.now().strftime("log_%Y%m%d_%H%M%S.log")
    log_path = os.path.join(log_dir, log_filename)

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )
    logging.info(f"Logging to file: {log_path}")
