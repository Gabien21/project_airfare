import sys
import os
import argparse
from datetime import datetime
import logging

from src.crawler.abay_form_oneway import choose_datetime, craw_pipeline
from src.etl.preprocessing_flight_prices import ETL
from src.etl.update_data import delete_old_tickets_and_flights
from src.modeling.preprocess_data_for_modeling import preprocess_for_modeling
from src.modeling.modeling_data import model_data
from src.utils.logger_utils import setup_logger

def main(months=1, start_date=None):
    setup_logger(log_dir="logs")
    logging.info("STARTING FULL PIPELINE")

    # === Crawl data from Abay ===
    if start_date:
        now = datetime.strptime(start_date, "%Y-%m-%d")
        logging.info(f"Crawling data starting from {start_date} for {months} month(s)...")
    else:
        now = None
        logging.info(f"Crawling data from today for {months} month(s)...")
    start_date_str, end_date_str = choose_datetime(now=start_date, num_month=months)

    for dest in ["DAD", "HAN"]:
        logging.info(f"Crawling SGN to {dest}...")
        craw_pipeline(
            departure="SGN",
            destination=dest,
            date_str=start_date_str,
            end_date_str=end_date_str,
            save_dir="data/raw"
        )

    # === ETL ===
    logging.info("Running ETL pipeline...")
    ETL(data_dir=datetime.now().strftime("%d_%m_%Y"))

    # === Clean old data ===
    logging.info("Deleting old ticket & schedule data...")
    delete_old_tickets_and_flights()

    # === Preprocessing for modeling ===
    logging.info("Preprocessing data for modeling...")
    preprocess_for_modeling()

    # === Model training ===
    logging.info("Training and evaluating model...")
    model_data()

    logging.info("ALL DONE!")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run airfare prediction pipeline")
    parser.add_argument("--months", type=int, default=1, help="Number of months to crawl from start_date")
    parser.add_argument("--start_date", type=str, help="Start date (DD-MM-YYYY). Default is today.")
    args = parser.parse_args()

    main(months=args.months, start_date=args.start_date)

# python run_pipeline.py --months 1
# python run_pipeline.py --start_date 01-05-2025 --months 1