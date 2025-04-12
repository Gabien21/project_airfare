from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import time
import os
import traceback

import logging
from tqdm import tqdm
import argparse

from src.utils.driver_utils import init_driver
from src.utils.logger_utils import setup_logger

def parse_args():
    parser = argparse.ArgumentParser(description="Abay.vn flight crawler CLI")

    parser.add_argument("--departure", type=str, required=True, help="Departure airport code (e.g. SGN)")
    parser.add_argument("--destination", type=str, required=True, help="Destination airport code (e.g. DAD)")
    parser.add_argument("--start_date", type=str, required=True, help="Start date (dd-mm-yyyy)")
    parser.add_argument("--end_date", type=str, required=True, help="End date (dd-mm-yyyy)")
    parser.add_argument("--save_dir", type=str, default="data/clean", help="Directory to save flight data")
    parser.add_argument("--driver_path", type=str, default="/usr/local/bin/chromedriver", help="Path to chromedriver")
    parser.add_argument("--headless", action="store_true", help="Run Chrome in headless mode")

    return parser.parse_args()

def select_departure(driver, code):
    try:
        input_box = driver.find_element(By.ID, "cphMain_ctl00_usrSearchFormD2_txtFrom")
        input_box.click()
        input_box.clear()
        logging.info("Opened departure list")

        airport = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, f"//div[@id='list-departure']//a[@airportcode='{code}']"))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", airport)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", airport)
        logging.info(f"Selected departure: {code}")
    except Exception as e:
        logging.error(f"Error selecting departure {code}: {e}")

def select_destination(driver, code):
    try:
        input_box = driver.find_element(By.ID, "cphMain_ctl00_usrSearchFormD2_txtTo")
        input_box.click()
        logging.info("Opened destination list")

        airport = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, f"//div[@id='list-arrival']//a[@airportcode='{code}']"))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", airport)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", airport)
        logging.info(f"Selected destination: {code}")
    except Exception as e:
        logging.error(f"Error selecting destination {code}:", e)

def select_departure_date(driver, date_str):
    try:
        icon = driver.find_element(By.CLASS_NAME, "ui-datepicker-trigger")
        icon.click()
        time.sleep(1)

        target = datetime.strptime(date_str, "%d-%m-%Y")
        current = datetime.now()
        month_diff = (target.year - current.year) * 12 + target.month - current.month

        if month_diff >= 3:
            for _ in range(month_diff - 2):
                next_btn = driver.find_element(By.XPATH, "//span[@class='ui-icon ui-icon-circle-triangle-e']")
                next_btn.click()
                time.sleep(0.5)
        time.sleep(2)
        day_btn = driver.find_element(By.XPATH, f"//td[contains(@onclick, '_selectDay') and contains(@onclick, '{target.month-1},2025')]//span[@class='ui-datepicker-day' and text()='{target.day}']")
        day_btn.click()
        logging.info(f"Selected departure date: {date_str}")
    except Exception as e:
        logging.error(f"Error selecting departure date {date_str}:", e)

def choose_next_day(driver):
    try:
        today = driver.find_element(By.XPATH, "//tr[@class='change-date']//li[@class='current']")
        next_day = today.find_element(By.XPATH, "following-sibling::li[1]")
        next_day.click()
        time.sleep(2)
        logging.info("Switched to next day")
    except Exception as e:
        logging.error("Cannot switch to next day:", e)

def get_flight_prices(driver):
    try:
        columns = [
            "Departure Location", "Departure Time", "Arrival Location", "Arrival Time",
            "Flight Duration", "Aircraft Type", "Ticket Price", "Passenger Type",
            "Number of Tickets", "Price per Ticket", "Taxes & Fees", "Total Price",
            "Carry-on Baggage", "Checked Baggage", "Refund Policy", "Scrape Time"
        ]
        flights_data = []
        crawl_start_time = datetime.now()

        # Wait until flight table appears
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "OutBound")))
        outbound_table = driver.find_element(By.ID, "OutBound")

        def rows_in_table(driver):
            rows = outbound_table.find_elements(By.CLASS_NAME, "i-result")
            return rows if len(rows) > 0 else False
        WebDriverWait(driver, 10).until(rows_in_table)
        rows = outbound_table.find_elements(By.CLASS_NAME, "i-result")
        total_flights = len(rows)
        if total_flights == 0:
            return pd.DataFrame()

        # Get selected date
        current_day_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((
                By.XPATH, "//table[@id='OutBound']//tr[@class='change-date']//li[@class='current']"
            ))
        )
        flight_date_text = current_day_element.text.strip()

        logging.info("="*60)
        logging.info("Collecting data for flight date: %s", flight_date_text.replace("\n", " "))
        # logging.info("Started at: %s", crawl_start_time.strftime('%Y-%m-%d %H:%M:%S'))
        logging.info("Total flights found: %d", total_flights)
        logging.info("="*60)

        success_count = 0
        time.sleep(3)


        for idx in tqdm(range(total_flights), desc="Crawling flights", unit="flight"):
            try:
                row = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "i-result"))
                )[idx]
                # flight_number = row.find_element(By.CLASS_NAME, "f-number").text.strip()

                detail_button = row.find_element(By.CLASS_NAME, "linkViewFlightDetail")
                detail_button.click()

                detail_html = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((
                        By.XPATH, f"(//tr[@class='flight-info-detail no-show'])[{idx+1}]"
                    ))
                )

                WebDriverWait(driver, 10).until(
                    lambda d: "table" in detail_html.get_attribute("innerHTML")
                )

                soup = BeautifulSoup(detail_html.get_attribute("outerHTML"), "html.parser")
                tables = soup.find("div").find_all("table", recursive=False)

                # Table 1 - Flight basics
                t1 = tables[0].find("tr").find_all("td")
                p0, p1, p2 = [td.find_all("p") for td in t1[:3]]
                departure_location = p0[0].text.strip()
                departure_time = p0[1].text.strip()
                flight_duration = p1[0].text.strip()
                aircraft_type = p1[-1].text.strip()
                arrival_location = p2[0].text.strip()
                arrival_time = p2[1].text.strip()
                ticket_price = t1[3].find("tr").find_all("td")[-1].text.strip().split("(")[0].strip()

                # Table 2 - Price breakdown
                try:
                    t2 = tables[1].find_all("tr")[1].find_all("td")
                    passenger_type, number_of_tickets, price_per_ticket, taxes_fees, total_price = [td.text.strip() for td in t2]
                except:
                    passenger_type = number_of_tickets = price_per_ticket = taxes_fees = total_price = None

                # Table 3 - Baggage
                try:
                    t3 = tables[2].find("tbody").find_all("tr")
                    carry_on_baggage = t3[1].find_all("td")[1].text.strip()
                    checked_baggage = t3[2].find_all("td")[1].text.strip()
                except:
                    carry_on_baggage = checked_baggage = None

                # Table 4 - Refund policy
                try:
                    t4 = tables[3].find("tbody").find_all("tr")[1:]
                    refund_policy = [tr.text.strip() for tr in t4]
                except:
                    refund_policy = None

                scrape_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                flights_data.append([
                    departure_location, departure_time, arrival_location, arrival_time,
                    flight_duration, aircraft_type, ticket_price, passenger_type,
                    number_of_tickets, price_per_ticket, taxes_fees, total_price,
                    carry_on_baggage, checked_baggage, refund_policy, scrape_time
                ])

                success_count += 1
                # logging.info("Flight %d/%d - %s collected successfully.", idx+1, total_flights, flight_number)
            except Exception as e:
                logging.warning("Failed to collect flight %d/%d: %s", idx+1, total_flights, e)
                traceback.print_exc()
                continue

        crawl_end_time = datetime.now()
        logging.info("→ %d/%d flights collected successfully.", success_count, total_flights)
        # logging.info("Ended at: %s | Duration: %ds",
        #              crawl_end_time.strftime('%Y-%m-%d %H:%M:%S'), (crawl_end_time - crawl_start_time).seconds)
        logging.info("=" * 60)

        return pd.DataFrame(flights_data, columns=columns)

    except Exception as e:
        logging.exception("Top-level error in get_flight_prices")
        traceback.print_exc()
        return pd.DataFrame()


def craw_pipeline(departure, destination, date_str, end_date_str=None, save_dir=None):
    url = "https://www.abay.vn"
    driver = init_driver(headless=True)
    driver.get(url)
    time.sleep(1)
    select_departure(driver, departure)
    select_destination(driver, destination)
    select_departure_date(driver, date_str)

    try:
        search_btn = driver.find_element(By.ID, "cphMain_ctl00_usrSearchFormD2_btnSearch")
        search_btn.click()
        logging.info("Clicked search button")
    except Exception as e:
        logging.error("Cannot click search button:", e)
        return
    
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, datetime.now().strftime("%d_%m_%Y"))
    os.makedirs(save_path, exist_ok=True)
    output_file = os.path.join(save_path, f"flight_prices_{departure}_to_{destination}.csv")
    file_exists = os.path.exists(output_file)

    while True:
        df = get_flight_prices(driver)
        if df.empty:
            logging.info("No data, skipping...")
        else:
            flight_date = datetime.strptime(df.iloc[0, 1].split()[-1], "%d/%m/%Y")
            end_date = datetime.strptime(end_date_str, "%d-%m-%Y")
            df.to_csv(output_file, mode='a', index=False, header=not file_exists)
            file_exists = True
            if flight_date.date() >= end_date.date():
                logging.info("Finished crawling until end date!")
                break
        choose_next_day(driver)
    driver.quit()
    del driver

def choose_datetime(now=None, num_month=1):
    if now is None:
        now = datetime.now()
    else:
        now = datetime.strptime(now, "%d-%m-%Y")
        print(now.strftime("%d_%m_%Y"))
    return (now.strftime("%d-%m-%Y"), ((now + relativedelta(months=num_month)).replace(day=1) - relativedelta(days=1)).strftime("%d-%m-%Y"))
    # return (now.strftime("%d-%m-%Y"), ((now + relativedelta(days=5))).strftime("%d-%m-%Y"))

if __name__ == "__main__":
    setup_logger(log_dir="logs")
    start_date_str, end_date_str = choose_datetime(now= "15-8-2025" ,num_month=3)
    craw_pipeline(departure="SGN",
                  destination="DAD",
                  date_str=start_date_str,
                  end_date_str=end_date_str,
                  save_dir="data/raw")

    craw_pipeline(departure="SGN",
                  destination="HAN",
                  date_str=start_date_str,
                  end_date_str=end_date_str,
                  save_dir="data/raw")
    
    # args = parse_args()
    # setup_logger(log_dir="logs")
    # driver = init_driver(driver_path="/usr/local/bin/chromedriver", headless=args.headless)
    # url = "https://www.abay.vn"

    # craw_pipeline(driver=driver,
    #               url=url,
    #               departure=args.departure,
    #               destination=args.destination,
    #               date_str=args.start_date,
    #               end_date_str=args.end_date,
    #               save_dir=args.save_dir)
    
#    PYTHONPATH=. python src/crawler/abay_form_oneway.py --departure SGN --destination DAD --start_date 01-04-2025 --end_date 30-06-2025 --save_dir data/raw --headless
#    PYTHONPATH=. python src/crawler/abay_form_oneway.py --departure SGN --destination HAN --start_date 01-04-2025 --end_date 30-06-2025 --save_dir data/raw --headless
#    PYTHONPATH=. python src/crawler/abay_form_oneway.py --departure VII --destination HAN --start_date 01-04-2025 --end_date 30-06-2025 --save_dir data/raw --headless
