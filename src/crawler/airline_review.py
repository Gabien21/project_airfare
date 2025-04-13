import os
import time
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import argparse
import logging
from datetime import datetime
import os

from src.utils.driver_utils import init_driver
from src.utils.logger_utils import setup_logger

def parse_args():
    parser = argparse.ArgumentParser(description="Tripadvisor.com.vn review crawler CLI")

    parser.add_argument("--airline", type=str, required=True, help="Airline name(e.g. VNA, VJ, Bam ) ")
    parser.add_argument("--driver_path", type=str, default="/usr/local/bin/chromedriver", help="Path to chromedriver")
    parser.add_argument("--save_dir", type=str, default="data/clean", help="Directory to save flight data")
    parser.add_argument("--headless", action="store_true", help="Run Chrome in headless mode")

    return parser.parse_args()

def extract_general_data(driver, url):
    try:
        logging.info("Start extracting general information about airline")
        driver.get(url)
        driver.execute_script("window.scrollBy(0, 1000);")
        time.sleep(5)
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, "html.parser")
    except Exception as e:
        logging.error("Start extracting general information about airline")

    logging.info("Start extracting general information about airline")

    try:
        name = soup.find('div', class_="jIkPg G u")
        phone = soup.find('div', class_="bOZAZ VNlYD")
        address = soup.find('div', class_="biGQs _P pZUbB W hmDzD")
        link_element = soup.find('div', class_="jWfod u")
        average_rating = soup.find('span', class_="ammfn")

        attributes = {}
        attribute_elements = soup.find_all('div', class_='HWAlD TQNLQ')
        for ele in attribute_elements:
            name_attribute = ele.find('span', class_='exvvN').text if ele.find('span', class_='exvvN') else ''
            rate = ele.find('span', class_='RkhSR').text if ele.find('span', class_='RkhSR') else ''
            attributes[name_attribute] = rate

        total_review = soup.find('span', class_="SSkub")
        total_ratings = {}
        rating_elements = soup.find_all('div', class_="jxnKb")
        for rating_element in rating_elements:
            name_rating = rating_element.find('div', class_='Ygqck o W q').text if rating_element.find('div', class_='Ygqck o W q') else ''
            count = rating_element.find('div', class_='biGQs _P fiohW biKBZ osNWb').text if rating_element.find('div', class_='biGQs _P fiohW biKBZ osNWb') else ''
            total_ratings[name_rating] = count

        popular_mentions_elements = soup.find('div', class_="TuqGj")
        popular_mentions = [element.text for element in popular_mentions_elements.find_all('span', class_="_T")]

        logging.info("✅ Finishing extracting information")
        return {
            "Name": name.text if name else "Not found",
            "Phone": phone.text if phone else "Not found",
            "Address": address.text if address else "Not found",
            "Website": [link['href'] for link in link_element.find_all('a', href=True) if 'http' in link['href']] if link_element else "Not found",
            "Average Rating": average_rating.text if average_rating else "Not found",
            "Total Review": total_review.text if total_review else "Not found",
            "Popular Mentions": popular_mentions,
            "Attributes": attributes,
            "Total Ratings": total_ratings
        }
    except Exception as e: 
        logging.error(f"❌ Error occur when extracting information : {e}")
    
    

def save_general_data(data, file_path, file_name):
    logging.info(f"Start saving data into {file_path}/{file_name}")

    directory = os.path.dirname(file_path)

    if not os.path.exists(directory):
        os.makedirs(directory)

    try:
        with open(f"{file_path}/{file_name}", "a") as file:
            file.write(f"Name: {data['Name']}\n")
            file.write(f"Phone: {data['Phone']}\n")
            file.write(f"Address: {data['Address']}\n")
            file.write(f"Website: {', '.join(data['Website'])}\n" if data['Website'] != "Not found" else "Website: Not found\n")
            file.write(f"Average Rating: {data['Average Rating']}\n")
            file.write(f"Total Review: {data['Total Review']}\n")
            file.write(f"Popular Mention: {data['Popular Mentions']}\n")
            file.write(f"Attributes: {data['Attributes']}\n")
            file.write(f"Total Rating: {data['Total Ratings']}\n")

        logging.info(f"✅ Finishing saving data into {file_path}/{file_name}")
    except Exception as e:
        logging.error(f"❌ Error when saving file {file_path}/{file_name}: {e}")

    return

def extract_reviews_1page(soup, page_number):
    logging.info(f"Starting extracting review data from page {page_number}")
    try:
        reviews_data = []
        review_elements = soup.find_all('div', class_="lwGaE A")
        for review_element in review_elements:
            rating = review_element.find("svg", class_="UctUV d H0").text if review_element.find("svg", class_="UctUV d H0") else ''
            title = review_element.find("div", class_="biGQs _P fiohW uuBRH").text if review_element.find("div", class_="biGQs _P fiohW uuBRH") else ''
            full_review = review_element.find("span", class_="JguWG").text if review_element.find("span", class_="JguWG") else ''
            information = review_element.find("div", class_="biGQs _P pZUbB ncFvv osNWb").text if review_element.find("div", class_="biGQs _P pZUbB ncFvv osNWb") else ''

            review_dict = {
                "Rating": rating,
                "Title": title,
                "Full Review": full_review,
                "Information": information
            }

            service_rating_table = review_element.find('div', class_="JxiyB f")
            if service_rating_table:
                service_ratings = []
                for element in service_rating_table.find_all('div', class_="msVPq"):
                    service_rating = element.find('svg', class_='UctUV d H0').text if element.find('svg', class_='UctUV d H0') else ''
                    service_info = element.find('div', class_='biGQs _P pZUbB osNWb').text if element.find('div', class_='biGQs _P pZUbB osNWb') else ''
                    service_ratings.append({"Service Rating": service_rating, "Service Info": service_info})
                review_dict["Service Ratings"] = service_ratings
            else:
                review_dict["Service Ratings"] = None

            reviews_data.append(review_dict)
        logging.info(f"✅ Finishing extracting review in page {page_number}")
        return reviews_data
    except Exception as e :
        logging.error(f"❌ Error when extracting review in page {page_number}: {e}")



def get_all_reviews(driver, url):
    driver.get(url)
    time.sleep(5)  
    page_number = 1
    all_reviews = []
    while True:
        logging.info(f"Start crawling review in page {page_number}")
        crawl_start_time = datetime.now()
        page_reviews = extract_reviews_1page(BeautifulSoup(driver.page_source, "html.parser"), page_number)
        all_reviews.extend(page_reviews)
        try:
            next_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[@aria-label='Next page']"))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
            next_button.click()
            time.sleep(4)  
        except Exception:
            print("No more pages available.")
            break
        crawl_end_time = datetime.now()
        logging.info(f"Finish crawling review in page {page_number}")
        logging.info("✅ Ended at: %s | Duration: %ds",
                     crawl_end_time.strftime('%Y-%m-%d %H:%M:%S'), (crawl_end_time - crawl_start_time).seconds)
        logging.info("=" * 60)
        page_number += 1
    return all_reviews


def save_review_data(all_reviews, file_path, file_name) :
    try:
        reviews_df = pd.DataFrame(all_reviews)
        reviews_df.to_csv(f"{file_path}/{file_name}", index=False, encoding="utf-8")
        logging.info(f"✅ Finishing saving review data in {file_path}/{file_name}")
    except Exception as e :
        logging.error(f"❌ Error when saving review data in {file_path}/{file_name}: {e}")

def main(driver, airline_name, save_dir):

    url = None
    if airline_name.lower() == 'vna':
        url = "https://www.tripadvisor.com/Airline_Review-d8729180-Reviews-Vietnam-Airlines"
    elif airline_name.lower() == 'vj' :
        url = "https://www.tripadvisor.com/Airline_Review-d8728891-Reviews-VietJetAir"
    elif airline_name.lower() == 'bamboo': 
        url = "https://www.tripadvisor.com/Airline_Review-d17550096-Reviews-Bamboo-Airways"
    else :
        logging.info(f"Cannot find review about airline: {airline_name}")
        return 
    
    

    # Step 1 : Extract general information about airline
    general_data = extract_general_data(driver, url)
    file_general_name = f"{airline_name.lower()}_general_info.txt"
    save_general_data(general_data, save_dir, file_general_name)

    # Step 2 : Extract review data about airline
    all_reviews = get_all_reviews(driver, url)
    file_review_name = f"{airline_name.lower()}_all_reviews_data.csv"
    save_review_data(all_reviews, save_dir, file_review_name)

    logging.info("Finish crawling phase")
    driver.quit()


if __name__ == "__main__":
    args = parse_args()
    setup_logger(log_dir="logs")
    driver = init_driver(driver_path=args.driver_path, headless=args.headless)

    main(driver=driver,
                  airline_name=args.airline,
                  save_dir=args.save_dir,
                  )
    
#    PYTHONPATH=. python src/crawler/airline_review.py --airline VJ --driver_path "../chromedriver-win64/chromedriver.exe" --save_dir data/raw/review --headless
#    PYTHONPATH=. python src/crawler/airline_review.py --airline VNA --driver_path "../chromedriver-win64/chromedriver.exe" --save_dir data/raw --headless
#    PYTHONPATH=. python src/crawler/airline_review.py --airline Bamboo --driver_path "../chromedriver-win64/chromedriver.exe" --save_dir data/raw --headless


    # python -m src.crawler.airline_review --airline Bamboo --driver_path "../chromedriver-win64/chromedriver.exe" --save_dir data/raw/review
