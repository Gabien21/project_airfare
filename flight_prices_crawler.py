from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
import time
from datetime import datetime
import pandas as pd
import os

service = Service(executable_path="chromedriver.exe")
driver = webdriver.Chrome(service=service)

def get_departure_place_list(driver):
    try:
        places_element = driver.find_element(By.CSS_SELECTOR, "div#list-departure div.domestic-col ul.code-list")
        html_content = places_element.get_attribute("outerHTML")
        soup = BeautifulSoup(html_content, "html.parser")
        places = soup.find_all("a")
        place_list = []
        for place in places:
            airport_code = place.get("airportcode")
            airport_name = place.find("b").text.strip() if place.find("b") else None
            place_list.append((airport_code, airport_name))
        return place_list
    except Exception as e:
        print("Không thể lấy danh sách sân bay:", e)
        return None
    
def get_destination_place_list(driver):
    try:
        places_element = driver.find_element(By.CSS_SELECTOR, "div#list-arrival div.domestic-col ul.code-list")
        html_content = places_element.get_attribute("outerHTML")
        soup = BeautifulSoup(html_content, "html.parser")
        places = soup.find_all("a")
        place_list = []
        for place in places:
            airport_code = place.get("airportcode")
            airport_name = place.find("b").text.strip() if place.find("b") else None
            place_list.append((airport_code, airport_name))
        return place_list
    except Exception as e:
        print("Không thể lấy danh sách sân bay:", e)
        return None
    
def select_departure(driver, department):
    try:
        departure_choice_click = driver.find_element(By.ID, "cphMain_ctl00_usrSearchFormD2_txtFrom")
        departure_choice_click.click()
        print("Đã mở danh sách chọn nơi đi")
        departure_choice_click.clear()
    except Exception as e:
        print("Không thể nhấn vào ô chọn nơi đi:", e)
        driver.quit()
        return
    try:
        vii_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, f"//div[@id='list-departure']//ul[@class='code-list']//a[@airportcode='{department}']"))
        )
        print(f"Đã tìm thấy sân bay {department}")
        driver.execute_script("arguments[0].scrollIntoView(true);", vii_element)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", vii_element)
        print(f"Đã chọn nơi đi là {department}")
    except Exception as e:
        print(f"Không thể tìm hoặc nhấn vào sân bay {department}:", e)
        driver.quit()
        return
    
def select_destination(driver, destination):
    try:
        destination_choice_click = driver.find_element(By.ID, "cphMain_ctl00_usrSearchFormD2_txtTo")
        destination_choice_click.click()
        print("Đã mở danh sách chọn nơi đến")
    except Exception as e:
        print("Không thể nhấn vào ô chọn nơi đến:", e)
        driver.quit()
        return
    try:
        vii_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, f"//div[@id='list-arrival']//ul[@class='code-list']//a[@airportcode='{destination}']"))
        )
        print(f"Đã tìm thấy sân bay {destination}")
        driver.execute_script("arguments[0].scrollIntoView(true);", vii_element)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", vii_element)
        print(f"Đã chọn nơi đến là {destination}")
    except Exception as e:
        print(f"Không thể tìm hoặc nhấn vào sân bay {destination}:", e)
        driver.quit()
        return
    
def get_flight_prices(driver):
    try:
        columns = [
            "Departure Location", "Departure Time", "Arrival Location", "Arrival Time",
            "Flight Duration", "Aircraft Type", "Ticket Price", "Passenger Type",
            "Number of Tickets", "Price per Ticket", "Taxes & Fees", "Total Price",
            "Carry-on Baggage", "Checked Baggage", "Refund Policy"
        ]
        flights_data = []

        crawl_start_time = datetime.now()

        # Đợi bảng chuyến bay hiển thị
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "OutBound"))
        )
        outbound_table = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "OutBound"))
        )
        rows = outbound_table.find_elements(By.CLASS_NAME, "i-result")
        total_flights = len(rows)

        # Đợi ngày đang chọn hiện ra
        current_day_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((
                By.XPATH, "//table[@id='OutBound']//tr[@class='change-date']//li[@class='current']"
            ))
        )
        flight_date_text = current_day_element.text.strip()

        print("=" * 60)
        print(f"📅 Đang lấy dữ liệu cho ngày bay: {flight_date_text}")
        print(f"⏰ Bắt đầu lúc: {crawl_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Tổng số chuyến bay hiển thị hôm nay: {total_flights}")
        print("=" * 60)

        success_count = 0

        for idx in range(total_flights):
            try:
                # Luôn tìm lại dòng để tránh stale
                row = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "i-result"))
                )[idx]

                # Số hiệu chuyến bay
                flight_number = WebDriverWait(row, 10).until(
                    lambda r: r.find_element(By.CLASS_NAME, "f-number")
                ).text.strip()

                # Nhấn nút xem chi tiết
                detail_button = WebDriverWait(row, 10).until(
                    lambda r: r.find_element(By.CLASS_NAME, "linkViewFlightDetail")
                )
                detail_button.click()
                time.sleep(1)

                # Lấy phần tử chi tiết
                detail_html = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((
                        By.XPATH, f"(//tr[@class='flight-info-detail no-show'])[{idx+1}]"
                    ))
                )

                soup = BeautifulSoup(detail_html.get_attribute("outerHTML"), "html.parser")
                tables = soup.find("div").find_all("table", recursive=False)

                # Table 1
                datas_table0 = tables[0].find("tr").find_all("td", recursive=False)
                flight_info = datas_table0[0].find_all("p")
                departure_location = flight_info[0].text.strip()
                departure_time = flight_info[1].text.strip()

                plane_info = datas_table0[1].find_all("p")
                flight_duration = plane_info[0].text.strip()
                aircraft_type = plane_info[-1].text.strip()

                arrival_info = datas_table0[2].find_all("p")
                arrival_location = arrival_info[0].text.strip()
                arrival_time = arrival_info[1].text.strip()

                price_info = datas_table0[3].find("tr").find_all("td")[-1]
                ticket_price = price_info.text.strip().split("(vận")[0].strip()

                # Table 2
                passenger_type = number_of_tickets = price_per_ticket = taxes_fees = total_price = None
                try:
                    datas_table1 = tables[1].find_all("tr")[1].find_all("td")
                    passenger_type = datas_table1[0].text.strip()
                    number_of_tickets = datas_table1[1].text.strip()
                    price_per_ticket = datas_table1[2].text.strip()
                    taxes_fees = datas_table1[3].text.strip()
                    total_price = datas_table1[4].text.strip()
                except Exception as e:
                    print(f"⚠️ Lỗi Table 2 (giá vé) của chuyến {idx+1}: {e}")

                # Table 3
                carry_on_baggage = checked_baggage = None
                try:
                    datas_table2 = tables[2].find("tbody").find_all("tr", recursive=False)
                    carry_on_baggage = datas_table2[1].find_all("td")[1].text.strip()
                    checked_baggage = datas_table2[2].find_all("td")[1].text.strip()
                except Exception as e:
                    print(f"⚠️ Lỗi Table 3 (hành lý) của chuyến {idx+1}: {e}")

                # Table 4
                refund_policy = None
                try:
                    datas_table3 = tables[3].find("tbody").find_all("tr", recursive=False)[1:]
                    refund_policy = [i.text.strip() for i in datas_table3]
                except Exception as e:
                    print(f"⚠️ Lỗi Table 4 (hoàn vé) của chuyến {idx+1}: {e}")

                flights_data.append([
                    departure_location, departure_time, arrival_location, arrival_time,
                    flight_duration, aircraft_type, ticket_price, passenger_type,
                    number_of_tickets, price_per_ticket, taxes_fees, total_price,
                    carry_on_baggage, checked_baggage, refund_policy
                ])
                success_count += 1
                print(f"✓ Chuyến bay {idx+1}/{total_flights} - {flight_number} lấy thành công.")
            except Exception as e:
                print(f"✗ Không thể lấy dữ liệu chuyến bay {idx+1}/{total_flights}: {e}")
                continue

        crawl_end_time = datetime.now()
        print(f"→ Đã lấy {success_count}/{total_flights} chuyến bay thành công.")
        print(f"✅ Kết thúc lúc: {crawl_end_time.strftime('%Y-%m-%d %H:%M:%S')} | Thời gian: {(crawl_end_time - crawl_start_time).seconds}s")
        print("=" * 60)

        return pd.DataFrame(flights_data, columns=columns)

    except Exception as e:
        print("❌ Lỗi ngoài cùng trong get_flight_prices:", e)
        return pd.DataFrame()


def choose_next_day(driver):
    try:
        current_day_button = driver.find_element(By.XPATH, "//table[@id='OutBound']//tr[@class='change-date']//li[@class='current']")
        next_day_button = current_day_button.find_element(By.XPATH, "following-sibling::li[1]")
        next_day_button.click()

        print("Đã chọn ngày bay tiếp theo!")

        time.sleep(3)
    except Exception as e:
        print("Không thể chọn ngày bay tiếp theo:", e)
        driver.quit()

def select_departure_date(driver, date_str):
    """
    Hàm chọn ngày đi trong form tìm kiếm vé máy bay.

    Args:
        driver: Selenium WebDriver instance.
        date_str (str): Ngày cần chọn theo định dạng "dd-mm-yyyy" (ví dụ: "26-03-2025").
    """
    try:
        # Tìm input ngày đi bằng ID
        datepicker_icon = driver.find_element(By.CLASS_NAME, "ui-datepicker-trigger")
        datepicker_icon.click()
        time.sleep(1)
        
        target_date = datetime.strptime(date_str, "%d-%m-%Y")
        target_day = target_date.day
        target_month = target_date.month
        target_year = target_date.year

        datepicker_header = driver.find_element(By.CLASS_NAME, "ui-datepicker-title")
        current_month_year = datepicker_header.text.strip()  # Ví dụ: "March 2025"
        current_month = int(current_month_year.split("ng")[1].strip())

        print(f"📅 Tháng hiện tại: {current_month}")
        print(f"📅 Tháng cần chọn: {target_month}")

        if target_month - current_month >= 3:
            for i in range(target_month - current_month - 2):
                next_month_button = driver.find_element(By.XPATH, "//span[@class='ui-icon ui-icon-circle-triangle-e']")
                next_month_button.click()
                time.sleep(1)
                print("📅 Đã chuyển sang tháng tiếp theo.")

        day_select_button = driver.find_element(By.XPATH, f"//td[contains(@onclick, '_selectDay') and contains(@onclick, '{target_month-1},2025')]//span[@class='ui-datepicker-day' and text()='{target_day}']")
        day_select_button.click()
        print(f"✅ Đã chọn ngày đi: {date_str}")

    except Exception as e:
        print(f"❌ Lỗi khi chọn ngày đi: {e}")

def craw_pipeline(driver, url, departure, destination, date_str, end_date_str=None):
    # Truy cập trang web
    driver.get(url)
    time.sleep(1)  
    # departure_list = get_departure_place_list(driver)
    # time.sleep(2)  
    select_departure(driver, departure)
    time.sleep(1)  
    select_destination(driver, destination)
    time.sleep(1)  
    select_departure_date(driver, date_str)
    time.sleep(1)
    try:
        get_price_page = driver.find_element(By.ID, "cphMain_ctl00_usrSearchFormD2_btnSearch")
        get_price_page.click()
        print("Đã tìm nơi bay mới!")
    except Exception as e:
        print("Không thể tìm nơi bay mới:", e)
        driver.quit()
    time.sleep(1)

    output_file = f"flight_prices_{departure}_to_{destination}.csv"
    file_exists = os.path.exists(output_file)

    for i in range(100000):
        df = get_flight_prices(driver)
        if df.empty:
            print(f"⚠️ Không có dữ liệu trong ngày {i+1}, bỏ qua ghi file.")
        else:
            print(df.loc[0, 'Departure Time'])
            if "/".join(end_date_str.split("-")) in df.loc[0, 'Departure Time']:
                df.to_csv(output_file, mode='a', index=False, header=not file_exists)
                file_exists = True  # Đảm bảo các lần sau không ghi header nữa
                print("Kết thúc quá trình crawl")
                time.sleep(1)
                break
            df.to_csv(output_file, mode='a', index=False, header=not file_exists)
            file_exists = True  # Đảm bảo các lần sau không ghi header nữa
        time.sleep(1)
        choose_next_day(driver)
        time.sleep(1)
    driver.back()
    time.sleep(1)
    driver.quit()

url = "https://www.abay.vn"
# craw_pipeline(driver, url, "SGN", "HAN", "26-05-2025", "27-06-2025")
craw_pipeline(driver, url, "SGN", "DAD", "27-03-2025", "27-06-2025")