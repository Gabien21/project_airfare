from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import undetected_chromedriver as uc

def init_driver(driver_path="/usr/local/bin/chromedriver", headless=False):
    options = uc.ChromeOptions()
    if headless:
        options.add_argument("--headless")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service(executable_path=driver_path)
    return webdriver.Chrome(service=service, options=options)