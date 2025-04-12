from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import undetected_chromedriver as uc
import tempfile

def init_driver(driver_path=None, headless=False, use_uc=True):
    """
    Initialize Chrome WebDriver with two modes:
    - use_uc=True: Use undetected_chromedriver to avoid bot detection.
    - use_uc=False: Use standard selenium.webdriver.Chrome.

    Args:
        driver_path (str): Path to the ChromeDriver executable.
        headless (bool): Whether to run in headless mode.
        use_uc (bool): Whether to use undetected_chromedriver.

    Returns:
        WebDriver: A Chrome WebDriver instance.
    """
    if use_uc:
        options = uc.ChromeOptions()
        if headless:
            options.add_argument("--headless")

        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        # Temporary user profile
        user_data_dir = tempfile.mkdtemp()
        options.add_argument(f"--user-data-dir={user_data_dir}")

        if driver_path is None:
            driver_path = "/home/gabien/chromedriver/chromedriver"

        driver = uc.Chrome(options=options, driver_executable_path=driver_path, headless=headless)
        return driver
    else:
        options = Options()
        if headless:
            options.add_argument("--headless")

        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        if driver_path is None:
            driver_path = "/usr/local/bin/chromedriver"

        service = Service(executable_path=driver_path)
        driver = webdriver.Chrome(service=service, options=options)
        return driver