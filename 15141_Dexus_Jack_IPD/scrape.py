from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium import webdriver
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import warnings
import requests
import logging
import random
import time
import csv
import os

warnings.filterwarnings("ignore")



base_url = "https://www.dexus.com"
job_name = "15141_Dexus_Jack_IPD Scrape using requests"
output_filename = (
    job_name.split("using")[0].strip().lower().replace(" ", "-") + "-sample.csv"
)
scrape_datetime = datetime.utcnow()
proxy_address = os.environ.get("HTTP_PROXY")


def retry(RETRY_START_SCRAPER):
    RETRIES = 5
    reattempt_delay_time = int(1.5 * 60)

    def wrapper(self, *args, **kwargs):
        success = RETRY_START_SCRAPER(self, *args, **kwargs)

        if not success:
            for attempt in range(2, RETRIES + 1):
                logging.info(
                    f"###########  "
                    f"{attempt}/{RETRIES} ATTEMPT - "
                    f"RETRYING IN AROUND {reattempt_delay_time // 60} MIN AND {reattempt_delay_time % 60} SECONDS..."
                    f"  ###########"
                )

                time.sleep(
                    random.uniform(reattempt_delay_time - 20, reattempt_delay_time + 20)
                )
                success = RETRY_START_SCRAPER(self, *args, **kwargs)

                if success:
                    break
        return success

    return wrapper


class Scraper:
    def __init__(self):
        self.MASTER_DF = pd.DataFrame()
        

        self.DEBUG = False
        if self.DEBUG:
            logging.basicConfig(
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                level=logging.DEBUG,
                datefmt="%d-%b-%y %H:%M:%S",
            )
        else:
            logging.basicConfig(
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                level=logging.INFO,
                datefmt="%d-%b-%y %H:%M:%S",
            )

        logging.info(f"STARTING SCRAPE... {job_name}")
        time.sleep(2)

    def get_proxies(self):
        if proxy_address:
            proxies = {"http": proxy_address, "https": proxy_address}
        else:
            proxies = None
        return proxies

    def load_driver(self, downloads_path=None):
        hdr = "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 \
          (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36"
        proxies = self.get_proxies()

        prefs = {}
        if downloads_path is not None:
            if not downloads_path:
                os.mkdir(downloads_path)
            prefs = {
                "download.default_directory": downloads_path,
            }

        # Chrome options
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("start-maximized")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_argument("--allow-running-insecure-content")
        chrome_options.add_argument("--window-size=1366,768")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_experimental_option("prefs", prefs)

        if proxy_address:
            chrome_options.add_argument('--proxy-server='+proxy_address)

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.driver.maximize_window()

        self.driver.get("https://www.google.com/")
        self.wait_for_page_to_load()


    def wait_for_page_to_load(self):
        while not self.driver.execute_script(
            "return document.readyState === 'complete';"
        ):
            time.sleep(1)
        time.sleep(1.5)

    def wait_for_element(driver, holder, xpath, name=None, t=10, all=False):
        if name:
            print(f"waiting for {name}")
        element = None
        tries = t
        while tries:
            try:
                if all:
                    element = driver.find_elements(holder, xpath)
                else:
                    element = driver.find_element(holder, xpath)
                break
            except:
                if name:
                    print(f"waiting for {name}")
                time.sleep(1)
                tries -= 1
        return element
    
    def get_soup(self, driver):
        html_content = self.driver.page_source

        soup = BeautifulSoup(html_content, 'html.parser')

        return soup

    def make_session(self, headers=None, params=None):
        s = requests.Session()
        proxies = self.get_proxies()
        if proxies is not None:
            s.proxies.update(proxies)
        if headers is not None:
            s.headers.update(headers)
        if params is not None:
            s.params.update(params)
        
        return s

    def make_request(self, url, max_retries=3):
        session = self.make_session()
        for attempt in range(max_retries):
            time.sleep(random.uniform(2.5, 3.5))
            response = session.get(url, timeout = 90)
            if response.status_code == 200:
                return response
            else:
                logging.error(f"Error: {response.status_code}. Retrying in 5 seconds...")
                time.sleep(random.uniform(5, 10))
        logging.error(f"Failed to retrieve data after {max_retries} attempts")
        return response


    def scrape_property_page(self, property_url):
        address = ""
        try:
            property_url_response = self.make_request(property_url)
            property_page_soup = BeautifulSoup(property_url_response.content, 'html.parser')
            address_div = property_page_soup.find('div', class_='address-bar')
            address = address_div.find('p').text.strip()

            availibility_div = property_page_soup.find('div', class_='component availability col-12 theme-enabled')
            table = availibility_div.find('table')

            headers = [th.text.strip() for th in table.find('thead').find_all('th')]
            headers = [header.replace(", pa", "") if header.endswith(", pa") else header for header in headers]
            headers = [header.replace("Space Options", "Space options").replace("Fit-out", "Fitout") for header in headers]

            rows = []
            for tr in table.find('tbody').find_all('tr'):
                row = [td.text.strip() for td in tr.find_all('td')]
                rows.append(row)

            df = pd.DataFrame(rows, columns=headers)
            df = df.loc[:, df.columns.notnull() & (df.columns != '')]
            df['Address'] = address

            return df
        except Exception as e:
            # Create a DataFrame with the data_url column
            property_data = pd.DataFrame({'data_url': [property_url]})
            logging.error(f"Error while scraping property page: {e}")
            return property_data# Return an empty DataFrame on error

    def scrape_main_page(self, base_url):
        
        columns = ['scrape_datetime', 'data_url', 'Address', 'Level', 'Space options', 'Availability', 'Price from', 'Outgoings', 'Floor area', 'Fitout']
        
        df = pd.DataFrame(columns=columns)
        
        main_page_url = base_url + "/leasing/office"
        main_page_response = self.make_request(main_page_url)

        if main_page_response.status_code == 200:
            logging.info("Successful request for the base URL.")
            
            main_page_soup = BeautifulSoup(main_page_response.content, 'html.parser')
            office_availabilities_li = main_page_soup.find_all('li', class_='nav-item active has-sub-menu')
            
            for availability in office_availabilities_li:
                availability_div = availability.find('div', class_='collapse sub-menu')
                
            availability_links = availability_div.find_all('span', class_='main-link-area')

            for link in availability_links:
                data_link = link['data-link']
                availability_url = base_url + data_link
                logging.info(f"Using selenium to while opening on the link: {availability_url}")

                self.load_driver()
                self.driver.get(availability_url)
                self.wait_for_page_to_load()
                # Define a while loop to click the "Load More" button until it's not found
                while True:
                    try:
                        # Find the "Load More" button element
                        load_more_button = WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "property-load-more-btn"))
                        )
                        # Click the "Load More" button
                        load_more_button.click()
                        # Wait for the page to load again
                        self.wait_for_page_to_load()
                    except:
                        # If the "Load More" button is not found, exit the loop
                        break
                # Wait for the page to load again
                self.wait_for_page_to_load()
                
                property_soup = self.get_soup(self.driver)
                self.driver.close()
                available_properties_div = property_soup.find_all('div', class_='properties-component col-sm-4')
                for available_property in available_properties_div:
                    property_title_element = available_property.find('a')
                    property_href = property_title_element.get('href')
                    property_title = property_title_element.get('title')
                    property_url = base_url + property_href
                    
                    logging.info(f"Scraping data for {property_title}...")
                    property_data = self.scrape_property_page(property_url)

                    if not property_data.empty:
                        property_data['scrape_datetime'] = scrape_datetime
                        property_data['data_url'] = property_url
                        df = pd.concat([df, property_data], ignore_index=True, axis = 0)
                        logging.info(f"Done scraping data for {property_title}.")
                    else:
                        logging.error(f"Problem in the property page for {property_title}")

            if not df.empty:
                logging.info("Done with scraping all data.")
                self.MASTER_DF = df
                success = True
                
            else:
                logging.error("No valid data scraped.")
                success= False
        else:
            logging.error("Error: Something went wrong with the base URL request!")
            success = False
            
        return success
   

    def scrape_data(self, base_url: str) -> bool:
        logging.info(f"PROCESSING PAGE: {base_url}")
        success = self.scrape_main_page(base_url)
       
        return success

    @retry
    def start_scraper(self, historical) -> list:
        self.historical = historical
        page_url = f"{base_url}"
        success = self.scrape_data(page_url)

        if not success:
            self.MASTER_DF = pd.DataFrame()
            logging.error("SCRAPER FAILED. RETRYING...")

        return success

def run(filename: str):
    scraper = Scraper()
    success = scraper.start_scraper(historical=False)

    if not success:
        logging.error("FINAL ATTEMPT FAILED. EXITING...")
        return

    outputs = scraper.MASTER_DF
    
    if not outputs.empty:
        if outputs.columns[0] == "scrape_datetime":
            logging.info("GENERATING FINAL OUTPUT...")
            outputs.to_csv(
                filename,
                encoding="utf-8",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                index=False,
            )
        else:
            logging.info(
                'MISSING "scrape_datetime" COLUMN OR IT IS NOT THE FIRST COLUMN. CSV FILE NOT GENERATED.'
            )

if __name__ == "__main__":
    run(filename=output_filename)
    logging.info("ALL DONE")

