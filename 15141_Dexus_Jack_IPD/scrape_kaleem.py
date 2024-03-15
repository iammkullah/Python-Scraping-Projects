# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 17:42:36 2024

@author: Muhammad Kaleem Ullah
"""
import os
import csv
import time
import random
import logging
import requests
import warnings
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Ignore all warnings
warnings.filterwarnings("ignore")

proxy_address = os.environ.get("HTTP_PROXY")

# region Scraper_Class
class Scraper:
    def __init__(self):
        self.MASTER_LIST = []
        self.MASTER_DF = pd.DataFrame()
        self.driver = None
        self.historical = None
        self.my_dir = None

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

        

    # region MISCELLANEOUS_FUNCTIONS
    def get_proxies(self):
        if proxy_address:
            proxies = {"http": proxy_address, "https": proxy_address}
        else:
            proxies = None
        return proxies

    # endregion

    # region SELENIUM_WEB_DRIVER_FUNCTIONS
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
        # self.driver.implicitly_wait(30)
        self.wait_for_page_to_load()


    def wait_for_page_to_load(self):
        while not self.driver.execute_script(
            "return document.readyState === 'complete';"
        ):
            time.sleep(1)  # Wait for 1 second before checking again
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
        # Get the HTML content of the current page
        html_content = self.driver.page_source

        # Create a BeautifulSoup object
        soup = BeautifulSoup(html_content, 'html.parser')

        return soup



# Function to make a request and handle retries
def make_request(url, headers=None, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an HTTPError for bad responses
            return response
        except requests.RequestException as e:
            print(f"Error: {e}. Retrying in 5 seconds...")
            time.sleep(random.uniform(5, 10))

    print(f"Failed to retrieve data after {max_retries} attempts")
    return response

# Function to scrape property page
def scrape_property_page(property_url):
    address = ""  # Initialize address variable
    try:
        property_url_response = make_request(property_url)
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
        print(f"Error while scraping property page: {e}")
        return property_data# Return an empty DataFrame on error

# Main scraping function
def scrape_main_page(base_url):
    # Define column names
    columns = ['scrape_datetime', 'data_url', 'Address', 'Level', 'Space options', 'Availability', 'Price from', 'Outgoings', 'Floor area', 'Fitout']
    # Create an empty DataFrame
    df = pd.DataFrame(columns=columns)
    
    main_page_url = base_url + "/leasing/office"
    main_page_response = make_request(main_page_url)

    if main_page_response.status_code == 200:
        print("Successful request for the base URL.")
        scrape_datetime = datetime.utcnow()
        main_page_soup = BeautifulSoup(main_page_response.content, 'html.parser')
        office_availabilities_li = main_page_soup.find_all('li', class_='nav-item active has-sub-menu')
        
        for availability in office_availabilities_li:
            availability_div = availability.find('div', class_='collapse sub-menu')
            
        availability_links = availability_div.find_all('span', class_='main-link-area')

        for link in availability_links:
            data_link = link['data-link']
            availability_url = base_url + data_link
            print(f"Using selenium to while opening on the link: {availability_url}")
            class_instance = Scraper()
            class_instance.load_driver()
            class_instance.driver.get(availability_url)
            class_instance.wait_for_page_to_load()
            # Define a while loop to click the "Load More" button until it's not found
            while True:
                try:
                    # Find the "Load More" button element
                    load_more_button = WebDriverWait(class_instance.driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "property-load-more-btn"))
                    )
                    # Click the "Load More" button
                    load_more_button.click()
                    # Wait for the page to load again
                    class_instance.wait_for_page_to_load()
                except:
                    # If the "Load More" button is not found, exit the loop
                    break
            # Wait for the page to load again
            class_instance.wait_for_page_to_load()
            
            property_soup = class_instance.get_soup(class_instance.driver)
            class_instance.driver.close()
            available_properties_div = property_soup.find_all('div', class_='properties-component col-sm-4')
            print(f"Number of properties currently available for lease are {len(available_properties_div)} ...")
            for available_property in available_properties_div:
                property_title_element = available_property.find('a')
                property_href = property_title_element.get('href')
                property_title = property_title_element.get('title')
                property_url = base_url + property_href
                
                print(f"Scraping data for {property_title}...")
                property_data = scrape_property_page(property_url)

                if not property_data.empty:
                    property_data['scrape_datetime'] = scrape_datetime
                    property_data['data_url'] = property_url
                    # Concatenate the data to the main DataFrame
                    df = pd.concat([df, property_data], ignore_index=True, axis = 0)
                    print(f"Done scraping data for {property_title}.")
                else:
                    print(f"Problem in the property page for {property_title}")
                    
        if not df.empty:
            print("Done with scraping all data.")
            return df
        else:
            print("No valid data scraped.")
    else:
        print("Error: Something went wrong with the base URL request!")

    return pd.DataFrame()

# Run the main scraping function
base_url = "https://www.dexus.com"
df = scrape_main_page(base_url)

# Save DataFrame to CSV
# Assuming 'output' is your DataFrame
df.to_csv('output.csv', encoding='utf-8', quotechar='"', quoting=csv.QUOTE_ALL, index=False)