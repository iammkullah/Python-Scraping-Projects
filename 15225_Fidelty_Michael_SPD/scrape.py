import os
import time
import csv
import shutil
import random
import logging
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

base_url = "https://www.actionsxchangerepository.fidelity.com/ShowDocument/ComplianceEnvelope.htm"
job_name = "15225_Fidelty_Michael_SPD Scrape using requests/selennium"
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

    def create_dir(self):

        temp = "resources"
        my_dir = os.path.abspath(temp)

        if not os.path.exists(my_dir):
            os.makedirs(my_dir)
            logging.info(f'Folder {temp} created in {my_dir}')
        else:
            logging.info(f'Folder {temp} already exists in {my_dir}')
        
        return my_dir

    def remove_dir(self):

        if os.path.isdir(self.my_dir):
            shutil.rmtree(self.my_dir)
            logging.info(f'{self.my_dir} removed successfully.')
        else:
            logging.info(f'{self.my_dir} do not exists...')
 
    def parse_stock_data(self, df, etf_ticker, etf_name, holdings_date):
        stock_data = {
        'scrape_datetime': [],
        'etf_ticker': [],
        'etf_name': [],
        'holdings_date':[],
        'ticker': [],
        'isin': [],
        'security_name': [],
        'security_type': [],
        'shares': [],
        'value': [],
        'pct_assets': []
        }
        
        for index, row in df.iterrows():
            stock_data['scrape_datetime'].append(scrape_datetime)
            stock_data['etf_ticker'].append(etf_ticker)
            stock_data['etf_name'].append(etf_name)
            stock_data['holdings_date'].append(holdings_date)
            stock_data['ticker'].append(row['Ticker'])
            stock_data['isin'].append(row['ISIN'])
            stock_data['security_name'].append(row['Security Name'])
            stock_data['security_type'].append(row['Security Type'])
            stock_data['shares'].append(row['Quantity Held'])
            stock_data['value'].append(row['Market Value'])
            stock_data['pct_assets'].append(row['% of Net Assets'])
            
        stock_data_df = pd.DataFrame(stock_data)
        
        return stock_data_df
    
    def getting_data(self, session, excel_url, etf_ticker):
        try:
            excel_url_response = session.get(excel_url)
            if excel_url_response.status_code == 200:
                logging.info(f"Sucessful request to load excel for {etf_ticker} ...")
                
                excel_data = BytesIO(excel_url_response.content)
                df = pd.read_excel(excel_data)
                
                holdings_date = df.iloc[1, 1]
                parsed_date = datetime.strptime(holdings_date, '%d-%b-%y')
                holdings_date = parsed_date.strftime('%m-%d-%Y')
                
                header_index = -1
                for index, row in df.iterrows():
                    if 'Ticker' in row.values and 'ISIN' in row.values:
                        header_index = index
                        break
                if header_index != -1:
                    new_header = df.iloc[header_index + 1:]  
                    new_header.columns = df.iloc[header_index].tolist() 
                    new_header.reset_index(drop=True, inplace=True)
                    df = new_header
                else:
                    logging.error("Ticker and ISIN not found in any row.")
                
                total_index = -1
                for index, row in df.iterrows():
                    if 'Total:' in row.values:
                        total_index = index
                        break
    
                if total_index != -1:
                    df = df.drop(df.index[total_index:])
                    df.reset_index(drop=True, inplace=True)
                else:
                    logging.error("Total: not found in any row.")
                
                return holdings_date, df
                
            else:
                logging.error(f"Something wrong while loading excel for {etf_ticker} ...")
        except:
            logging.error(f"Something wrong while loading excel for {etf_ticker} ...")
            return pd.DataFrame()
    
    def scrape_data(self, base_url: str) -> bool:
        logging.info(f"PROCESSING PAGE: {base_url}")
        
        session = self.make_session()
        
        cwd = os.getcwd()
        file_name = "table1.csv"
        file_path = os.path.join(cwd, file_name)
        table1 = pd.read_csv(file_path)
        self.load_driver()
        
        for index, row in table1.iterrows():
            etf_ticker = row['etf_ticker']
            etf_name = row['etf_name']
            url = row['url']
            try:
                self.driver.get(url)
                self.wait_for_page_to_load()
                daily_tab = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="DALYTab"]')))
                daily_tab.click()
                
                excel_link = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//a[contains(@href, "documentExcel.htm")]'))
                )
                
                excel_url = excel_link.get_attribute('href')
            
            except:
                excel_url = "Not Available"
            
            logging.info(f"Getting data for ETF Ticker: {etf_ticker}")
            if excel_url != "Not Available":
                logging.info(f"Got the Daily Holdings Report link for {etf_ticker}")
                
                holdings_date, df = self.getting_data(session, excel_url, etf_ticker)
                if not df.empty:
                    data = self.parse_stock_data(df,etf_ticker, etf_name, holdings_date)
                    self.MASTER_DF = pd.concat([self.MASTER_DF, data], axis = 0, ignore_index=True)
                    self.MASTER_DF = self.MASTER_DF.reset_index(drop=True)
                else:
                    logging.info(f"Something went wront with getting Daily Holding Data for {etf_ticker}")
                    
            else:
                logging.info(f"No Daily Holdings Report link for {etf_ticker}")
                
        if not self.MASTER_DF.empty:
            success = True
        else:
            success = False
        self.driver.close()
        return success

    @retry
    def start_scraper(self, historical) -> list:
        self.historical = historical
        page_url = f"{base_url}"
        success = self.scrape_data(page_url)

        if not success:
            self.MASTER_DF = pd.DataFrame()
            self.remove_dir()
            logging.error("SCRAPER FAILED. RETRYING...")

        return success

def run(filename: str):
    scraper = Scraper()
    success = scraper.start_scraper(historical=False)

    if not success:
        logging.error("FINAL ATTEMPT FAILED. EXITING...")
        return

    outputs = scraper.MASTER_DF
    
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

