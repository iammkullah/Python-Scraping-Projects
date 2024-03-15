import os
import time
import csv
import shutil
import random
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


base_url = "https://www1.hkexnews.hk/search/titlesearch.xhtml"
job_name = "15206_HKEX_David_IPD Scrape using requests"
output_filename = (
    job_name.split("using")[0].strip().lower().replace(" ", "-") + "-sample.csv"
)
scrape_datetime = datetime.utcnow()
proxy_address = os.environ.get("HTTP_PROXY")

headers = {
    'authority': 'www1.hkexnews.hk',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'content-type': 'application/x-www-form-urlencoded',
    # 'cookie': 'JSESSIONID=e3hDumvG-qso0WzzK9Myw3pZJQ_qzBgfWTkx30HO.s106; TS014a5f8b=015e7ee6035d4a85d641012985a8b1404051f01ab31368731f716ef39095eca8abba6afae95ade1c7423e6949c217926d41c8767568b51b4669c0094f5c07f35eae492ba7a; OptanonAlertBoxClosed=2024-03-15T17:46:59.322Z; AMCV_DD0356406298B0640A495CB8%40AdobeOrg=179643557%7CMCIDTS%7C19798%7CMCMID%7C49991872597725185027758006491147405703%7CvVersion%7C5.5.0; s_cc=true; TS38b16b21027=086f2721efab20009e22524ec26f0263c6c5ad13110bc177ea046d512006642594c00dbbc54d858f08d31d8a9e1130000b0b6581b251caf555527c295578206513d46e8a843c9e178e1e4cb5c621eefc968c86d8b35a65832d22fdb22c0688b1; mp_f99fd93d342102be249005dee41b33da_mixpanel=%7B%22distinct_id%22%3A%20%22%24device%3A18e433919a554d-08671725eb7bcd-26001b51-144000-18e433919a554d%22%2C%22%24device_id%22%3A%20%2218e433919a554d-08671725eb7bcd-26001b51-144000-18e433919a554d%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%7D; OptanonConsent=isGpcEnabled=0&datestamp=Fri+Mar+15+2024+22%3A57%3A56+GMT%2B0500+(Pakistan+Standard+Time)&version=202303.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0004%3A1%2CC0002%3A1&geolocation=PK%3BIS&AwaitingReconsent=false; TS0168982d=015e7ee603e9565f167e35ef31d05e076855ef2f0a4bf451307cdfaf946d46df34f688a73f84f7fbbb79f2342a21c9e7fc4cf87a5f; TS4e849b71027=08754bc291ab2000cd14056f79ae823a1b33679f525f45de6c338d60352c58fec331ccbf5aae8bb7086922915e11300072ff679dc87e8d422ad4cb3ae1028a8c2850d3644be6b838dc2b9993b95cd0bfa6e4cf1f920e2dc848c4f1b998bfd313',
    'origin': 'https://www1.hkexnews.hk',
    'referer': 'https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=en',
    'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
}

params = {
    'lang': 'en',
}

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
    
    def date_range(self):
        today = datetime.now().date()
        one_month_ago = today - timedelta(days=30)

        return one_month_ago, today


    def scrape_data(self, base_url: str) -> bool:
        logging.info(f"PROCESSING PAGE: {base_url}")
        
        success = True
        try:
            from_date, to_date = self.date_range()
            title = "CSRC"
            
            session = self.make_session(headers, params = params)
        
            data = {
                'lang': 'EN',
                'category': '0',
                'market': 'SEHK',
                'searchType': '0',
                'documentType': '-1',
                't1code': '-2',
                't2Gcode': '-2',
                't2code': '-2',
                'stockId': '-1',
                'from': from_date.strftime('%Y%m%d'),
                'to': to_date.strftime('%Y%m%d'),
                'MB-Daterange': '0',
                'title': title,
            }
            
            response = session.post(base_url, data=data)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            data = []
            
            table_body = soup.find('tbody')
            rows = table_body.find_all('tr')
            
            for row in rows:
                keyword = soup.find('input', {'id': 'newsTitle'}).get('value')
                stock_code = row.find('td', {'class': 'stock-short-code'}).get_text().strip().split('Stock Code: ')[1]
                stock_name = row.find('td', {'class': 'stock-short-name'}).get_text().strip().split('Stock Short Name: ')[1]
                news_id = row.find('a').get('href').split('/')[-1].split('.')[0]
                news_date_time = row.find('td', {'class': 'release-time'}).get_text().strip().split('Release Time: ')[1]
                news_link = 'https://www1.hkexnews.hk' + row.find('a').get('href')
                headline = row.find('div', {'class': 'headline'}).get_text().strip()
                text = row.find('a').get_text().strip()
                news_title = headline + " " + text
                news_text = news_title
                scrape_time = scrape_datetime  
            
                data.append({
                    'keyword': keyword,
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'news_id': news_id,
                    'news_date_time': news_date_time,
                    'news_link': news_link,
                    'news_title': news_title,
                    'news_text': news_text,
                    'scrape_datetime': scrape_time
                })
            
            self.MASTER_DF = pd.DataFrame(data)
            
        except:
            success = False

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
    
    if not outputs.empty:
        if outputs.columns[-1] == "scrape_datetime":
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
    else:
        logging.error("No data scraped ...")


if __name__ == "__main__":
    run(filename=output_filename)
    logging.info("ALL DONE")

