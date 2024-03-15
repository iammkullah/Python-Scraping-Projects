from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import requests
import logging
import random
import shutil
import time
import csv
import os


# region configuration
base_url = "https://www.npa.go.jp/publications/statistics/koutsuu/toukeihyo_e.html"
job_name = "14753 Marcel IPD Scrape using requests"
output_filename = (
    job_name.split("using")[0].strip().lower().replace(" ", "-") + "-sample.csv"
)
scrape_datetime = datetime.utcnow()
proxy_address = os.environ.get("HTTP_PROXY")


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://www.npa.go.jp/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
}

# endregion


# region DECORATORS
def retry(RETRY_START_SCRAPER):
    RETRIES = 5
    reattempt_delay_time = int(1.5 * 60)  # In seconds -> Actual delay will be +-20 sec

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


# endregion


# region Scraper_Class
class Scraper:
    def __init__(self):
        
        self.MASTER_DF = pd.DataFrame()
        self.historical = None
        self.my_dir = None
        

        # Add a global flag to track whether December has been processed
        self.december_processed = False

        self.dec_df = pd.DataFrame()
        self.month_df = pd.DataFrame()

        # Creating an empty DataFrame with specified column names
        self.output_columns = ['scrape_datetime', 'PERIOD_START', 'PERIODICITY', 'METRIC', 'UNIT', 'VALUE']
        self.output = pd.DataFrame(columns=self.output_columns)    

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

    # region MISCELLANEOUS_FUNCTIONS
    def get_proxies(self):
        if proxy_address:
            proxies = {"http": proxy_address, "https": proxy_address}
        else:
            proxies = None
        return proxies

    # endregion

    # region REQUESTS_SESSION_FUNCTIONS
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
    # endregion
    
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
    # endregion 

    
    
    # start helper fucntions
    def make_request(self, url, max_retries=3):
        session = self.make_session(headers)
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

    def read_nov_file(self, excel_path):
       self.month_df = pd.read_excel(excel_path, skiprows=5,  header=None, index_col=False)
       self.month_df = self.month_df.dropna(axis=1, how='all')
       self.month_df = self.month_df.dropna(axis = 0, how = 'all')
       nov_column_names = [
          "Month",
          "Traffic Accidents (Provisional)",
          "Traffic Accidents (Per Day)",
          "Traffic Accidents (Change)",
          "Traffic Accidents (Component ratio)",
          "Fatalities",
          "Fatalities (Per Day)",
          "Fatalities (Change)",
          "Fatalities (Component Ratio)",
          "Injuries (Provisional)",
          "Injuries (Per day)",
          "Injuries (Change)",
          "Injuries (Component ratio)"
       ]
       self.month_df.columns = nov_column_names
       self.month_df.reset_index(drop=True, inplace=True)
       
       return self.month_df
   
    def read_dec_file(self, excel_path):
        self.dec_df = pd.read_excel(excel_path, skiprows=4,  header=None, index_col=False, usecols=lambda x: x not in [0])
        self.dec_df = self.dec_df.dropna(axis = 1, how='all')
        self.dec_df = self.dec_df.dropna(axis = 0, how = 'all')
        
        dec_column_names = ['Year', 
                            'Traffic Accidents', 'Traffic Accidents (Index)', 
                            'Injuries', 'Injuries (Index)', 'Fatalities', 'Fatalities (Index)',  
                            'Fatalities per 100,000 persons', 'Fatalities per 100,000 persons (Index)']
        self.dec_df.columns = dec_column_names
        self.dec_df.reset_index(drop=True, inplace=True)
        
        return self.dec_df
     

    def update_output_dataframe(self, year, month):
        logging.info(f"Appending data for the {year} ...")
    
        month_dict = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
                      'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12}
        
        rows = []
        
        total_traffic_accident_year = 0
        total_fatalities_year = 0
        total_injuries_year = 0
        
        if self.dec_df is not None:
        
            for index, row in self.month_df.iterrows():
                
                month_month_df = row['Month']
                
                if month_month_df == 'Total':
                    total_traffic_accident_year = row['Traffic Accidents (Provisional)']
                    total_fatalities_year = row['Fatalities']
                    total_injuries_year = row['Injuries (Provisional)']
                    
                    if month == 'December':
                        dec_year = year
                        
                        if self.dec_df.empty or dec_year not in self.dec_df['Year'].values:
                            continue
                        
                        dec_traffic_accidents = self.dec_df.loc[self.dec_df['Year'] == dec_year, 'Traffic Accidents'].values[0]
                        dec_fatalities = self.dec_df.loc[self.dec_df['Year'] == dec_year, 'Fatalities'].values[0]
                        dec_injuries = self.dec_df.loc[self.dec_df['Year'] == dec_year, 'Injuries'].values[0]

                        dec_traffic_accident_year = dec_traffic_accidents - total_traffic_accident_year
                        dec_fatalities_year = dec_fatalities - total_fatalities_year
                        dec_injuries_year = dec_injuries - total_injuries_year
                        
                        rows.append({'scrape_datetime': scrape_datetime.isoformat(), 'PERIOD_START': f"{dec_year}-12-01", 'PERIODICITY': 'M',
                                     'METRIC': 'Traffic Accidents (Provisional)', 'UNIT': 'Number', 'VALUE': dec_traffic_accident_year})
                        rows.append({'scrape_datetime': scrape_datetime.isoformat(), 'PERIOD_START': f"{dec_year}-12-01", 'PERIODICITY': 'M',
                                     'METRIC': 'Fatalities', 'UNIT': 'Number', 'VALUE': dec_fatalities_year})
                        rows.append({'scrape_datetime': scrape_datetime.isoformat(), 'PERIOD_START': f"{dec_year}-12-01", 'PERIODICITY': 'M',
                                     'METRIC': 'Injuries (Provisional)', 'UNIT': 'Number', 'VALUE': dec_injuries_year})
                    
                    continue
                
                if month_month_df == 'December':
                    continue
                
                month_num = month_dict.get(month_month_df)
            
                period_start = f"{year}-{month_num:02d}-01"
                periodicity = 'M'
            
                metrics = ['Traffic Accidents (Provisional)', 'Fatalities', 'Injuries (Provisional)']
                unit = 'Number'
            
                for metric in metrics:
                    metric_value = row[metric]
            
                    new_row = {'scrape_datetime': scrape_datetime.isoformat(), 'PERIOD_START': period_start, 'PERIODICITY': periodicity,
                               'METRIC': metric, 'UNIT': unit, 'VALUE': metric_value}
            
                    rows.append(new_row)
        
        self.output = pd.concat([self.output, pd.DataFrame(rows)], ignore_index=True)

        self.output = self.output.drop_duplicates()
        
        self.output = self.output.dropna(axis=1, how='all')
        
        return self.output

    def download_and_rename_xlsx(self, excel_url, download_path, month, year):
        
        response = self.make_request(excel_url)
        
        if response.status_code == 200:
        
            downloaded_file = f"{download_path}/{month}_data.xlsx"  

            with open(downloaded_file, 'wb') as file:
                file.write(response.content)
            logging.info(f"Excel file for {month} downloaded and saved as {downloaded_file}.")
            
            if month != 'December':
                
                self.month_df = self.read_nov_file(downloaded_file)
        
                self.output = self.update_output_dataframe(year, month)
                
                
            elif month == 'December':
                self.dec_df = self.read_dec_file(downloaded_file)
                logging.info(f"Reading the {month} file ...")
                self.output = self.update_output_dataframe(year, month)
            
        else:
            logging.error(f"Failed to download Excel file for {month} .")
       
    def download_and_rename_xls(self, excel_url, download_path, month, year):
        
        response = self.make_request(excel_url)
        
        if response.status_code == 200:
            
            downloaded_file = f"{download_path}/{month}_data.xls"  

            with open(downloaded_file, 'wb') as file:
                file.write(response.content)

            logging.info(f"Excel file for {month} downloaded and saved as {downloaded_file}.")
            
            if month != 'December':
                
                self.month_df = self.read_nov_file(downloaded_file)
        
                self.output = self.update_output_dataframe(year, month)
                
                
            elif month == 'December':
                self.dec_df = self.read_dec_file(downloaded_file)
                
                self.output = self.update_output_dataframe(year, month)
                    
        else:
            logging.error(f"Failed to download Excel file for {month} .")
            
    def download_csv(self, url, download_path, month, year):
        
        if month == 'December' and self.december_processed:
            logging.info("December data has already been downloaded ...")
            
            logging.info(f"Appending the december data for the {year} ...")
            self.output = self.update_output_dataframe(year, month)
            return None    
        
        response = self.make_request(url)
        if response.status_code == 200:
            logging.info(f"Successful reqesut at monthly report page for {month}:{year} at :{url}")
            try:
                soup = BeautifulSoup(response.content, 'html.parser')
                div = soup.find('div', class_ = 'stat-dataset_list-body')
                articles = div.find_all('article', class_ = 'stat-dataset_list-item')
                
                target_li = None  
                excel_link = None 
                for article in articles:
                    if '1-2' in article.text or '1-3' in article.text:
                        uls = article.find('ul')
                        lis = uls.find_all('li')
                        for li in lis:
                            if month != 'December':
                                if '1-2' in li.text:
                                    target_li = li
                                    break
                            if month == 'December':
                                if '1-3' in li.text:
                                    target_li = li
                                    self.december_processed = True  
                                    break
                            else:
                                logging.info(f'Skipping {month} ...')
                                return 
                    else:
                        continue
                    
                if target_li is not None:            
                    next_sibling = target_li.find_next_sibling()
                    try:
                        excel_link = next_sibling.find('a', class_ = 'stat-dl_icon stat-icon_4 stat-icon_format js-dl stat-download_icon_left')
                        excel_url = f"https://www.e-stat.go.jp{excel_link['href']}"
                        
                    except:
                        excel_link = next_sibling.find('a', class_ = 'stat-dl_icon stat-icon_0 stat-icon_format js-dl stat-download_icon_left')
                        excel_url = f"https://www.e-stat.go.jp{excel_link['href']}"
                          
                    logging.info(f"Downaloding and renaming the file for {month}...")
                    self.download_and_rename_xlsx(excel_url, download_path, month, year)
                    
                elif excel_link is None:
                    excel_link = soup.find('a', class_ = 'stat-dl_icon stat-icon_0 stat-icon_format js-dl stat-download_icon_left')
                    excel_url = f"https://www.e-stat.go.jp{excel_link['href']}"
                
                    logging.info(f"Downaloding and renaming the file for {month}...")
                    self.download_and_rename_xls(excel_url, download_path, month,  year)
                    
                else:
                    logging.error(f"Traffic accidents occurrence by month data is not available in {month}")
                    
                if month == 'November':
                    
                    if not self.december_processed:
                        month = "December"
                        dec_url = 'https://www.e-stat.go.jp/en/stat-search/files?page=1&layout=datalist&toukei=00130002&tstat=000001032793&cycle=7&year=20230&month=0&tclass'
                        
                        dec_response = self.make_request(dec_url) 

                        if dec_response.status_code == 200:
                            print("Successful request at dec_url:", dec_url)
                            
                            soup = BeautifulSoup(dec_response.content, 'html.parser')
                            div = soup.find('div', class_ = 'stat-dataset_list-body')
                            article = div.find('article', class_ = 'stat-dataset_list-item')

                            try:
                                excel_link = article.find('a', class_ = 'stat-dl_icon stat-icon_4 stat-icon_format js-dl stat-download_icon_left')
                                excel_url = f"https://www.e-stat.go.jp{excel_link['href']}"
                                
                            except:
                                excel_link = articles.find('a', class_ = 'stat-dl_icon stat-icon_0 stat-icon_format js-dl stat-download_icon_left')
                                excel_url = f"https://www.e-stat.go.jp{excel_link['href']}"
                                
                            print("Downaloding and renaming the file for december data ...")
                            self.download_and_rename_xlsx(excel_url, download_path, month, year)
                            self.december_processed = True
                            
            except:
                logging.error(f"Different layout of this particular page for {month}")
            
        else:
            logging.error(f"Request failed for {month} while downloading file at {url} with status code: {response.status_code}")


    

    def scrape_data(self, base_url: str) -> bool:
        logging.info(f"PROCESSING PAGE: {base_url}")
        
        success = True
        # region DEFINE_YOUR_SCRAPER
        
        response = self.make_request(base_url)

        if response.status_code == 200:
            logging.info(f"Successful reqesut at {base_url}")
            
            soup = BeautifulSoup(response.content, 'html.parser')
            div = soup.find('div', class_="anchorLink")
            next_sibling = div.find_next_sibling()
            
            monthly_reports = []
            
            for h4_tag in next_sibling.find_all('h4'):
                
                year_str = ''.join(filter(str.isdigit, h4_tag.text.strip('【 】 New!\xa0')))
                year = int(year_str)
                
                if year >= 2019:
                    
                    for a_tag in h4_tag.find_next('p').find_all('a'):
                        month = a_tag.text.strip()
                        link = a_tag['href']
                        monthly_reports.append({
                            'Year': year,
                            'Month': month,
                            'Link': link
                        })
                    
                    logging.info(f"Monthly data got fetched for {year}....") 
                    
                    last_month = monthly_reports[-1]
                    
                    if last_month['Month'] != 'December':
                        url = last_month['Link']
                        month = last_month['Month']
                        year = last_month['Year']
                        
                        year_folder = f"{os.getcwd()}/{year}"
                        os.makedirs(year_folder, exist_ok=True)
                        
                        self.download_csv(url, year_folder, month, year)
                        
                    else:
                        
                        last_two_months = monthly_reports[-2:]
                        for month_info in last_two_months:
                            url = month_info['Link']
                            month = month_info['Month']
                            year = month_info['Year']
                            
                            year_folder = f"{os.getcwd()}/{year}"
                            os.makedirs(year_folder, exist_ok=True)
                            
                            self.download_csv(url, year_folder, month, year)
                            
                    shutil.rmtree(year_folder)
                    if not self.historical:
                        logging.info(f"Done with scraping data for {year}")
                        self.MASTER_DF = self.output
                        break
                                
                else:
                    logging.info(f"Done with scraping data till {year + 1}")
                    self.MASTER_DF = self.output
                    break
                       
        else:
            logging.error(f"Request failed with status code: {response.status_code}")
            success = False 
        
        # endregion
        return success

    # region START_SCRAPER
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

    # endregion


def run(filename: str):
    scraper = Scraper()
    success = scraper.start_scraper(historical=True)

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
    else:
        logging.error("No data scraped ...")
    

if __name__ == "__main__":
    run(filename=output_filename)
    logging.info("ALL DONE")

