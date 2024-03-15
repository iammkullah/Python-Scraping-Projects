import os
import csv
import time
import shutil
import random
import pikepdf
import logging
import requests
import calendar
import pandas as pd
from io import BytesIO
from datetime import datetime
from bs4 import BeautifulSoup


base_url = "https://www.ahrinet.org/analytics/statistics/monthly-shipments"
job_name = "15171 AHRI Jack IPD Scrape using requests"
output_filename = (
    job_name.split("using")[0].strip().lower().replace(" ", "-") + "-sample.csv"
)
scrape_datetime = datetime.utcnow()
proxy_address = os.environ.get("HTTP_PROXY")

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

class Scraper:
    def __init__(self):
        self.MASTER_DF = pd.DataFrame()
        
        self.historical = None
        self.my_dir = None
        
        # Creating an empty DataFrame with specified column names
        self.output_columns = ['scrape_datetime', 'Month_to_Date_Units', 'Product_Type', 'month']
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
    # endregion 

    def extract_excel_link_from_pdf(self, pdf_content):
        try:
            excel_link = ''
            # Write the PDF content to a temporary file
            with open("temp_pdf.pdf", "wb") as f:
                f.write(pdf_content)
            
            pdf_file = pikepdf.Pdf.open("temp_pdf.pdf")    
            urls = []

            for page in pdf_file.pages:
                if "/Annots" in page.keys():
                    for annots in page.get("/Annots"):
                        if annots.get("/A") is not None:
                            url = annots.get("/A").get("/URI")
                            if url is not None:
                                urls.append(url)
                                urls.append(" ; ")
            for url in urls:
                if ".xls" in str(url):
                    excel_link = str(url)
            if not excel_link:
                excel_link = ''
                logging.info("No excel link in the pdf ...")
            
        except:
            logging.error("Error in extracting excel link from pdf ...")
            excel_link = ''
            
        finally:
            pdf_file.close()
            os.remove("temp_pdf.pdf")
            return excel_link
    
    def read_excel(self, df, month, year):
        
        try:
            
            df_excel = df
            df_excel = df_excel[['Product_Type', 'Month_to_Date_Units']]
            
            month_number = datetime.strptime(month, '%B').month
            year = int(year)
            
            last_day = calendar.monthrange(year, month_number)[1]
            
            # Create a date object for the last day of the month
            date_object = datetime(year, month_number, last_day)
            date_string = date_object.strftime('%Y/%m/%d')        
            
            df_excel['month'] = date_string
            df_excel['scrape_datetime'] = scrape_datetime
        except:
            logging.error("Something wrong with reading excel ...")
        
        finally:
            return df_excel
    

    def scrape_data(self, base_url: str) -> bool:
        logging.info(f"PROCESSING PAGE: {base_url}")
        success = True
        try:
            session = self.make_session()
            response = session.get(base_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('td')
                rows = reversed(rows)
                for row in rows:
                    year = row.find('h5').get_text(strip=True)
                    links = row.find_all('a')
                    
                    for link in links:
                        month = link.get_text(strip=True)
                        month_link = link['href']
                        
                        if "www.ahrinet.org" not in month_link:
                            month_link = "https://www.ahrinet.org" + month_link
                        
                        if month_link.endswith('.pdf'):
                            monthly_link_response = session.get(month_link)
                            if monthly_link_response.status_code == 200:
                                logging.info(f"Sucessful request to download PDF from {month_link} ...")
                                pdf_content = monthly_link_response.content
                                excel_link = self.extract_excel_link_from_pdf(pdf_content)
                                if not excel_link:
                                    logging.error(f"Something went wrong while getting excel_link from pdf for {month}, {year} ...")
                                    continue
                                excel_link_response = session.get(excel_link)
                                if excel_link_response.status_code == 200:
                                    logging.info(f"Sucessful request to download excel for {month} ...")
                                    # Read the content of the response (Excel file) into a BytesIO object
                                    excel_data = BytesIO(excel_link_response.content)
                                    
                                    # Read Excel file from BytesIO object into a DataFrame
                                    df = pd.read_excel(excel_data)
                                    data = self.read_excel(df, month, year)
                            else:
                                logging.error(f"Something wrong with the request for {month_link} ...")
                            
                        else:
                            monthly_link_response = session.get(month_link)
                            if monthly_link_response.status_code == 200:
                                logging.info(f"Sucessful request to {month_link} ...")
                                monthly_link_soup = BeautifulSoup(monthly_link_response.content, 'html.parser')
                                div = monthly_link_soup.find('div', class_='coh-container coh-wysiwyg')
                                links = div.find_all('a')
                                desire_link = links[-1]
                                excel_link = desire_link.get('href')
                                if not ".xls" in excel_link:
                                    logging.info(f"No excel link is present in {month}, {year} ...")
                                    continue
                                if "www.ahrinet.org" not in excel_link:
                                    excel_link = "https://www.ahrinet.org" + excel_link
                                
                                excel_link_response = session.get(excel_link)
                                if excel_link_response.status_code == 200:
                                    logging.info(f"Sucessful request to download excel for {month} ...")
                                    # Read the content of the response (Excel file) into a BytesIO object
                                    excel_data = BytesIO(excel_link_response.content)
                                    
                                    # Read Excel file from BytesIO object into a DataFrame
                                    df = pd.read_excel(excel_data)
                                    data = self.read_excel(excel_data, month, year)
                                
                            else:
                                logging.error(f"Something wrong with the request for {month_link} ...")
                            
                        self.output = pd.concat([self.output, data], axis = 0, ignore_index=True)
                        self.output = self.output.reset_index(drop=True)
                        logging.info(f"Done scrapping for Year: {year}, Month: {month}")
                        self.MASTER_DF = self.output
                        
                        if not self.historical:
                            return success
                        
            logging.info(f"All the data got scrape till {month}, {year}")
            self.MASTER_DF = self.output
            success = True
        except:
            logging.error("Issue with scraping ...")
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
            
            logging.error("SCRAPER FAILED. RETRYING...")

        return success

    # endregion


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
    else:
        logging.error("No data scraped ...")
        
if __name__ == "__main__":
    run(filename=output_filename)
    logging.info("ALL DONE")
