# -*- coding: utf-8 -*-
"""
Created on Tue Jan 23 12:34:47 2024

@author: Muhammad Kaleem Ullah
"""

import os
import csv
import time
import shutil
import random
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

# URL and headers based on the provided information
base_url = "https://www.npa.go.jp/publications/statistics/koutsuu/toukeihyo_e.html"

# Add a global flag to track whether December has been processed
december_processed = False
historical = False

dec_df = pd.DataFrame()
month_df = pd.DataFrame()

# Creating an empty DataFrame with specified column names
output_columns = ['scrape_datetime', 'PERIOD_START', 'PERIODICITY', 'METRIC', 'UNIT', 'VALUE']
output = pd.DataFrame(columns=output_columns)

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

# Function to make a request and handle retries
def make_request(url, headers, max_retries=3):
    for attempt in range(max_retries):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response
        else:
            print(f"Error: {response.status_code}. Retrying in 5 seconds...")
            time.sleep(random.uniform(5, 10))
    print(f"Failed to retrieve data after {max_retries} attempts")
    return response

def read_nov_file(excel_path):

   # Read the Excel file starting from the 5th row, and use the specified column names.
   month_df = pd.read_excel(excel_path, skiprows=5,  header=None, index_col=False)
   # Drop columns with all NaN values
   month_df = month_df.dropna(axis=1, how='all')
   month_df = month_df.dropna(axis = 0, how = 'all')
   # Specify the desired column names
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
   # Assign column names
   month_df.columns = nov_column_names
   # Reset the index
   month_df.reset_index(drop=True, inplace=True)
   
   return month_df

def read_dec_file(excel_path):
    # Read the Excel file starting from the 5th row, and use the specified column names.
    dec_df = pd.read_excel(excel_path, skiprows=4,  header=None, index_col=False, usecols=lambda x: x not in [0])
    # Drop columns with all NaN values
    dec_df = dec_df.dropna(axis = 1, how='all')
    dec_df = dec_df.dropna(axis = 0, how = 'all')
    
    dec_column_names = ['Year', 
                        'Traffic Accidents', 'Traffic Accidents (Index)', 
                        'Injuries', 'Injuries (Index)', 'Fatalities', 'Fatalities (Index)',  
                        'Fatalities per 100,000 persons', 'Fatalities per 100,000 persons (Index)']
    # Assign column names
    dec_df.columns = dec_column_names
    # Reset the index
    dec_df.reset_index(drop=True, inplace=True)
    
    return dec_df

def update_output_dataframe(month_df, output, year, month, scrape_datetime):
    global dec_df
    
    # Create a dictionary to map month names to their respective numeric values
    month_dict = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
                  'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12}
    
    # Create an empty list to store rows
    rows = []
    
    # Initialize total variables
    total_traffic_accident_year = 0
    total_fatalities_year = 0
    total_injuries_year = 0
    
    # Check if dec_df is not None (initialized)
    if dec_df is not None:
        # Iterate over each row in month_df
        for index, row in month_df.iterrows():
            # Extract month and convert it to numeric value
            month_month_df = row['Month']
            
            # Check if the month is 'Total', skip the row and store totals
            if month_month_df == 'Total':
                total_traffic_accident_year = row['Traffic Accidents (Provisional)']
                total_fatalities_year = row['Fatalities']
                total_injuries_year = row['Injuries (Provisional)']
                
                # Skip processing for December
                if month == 'December':
                    # Get the year from month_df
                    dec_year = year
                    
                    # Check if dec_df has data for the specified year
                    if dec_df.empty or dec_year not in dec_df['Year'].values:
                        # Skip calculations for December if dec_df is empty or no data for the year
                        continue
                    
                    dec_traffic_accidents = dec_df.loc[dec_df['Year'] == dec_year, 'Traffic Accidents'].values[0]
                    dec_fatalities = dec_df.loc[dec_df['Year'] == dec_year, 'Fatalities'].values[0]
                    dec_injuries = dec_df.loc[dec_df['Year'] == dec_year, 'Injuries'].values[0]

                    # Subtract the accumulated totals from month_df
                    dec_traffic_accident_year = dec_traffic_accidents - total_traffic_accident_year
                    dec_fatalities_year = dec_fatalities - total_fatalities_year
                    dec_injuries_year = dec_injuries - total_injuries_year
                    
                    # Append the totals for December to the output DataFrame
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
        
            # Extract values from the row
            period_start = f"{year}-{month_num:02d}-01"
            periodicity = 'M'
        
            metrics = ['Traffic Accidents (Provisional)', 'Fatalities', 'Injuries (Provisional)']
            unit = 'Number'
        
            # Iterate over each metric
            for metric in metrics:
                metric_value = row[metric]
        
                # Create a dictionary for the new row
                new_row = {'scrape_datetime': scrape_datetime.isoformat(), 'PERIOD_START': period_start, 'PERIODICITY': periodicity,
                           'METRIC': metric, 'UNIT': unit, 'VALUE': metric_value}
        
                # Append the new row to the list
                rows.append(new_row)
    
    # Concatenate the list of rows to the existing output DataFrame
    output = pd.concat([output, pd.DataFrame(rows)], ignore_index=True)
    # Assuming 'output' is your DataFrame
    output = output.drop_duplicates()
    
    output = output.dropna(axis=1, how='all')
    
    return output

def download_and_rename_xlsx(excel_url, download_path, month, scrape_datetime, year):
    global output
    global dec_df
    global month_df
    
    response = make_request(excel_url, headers= headers)
    
    if response.status_code == 200:
        # Get the name of the downloaded file
        downloaded_file = f"{download_path}/{month}_data.xlsx"  # Change the file extension to '.xlsx'

        # Save the content of the response to the downloaded file
        with open(downloaded_file, 'wb') as file:
            file.write(response.content)
        print(f"Excel file for {month} downloaded and saved as {downloaded_file}.")
        
        if month != 'December':
            # Read the November file
            month_df = read_nov_file(downloaded_file)
    
            # Update the output DataFrame
            output = update_output_dataframe(month_df, output, year, month, scrape_datetime)
            
            
        elif month == 'December':
            # Read the December file
            dec_df = read_dec_file(downloaded_file)
            print(f"Reading the {month} file ...")
            # Update the output DataFrame
            output = update_output_dataframe(month_df, output, year, month, scrape_datetime)
        
    else:
        print(f"Failed to download Excel file for {month} .")
   
def download_and_rename_xls(excel_url, download_path, month, scrape_datetime, year):
    
    global output
    global dec_df
    
    response = make_request(excel_url, headers= headers)
    
    if response.status_code == 200:
        # Get the name of the downloaded file
        downloaded_file = f"{download_path}/{month}_data.xls"  # Change the file extension to '.xlsx'

        # Save the content of the response to the downloaded file
        with open(downloaded_file, 'wb') as file:
            file.write(response.content)

        print(f"Excel file for {month} downloaded and saved as {downloaded_file}.")
        
        if month != 'December':
            # Read the November file
            month_df = read_nov_file(downloaded_file)
    
            # Update the output DataFrame
            output = update_output_dataframe(month_df, output, year, month, scrape_datetime)
            
            
        elif month == 'December':
            # Read the December file
            dec_df = read_dec_file(downloaded_file)
            
            # Update the output DataFrame
            output = update_output_dataframe(month_df, output, year, month, scrape_datetime)
                
    else:
        print(f"Failed to download Excel file for {month} .")
        
def download_csv(url, headers, download_path, month, scrape_datetime, year):
    
    global december_processed  # Access the global flag
    global dec_df
    global output
           
    
    if month == 'December' and december_processed:
        print("December has already been downloaded ...")
        # Update the output DataFrame
        print(f"Appending the december data for {year} ...")
        output = update_output_dataframe(month_df, output, year, month, scrape_datetime)
        return None    
    
    response = make_request(url, headers)
    if response.status_code == 200:
        print(f"Successful reqesut at monthly report page for {month}-{year}:", url)
        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            div = soup.find('div', class_ = 'stat-dataset_list-body')
            articles = div.find_all('article', class_ = 'stat-dataset_list-item')
            
            target_li = None  # Initialize target_li to None
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
                                december_processed = True  # Set the flag to True after processing December
                                break
                        else:
                            print(f'Skipping {month} ...')
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
                      
                print(f"Downaloding and renaming the file for {month}...")
                download_and_rename_xlsx(excel_url, download_path, month, scrape_datetime, year)
                
            elif excel_link is None:
                excel_link = soup.find('a', class_ = 'stat-dl_icon stat-icon_0 stat-icon_format js-dl stat-download_icon_left')
                excel_url = f"https://www.e-stat.go.jp{excel_link['href']}"
            
                print(f"Downaloding and renaming the file for {month}...")
                download_and_rename_xls(excel_url, download_path, month, scrape_datetime,  year)
                
            else:
                print(f"Traffic accidents occurrence by month data is not available in {month}")
                
            if month == 'November':
                
                if not december_processed:
                    month = "December"
                    dec_url = 'https://www.e-stat.go.jp/en/stat-search/files?page=1&layout=datalist&toukei=00130002&tstat=000001032793&cycle=7&year=20230&month=0&tclass'
                    
                    dec_response = make_request(dec_url, headers) 

                    if dec_response.status_code == 200:
                        print("Successful request at dec_url:", dec_url)
                        
                        soup = BeautifulSoup(dec_response.content, 'html.parser')
                        div = soup.find('div', class_ = 'stat-dataset_list-body')
                        articles = div.find('article', class_ = 'stat-dataset_list-item')

                        try:
                            excel_link = articles.find('a', class_ = 'stat-dl_icon stat-icon_4 stat-icon_format js-dl stat-download_icon_left')
                            excel_url = f"https://www.e-stat.go.jp{excel_link['href']}"
                            
                        except:
                            excel_link = articles.find('a', class_ = 'stat-dl_icon stat-icon_0 stat-icon_format js-dl stat-download_icon_left')
                            excel_url = f"https://www.e-stat.go.jp{excel_link['href']}"
                            
                        print("Downaloding and renaming the file for december data ...")
                        download_and_rename_xlsx(excel_url, download_path, month, scrape_datetime, year)
                        december_processed = True
                        
        except:
            print(f"Different layout of this particular page for {month}")
        
    else:
        print(f"Request failed for {month} while downloading file at {url} with status code: {response.status_code}")

# Make the request using the function
response = make_request(base_url, headers)

# Check the response
if response.status_code == 200:
    print("Successful reqesut at base_url:", base_url)
    
    # Get the current date and time for the scrape_datetime column
    scrape_datetime = datetime.utcnow()
    
    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    div = soup.find('div', class_="anchorLink")
    next_sibling = div.find_next_sibling()
    
    # Assuming 'next_sibling' is the BeautifulSoup object representing the next sibling after 'div'
    monthly_reports = []
    
    for h4_tag in next_sibling.find_all('h4'):
        # Extract numeric part from the 'h4' tag's text
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
            
            print(f"Monthly data got fetched {year}....") 
            
            last_month = monthly_reports[-1]
            
            if last_month['Month'] != 'December':
                url = last_month['Link']
                month = last_month['Month']
                year = last_month['Year']
                
                # Create a folder for the year
                year_folder = f"{os.getcwd()}/{year}"
                os.makedirs(year_folder, exist_ok=True)
                
                download_csv(url, headers, year_folder, month, scrape_datetime, year)
                
            else:
                last_two_months = monthly_reports[-2:]
                for month_info in last_two_months:
                    url = month_info['Link']
                    month = month_info['Month']
                    year = month_info['Year']
                    
                    # Create a folder for the year
                    year_folder = f"{os.getcwd()}/{year}"
                    os.makedirs(year_folder, exist_ok=True)
                    
                    download_csv(url, headers, year_folder, month, scrape_datetime, year)            
            
            # Delete the year folder and its contents
            shutil.rmtree(year_folder)
            
            if not historical:
                print(f"Done with scraping data for {year}")
                break
        else:
            print(f"Done with scraping data till {year + 1}")
            break
           
    # Save DataFrame to CSV
    # Assuming 'output' is your DataFrame
    output.to_csv('output.csv', encoding='utf-8', quotechar='"', quoting=csv.QUOTE_ALL, index=False)
    
else:
    print(f"Request failed with status code: {response.status_code}")
        
