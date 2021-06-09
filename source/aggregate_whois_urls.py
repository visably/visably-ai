import json
import pandas as pd
from collections import Counter
from zipfile import ZipFile
from glob import glob
from tqdm import tqdm_notebook
import numpy as np

data_folder = "/hdd/data/whois/raw"
year_folders = [x for x in glob(f"{data_folder}/*") if "zip" not in x]
print("Number of years: ", len(year_folders))
all_date_files = []
for year in year_folders:

    month_folders = glob(f"{year}/*")
    for month in month_folders:
        if ".zip" in month:
            continue
            
        if ".csv" in month or "xlsx" in month:
            all_date_files.append(month)
        else:
            all_date_files.extend([x for x in glob(f"{month}/*") if ".csv" in x or ".xlsx" in x])
            
print(len(all_date_files))
year_counter = Counter()
month_counter = Counter()
for d in all_date_files:
    year = d.split("/")[5]
    year_counter[year] += 1
    if year == "2017":
        month = d.split("/")[-1].split("-")[1]
        month_counter[month] += 1

all_data_list = []

for i, day in enumerate(tqdm_notebook(all_date_files)):  
    if i % 500 == 0 and i != 0:
    	all_data = pd.concat(all_data_list)
    	all_data.to_csv(f"/hdd/data/whois/unique_whois_domains/unique_whois_urls_{i}.csv", index=False)

    	del all_data
    	all_data_list = []

    if ".zip" in day:
        continue
    if ".csv" in day:
        try:
            data = pd.read_csv(day, encoding = "ISO-8859-1")[["domain_name"]]
        except:
            print("Failed to load file: ", day)
            continue
    elif ".xlsx" in day:
        data = pd.read_excel(day, sort=True)[["domain_name"]]

    all_data_list.append(data)
    