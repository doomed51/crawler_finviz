#####  README
##### Run from directory ./venv/finviz/finviz/spiders
##### Run with cmd: < scrapy crawl finvizscraper > 
##### Don't forget to activate venv !!

##### This code is built upon the tutorial here: https://www.babbling.fish/scraping-for-a-job/

import scrapy
import pandas as pd

from bs4 import BeautifulSoup
from scrapy.utils.python import retry_on_eintr
from openpyxl import workbook, load_workbook
from openpyxl.worksheet import worksheet
#from itertools import product

#TODO make the filename relative...
filePath = "F:/workbench/crawler_finviz/venv/Sample.xlsm"

#load up the excel file 
workbook = load_workbook(filename=filePath)
df = pd.read_excel(filePath, "Valuations")
#df.set_index('Symbol', inplace=True)

#select sheet by name 
#sheet = workbook["Valuations"]

# set the URLs that need to be crawled
# this is the unique set of Symbols from the excel doc 
urls = []
for item in df['Symbol'].unique():
    urls.append(f"http://finviz.com/quote.ashx?t={item}")

# 
#:::::::::::::::::::::::::::::::
#
# debug code :::::::::::::::::::
#
#:::::::::::::::::::::::::::::::
#
# 
print("")
print("-------------")
print("#############")
print("")

#df.at['IEP', "Market Cap"] = 13.2
#print(df.loc['IEP'])
#print(df['Market Cap'])
print(df['Symbol'].unique())

print("")
print("#############")
print("-------------")
print("")