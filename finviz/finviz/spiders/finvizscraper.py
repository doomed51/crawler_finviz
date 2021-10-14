#####  README
##### Run from directory ./venv/finviz/finviz/spiders
##### Run with cmd: < scrapy crawl finvizscraper > 
##### Don't forget to activate venv !!

##### This code is built upon the tutorial here: https://www.babbling.fish/scraping-for-a-job/

from typing import Text
import scrapy
import pandas as pd

from bs4 import BeautifulSoup
from scrapy.utils.python import retry_on_eintr
from decimal import Decimal

from openpyxl import workbook, load_workbook
from openpyxl.worksheet import worksheet
#from itertools import product

#TODO make the filename relative...
filePath = "F:/workbench/crawler_finviz/venv/Sample.xlsm"
decimalConversion = {'M' : 6, 'B': 9}

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

# converts 'B' and 'M' suffixes into a number 
# from here: https://stackoverflow.com/questions/11896560/how-can-i-consistently-convert-strings-like-3-71b-and-4m-to-numbers-in-pytho
def text_to_num(text):
        if text[-1] in decimalConversion:
            num, magnitude = text[:-1], text[-1]
            return Decimal(num) * 10 ** decimalConversion[magnitude]
        else:
            return Decimal(text)

# spider definition
class FinvizscraperSpider(scrapy.Spider):
    name = 'finvizscraper'
    allowed_domains = ['finviz.com']
    start_urls = urls #['http://finviz.com/']

    def parse(self, response):
        soup = BeautifulSoup(response.text, features="lxml")

        #find the stats table
        #tickerStats = soup.find("table", {"class":"snapshot-table2"})

        #find all tabledata 
        #statsTable = soup.find_all("table",{"class":"snapshot-table2"})

        # find and set the ticker/symbol
        ticker = soup.find("a", {"id" :"ticker"}).text
        
        # POPULATE MARKETCAP 
        # find and set the marketcap 
        marketCap = soup.find("td", text="Market Cap").find_next_sibling("td").text
        
        # convert into number 
        marketCap = text_to_num(marketCap)
        
        #populate the df with the marketcap data 
        df.loc[df['Symbol'] == ticker,'Market Cap'] = marketCap
        
        # TODO POPULATE CURRENT PRICE 
        currentPrice = soup.find("td", text="Price").find_next_sibling("td").text
        df.loc[df['Symbol'] == ticker,'Current Price'] = currentPrice
        
        # TODO POPULATE PRICE TO BOOK 
        priceToBook = soup.find("td", text="P/B").find_next_sibling("td").text
        df.loc[df['Symbol'] == ticker,'Price-to-Book'] = priceToBook
        
        # TODO POPULATE COMPANY NAME 

        print("")
        print("-------------")
        print("#############")
        print("")

        print(marketCap)
        #df.loc[df['Symbol'] == ticker]['Market Cap'] = marketCap
        
        print(df.loc[df['Symbol'] == ticker])
                
        print("")
        print("#############")
        print("-------------")
        print("")
    
        #for tickerStat in tickerStats:
            #how does beautifulsoup organize/structure the tabledata 

# 
#:::::::::::::::::::::::::::::::
#
# debug code :::::::::::::::::::
#
#:::::::::::::::::::::::::::::::
#
# 


#df.at['IEP', "Market Cap"] = 13.2
#print(df.loc['IEP'])
#print(df['Market Cap'])
#print(df['Symbol'].unique())