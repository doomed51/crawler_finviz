#####  README
##### Run from directory ./venv/finviz/finviz/spiders
##### Run with cmd: < scrapy crawl finvizscraper > 
##### Don't forget to activate venv !!

##### This code is built upon the tutorial here: https://www.babbling.fish/scraping-for-a-job/

import openpyxl
import scrapy
import pandas as pd
import openpyxl

from scrapy.utils.python import retry_on_eintr
from bs4 import BeautifulSoup
from decimal import Decimal

from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging

decimalConversion = {'M' : 6, 'B': 9}

#TODO make the filename relative...
# Load Valuations sheet in the excel file into a dataframe
filePath = "F:/workbench/crawler_finviz/venv/Sample.xlsm"
valuations_df = pd.read_excel(filePath, "Valuations") 
#valuations_df = openpyxl.load_workbook(filePath, keep_vba=True)


# set the URLs that need to be crawled
# this is the unique set of Symbols from the excel doc 
urls = []
for item in valuations_df['Symbol'].unique():
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


        # find and set the ticker/symbol
        ticker = soup.find("a", {"id" :"ticker"}).text
        
        # POPULATE MARKETCAP 
        #::::::::::::::::::::::::::::
        # find and set the marketcap 
        marketCap = soup.find("td", text="Market Cap").find_next_sibling("td").text
        
        # convert into number 
        marketCap = text_to_num(marketCap)
        
        #populate the valuations_df with the marketcap data 
        valuations_df.loc[valuations_df['Symbol'] == ticker,'Market Cap'] = marketCap
        
        # TODO POPULATE CURRENT PRICE 
        #::::::::::::::::::::::::::::
        currentPrice = soup.find("td", text="Price").find_next_sibling("td").text
        valuations_df.loc[valuations_df['Symbol'] == ticker,'Current Price'] = currentPrice
        
        # TODO POPULATE PRICE TO BOOK 
        #::::::::::::::::::::::::::::
        priceToBook = soup.find("td", text="P/B").find_next_sibling("td").text
        valuations_df.loc[valuations_df['Symbol'] == ticker,'Price-to-Book'] = priceToBook
        
        #print(valuations_df)
        # TODO POPULATE COMPANY NAME 
        #::::::::::::::::::::::::::::

        print("Scraped: " + ticker)

# code to run the spider as a script 
print("Starting to scrape!!")
print("")
runner = CrawlerRunner()

d = runner.crawl(FinvizscraperSpider)
d.addBoth(lambda _: reactor.stop())

reactor.run() #script will block here until the crawling is finished 
print("")
print ("Scraping complete!!")
print("")

# TODO fix index printing and symbol column name issue
myBook = openpyxl.load_workbook(filePath, keep_vba=True)

with pd.ExcelWriter(filePath, engine='openpyxl') as writer: 
    writer.book = myBook
    writer.sheets = dict((ws.title, ws) for ws in myBook.worksheets)
    writer.vba_archive = myBook.vba_archive

    #valuations_df.to_excel(writer, sheet_name='Valuations', header=True, index=True, startrow=1, startcol=0, columns=['A'])
    valuations_df.to_excel(writer, sheet_name='Valuations', header=True, index=False)

    writer.save()