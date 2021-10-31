#####       HOW TO RUN THE CRAWLER
#####
##### Run from directory ./venv/finviz/finviz/spiders
##### Run with cmd: < scrapy crawl finvizscraper > 
##### OR simply call finvizscraper.py from cmd
#####
#####       BASIC OVERVIEW OF CODE
#####
##### READS in 'Sample.xlsx file
##### BUILDS the list of URLS to be crawled from the excel file
##### CRAWLS individual stock pages on finviz.com
##### CREATES  a panda dataframe 'valuations_df' of form:
#####  (index) | Symbol | marketCap | target price | ... etc  
#####
##### WRITE the dataframe to 
#####       an excel file using: writeScrapedDataToExcel


from os import write
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

#####
# declaring global variables
#####

# set the URLs that need to be crawled
# this is the unique set of Symbols from valuations_df
urls = ['https://finviz.com/insidertrading.ashx?tc=1']
#for item in urls:
#    urls.append(f"http://finviz.com/quote.ashx?t={item}")

df = []

#####
# Spider definition -
# Target Site:  https://finviz.com/insidertrading.ashx?tc=1
#               > Insider Transactions (Buys)
# Notes:
# - Builds a panda dataframe 'insiderBuys_df' of form:
# 
#####
class FinvizscraperSpider_insider(scrapy.Spider):
    name = 'finvizscraper_insiderTransactions'
    allowed_domains = ['finviz.com']
    start_urls = urls #['http://finviz.com/']


    def parse(self, response):
        soup = BeautifulSoup(response.text, features="lxml")
        #soup = BeautifulSoup(page.text, 'html.parser')
        
        bodyTable = soup.find("table", {"class": "body-table"})
        #bodyTableRows = bodyTable.find_all("tr")
        #print(bodyTable)
        
        bodyTableRows = []
        for child in bodyTable.children:
            newRow = []
            for td in child:
                try:
                    newRow.append(td.text.replace("\n"," "))
                except:
                    continue
            if len(newRow) > 0:
                bodyTableRows.append(newRow)
                #print(newRow)
                
        myCols = [ ele for ele in bodyTableRows[0] if ele.strip() ]                    

        df = pd.DataFrame(bodyTableRows[1:], columns=myCols)

        print(df)


# code to run the spider as a script 
print("Starting to scrape!!")
print("")

runner = CrawlerRunner()

d = runner.crawl(FinvizscraperSpider_insider)
d.addBoth(lambda _: reactor.stop())

reactor.run() #script will block here until the crawling is 

# finished 
print("")
print ("Scraping complete!!")
print("")
