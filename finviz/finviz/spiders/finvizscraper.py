#####  README
##### Run from directory ./venv/finviz/finviz/spiders
##### Run with cmd: < scrapy crawl finvizscraper > 
##### Don't forget to activate venv !!

##### This code is built upon the tutorial here: https://www.babbling.fish/scraping-for-a-job/

import scrapy

from bs4 import BeautifulSoup
from scrapy.utils.python import retry_on_eintr
#from itertools import product

tickers = "IEP"
urls = []

urls.append(f"http://finviz.com/quote.ashx?t={tickers}")

class FinvizscraperSpider(scrapy.Spider):
    name = 'finvizscraper'
    allowed_domains = ['finviz.com']
    start_urls = urls #['http://finviz.com/']

    def parse(self, response):
        soup = BeautifulSoup(response.text, features="lxml")

        #find the stats table
        tickerStats = soup.find("table", {"class":"snapshot-table2"})

        #find all tabledata 
        statsTable = soup.find_all("table",{"class":"snapshot-table2"})

        marketCap = soup.find("td", text="Market Cap").find_next_sibling("td").text

        print("#############")
        print("#############")
        print("")
        
        print(marketCap)
        
        print("")
        print("#############")
        print("#############")
    
        #for tickerStat in tickerStats:
            #how does beautifulsoup organize/structure the tabledata 