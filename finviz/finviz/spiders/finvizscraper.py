#####  README
##### This code is built upon the tutorial here: 
##### https://www.babbling.fish/scraping-for-a-job/
#####
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

#TODO make the filename relative...
# Load Valuations sheet in the excel file into 
# a dataframe 'valuations_df'
filePath = "F:/workbench/crawler_finviz/venv/Sample.xlsx"
valuations_df = pd.read_excel(filePath, "Valuations") 

# set the URLs that need to be crawled
# this is the unique set of Symbols from valuations_df
urls = []
for item in valuations_df['Symbol'].unique():
    urls.append(f"http://finviz.com/quote.ashx?t={item}")

#####
# converts 'B' and 'M' suffixes into a number 
# from here: https://stackoverflow.com/questions/11896560/how-can-i-consistently-convert-strings-like-3-71b-and-4m-to-numbers-in-pytho
#####
decimalConversion = {'M' : 6, 'B': 9}
def text_to_num(text):
        if text[-1] in decimalConversion:
            num, magnitude = text[:-1], text[-1]
            return Decimal(num) * 10 ** decimalConversion[magnitude]
        else:
            return Decimal(text)

#####
# Spider definition -
# Target Site: finviz > individual stock pages 
# e.g. https://finviz.com/quote.ashx?t=msft&ty=c&ta=1&p=d
# Notes:
# - Builds a panda dataframe 'valuations_df' of form:
#       (index) | Symbol | marketCap | current price | ... etc  
# - Normalizes values with '%', or 'M'/'B' into dtype(float)
#####
class FinvizscraperSpider(scrapy.Spider):
    name = 'finvizscraper'
    allowed_domains = ['finviz.com']
    start_urls = urls #['http://finviz.com/']


    def parse(self, response):
        soup = BeautifulSoup(response.text, features="lxml")
        
        # find and set the ticker/symbol
        ticker = soup.find("a", {"id" :"ticker"}).text
        print("Scraped: " + ticker)
        
        # 52W High
        high_52w = soup.find("td", text="52W High").find_next_sibling("td").text
        valuations_df.loc[valuations_df['Symbol'] == ticker,'52W High'] = float(high_52w.strip('%'))/100

        # 52W Low
        low_52w = soup.find("td", text="52W Low").find_next_sibling("td").text
        valuations_df.loc[valuations_df['Symbol'] == ticker,'52W Low'] = float(low_52w.strip('%'))/100

        # Current Price 
        currentPrice = soup.find("td", text="Price").find_next_sibling("td").text
        valuations_df.loc[valuations_df['Symbol'] == ticker,'Current Price'] = float(currentPrice)

        # Current Ratio
        currentRatio = soup.find("td", text="Current Ratio").find_next_sibling("td").text
        if currentRatio == '-':
            valuations_df.loc[valuations_df['Symbol'] == ticker,'Current Ratio'] = 0
        else:
            valuations_df.loc[valuations_df['Symbol'] == ticker,'Current Ratio'] = float(currentRatio)
        
        # Dividend Yield
        dividendYield = soup.find("td", text="Dividend %").find_next_sibling("td").text
        if dividendYield == '-':
            valuations_df.loc[valuations_df['Symbol'] == ticker,'Dividend Yield'] = 0
        else:
            valuations_df.loc[valuations_df['Symbol'] == ticker,'Dividend Yield'] = float(dividendYield.strip('%'))/100

        # Income (ttm)
        income = soup.find("td", text="Income").find_next_sibling("td").text
        if income == '-':
            valuations_df.loc[valuations_df['Symbol'] == ticker,'Income'] = 0
        else: 
            valuations_df.loc[valuations_df['Symbol'] == ticker,'Income'] = float(text_to_num(income))

        # MARKETCAP 
        marketCap = soup.find("td", text="Market Cap").find_next_sibling("td").text
        marketCap = float(text_to_num(marketCap))
        valuations_df.loc[valuations_df['Symbol'] == ticker,'Market Cap'] = marketCap
        
        # PRICE TO BOOK 
        priceToBook = soup.find("td", text="P/B").find_next_sibling("td").text
        valuations_df.loc[valuations_df['Symbol'] == ticker,'Price-to-Book'] = float(priceToBook)
        
        # Profit Margin
        profitMargin = soup.find("td", text="Profit Margin").find_next_sibling("td").text
        if profitMargin == '-':
            valuations_df.loc[valuations_df['Symbol'] == ticker,'Profit Margin'] = 0
        else:
            valuations_df.loc[valuations_df['Symbol'] == ticker,'Profit Margin'] = float(profitMargin.strip('%'))/100

        # Sales 
        sales = soup.find("td", text="Sales").find_next_sibling("td").text
        if sales == '-':
            valuations_df.loc[valuations_df['Symbol'] == ticker,'Sales'] = 0
        else:
            valuations_df.loc[valuations_df['Symbol'] == ticker,'Sales'] = float(text_to_num(sales))

        # Total Debt/Eq 
        debtEquityRatio = soup.find("td", text="Debt/Eq").find_next_sibling("td").text
        if debtEquityRatio == '-':
            valuations_df.loc[valuations_df['Symbol'] == ticker,'Total Debt/Equity'] = 0
        else:
            valuations_df.loc[valuations_df['Symbol'] == ticker,'Total Debt/Equity'] = float(debtEquityRatio)
        
        # Operating Margin
        operatingMargin = soup.find("td", text="Oper. Margin").find_next_sibling("td").text
        if operatingMargin == '-': #convert to float if it is not empty
            valuations_df.loc[valuations_df['Symbol'] == ticker,'Operating Margin'] = 0
        else:
            valuations_df.loc[valuations_df['Symbol'] == ticker,'Operating Margin'] = float(operatingMargin.strip('%'))/100
            
       # Quick Ratio
        quickRatio = soup.find("td", text="Quick Ratio").find_next_sibling("td").text
        if quickRatio == '-':
            valuations_df.loc[valuations_df['Symbol'] == ticker, 'Quick Ratio'] = 0
        else:
            valuations_df.loc[valuations_df['Symbol'] == ticker, 'Quick Ratio'] = float(quickRatio)

        # Target Price - Analyst mean price
        targetPrice = soup.find("td", text="Target Price").find_next_sibling("td").text
        if targetPrice == '-':
            valuations_df.loc[valuations_df['Symbol'] == ticker,'Analyst Mean Target'] = 0
        else:
            valuations_df.loc[valuations_df['Symbol'] == ticker,'Analyst Mean Target'] = float(targetPrice)


        # less important
        # TODO Cash/sh
        # TODO EPS(ttm) + EPS past 5Y + EPS next Y + EPS next 5Y
        # TODO ROE
        # TODO Insider Own
        # TODO Insider Trans
        # TODO Sales past 5Y
        # TODO Perf Year + Perf Quarter
        # TODO P/E + Forward P/E
        # TODO P/S
        # TODO P/B
        
        # less important
        # TODO Price 
        # TODO sales/share -> Sales; Shs Outstanding | Shs Float
        # TODO Sector
        # TODO POPULATE COMPANY NAME 

#####
# Writes the dataframe 'valuations_df' created from scraping 
# finviz to an excel file
#####
def writeScrapedDataToExcel():
    # TODO fix index printing and symbol column name issue
    myBook = openpyxl.load_workbook(filePath)

    with pd.ExcelWriter(filePath, engine='openpyxl') as writer: 
        writer.book = myBook
        writer.sheets = dict((ws.title, ws) for ws in myBook.worksheets)
        #writer.vba_archive = myBook.vba_archive

        valuations_df.to_excel(writer, sheet_name='Valuations', header=True, index=False)

        writer.save()

#####
# Computes a 'score' for each symbol in valuations_df
##### 
def computeScore_base(): 
    print("computing scores....")
    print("")
    
    myThresholds = {
            "Current Ratio":    1.0,
            "Operating Margin": 0.1,
            "Profit Margin":    0.7,
            "Quick Ratio":      1.0,
            "Dividend Yield":   0.01,
            "Total Debt/Equity":0.7,
            "Price-to-Book":    1 

    }

    valuations_df['Dumb Score'] = valuations_df.apply( lambda row: computeDumbScore(row, myThresholds), axis=1)

#####
# util function
## 
# Computes a 'dumb' i.e. unsophisticated Score based on some
# basic financial parameters, where a higher Score is better.
#
# desc:
#       score = sum of all threshold values where 
#           if value is better than thresold, then score +1 
#           otherwise score -1 
##### 
def computeDumbScore(row, thresholds):
    myScore = 0
    
    for item in thresholds:
        if item in ['Total Debt/Equity', 'Price-to-Book']: # Lower is better
            if float(row[item]) >= thresholds[item]:
                myScore -= 1
                print("+1-inverse")
            else:
                myScore += 1
                print("-----1-inverse")
        
        else: # Higher is better
            if float(row[item]) >= thresholds[item]:
                myScore += 1
                print("+1")
            else:
                myScore -= 1
                print("-----1")

    if row['Current Price'] < row['Analyst Mean Target']:
        myScore += 1
    elif row['Current Price'] >= row['Analyst Mean Target']:
        myScore -= 1 

    return myScore

# code to run the spider as a script 
#print("Starting to scrape!!")
print("")

#runner = CrawlerRunner()

#d = runner.crawl(FinvizscraperSpider)
#d.addBoth(lambda _: reactor.stop())

#reactor.run() #script will block here until the crawling is 

# finished 

print("")
print ("Scraping complete!!")
print("")

#writeScrapedDataToExcel()


computeScore_base()
print(valuations_df)

