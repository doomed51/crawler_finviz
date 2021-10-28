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
# Util function
## 
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
# Util function
## 
# helper function to speed up implementation of new columns 
# Populates valuations_df 
#   
# args:
# textLabel: title of the column in DF 
# valueType: percent, millionBillion, or empty string
# scrapedValue: value from beautifulsoup.soup lookup
# ticker: ticker being populated 
#####
def populateColumn(textLabel, valueType, scrapedValue, ticker):
    #scrapedValue = soup.find("td", text=textLabel).find_next_sibling("td").text
    if scrapedValue == '-':
        valuations_df.loc[valuations_df['Symbol'] == ticker,textLabel] = 0
    
    elif valueType == 'millionBillion':
        valuations_df.loc[valuations_df['Symbol'] == ticker,textLabel] = float(text_to_num(scrapedValue))
    
    elif valueType == 'percent':
        valuations_df.loc[valuations_df['Symbol'] == ticker,textLabel] = float(scrapedValue.strip('%'))/100
    
    else:
        valuations_df.loc[valuations_df['Symbol'] == ticker,textLabel] = float(scrapedValue)

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
        nextVal = soup.find("td", text="52W High").find_next_sibling("td").text
        populateColumn('52W High', 'percent', nextVal, ticker)
        
        # 52W Low
        nextVal = soup.find("td", text="52W Low").find_next_sibling("td").text
        populateColumn('52W Low', 'percent', nextVal, ticker)
        
        # Current Price 
        nextVal = soup.find("td", text="Price").find_next_sibling("td").text
        populateColumn('Current Price', '', nextVal, ticker)
     
        # Current Ratio
        nextVal = soup.find("td", text="Current Ratio").find_next_sibling("td").text
        populateColumn('Current Ratio', '', nextVal, ticker)
        
        # Dividend Yield
        nextVal = soup.find("td", text="Dividend %").find_next_sibling("td").text
        populateColumn('Dividend %', 'percent', nextVal, ticker)

        # Income (ttm)
        nextVal = soup.find("td", text="Income").find_next_sibling("td").text
        populateColumn('Income', 'millionBillion', nextVal, ticker)

        # MARKETCAP 
        nextVal = soup.find("td", text="Market Cap").find_next_sibling("td").text
        populateColumn('Market Cap', 'millionBillion', nextVal, ticker)
        
        # Operating Margin
        nextVal = soup.find("td", text="Oper. Margin").find_next_sibling("td").text
        populateColumn('Oper. Margin', 'percent', nextVal, ticker)
      
        # PRICE TO BOOK 
        nextVal = soup.find("td", text="P/B").find_next_sibling("td").text
        populateColumn('P/B', '', nextVal, ticker)
        
        # Profit Margin
        nextVal = soup.find("td", text="Profit Margin").find_next_sibling("td").text
        populateColumn('Profit Margin', 'percent', nextVal, ticker)

        # Quick Ratio
        nextVal = soup.find("td", text="Quick Ratio").find_next_sibling("td").text
        populateColumn('Quick Ratio', '', nextVal, ticker)

        # Sales 
        nextVal = soup.find("td", text="Sales").find_next_sibling("td").text
        populateColumn('Sales', 'millionBillion', nextVal, ticker)
        
        # Total Debt/Eq 
        nextVal = soup.find("td", text="Debt/Eq").find_next_sibling("td").text
        populateColumn('Debt/Eq', '', nextVal, ticker)
       
        # Target Price - Analyst mean price
        nextVal = soup.find("td", text="Target Price").find_next_sibling("td").text
        populateColumn('Target Price', '', nextVal, ticker)


        # less important
        # Cash/sh
        # TODO add this to Dumb Score calculation
        nextVal = soup.find("td", text="Cash/sh").find_next_sibling("td").text
        populateColumn('Cash/sh', '', nextVal, ticker)

        # TODO EPS(ttm) + EPS next Y
        nextVal = soup.find("td", text="EPS next Y").find_next_sibling("td").text
        populateColumn('EPS next Y', '', nextVal, ticker)

        nextVal = soup.find("td", text="EPS (ttm)").find_next_sibling("td").text
        populateColumn('EPS (ttm)', '', nextVal, ticker)
       
        # ROE
        nextVal = soup.find("td", text="ROE").find_next_sibling("td").text
        populateColumn('ROE', 'percent', nextVal, ticker )

        # Insider Own
        nextVal = soup.find("td", text="Insider Own").find_next_sibling("td").text
        populateColumn('Insider Own', 'percent', nextVal, ticker)

        # Insider Trans
        nextVal = soup.find("td", text="Insider Trans").find_next_sibling("td").text
        populateColumn('Insider Trans', 'percent', nextVal, ticker)

        # Sales past 5Y
        nextVal = soup.find("td", text="Sales past 5Y").find_next_sibling("td").text
        populateColumn('Sales past 5Y', 'percent', nextVal, ticker)

        # Perf Year + Perf Quarter
        nextVal = soup.find("td", text="Perf Year").find_next_sibling("td").text
        populateColumn('Perf Year', 'percent', nextVal, ticker)

        nextVal = soup.find("td", text="Perf Quarter").find_next_sibling("td").text
        populateColumn('Perf Quarter', 'percent', nextVal, ticker)

        # P/E + Forward P/E
        nextVal = soup.find("td", text="P/E").find_next_sibling("td").text
        populateColumn('P/E', '', nextVal, ticker)

        nextVal = soup.find("td", text="Forward P/E").find_next_sibling("td").text
        populateColumn('Forward P/E', '', nextVal, ticker)

        # P/S
        nextVal = soup.find("td", text="P/S").find_next_sibling("td").text
        populateColumn('P/S', '', nextVal, ticker)

        # P/B
        nextVal = soup.find("td", text="P/B").find_next_sibling("td").text
        populateColumn('P/B', '', nextVal, ticker)
        
        # less important
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
            "Oper. Margin": 0.1,
            "Profit Margin":    0.7,
            "Quick Ratio":      1.0,
            "Dividend %":   0.01,
            "Debt/Eq":0.7,
            "P/B":    1 

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
        if item in ['Debt/Eq', 'P/B']: # Lower is better
            if float(row[item]) >= thresholds[item]:
                myScore -= 1
            else:
                myScore += 1        
        
        else: # Higher is better
            if float(row[item]) >= thresholds[item]:
                myScore += 1
            else:
                myScore -= 1
                
    if row['Current Price'] < row['Target Price']:
        myScore += 1
    elif row['Current Price'] >= row['Target Price']:
        myScore -= 1 

    return myScore

# code to run the spider as a script 
#print("Starting to scrape!!")
print("")

runner = CrawlerRunner()

d = runner.crawl(FinvizscraperSpider)
d.addBoth(lambda _: reactor.stop())

reactor.run() #script will block here until the crawling is 

# finished 

print("")
print ("Scraping complete!!")
print("")

computeScore_base()

writeScrapedDataToExcel()

print(valuations_df)

