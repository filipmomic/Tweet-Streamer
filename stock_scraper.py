# import requests

# from bs4 import BeautifulSoup

# import pandas as pd
# URL="https://finviz.com/screener.ashx?v=111&f=cap_microunder,sh_curvol_o1000&o=volume"

# page=requests.get(URL)


# soup=BeautifulSoup(page.content, "html.parser")
# print(type(soup))
# print(soup)
# results=soup.find(id_="screener-views-table")
# print(results)

#import yfinance as yahooFinance
 
# Here We are getting Facebook financial information
# We need to pass FB as argument for that
#GetFacebookInformation = yahooFinance.Ticker("FB")
 
# whole python dictionary is printed here
#print(GetFacebookInformation.info)
import requests
import pandas as pd
import requests, bs4

def get_stock_tickers(n=10):
    url = 'https://finviz.com/screener.ashx?v=111&f=cap_microunder,sh_curvol_o1000&o=volume'
    headers = {"User-Agent":"Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    soup = bs4.BeautifulSoup(response.text, 'lxml')
    table_id = soup.find('table', id="screener-views-table")
    ticker  = soup.find_all('a', class_ ='screener-link-primary')

    objects = []
    for item in ticker:
        dictObj = {'rule': f'{str(item.getText())} -is:retweet', 'tag': str(item.getText())}
        objects.append(dictObj)
        
    return(objects)
