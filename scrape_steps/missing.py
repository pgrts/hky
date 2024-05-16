import requests
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
from html.parser import HTMLParser
import TopDownHockey_Scraper.TopDownHockey_NHL_Scraper as tdhnhlscrape

missing = ['2023020490',
'2023020829',
'2023020807',
'2023020628',
'2023020359',
'2023020782',
'2023020666',
'2023020496',
'2023020533',
'2023020397',
'2023020550',
'2023020733',
'2023020457',
'2023020444',
'2023020383',
'2023020676',
'2023020582',
'2023020646',
'2023020516',
'2023020769',
'2023020429',
'2023020616',
'2023020415',
'2023020464',
'2023020597',
'2023020567',
'2023020685',
'2023020798']

espn_dict = [{'2023020464', '401559693'},{'2023020798', '401458671'}, {'2023020685', '401559913'}]
def use_api(url):
    data = requests.get(url).json()
    return data

def create_shift_chart(game_id):
    shift_data = use_api("https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId=" + game_id + "%20and%20((duration%20!=%20%2700:00%27%20and%20typeCode%20=%20517)%20or%20typeCode%20!=%20517%20)&exclude=detailCode&exclude=duration&exclude=eventDetails&exclude=teamAbbrev&exclude=teamName")["data"]
    shiftdf = pd.DataFrame(shift_data, columns = ['playerId', 'gameId', 'startTime', 'endTime', 'teamId', 'period', 'firstName', 'lastName'])
    return shiftdf

def scrape_pbp(game_id):
    hockey_scraper.scrape_games(game_id, False, data_format='Pandas')

tdhnhlscrape.full_scrape(['2023020027']).to_csv('p4sps.csv')
