from bs4 import BeautifulSoup as soup
import requests
import json
import pandas as pd
import os
#import time
#from html.parser import HTMLParser
from datetime import date, timedelta
from io import StringIO


nhl_teams = ["ANA", "ARI", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ",
    "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", "NYI", "NYR",
    "OTT", "PHI", "PIT", "SJS", "SEA", "STL", "TBL", "TOR", "VAN", "VGK", "WPG", "WSH"]

def use_api(url):
    #url = string_1 + value + string_2
    data = requests.get(url).json()
    return data

def roster_df(team):
    playerType = ["forwards", "defensemen", "goalies"]
    data = use_api('https://api-web.nhle.com/v1/roster/'+ team + '/20232024')
    cols = ['playerID','firstName','lastName','number','mugshot','pos','height','weight','url']
    

    id_lis = []
    first_lis = []
    last_lis = []
    mug_lis = []
    pos_lis = []
    url_lis = []
    pos_lis = []
    height_lis = []
    weight_lis = []
    num_lis = []
    
    for each in playerType:
        for ele in data[each]:          
            id_lis.append(ele['id'])
            url_lis.append('https://www.nhl.com/player/' + str(ele['id']))
            pos_lis.append(ele['positionCode'])
            first_lis.append(ele['firstName']['default'])
            last_lis.append(ele['lastName']['default'])
            mug_lis.append(ele['headshot'])
            height_lis.append(ele['heightInInches'])
            weight_lis.append(ele['weightInPounds'])
            try:
                num_lis.append(ele['sweaterNumber'])
            except:
                num_lis.append('NULL')
            
    return pd.DataFrame(list(zip(id_lis,first_lis,last_lis,num_lis,mug_lis,pos_lis,height_lis,weight_lis,url_lis)),columns = cols)

#print(roster_df('CAR'))

def every_roster_sql(nhl_teams):
    for team in nhl_teams:
        roster_df(team).to_csv(team + '_roster_' + str(date.today()) + '.csv')#, engine)
        print(team)

#every_roster_sql(nhl_teams)
                               
def stats_df(team):
    data = use_api('https://api-web.nhle.com/v1/club-stats/' + team + '/20232024')
    playerType = ['skaters','goalies']
    cols = ['playerId', 'gamesPlayed', 'goals', 'assists', 'points', 'plusMinus',
            'penaltyMinutes', 'shots', 'shootingPctg']

    
#print(roster_df('COL'))
