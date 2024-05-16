import requests
import json
import csv
import pandas as pd
import os
from html.parser import HTMLParser
import sqlalchemy as db
import sqlalchemy.orm
import psycopg2
from sqlalchemy import URL, create_engine, select
from psycopg2 import sql
import psycopg2.extras as extras
import time

nhl_team_array = ["ANA", "ARI", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ",
    "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", "NYI", "NYR",
    "OTT", "PHI", "PIT", "SJS", "SEA", "STL", "TBL", "TOR", "VAN", "VGK", "WPG", "WSH"]

nhl_team_array2 = ["ANA", "ARI"]

#glossary = "https://api.nhle.com/stats/rest/en/glossary"
#game_id = "gameId=2021020001"
#shift_chart = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=" + game_id
idk_stats = "https://api.nhle.com/stats/rest/en/skater/puckPossessions?cayenneExp=seasonId=20232024&limit=-1&start=0"
idk_stats_2022 = "https://api.nhle.com/stats/rest/en/skater/puckPossessions?cayenneExp=seasonId=20222023&limit=-1&start=0"
#player_stats = "https://api-web.nhle.com/v1/skater-stats-leaders/20232024/2?categories=" + stat_category + "&limit=-1"
#csv_filename = "C:/Users/pgrts/Desktop/python/scrape_steps/" + team + "_roster.csv"


url_hky = URL.create(
    "postgresql+psycopg2",
    username="postgres",
    password="p33Gritz!!",  
    host="localhost",
    port="5432",
    database="hockey",
)
try:
    engine = db.create_engine(url_hky)
except:
    print("I am unable to connect to the engine")

try:
    conn = psycopg2.connect(
        host="localhost",
        database="hockey",
        user="postgres",
        password="p33Gritz!!",
        port="5432"
    )
except:
    print("I am unable to connect to the psycopg2")
    
#with open(csv_filename, "w") as my_file:
    #my_file.write(header)
    #my_file.write("\n")
for team in nhl_team_array2:
    skater_table = str(team.lower()) + "_skaters"
    goalie_table = str(team.lower()) +  "_goalies"
    API_players = "https://api-web.nhle.com/v1/club-stats/" + team + "/20232024/2"
    response = requests.get(API_players)
    data = response.json()
    #print(data)
   
    playerType = {"skaters" : skater_table, "goalies" : goalie_table}
    for key, value in playerType.items():
        for roster in data[key]:
            for ke, name in roster.items():
                if ke == 'firstName':
                    newN = ''
                    for k,v in name.items():
                        if k == "default":
                            newN = v
                    roster.update({ke:name, ke: newN})
                    #print(ke + '->' + newN)
                if ke == 'lastName':
                    newN = ''
                    for k,v in name.items():
                        if k == "default":
                            #print(v)
                            newN = v                        
                        #newN = v
                    roster.update({ke:name, ke: newN})
                    #print(ke + '->' + newN)
        df = pd.DataFrame(data[key])
        df.to_csv(value + '.csv')
        df.to_sql(value, engine, if_exists = 'replace')
        #print(stuff)
''' 
 
    playerType = {"skaters" : skater_table, "goalies" : goalie_table}
    for key, value in playerType.items():
        #for item in data[key]:
        df = pd.DataFrame(data[key])
        names = ['firstName', 'lastName']
        for name in names:
            
            
        #df.to_csv(value + '.csv')#, engine, if_exists='append')

    for x in data["skaters"]:
        my_file.write((str(x["playerId"])))
        my_file.write(",")
        my_file.write(team)
        my_file.write(",")
        my_file.write(str(x["firstName"]["default"]))
        my_file.write(",")            
        my_file.write(str(x["lastName"]["default"]))
        my_file.write(",")
        my_file.write(str(x["positionCode"]))
        my_file.write(",")
        my_file.write(str(x["gamesPlayed"]))
        my_file.write(",")
        my_file.write(str(x["goals"]))
        my_file.write(",")
        my_file.write(str(x["assists"]))
        my_file.write(",")
        my_file.write(str(x["points"]))
        my_file.write(",")
        my_file.write(str(x["plusMinus"]))
        my_file.write("\n")
'''

    

