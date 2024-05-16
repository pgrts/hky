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

#dictDf.to_sql(


#header = 
#glossary = "https://api.nhle.com/stats/rest/en/glossary"
#game_id = "gameId=2021020001"
#shift_chart = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=" + game_id
#idk_stats = "https://api.nhle.com/stats/rest/en/skater/puckPossessions?cayenneExp=seasonId=20232024&limit=-1&start=0"
#idk_stats_2022 = "https://api.nhle.com/stats/rest/en/skater/puckPossessions?cayenneExp=seasonId=20222023&limit=-1&start=0"

#goalie_stats = "https://api.nhle.com/stats/rest/en/goalie/savesByStrength?isAggregate=false&isGame=false&sort=%5B%7B%22property%22:%22evSavePct%22,%22direction%22:%22ASC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=50&factCayenneExp=gamesPlayed%3E=1&cayenneExp=gameTypeId=2%20and%20seasonId%3C=20232024%20and%20seasonId%3E=20232024"
csv_filename = "C:/Users/pgrts/Desktop/python/scrape_steps/" + "goalie_stats2.csv"
the_table = "goalie_stats"
goalie_stats2 = "https://api.nhle.com/stats/rest/en/goalie/savesByStrength?isAggregate=false&isGame=false&sort=%5B%7B%22property%22:%22evSavePct%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22DESC%22%7D%5D&start=0&limit=50&factCayenneExp=gamesPlayed%3E=1&cayenneExp=gameTypeId=2%20and%20seasonId%3C=20232024%20and%20seasonId%3E=20232024"
goalie_stats3 = "https://api.nhle.com/stats/rest/en/goalie/savesByStrength?isAggregate=false&isGame=false&sort=%5B%7B%22property%22:%22evSavePct%22,%22direction%22:%22ASC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=41&factCayenneExp=gamesPlayed%3E=1&cayenneExp=gameTypeId=2%20and%20seasonId%3C=20232024%20and%20seasonId%3E=20232024"

r = requests.get(goalie_stats3)
data = r.json()
r2 = requests.get(goalie_stats2)
data2 = r2.json()

data3 = data["data"] + data2["data"]
df=pd.DataFrame(data3)


df.to_csv(csv_filename)

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
    
df.to_sql(the_table, engine, if_exists='replace')

'''  
with open(csv_filename, "w") as my_file:
    my_file.write(header)
    my_file.write("\n")
    for team in nhl_team_array:
        API_players = "https://api-web.nhle.com/v1/club-stats/" + team + "/20232024/2"
        response = requests.get(API_players)
        data = response.json()
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

for team1 in nhl_team_name:
    for team in nhl_team_array:
        nhl_dict[team1] = team
        nhl_team_array.remove(team)
        break
    
dictDf = pd.DataFrame.from_dict(nhl_super_dict, orient='index')

'''

