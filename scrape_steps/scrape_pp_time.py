import requests
from bs4 import BeautifulSoup
import pandas as pd
from html.parser import HTMLParser
import sqlalchemy as db
import sqlalchemy.orm
import psycopg2
from sqlalchemy import URL, create_engine, select
from psycopg2 import sql
import psycopg2.extras as extras
import json
import datetime
import csv

nhl_team_array = ["ANA", "ARI", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ",
    "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", "NYI", "NYR",
    "OTT", "PHI", "PIT", "SJS", "SEA", "STL", "TBL", "TOR", "VAN", "VGK", "WPG", "WSH"]

nhl_team_name = ["Anaheim Ducks", "Arizona Coyotes", "Boston Bruins", "Buffalo Sabres", "Calgary Flames", "Carolina Hurricanes", "Chicago Blackhawks", "Colorado Avalanche",
                 "Columbus Blue Jackets", "Dallas Stars", "Detroit Red Wings", "Edmonton Oilers", "Florida Panthers", "Los Angeles Kings", "Minnesota Wild", "Montréal Canadiens", "Nashville Predators",
                "New Jersey Devils", "New York Islanders", "New York Rangers", "Ottawa Senators", "Philadelphia Flyers", "Pittsburgh Penguins", "San Jose Sharks", "Seattle Kraken", "St. Louis Blues",
                 "Tampa Bay Lightning", "Toronto Maple Leafs", "Vancouver Canucks", "Vegas Golden Knights", "Winnipeg Jets", "Washington Capitals"]
nhl_dict = {'Team' : nhl_team_array, 'teamFullName' : nhl_team_name}

#dictDf.to_csv("teamNameDictionary.csv")

#w1, 50toi
#pp_time = \
#"https://www.nhl.com/stats/teams?report=powerplaytime&reportType=season&seasonFrom=20232024&seasonTo=20232024&gameType=2&filter=gamesPlayed,gte,1&sort=timeOnIcePp&page=0&pageSize=50"
#all scores, 10toi
pp_time_url = \
"https://api.nhle.com/stats/rest/en/team/powerplaytime?isAggregate=false&isGame=false&sort=%5B%7B%22property%22:%22timeOnIce5v4%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22teamId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=50&factCayenneExp=gamesPlayed%3E=1&cayenneExp=gameTypeId=2%20and%20seasonId%3C=20232024%20and%20seasonId%3E=20232024"
#all scores, 10toi
#pp_url = \
#"https://www.naturalstattrick.com/playerteams.php?fromseason=20232024&thruseason=20232024&stype=2&sit=5v4&score=all&stdoi=oi&rate=r&team=ALL&pos=S&loc=B&toi=10&gpfilt=none&fd=&td=&tgp=410&lines=single&draftteam=ALL"

pp_filename = "pp_time.csv"
pp_table = "pp_time"

r = requests.get(pp_time_url)
data = r.json()

#df3 = pd.DataFrame.from_dict(data["data"])
#df2 = df3[["teamFullName", "timeOnIce5v4"]]

#df2["timeOnIce5v4"] = pd.to_datetime(df2["timeOnIce5v4"], unit='m')
#print(df2)
#df = 
#df['team'] = dictDf['team'].map(nhl_dict)
#print(df)
df2 = pd.merge(pd.DataFrame(data["data"], columns=["teamFullName","timeOnIce5v4"]), \
               pd.DataFrame.from_dict(nhl_dict), on='teamFullName', how='inner')

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
    
df2.to_sql(pp_table,engine,if_exists='replace')
'''
#dictDf.to_sql("dictionary_teams", engine, if_exists='replace')
              
#df2 = pd.json_normalize(data['data'], ['teamFullName']['timeOnIce5v4'])
                                
#df.columns = ['Team', 'TOI']
#print(df)


#df = pd.DataFrame(columns=['TOI', 'Team'])

#lst=[]


for item in data["data"]:
    
    toi = item["timeOnIce5v4"]
    teamname = item["teamFullName"]
    convert = str(datetime.timedelta(seconds=toi))

    df = df.concat({'TOI' : convert, 'Team' : teamname}, ignore_index=true)
print(df)
        


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

df = pd.read_html(pp_time_url, header=0, index_col = 0, na_values=[""])[0]
df.to_csv(pp_filename)
df.to_sql(pp_table, engine, if_exists='replace')
'''
