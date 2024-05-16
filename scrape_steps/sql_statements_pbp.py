from datetime import date, timedelta
from bs4 import BeautifulSoup
import pandas as pd
from html.parser import HTMLParser
import sqlalchemy as db
import sqlalchemy.orm
import psycopg2
from sqlalchemy import URL, create_engine, select
from psycopg2 import sql
from api_stuff3 import get_yesterdays_games, use_api, get_teams, get_roster
import hockey_scraper

url_hky = URL.create(
    "postgresql+psycopg2",
    username="postgres",
    password="p33Gritz!!",  
    host="localhost",
    port="5432",
    database="playbyplay2",
)
try:
    engine = db.create_engine(url_hky)
except:
    print("I am unable to connect to the engine")

try:
    conn = psycopg2.connect(
        host="localhost",
        database="playbyplay2",
        user="postgres",
        password="p33Gritz!!",
        port="5432"
    )
except:
    print("I am unable to connect to the psycopg2")

def by_game(game_id):
    #game_id_chop = game_id[4:]
    sqlquery = ' where "Game_Id" =' + game_id[4:]
    #sql = '''where "Game_Id" = {game_id_chop};'''
    return sqlquery

def generate_database_yesterday():
    hockey_scraper.scrape_date_range(yesterday, today, True)
    pbpdf = pd.read_csv("nhl_pbp_" + datestring + ".csv")
    shiftdf = pd.read_csv("nhl_shifts_" + datestring + ".csv")
    pbpdf.to_sql(pbp_table, engine, if_exists='append')
    shiftdf.to_sql(shifts_table, engine, if_exists = 'append')
    
today = str(date.today())
yesterday = str(date.today() - timedelta(days = 1))
datestring = yesterday + "--" + today

pbp_table = "pbp_" + datestring
shifts_table = "shifts_" + datestring

#games_str = map(str, get_yesterdays_games())

#sqlquery = 'select * from "' + pbp_table + '" where "Game_Id" =' + '2023020929'[4:]
#print(sqlquery)

#df = pd.read_sql_query(by_game('2023020929'),engine)
#print(df)

columns = '''select "Event", "Strength", "Ev_Zone", "Home_Zone", "Away_Zone", "Type", "Away_Team", "Home_Team", "p1_name", "p1_ID", "p2_name", "p2_ID", "p3_name", "p3_ID", "awayPlayer1", "awayPlayer1_id"'''
from_pbp = ' from "' + pbp_table + '"'
from_shifts = ' from "' + shifts_table + '"'
even_strength = '''where "Strength" = '5x5' '''

end = ";"

def by_id(playerid):
    return '''where 'playerId' == ''' + playerid +
'''
for game in games_str:
    teams = get_teams(game)
    awayTeam = teams[0]
    homeTeam = teams[1]
    print(teams)
    print(awayTeam)
    print(homeTeam)
    #df = pd.read_sql_query(columns + by_game(game), engine)
    #print(df)

''' 



'''
generate_sql:
    game_id = get_yesterdays_games #list
    #scrape list, return pbp + shift data
    df.to_sql("blahblah"+ game_id[i], engine, if_exists = 'append')
    
        #'select distinct(game_id[i])
            
#select distinct("Game_Id") from "pbp_2024-02-27--2024-02-28";

with open(pbpcsv, "r") as my_file:
 
    


("awayPlayer1_id", "awayPlayer2_id", "awayPlayer3_id", "awayPlayer4_id", "awayPlayer5_id");
'''   
