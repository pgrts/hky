from bs4 import BeautifulSoup as bs
import requests
import json
import pandas as pd
import os
import sqlalchemy as db
import sqlalchemy.orm
import psycopg2
from sqlalchemy import URL, create_engine, select, text
from psycopg2 import sql
from datetime import date, datetime, timedelta
from io import StringIO
from functools import reduce

yesterday  = date.today() - timedelta(days = 1)
today = date.today()

nhl_teams = ["ANA", "ARI", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ",
    "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", "NYI", "NYR",
    "OTT", "PHI", "PIT", "SJS", "SEA", "STL", "TBL", "TOR", "VAN", "VGK", "WPG", "WSH"]

#nhl_tablenames = ['only_ANA_shots_home', 'only_ANA_shots_away', 'only_ARI_shots_home', 'only_ARI_shots_away', 'only_BOS_shots_home', 'only_BOS_shots_away', 'only_BUF_shots_home', 'only_BUF_shots_away', 'only_CGY_shots_home', 'only_CGY_shots_away', 'only_CAR_shots_home', 'only_CAR_shots_away', 'only_CHI_shots_home', 'only_CHI_shots_away', 'only_COL_shots_home', 'only_COL_shots_away', 'only_CBJ_shots_home', 'only_CBJ_shots_away', 'only_DAL_shots_home', 'only_DAL_shots_away', 'only_DET_shots_home', 'only_DET_shots_away', 'only_EDM_shots_home', 'only_EDM_shots_away', 'only_FLA_shots_home', 'only_FLA_shots_away', 'only_LAK_shots_home', 'only_LAK_shots_away', 'only_MIN_shots_home', 'only_MIN_shots_away', 'only_MTL_shots_home', 'only_MTL_shots_away', 'only_NSH_shots_home', 'only_NSH_shots_away', 'only_NJD_shots_home', 'only_NJD_shots_away', 'only_NYI_shots_home', 'only_NYI_shots_away', 'only_NYR_shots_home', 'only_NYR_shots_away', 'only_OTT_shots_home', 'only_OTT_shots_away', 'only_PHI_shots_home', 'only_PHI_shots_away', 'only_PIT_shots_home', 'only_PIT_shots_away', 'only_SJS_shots_home', 'only_SJS_shots_away', 'only_SEA_shots_home', 'only_SEA_shots_away', 'only_STL_shots_home', 'only_STL_shots_away', 'only_TBL_shots_home', 'only_TBL_shots_away', 'only_TOR_shots_home', 'only_TOR_shots_away', 'only_VAN_shots_home', 'only_VAN_shots_away', 'only_VGK_shots_home', 'only_VGK_shots_away', 'only_WPG_shots_home', 'only_WPG_shots_away', 'only_WSH_shots_home', 'only_WSH_shots_away']

def use_api(url):
    #url = string_1 + value + string_2
    return requests.get(url).json()

def get_games(date):   
    game_list = use_api('https://api-web.nhle.com/v1/score/' + date)["games"]
    return [d['id'] for d in game_list]

'''
def append_rosters():
    gamelis = pd.read_sql("fullschedule", engine)["ID"].tolist()
    for each in gamelis:
'''        

url_hky = URL.create(
    "postgresql+psycopg2",
    username="postgres",
    password="p33Gritz!!",  
    host="localhost",
    port="5432",
    database="playbyplay3",
)
try:
    engine = db.create_engine(url_hky)
except:
    print("I am unable to connect to the engine")

try:
    conn = psycopg2.connect(
        host="localhost",
        database="playbyplay3",
        user="postgres",
        password="p33Gritz!!",
        port="5432"
    )
except:
    print("I am unable to connect to the psycopg2")

cur=conn.cursor()

def multi_players(home, team, playerlis):
    values = [team, 'SHOT', 'MISS', 'GOAL']
    
    query = '''select {event_var},"event_detail",{game_score}, {gamer},{x_coord},{y_coord},{team},{event_player_1},{event_player_2},"event_player_3","game_period",{strength},{home_zero},
    {home_one},{home_two},{home_three},{home_four},{home_five},
    {away_zero},{away_one},{away_two},{away_three},{away_four},{away_five}
    from {table} where {homey} = %s
    and {zoney} = 'Off'
    and ({event_var} = %s or {event_var} = %s or {event_var} = %s)'''
    new = ''
    if home == "home_team":
        for each in playerlis:
            new += ' and %s in ({home_zero}, {home_one}, {home_two}, {home_three}, {home_four}, {home_five})'
            values.append(each)
    if home == "away_team":
        for each in playerlis:
            new += ' and %s in ({away_zero}, {away_one}, {away_two}, {away_three}, {away_four}, {away_five})'
            values.append(each)
    if not playerlis:
        new += ';'
    newquery = query + new + ';'
    sdf = sql.SQL(newquery).format(
            table=sql.Identifier(team + '_shots_' + home[:4]),
            event_var =sql.Identifier("event_type"),
            gamer=sql.Identifier("game_id"),
            x_coord=sql.Identifier("coords_x"),
            y_coord=sql.Identifier("coords_y"),
            team=sql.Identifier("event_team"),
            event_player_1=sql.Identifier("event_player_1"),
            event_player_2=sql.Identifier("event_player_2"),
            game_score=sql.Identifier("game_score_state"),
            strength= sql.Identifier("game_strength_state"),
            homey=sql.Identifier(home),
            zoney=sql.Identifier("event_zone"),
            home_zero=sql.Identifier("home_on_1"),
            home_one=sql.Identifier("home_on_2"),
            home_two=sql.Identifier("home_on_3"),
            home_three=sql.Identifier("home_on_4"),
            home_four=sql.Identifier("home_on_5"),
            home_five=sql.Identifier("home_on_6"),
            away_zero=sql.Identifier("away_on_1"),
            away_one=sql.Identifier("away_on_2"),
            away_two=sql.Identifier("away_on_3"),
            away_three=sql.Identifier("away_on_4"),
            away_four=sql.Identifier("away_on_5"),
            away_five=sql.Identifier("away_on_6"))
    #print(cur.mogrify(sdf,values))
    cur.execute(sdf,values)
    cols = cur.execute(sdf,values)
    df = pd.DataFrame(cur.fetchall(), columns = [desc[0] for desc in cur.description])
    return df

multi_players("home_team", 'BUF', ['ALEX TUCH']).to_csv('a2sdfds.csv')
print('succ')

              
def multi_players_awayhome(team, playerlis):
    df_home = multi_players('home_team',team,playerlis)
    df_away = multi_players('away_team',team,playerlis)
    return pd.concat([df_home,df_away])

def multi_testdf(team, playerlis):
    return multi_players_awayhome(team,playerlis)

#def create_xG_column():
    
#multi_players('home_team', 'ANA', ['FRANK VATRANO']).to_csv('asdfds.csv',index=False)

#print(multi_testdf('COL'))

#multi_testdf(team,['JONATHAN DROUIN','CALE MAKAR'])

def team_ev_w1(team, home):
    query = '''select * from {table}
where "event_team" = %s
and "event_zone" = 'Off'
and "game_strength_state" = '5v5'
and "game_score_state" in ('0v1', '1v0', '0v0', '1v2', '2v1', '1v1', '2v3', '3v2', '2v2', '3v4', '4v3', '3v3', 
'4v5', '5v4', '4v4', '5v6', '6v5', '5v5', '6v7', '7v6', '6v6', '7v8', '8v7', '7v7', '8v9', '9v8', '8v8', '9v10', 
'10v9', '9v9', '10v11', '11v10', '10v10', '11v12', '12v11', '11v11', '12v13', '13v12', 
'12v12', '13v14', '14v13', '13v13', '14v15', '15v14', '14v14', '15v16', '16v15', '15v15')'''
    sdf = sql.SQL(query).format(
         table=sql.Identifier(team + '_shots_' + home[:4]))
    cur.execute(sdf, (team,))
    cols = cur.execute(sdf,(team,))
    df = pd.DataFrame(cur.fetchall(), columns = [desc[0] for desc in cur.description])
    return df

#def team_with_player_ev_w1(playerlis, team, home):

    
def team_ev_w1_union(team):
    return pd.concat([team_SVA_w1(team,"home_team"), team_SVA_w1(team,"away_team")])   
    
#print(team_SVA_w1_union('DAL'))

#pd.read_csv('C:/Users/pgrts/Downloads/shots_2007-2022/shots_2007-2022.csv').to_sql('shots_2007_2022',engine)
#lis = []
#for each in nhl_teams:
   # print('"' + each + 'gameday_rosters"' +',')
#print(lis)


def group_by_strength(df):
    return df.groupby(df.game_strength_state)

def PK_of(grouped):
    return grouped.get_group('4v5')

def PP_of(grouped):
    return grouped.get_group('5v4')

def EV_of(grouped):
    return grouped.get_group('5v5')

#def player_on(df):
    #df.where(player
#print(PK_of(group_by_strength(multi_players_awayhome('COL',['JONATHAN DROUIN','CALE MAKAR']))))


#def roster_shots(game):
    #for each in get_roster_by_game[home]:
        #sql.SQL('playername = {player}
#print(roster_shots('2023020061')


#for each in nhl_teams:
    #print('"' + each + '_gameday_rosters",')

#def use_rosters_sql():
    #for each in nhl_teams:
        #rosters_sql(each)
                 

