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
import matplotlib.pyplot as plt

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

def roster_names(team):
    roster_df = pd.read_sql(team + '_fullroster',engine)[['playerID','firstName','lastName']]
    name_roster = roster_df['firstName'] + ' ' + roster_df['lastName']
    id_lis = roster_df['playerID'].tolist()
    return dict(map(lambda i,j : (i,j) , id_lis,list(map(lambda x: x.upper(), name_roster.tolist()))))# {'playerId' : id_lis, 'name' : list(map(lambda x: x.upper(), name_roster.tolist()))}
    #df['firstName'] + df['lastName']
    #df = pd.read_sql_query(query, engine)
    #df.where(df['event_team'] == team)

#print(list(roster_names('BUF').values()))

def multi_players(home, team, playerlis):
    values = [team, 'SHOT', 'MISS', 'GOAL']
    
    query = '''select {event_var},{game_score}, {gamer},{x_coord},{y_coord},{team},{event_player_1},{event_player_2},{strength},{home_zero},
    {home_one},{home_two},{home_three},{home_four},{home_five}, {home_six},
    {away_zero},{away_one},{away_two},{away_three},{away_four},{away_five}, {away_six}
    from {table} where {homey} = %s
    and {zoney} = 'Off'
    and ({event_var} = %s or {event_var} = %s or {event_var} = %s)'''
    new = ''
    if home == "home_team":
        for each in playerlis:
            new += ' and %s in ({home_one}, {home_two}, {home_three}, {home_four}, {home_five})'
            values.append(each)
    if home == "away_team":
        for each in playerlis:
            new += ' and %s in ({away_one}, {away_two}, {away_three}, {away_four}, {away_five})'
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
            home_six=sql.Identifier("home_on_7"),            
            away_zero=sql.Identifier("away_on_1"),
            away_one=sql.Identifier("away_on_2"),
            away_two=sql.Identifier("away_on_3"),
            away_three=sql.Identifier("away_on_4"),
            away_four=sql.Identifier("away_on_5"),
            away_five=sql.Identifier("away_on_6"),
            away_six=sql.Identifier("away_on_7"),)
    #print(sdf)
    cur.execute(sdf,values)
    cols = cur.execute(sdf,values)
    df = pd.DataFrame(cur.fetchall(), columns = [desc[0] for desc in cur.description])
    return df

def buff(playerlis):
    query = '''select * from "BUF_shots_home" 
    where "event_zone" = 'Off';'''
    df = pd.read_sql_query(query, engine)
    players = ['home_on_1', 'home_on_2', 'home_on_3', 'home_on_4', 'home_on_5', 'home_on_6', 'home_on_7']
    return df[players].isin(playerlis)
    #return df.isin(playerlis)

def isolated_impact(team):
    for each in list(roster_names('BUF').values()):
        df = multi_players("home_team", 'BUF', [each])
        df_good = df.where(df['event_team'] == team)
        df_bad = df.where(df['event_team'] != team)
        plt.scatter(df_good['coords_x'], df_good['coords_y'],color='blue')
        plt.scatter(df_bad['coords_x'], df_bad['coords_y'],color='red')
        plt.xlim(-100,100)
        plt.ylim(-43,43)
        plt.show()
        break
isolated_impact('BUF')
#print(roster_names('BUF'))
'''
'''
