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
import numpy as np
import time
from time import perf_counter
import math

nhl_teams = ["ANA", "ARI", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ", "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", "NYI", "NYR", "OTT", "PHI", "PIT", "SJS", "SEA", "STL", "TBL", "TOR", "VAN", "VGK", "WPG", "WSH"]

'''
x = [-78, -50,66]
y = [-13, -32,1] 
plt.scatter(x, y) 
plt.show()
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
#conn.autocommit = True

left = ['ANA', 'ARI', 'BOS', 'BUF', 'CBJ', 'EDM', 'NSH', 'NJD', 'NYI', 'OTT', 'PIT', 'SJS', 'SEA', 'STL', 'TBL']


    
def find_team_sides(teamlis):
    leftlis = []
    rightlis = []
    for team in teamlis:
        query = '''select AVG(coords_x) from "''' + team + '''_shots_home" 
    where "game_period" in ('1','3')
    and "event_team" = ''' + "'" + team + "';"
        try:
            cur.execute(query)
            conn.commit()
        except:
            print('Error!!!!!! ' + team)
        avg = [r[0] for r in cur.fetchall()][0]
        if avg < 1:
            leftlis.append(team)
        if avg > 1:
            rightlis.append(team)
                
    print("left teams: ")
    print(leftlis)
    print("right teams: ")
    print(rightlis)
    
#find_team_sides(nhl_teams)



def period_data(team, home, second):
    if second == False:
        periodstring = '''" where "game_period" in ('2','4')'''
    if second == True:
        periodstring = '''" where "game_period" in ('1','3')'''
        
    teamstring = ''' and "event_team" = ''' + "'" + team + "';"
    query = '''select AVG("coords_x") from "''' + team + '''_shots_''' + home + periodstring + teamstring
    #print(query)
    #print(team)
    #print(pd.read_sql_query(query, engine)["avg"])
    #print("game is " + game)
    #print("avg x coord for " + team + ": " + str(pd.read_sql_query(query, engine)))
    #plot_rink(pd.read_sql_query(query, engine), team)
    
#for team in nhl_teams:
    #period_data(team, "home", second = False)

def flip_right_away(team,game):
    string = '"' + team + '_shots_away"'
    query = '''update ''' + string + '''
    set "coords_x"="coords_x"*-1
    where "coords_x" is Not NULL
    and "coords_x" != 0
    and game_id = ''' + game + ";"
    #print(query)
    try:
        cur.execute(query)
        conn.commit()
        print('Away Success: ' + team + ' ' + game)
    except:
        print('Away Error!!!!!! ' + team + ' ' + game)

#flip_right_away('ANA', str(2023020031))
#conn.close()
        
def flip_left_home(team):
    string = '"' + team + '_shots_home"'
    query = '''update ''' + string + '''
    set "coords_x"="coords_x"*-1
    where "coords_x" is Not NULL
    and "coords_x" != 0;'''
    try:
        cur.execute(query)
        conn.commit()
        print('Home Success: ' + team)
    except:
        print('Home Error!!!!!! ' + team)


    
#create dict of opponent + game_id so away team data can be flipped to same sides
def left_homeys():
    for team in left_lis:
        #turn 2nd per + OT coords towards same side
        
        flip_left_home(team)
        
        query = '''select "away_team", "ID" from fullschedule
        where "home_team" = ''' + "'" + team + "';"
        res = dict(map(lambda i,j : (i,j) , pd.read_sql_query(query, engine)['away_team'].tolist(),pd.read_sql_query(query, engine)['ID'].tolist()))
        for x in res:
            flip_right_away(str(x),str(res[x]))
    
#left_homeys(left)
#conn.close()

def flip_all_second_data():
    
    periodstring = ''' where "game_period" in ('2','4')'''
    
    for team in nhl_teams:
        query = '''update "''' + team + '_shots_home" set "coords_x"="coords_x"*-1''' + periodstring + ''' and "coords_x" is Not NULL and "coords_x" != 0;'''
        queryaway = '''update "''' + team + '_shots_away" set "coords_x"="coords_x"*-1''' + periodstring + ''' and "coords_x" is Not NULL and "coords_x" != 0;'''
        try:
            cur.execute(query)
            cur.execute(queryaway)            
            conn.commit()
            print('Success for ' + team)
        except:
            print('error for ' + team)
            
#flip_all_second_data()
    
from hockey_rink import NHLRink, RinkImage
#rink.plot(

    
def plot_rink(df, team):
    df2 = df.where(df['event_team'] == team)
    #df2.to_csv('frank_for.csv',index=False)
    df3 = df.where(df['event_team'] != team)
    #df3.to_csv('frank_against.csv',index=False)
    
    x = df2['coords_x']
    y = df2['coords_y']
    plt.scatter(x, y,color='blue')
    x2 = df3['coords_x']
    y2 = df3['coords_y']
    plt.scatter(x2, y2,color='red')
    plt.xlim(-100,100)
    plt.ylim(-43,43)
    plt.show()
    
def shots_team(team):
    columns = ["coords_x", "coords_y", "game_seconds","event_type", "event_detail", "event_team", "game_score_state"]

    shot_query = '''select * from "''' + team + '''_shots_home" 
    where "event_zone" = 'Off'
    and "game_period" in ('2','4');'''
    
    return pd.read_sql_query(shot_query, engine)[columns]

def rev_plot_rink(df, team):
    df2 = df.where(df['event_team'] == team)
    #df2.to_csv('frank_for.csv',index=False)
    df3 = df.where(df['event_team'] != team)
    #df3.to_csv('frank_against.csv',index=False)
    
    x = df2['coords_x']
    y = df2['coords_y']
    plt.scatter(x, y,color='blue')
    x2 = df3['coords_x']
    y2 = df3['coords_y']
    plt.scatter(x2, y2,color='red')
    plt.xlim(-100,100)
    plt.ylim(-43,43)
    plt.show()

def flip_period_y_coords():
    periodstring = ''' where "game_period" in ('2','4')'''
    
    for team in nhl_teams:
        query = '''update "''' + team + '_shots_home" set "coords_y"="coords_y"*-1''' + periodstring + ''' and "coords_y" is Not NULL and "coords_y" != 0;'''
        queryaway = '''update "''' + team + '_shots_away" set "coords_y"="coords_y"*-1''' + periodstring + ''' and "coords_y" is Not NULL and "coords_y" != 0;'''
        try:
            cur.execute(query)
            cur.execute(queryaway)            
            conn.commit()
            print('Success for ' + team)
        except:
            print('error for ' + team)
            
#flip_period_y_coords()
#rev_plot_rink(shots_team('BUF'), 'BUF')

def avg_x_coord():
    query = '''select AVG(coords_x) from '''
    for team in nhl_teams:
        teamstring = '"' + team + '_shots_away"'
        eventstring = ''' where "event_team" = ''' + "'" + team + "';"
        #print(query + teamstring + eventstring)
        try:
            cur.execute(query + teamstring + eventstring)
            print(team + "success")
        except:
            print(error)
        print([r[0] for r in cur.fetchall()][0])

#def plot_fun(df):
    
#avg_x_coord()
def goals_vs_shots():
    columns = ["coords_x", "coords_y", "game_seconds","event_type", "event_detail", "event_team", "game_score_state"]

    goal_query = '''select * from "BUF_shots_home" 
    where "event_zone" = 'Off'
    and "event_type" = 'GOAL';'''
    shot_query = '''select * from "BUF_shots_home" 
    where "event_zone" = 'Off'
    and "event_type" != 'GOAL';'''

    shotdf = pd.read_sql_query(shot_query, engine)[columns]
    goaldf = pd.read_sql_query(goal_query, engine)[columns]
    rev_plot_rink(goaldf, 'BUF')
    
#goals_vs_shots()


    #while home:
   # return "poop"    
#rev_plot_rink(

    
#lis = ["'home_on_1'", "'home_on_2'", "'home_on_3'", "'home_on_4'", "'home_on_5'", "'home_on_6'"]
#conditions = []
#for each in lis:
    #conditions.append('df[' + each + '] = player')
#print(conditions)
       
def numpy_test(team, player):
    query = '''select * from "''' + team + '''_shots_home" 
    where "event_zone" = 'Off';'''
    df = pd.read_sql_query(query, engine) 
    #return df[(df['home_on_1'] == player) | (df['home_on_2'] == player) | (df['home_on_3'] == player) | (df['home_on_4'] == player) | (df['home_on_5'] == player) | (df['home_on_6'] == player)]
    return df.loc[np.where((df['home_on_1'] == player) | (df['home_on_2'] == player) | (df['home_on_3'] == player) | (df['home_on_4'] == player) | (df['home_on_5'] == player) | (df['home_on_6'] == player))]

def time_test():
    time_start = time.perf_counter()
    numpy_test('BUF', 'JORDAN GREENWAY')
    time_end = perf_counter()
    time_duration = time_end - time_start
    print(f'Took {time_duration} seconds')


def df_concat(team, playerOne, playerTwo):
    return df[(player_on_ice(team, playerOne)) & player_on_ice(team, playerTwo)]

def shot_angle(x,y,left):
    #print('x: ' + str(x))
    #print('y: ' + str(y))
    
    if left == True:
        net_x = -89
        
        #neg_post = [-89,-3]
        #pos_post = [-89,3]
        
    if left == False:
        net_x = 89
        
        #neg_post = [89,-3]
        #pos_post = [89,3]
        
    yc = [-3,3]
    net = 6

    #print('net_1: ' + str(net_x) + ',' + str(yc[0]))
    #print('net_2: ' + str(net_x) + ',' + str(yc[1]))
          
    a = [x-net_x,y-yc[0]]
    b = [x-net_x,y-yc[1]]
    
    #print("neg_vec: " + str(a))
    #print("pos_vec: " + str(b))

    dot_prod = (a[0] * b[0]) + (a[1] * b[1])

    #print('dot_prod: ' + str(dot_prod))
    
    a_mag = math.sqrt((a[0]*a[0]) + (a[1]*a[1]))
    #print('a_mag: ' + str(a_mag))
    
    b_mag = math.sqrt((b[0]*b[0]) + (b[1]*b[1]))
    #print('b_mag: ' + str(b_mag))
    
    angle = (dot_prod) / (a_mag * b_mag)
    #print('angle: ' + str(math.degrees(math.acos(angle))))

    distance = (a_mag + b_mag)/2
    #print('distance: ' + str(distance))
    
    if left == True:
        while x < -89:
            return [{'distance': str(distance)}, {'angle': '0'}]
        
    if left == False:
        while x > 89:
            return [{'distance': str(distance)}, {'angle': '0'}]
                
    return [{'distance': str(distance)}, {'angle' : str(math.degrees(math.acos(angle)))}]

    #print(math.degrees(math.atan((6/a_mag))))

def create_angle_column(row):
    angle_dict = shot_angle(row['coords_x'], row['coords_y'], left=True)
    
#shot_angle(89,0,left=False)

def LabelShotRebound(team):
    query = '''select * from "''' + team + '_shots_home"' + '''where "event_zone" = 'Off';'''
    df.loc[np.where(df['event_team'] == team)]
    
    
def group_by_strength(df):
    return df.groupby(df.game_strength_state)

def PK_of(grouped):
    return grouped.get_group('4v5')

def PP_of(grouped):
    return grouped.get_group('5v4')

def EV_of(grouped):
    return grouped.get_group('5v5')

def xG(team):
    query = '''select * from "''' + team + '_shots_home"' + '''where "event_zone" = 'Off';'''

    df = pd.read_sql_query(query, engine)
    #xgf = df.where(df['event_team'] != team)    
    xGF = df.loc[np.where(df['event_team'] == team)]
    xGA = df.loc[np.where(df["event_team"] != team)]
    return xGF

#df.where(df['event_team'] == team)
print(xG('BUF'))
#rev_plot_rink(EV_of(group_by_strength(xG('BUF'))),'BUF')#.to_csv('buf_xgf.csv',encoding='cp1252')
    
def multi_players(team, playerlis):
    query = '''select * from "''' + team + '''_shots_home" 
    where "event_zone" = 'Off';'''
    df = pd.read_sql_query(query, engine)
    numPlayers = len(playerlis)
    if numPlayers == 0:
        return "NO PLAYERS"
    if numPlayers == 1:
        return df.loc[np.where((df['home_on_1'] == playerlis[0]) | (df['home_on_2'] == playerlis[0]) | (df['home_on_3'] == playerlis[0]) | (df['home_on_4'] == playerlis[0]) | (df['home_on_5'] == playerlis[0]) | (df['home_on_6'] == playerlis[0]))]
    if numPlayers == 2:
        return df.loc[np.where(((df['home_on_1'] == playerlis[0]) | (df['home_on_2'] == playerlis[0]) | (df['home_on_3'] == playerlis[0]) | (df['home_on_4'] == playerlis[0]) | (df['home_on_5'] == playerlis[0]) | (df['home_on_6'] == playerlis[0]))
        & ((df['home_on_1'] == playerlis[1]) | (df['home_on_2'] == playerlis[1]) | (df['home_on_3'] == playerlis[1]) | (df['home_on_4'] == playerlis[1]) | (df['home_on_5'] == playerlis[1]) | (df['home_on_6'] == playerlis[1])))]
    if numPlayers == 3:
        return df.loc[np.where(((df['home_on_1'] == playerlis[0]) | (df['home_on_2'] == playerlis[0]) | (df['home_on_3'] == playerlis[0]) | (df['home_on_4'] == playerlis[0]) | (df['home_on_5'] == playerlis[0]) | (df['home_on_6'] == playerlis[0]))
        & ((df['home_on_1'] == playerlis[1]) | (df['home_on_2'] == playerlis[1]) | (df['home_on_3'] == playerlis[1]) | (df['home_on_4'] == playerlis[1]) | (df['home_on_5'] == playerlis[1]) | (df['home_on_6'] == playerlis[1]))
	& ((df['home_on_1'] == playerlis[2]) | (df['home_on_2'] == playerlis[2]) | (df['home_on_3'] == playerlis[2]) | (df['home_on_4'] == playerlis[2]) | (df['home_on_5'] == playerlis[2]) | (df['home_on_6'] == playerlis[2])))]
    if numPlayers == 4:
        return df.loc[np.where(((df['home_on_1'] == playerlis[0]) | (df['home_on_2'] == playerlis[0]) | (df['home_on_3'] == playerlis[0]) | (df['home_on_4'] == playerlis[0]) | (df['home_on_5'] == playerlis[0]) | (df['home_on_6'] == playerlis[0]))
        & ((df['home_on_1'] == playerlis[1]) | (df['home_on_2'] == playerlis[1]) | (df['home_on_3'] == playerlis[1]) | (df['home_on_4'] == playerlis[1]) | (df['home_on_5'] == playerlis[1]) | (df['home_on_6'] == playerlis[1]))
        & ((df['home_on_1'] == playerlis[2]) | (df['home_on_2'] == playerlis[2]) | (df['home_on_3'] == playerlis[2]) | (df['home_on_4'] == playerlis[2]) | (df['home_on_5'] == playerlis[2]) | (df['home_on_6'] == playerlis[2]))
        & ((df['home_on_1'] == playerlis[3]) | (df['home_on_2'] == playerlis[3]) | (df['home_on_3'] == playerlis[3]) | (df['home_on_4'] == playerlis[3]) | (df['home_on_5'] == playerlis[3]) | (df['home_on_6'] == playerlis[3])))]
    if numPlayers == 5:
        return df.loc[np.where(((df['home_on_1'] == playerlis[0]) | (df['home_on_2'] == playerlis[0]) | (df['home_on_3'] == playerlis[0]) | (df['home_on_4'] == playerlis[0]) | (df['home_on_5'] == playerlis[0]) | (df['home_on_6'] == playerlis[0]))
        & ((df['home_on_1'] == playerlis[1]) | (df['home_on_2'] == playerlis[1]) | (df['home_on_3'] == playerlis[1]) | (df['home_on_4'] == playerlis[1]) | (df['home_on_5'] == playerlis[1]) | (df['home_on_6'] == playerlis[1]))
        & ((df['home_on_1'] == playerlis[2]) | (df['home_on_2'] == playerlis[2]) | (df['home_on_3'] == playerlis[2]) | (df['home_on_4'] == playerlis[2]) | (df['home_on_5'] == playerlis[2]) | (df['home_on_6'] == playerlis[2]))
        & ((df['home_on_1'] == playerlis[3]) | (df['home_on_2'] == playerlis[3]) | (df['home_on_3'] == playerlis[3]) | (df['home_on_4'] == playerlis[3]) | (df['home_on_5'] == playerlis[3]) | (df['home_on_6'] == playerlis[3]))
        & ((df['home_on_1'] == playerlis[4]) | (df['home_on_2'] == playerlis[4]) | (df['home_on_3'] == playerlis[4]) | (df['home_on_4'] == playerlis[4]) | (df['home_on_5'] == playerlis[4]) | (df['home_on_6'] == playerlis[4])))] 
    
#df = multi_players('BUF', ['ALEX TUCH', 'JEFF SKINNER','RASMUS DAHLIN'])
#EV_of(group_by_strength(df)).to_csv('werwe.csv', encoding = 'utf-8')

def time_test2():
    time_start = time.perf_counter()
    multi_players('BUF', ['RASMUS DAHLIN', 'ALEX TUCH', 'JJ PETERKA', 'JEFF SKINNER'])
    time_end = perf_counter()
    time_duration = time_end - time_start
    print(f'Took {time_duration} seconds')

#time_test2()
'''
lis = []
x=0
for x in range(20):
    lis.append(str(x) + 'v' + str(x))

    j = x-3
    k = x-4
    l = x-5
    m = x-6
    p = x-7
    t = x-8
    o = x-9
    y = x-10
    numlis = [j,k,l,m,p,t,o,y]
    for each in numlis:
        if each > -1:
            lis.append(str(x) + 'v' + str(each))

print(lis)

from dataclasses import dataclass
@dataclass
class score_effects:
    homevaway: double,
    strength: string,
    event_team_home: 
'''   

def apply_score_effects(df):
    #create SVA column
    #https://evolving-hockey.com/glossary/score-adjustments/

    sva = [
        {'5v5' :
            [
                {'home' : [{'0v3' : .859}, {'0v2', .881}, {'0v1' : .909}, {'0v0': .968}, {'1v0' : 1.037}, {'2v0' : 1.078}, {'3v0': 1.109}]},
                {'away' : [{'0v3' : 1.197}, {'0v2', 1.155}, {'0v1' : 1.111}, {'0v0': 1.034}, {'1v0' : 0.966}, {'2v0' : 0.933}, {'3v0': .911}]}
            ]
        },
        {'4v4' :
            [
                {'home' : [{'0v3' : .933}, {'0v2', .881}, {'0v1' : .909}, {'0v0': .968}, {'1v0' : 1.037}, {'2v0' : 1.078}, {'3v0': 1.109}]},
                {'away' : [{'0v3' : 1.077}, {'0v2', 1.155}, {'0v1' : 1.111}, {'0v0': 1.034}, {'1v0' : 0.966}, {'2v0' : 0.933}, {'3v0': .911}]}
            ]
        }
    ]
    
    #closelis = ['0v1', '1v0', '0v0', '1v2', '2v1', '1v1', '2v3', '3v2', '2v2', '3v4', '4v3', '3v3', '4v5', '5v4', '4v4', '5v6', '6v5', '5v5',
    #'6v7', '7v6', '6v6', '7v8', '8v7', '7v7', '8v9', '9v8', '8v8', '9v10', '10v9', '9v9', '10v11', '11v10', '10v10', '11v12', '12v11',
    #'11v11', '12v13', '13v12', '12v12', '13v14', '14v13', '13v13', '14v15', '15v14', '14v14', '15v16', '16v15', '15v15']
    
    home_up_1 = ['1v0', '2v1', '3v2', '4v3', '5v4', '6v5', '7v6', '8v7', '9v8', '10v9', '11v10', '12v11', '13v12', '14v13', '15v14', '16v15', '17v16', '18v17', '19v18']
    
    home_up_2 = ['2v0', '3v1', '4v2', '5v3', '6v4', '7v5', '8v6', '9v7', '10v8', '11v9', '12v10', '13v11', '14v12', '15v13', '16v14', '17v15', '18v16', '19v17']

    home_up_3 = ['3v0', '4v1', '4v0', '5v2', '5v1', '5v0', '6v3', '6v2', '6v1', '6v0', '7v4', '7v3', '7v2', '7v1', '7v0', '8v5', '8v4', '8v3', '8v2', '8v1', '8v0', '9v6', '9v5', '9v4', '9v3', '9v2', '9v1', '9v0', '10v7',
                 '10v6', '10v5', '10v4', '10v3', '10v2', '10v1', '10v0', '11v8', '11v7', '11v6', '11v5', '11v4', '11v3', '11v2', '11v1','12v9', '12v8', '12v7', '12v6', '12v5', '12v4', '12v3', '12v2', '13v10', '13v9', '13v8', '13v7',
                 '13v6', '13v5', '13v4', '13v3', '14v11', '14v10', '14v9', '14v8', '14v7', '14v6', '14v5', '14v4', '15v12', '15v11', '15v10', '15v9', '15v8', '15v7', '15v6', '15v5', '16v13', '16v12', '16v11', '16v10', '16v9', '16v8', '16v7', '16v6', '17v14', '17v13', '17v12', '17v11', '17v10', '17v9', '17v8', '17v7', '18v15', '18v14', '18v13', '18v12', '18v11', '18v10', '18v9', '18v8', '19v16', '19v15', '19v14', '19v13', '19v12', '19v11', '19v10', '19v9']
    
    tied = ['0v0', '1v1', '2v2', '3v3', '4v4', '5v5', '6v6', '7v7', '8v8', '9v9', '10v10', '11v11', '12v12', '13v13', '14v14', '15v15', '16v16', '17v17', '18v18', '19v19']
    
    away_up_1 = ['0v1', '1v2', '2v3', '3v4', '4v5', '5v6', '6v7', '7v8', '8v9', '9v10', '10v11', '11v12', '12v13', '13v14', '14v15']
    
    away_up_2 = ['0v2', '1v3', '2v4', '3v5', '4v6', '5v7', '6v8', '7v9', '8v10', '9v11', '10v12', '11v13', '12v14', '13v15', '14v16']

    away_up_3 = ['0v3', '0v4', '0v5', '0v6', '0v7', '0v8', '0v9', '0v10', '1v4', '1v5', '1v6', '1v7', '1v8', '1v9', '1v10', '1v11', '2v5', '2v6', '2v7', '2v8', '2v9', '2v10', '2v11', '2v12', '3v6', '3v7', '3v8',
                '3v9', '3v10', '3v11', '3v12', '3v13', '4v7', '4v8', '4v9', '4v10', '4v11', '4v12', '4v13', '4v14', '5v8', '5v9', '5v10', '5v11', '5v12', '5v13', '5v14', '5v15', '6v9', '6v10', '6v11', '6v12', '6v13',
                '6v14', '6v15', '6v16', '7v10', '7v11', '7v12', '7v13', '7v14', '7v15', '7v16', '7v17', '8v11', '8v12', '8v13', '8v14', '8v15', '8v16', '8v17', '8v18', '9v12', '9v13', '9v14', '9v15', '9v16', '9v17', '9v18', '9v19']

    scorestates = [home_up_1, home_up_2, home_up_3, tied, away_up_1, away_up_2, away_up_3]
    strengths = ["Ev5", "6v4", "5v3","Ev4","5v5","6vE","5v4","4vE","4v5","3v3","5v6","4v3","3v5","4v4","6v5","5vE"]
    
    #print(sva[0].get['5v5'][0]['home'][0]["0v3"])
    
    #print(*[key for i in languages for key in i.keys()], sep = "\n")
    #away_1 = df[(df['game_score_state'].isin(away_up_1)) & (df['game_strength_state'] == '5v5')]
   #print(fenwick_events_adj[0]['5v5'][0]['home'])
    #return df[(df['game_score_state'].isin(away_up_1)) & (df['game_strength_state'] == '5v5')]
    
#df = multi_players('BUF', ['RASMUS DAHLIN', 'ALEX TUCH'])
#df.to_csv('p2qpq22.csv',encoding='cp1252')
#apply_score_effects('butt')

   # np.where(df['game_score_state']
    
#print(apply_score_effects(df))
#, 'RASMUS DAHLIN', 'TAGE THOMPSON', 'DYLAN COZENS'
#SUCC



    
#rev_plot_rink(pd.read_sql_query(query, engine), 'BUF')
    
#flip_left_home()
#right = ["CGY", "CAR", "CHI", "COL", "DAL", "DET", "FLA", "LAK", "MIN", "MTL", "NYR", "PHI", "TOR", "VAN", "VGK", "WPG", "WSH"]

#print(len(left))
#print(len(right))
#def team_sched(team, home):
    #query = '''select "ID" from fullschedule
#where "home_team" = ''' + "'" + team + "'" + ";"
    #for each in pd.read_sql_query(query, engine)['ID']:
       # period_data(team, home, each, second=True) 

    
#team_sched('COL', "home")
    
#plot_2nd_per('TOR', "home")          
#everyplayer = '''select distinct "shooterPlayerId" from shots_recent
#where season = %s;'''
