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


def use_api(url):
    return requests.get(url).json()

#print(use_api())

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
    database="pbp5",
)
try:
    engine = db.create_engine(url_hky)
except:
    print("I am unable to connect to the engine")

try:
    conn = psycopg2.connect(
        host="localhost",
        database="pbp5",
        user="postgres",
        password="p33Gritz!!",
        port="5432"
    )
except:
    print("I am unable to connect to the psycopg2")

cur=conn.cursor()
#conn.autocommit = True

left = ['ANA', 'ARI', 'BOS', 'BUF', 'CBJ', 'EDM', 'NSH', 'NJD', 'NYI', 'OTT', 'PIT', 'SJS', 'SEA', 'STL', 'TBL']

def create_pickle():
    pd.read_sql("shifts",engine).to_pickle('shifts_20232024.pkl')
    print("hollas")
    
#create_pickle()

#shots_team('COL').to_csv('3193.csv')
#shots_team('COL').to_csv('col.csv',encoding='utf-8-sig')

def match_left_right():
    for team in left:
        query = '''select * from "shots"
        where "home_team" = ' ''' + team + ''''
        and "game_period" in ('2','4');'''
        query = '''update "shots" set "coords_x"="coords_x"*-1 where "home_team" = ''' + "'" + team + "'" + ''' and "coords_x" is Not NULL and "coords_x" != 0;'''
        query2 = '''update "shots" set "coords_y"="coords_y"*-1 where "home_team" = ''' + "'" + team + "'" + ''' and "coords_x" is Not NULL and "coords_x" != 0;'''
        
        print(query)
        cur.execute(query)
        print('x success')
        conn.commit()

        print(query2)
        cur.execute(query2)
        print('y success')
        conn.commit()
        

def test_per2_flip(team):
    query = '''update "shots" set "coords_x"="coords_x"*-1 where "home_team" = ''' + "'" + team + "'" + '''and "game_period" in ('2','4') and "coords_x" is Not NULL and "coords_x" != 0;'''
    query2 = '''update "shots" set "coords_y"="coords_y"*-1 where "home_team" = ''' + "'" + team + "'" + '''and "game_period" in ('2','4') and "coords_y" is Not NULL and "coords_y" != 0;'''
    
    print(query)
    cur.execute(query)
    print('x success')
    conn.commit()
    
    print(query2)
    cur.execute(query2)
    print('y success')
    conn.commit()


def flip_2nd_4th():
    query = '''update "shots" set "coords_x"="coords_x"*-1 where "game_period" in ('2','4') and "coords_x" is Not NULL and "coords_x" != 0;'''
    query2 = '''update "shots" set "coords_y"="coords_y"*-1 where "game_period" in ('2','4') and "coords_y" is Not NULL and "coords_y" != 0;'''
    
    print(query)
    cur.execute(query)
    print('x success')
    conn.commit()

    print(query2)
    cur.execute(query2)
    print('y success')
    conn.commit()

#flip_2nd_4th()

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


    
from hockey_rink import NHLRink, RinkImage
import numpy.linalg as la

    
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
    
def rev_plot_rink():
    query = '''select "coords_x","coords_y" from shots
where ("event_team" = "home_team")
and "event_zone" = 'Off';'''
    df = pd.read_sql_query(query,engine)
    query2 = '''select "coords_x","coords_y" from shots
where ("event_team" = "away_team")
and "event_zone" = 'Off';'''
    df2 = pd.read_sql_query(query2,engine)
    #ls = [2023020078, 2023020242, 2023020251, 2023020254, 2023020267, 2023020573]
    
    #df2 = df.where(df['event_team'] == df['home_team'])
    #df2.to_csv('frank_for.csv',index=False)
    #df3 = df.where(df['event_team'] != df['away_team'])
    #df.to_csv('franft.csv',index=False)
    
    x = df['coords_x']
    y = df['coords_y']
    plt.scatter(x, y,color='blue')
    x2 = df2['coords_x']
    y2 = df2['coords_y']
    plt.scatter(x2, y2,color='red')
    plt.xlim(-100,100)
    plt.ylim(-43,43)
    plt.show()

#rev_plot_rink()

#update "shots"
#set "coords_x"="coords_x"*-1
#where "game_id" in (2023020242, 2023020251, 2023020254, 2023020267, 2023020573)
#and "coords_x" is Not NULL and "coords_x" != 0;

# DONE

#same for coords_y
#rev_plot_rink()

def flip_special_games():
    query = '''select distinct(game_id) from "shots" where "event_team" = "away_team"
and "event_zone" = 'Off'
and "coords_x" < 25;'''
    print(pd.read_sql_query(query,engine)['game_id'].tolist())
    #df_home = np.where(df['event_team'] == df['home_team'])
    #df_away = np.where(df['away_team'] == df['event_team'])

    #df_home_bad = df.where(df['coords_x'] > -25)
    #df_away_bad = df.where(df['coords_x'] < 25)
    #x = df_home_bad['coords_x']
    #y = df_home_bad['coords_y']
    #plt.scatter(x, y,color='blue')
    #x2 = df_away_bad['coords_x']
    #y2 = df_away_bad['coords_y']
    #plt.scatter(x2, y2,color='red')
    #plt.xlim(-100,100)
    #plt.ylim(-43,43)
    #plt.show()
    #df_home.to_csv('home.csv')
    #df_away_bad.to_csv('away_bad.csv')

#flip_special_games()
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

def shots_team(team):

    shot_query = '''select * from "shots"
    where ''' + "'" + team + "'" + '''in ("home_team", "away_team") and "event_zone" = 'Off';'''
    
    return pd.read_sql_query(shot_query, engine)

#goals_vs_shots()


    #while home:
   # return "poop"    
#rev_plot_rink(

    
#lis = ["'home_on_1'", "'home_on_2'", "'home_on_3'", "'home_on_4'", "'home_on_5'", "'home_on_6'"]
#conditions = []
#for each in lis:
    #conditions.append('df[' + each + '] = player')
#print(conditions)
   
def game_dictionary(df):
    game_lis = df.game_id.unique()
    DataFrameDict = {elem : pd.DataFrame() for elem in game_lis}

    for key in DataFrameDict.keys():
        DataFrameDict[key] = df[:][df.game_id == key]

    return DataFrameDict

#game_dictionary(shots_team('COL'))[2023020008].to_csv('0101.csv')    
#game_flurries(game_dictionary(shots_team('COL'))[2023020008]).to_csv('2023020008.csv')

def df_concat(team, playerOne, playerTwo):
    return df[(player_on_ice(team, playerOne)) & player_on_ice(team, playerTwo)]

def shot_angle(v1, v2):
    """ Returns the angle in radians between vectors 'v1' and 'v2'    """
    cosang = np.dot(v1, v2)
    sinang = la.norm(np.cross(v1, v2))
    return round(np.rad2deg(np.arctan2(sinang, cosang)),4)

#only works on events in offensive or defensive zone
def process_coords(x,y):

    if x < 0:
        net_x = -89

    if x > 0:
        net_x = 89

    if (x<24) and (x>-24):
        print('neutral zone shot, invalid')
        return [{'distance': np.nan}, {'angle': np.nan}]
        
    if (x == 'nan') or (y == 'nan'):
        print('invalid coords')
        return [{'distance': np.nan}, {'angle': np.nan}]
        
    if(x > 89) or (x < -89) or (y>43) or (y<-43):
        return [{'distance': np.nan}, {'angle': np.nan}]
    
    yc = [-3,3]
    net = 6

    a = [x-net_x,y-yc[0]]
    b = [x-net_x,y-yc[1]]

    net_arr = np.array((net_x, 0))
    point_arr = np.array((x,y))
    dist = np.linalg.norm(net_arr - point_arr)
    angle = shot_angle(a,b)
    if (y<0):
        angle = angle * -1
        
    return [{'distance': round(dist, 2)}, {'angle' : angle}]

#print(process_coords(85,-32))

def ang_row(row):
    return process_coords(row['coords_x'],row['coords_y'])[1]['angle']
    
def dist_row(row):
    return process_coords(row['coords_x'],row['coords_y'])[0]['distance']

def create_angdist(df):
    df['shot_angle'] = df.apply(ang_row, axis=1)
    df['shot_distance'] = df.apply(dist_row, axis=1)
    #cols = ['coords_x','coords_y','shot_angle','shot_distance','event_type']
    return df#[cols]

#ang, dist
def create_shot_columns():
    query = '''select * from "data" 
where "event_type" in ('SHOT','GOAL','MISS')
and "event_zone" = 'Off';'''
    
    df = pd.read_sql_query(query,engine)
    
    df = create_angdist(df)

    df.to_sql('shots2',engine)
    print('shots2 sql success')

#@create_shot_columns()

#create_angdist((shots_team('COL'))).to_csv('asdfds.csv')
               
#shot_angle(89,0,left=False)

'''
def game_group(df):
        

    else:
        df.loc[ind]['prior_event'] = np.nan
        df.loc[ind]['secondsSinceLastEvent'] = np.nan


def dist_points(x1,y1,x2,y2):
    arr1 = np.array(x1,y1)
    arr2 = np.array(x2,y2)
    return np.linalg.norm(arr1 - arr2)
'''    
def create_prior_event_cols(df):

    #query = '''select * from micro union all select * from shots'''
    
    #df = pd.read_sql_query(query,engine)
    game_group = df.groupby(df.game_id)
    #gamelis = df['game_id'].drop_duplicates().reset_index(drop=True).tolist()
    #for game in gamelis:
        #game_df = game_group.get_group(game)
    dflis = []
    for x, group in game_group:
        pergrp = group.groupby(['game_period'])
   
        for each in [1,2,3]:
            grpdf = pergrp.get_group(each).sort_values(by=['game_seconds'])

            
            create_angdist(grpdf)
            
            grpdf['prior_event'] = grpdf.event_type.shift(1,fill_value=np.nan)
            timey = grpdf.game_seconds - grpdf.game_seconds.shift(1)
            grpdf['time_since_prior_event'] = timey
            grpdf['prior_event_team'] = grpdf.event_team.shift(1,fill_value=np.nan)
            
            cols = ['prior_event','time_since_prior_event','prior_event_team']#,'distance_from_prior_event']
            #print(grpdf.coords_x.shift(1))
            #print(grpdf.coords_y.shift(1))

            #print(grpdf.coords_x)
            #print(grpdf.coords_y)
            grpdf['xdiff'] = grpdf.coords_x - grpdf.coords_x.shift(1,fill_value=np.nan)
            grpdf['ydiff'] = grpdf.coords_y - grpdf.coords_y.shift(1,fill_value=np.nan)
            
            #first event of period excluded
            grpdf[cols].iloc[0] = np.nan

            
            dflis.append(grpdf)
            break
    #print(df['distance_from_prior_event'])
    fdf = pd.concat(dflis)
    print('succ')
    return fdf


#df = pd.read_csv('f1.csv')              
#create_prior_event_cols(df).to_csv('timeyty2.csv')

    
def group_by_strength(df):
    return df.groupby(df.game_strength_state)

def PK_of(grouped):
    return grouped.get_group('4v5')

def PP_of(grouped):
    return grouped.get_group('5v4')

def EV_of(grouped):
    return grouped.get_group('5v5')




#df.where(df['event_team'] == team)
#print(xG('BUF'))
#rev_plot_rink(EV_of(group_by_strength(xG('BUF'))),'BUF')#.to_csv('buf_xgf.csv',encoding='cp1252')

def flatten(xss):
    return [x for xs in xss for x in xs]

def set_ls(x):
    return list(dict.fromkeys(x))

def set_ls2(x):
    return list(dict.fromkeys(flatten(x)))

def SQL_in(items):
    return f"""('{"', '".join(items)}')"""

home_cols = ["home_on_1","home_on_2","home_on_3","home_on_4","home_on_5","home_on_6"]
away_cols = ["away_on_1","away_on_2","away_on_3","away_on_4","away_on_5","away_on_6"]
    
cols = home_cols + away_cols
    
def create_player_df(player):   
    query = '''select * from shots where ''' + "'" + player + "'" + ''' in 
    ("home_on_1","home_on_2","home_on_3","home_on_4","home_on_5","home_on_6",
     "away_on_1","away_on_2","away_on_3","away_on_4","away_on_5","away_on_6")
     and "event_zone" = 'Off'
     UNION ALL
     select * from micro where ''' + "'" + player + "'" + ''' in 
    ("home_on_1","home_on_2","home_on_3","home_on_4","home_on_5","home_on_6",
     "away_on_1","away_on_2","away_on_3","away_on_4","away_on_5","away_on_6")
     and "event_zone" = 'Off';'''

    return pd.read_sql_query(query, engine).replace(u'\xa0', ' ')

def tmms_df(playerlis,home):
    if home ==True:
        cols = '''("home_on_1","home_on_2","home_on_3","home_on_4","home_on_5","home_on_6")'''
    if home == False:
        cols = '''("away_on_1","away_on_2","away_on_3","away_on_4","away_on_5","away_on_6")'''
    query = ''
    for table in ["shots","micro"]:
        query += '''select * from ''' + table + " where "
        for each in playerlis:
            query += "'" + each + "'" + " in " + cols + ''' and "event_zone" = 'Off' '''
            query+= ''' or '''
        query = query[:-3]
        query += 'UNION ALL '
    query = query[:-11]
    query+= ';'
    return pd.read_sql_query(query, engine).replace(u'\xa0', ' ')

#df=tmms_df(['JONATHAN DROUIN'],home=True)
#df.to_csv('jdhome.csv')

#team1 home/away is home/away bool
#team2 is opposite
def opps_df(team1,team2,home):
    if home ==True:
        cols = '''("home_on_1","home_on_2","home_on_3","home_on_4","home_on_5","home_on_6")'''
        oppcols = '''("away_on_1","away_on_2","away_on_3","away_on_4","away_on_5","away_on_6")'''
        
    if home == False:
        cols = '''("away_on_1","away_on_2","away_on_3","away_on_4","away_on_5","away_on_6")'''
        oppcols = '''("home_on_1","home_on_2","home_on_3","home_on_4","home_on_5","home_on_6")'''
    query = ''
    for table in ["shots","micro"]:
        query += '''select * from ''' + table + " where "
        for each in team1:
            query += "'" + each + "'" + " in " + cols + ''' and "event_zone" = 'Off' '''
            query+= ''' or '''
        query = query[:-3]
        query += 'UNION ALL '
        query += '''select * from ''' + table + " where "
        for each in team2:
            query += "'" + each + "'" + " in " + oppcols + ''' and "event_zone" = 'Off' '''
            query+= ''' or '''
        query = query[:-3]
        query += 'UNION ALL '            
    query = query[:-11]
    query+= ';'
    
    df = pd.read_sql_query(query, engine).replace(u'\xa0', ' ')
    
    dfls = []
    
    for player1,player2 in zip(team1,team2):
        if home == True:
            p1_mask = ((df['home_on_1'] == player1) | (df['home_on_2'] == player1) | (df['home_on_3'] == player1) | (df['home_on_4'] == player1) | (df['home_on_5'] == player1) | (df['home_on_6'] == player1))
            p2_mask = ((df['away_on_1'] == player2) | (df['away_on_2'] == player2) | (df['away_on_3'] == player2) | (df['away_on_4'] == player2) | (df['away_on_5'] == player2) | (df['away_on_6'] == player2))
        if home == False:
            p1_mask = ((df['away_on_1'] == player1) | (df['away_on_2'] == player1) | (df['away_on_3'] == player1) | (df['away_on_4'] == player1) | (df['away_on_5'] == player1) | (df['away_on_6'] == player1))
            p2_mask = ((df['home_on_1'] == player2) | (df['home_on_2'] == player2) | (df['home_on_3'] == player2) | (df['home_on_4'] == player2) | (df['home_on_5'] == player2) | (df['home_on_6'] == player2))
    
        df[~p2_mask] #without each player in team2
        
'''        
    if home == True:
        p1_mask = ((df['home_on_1'] == player1) | (df['home_on_2'] == player1) | (df['home_on_3'] == player1) | (df['home_on_4'] == player1) | (df['home_on_5'] == player1) | (df['home_on_6'] == player1))
        p2_mask = ((df['away_on_1'] == player2) | (df['away_on_2'] == player2) | (df['away_on_3'] == player2) | (df['away_on_4'] == player2) | (df['away_on_5'] == player2) | (df['away_on_6'] == player2))
    if home == False:
        p1_mask = ((df['away_on_1'] == player1) | (df['away_on_2'] == player1) | (df['away_on_3'] == player1) | (df['away_on_4'] == player1) | (df['away_on_5'] == player1) | (df['away_on_6'] == player1))
        p2_mask = ((df['home_on_1'] == player2) | (df['home_on_2'] == player2) | (df['home_on_3'] == player2) | (df['home_on_4'] == player2) | (df['home_on_5'] == player2) | (df['home_on_6'] == player2))
'''

opps_df(['JONATHAN DROUIN','NATHAN MACKINNON'],['MIRO HEISKENAN','JASON ROBERTSON'],home=True)#.to_csv('jsnmmhjrhome.csv')

#opps_df('JONATHAN DROUIN')      
#playerlis = ['JONATHAN DROUIN','NATHAN MACKINNON']
#multi_player_df(playerlis).to_csv('p101.csv')
        
#create_player_df('JONATHAN DROUIN').to_csv('333.csv')

#df = create_player_df('JONATHAN DROUIN')

#df = pd.read_csv('333.csv')
#create_angdist(df).to_csv('433.csv')

'''    
def expected_df(df,team,player):
    shot = df["event_type"].isin(['SHOT','MISS','GOAL'])
    ls = []         
    shotdf = create_angdist(df[shot])
    for game in df['game_id'].unique():
        ls.append(game_rebounds(df,game))
    
    FF = df.loc[np.where(df['event_team'] == team)]
    FA = df.loc[np.where(df['event_team'] != team)]

    #xGF = FF['shot_distance'] * 
    
    return pd.concat(ls)

'''

#expected_df(df, 'COL', 'JONATHAN DROUIN').to_csv('043.csv')
    
#df[0] = with player, df[1] = without player
def df_with_player(df,player,home):
        
    home_mask = ((df['home_on_1'] == player) | (df['home_on_2'] == player) | (df['home_on_3'] == player) | (df['home_on_4'] == player) | (df['home_on_5'] == player) | (df['home_on_6'] == player))
    away_mask = ((df['away_on_1'] == player) | (df['away_on_2'] == player) | (df['away_on_3'] == player) | (df['away_on_4'] == player) | (df['away_on_5'] == player) | (df['away_on_6'] == player))
        
    if home == 0:
        return [df[home_mask], df[~home_mask]]
    if home == 1:
        return [df[away_mask], df[~away_mask]]
    if home == 2:
        return [df[(home_mask | away_mask)], df[~(home_mask | away_mask)]]

def player_comp(df,player,home):
    
    #df = expected_df(df,'COL',player)
    
    without = df_with_player(df,player,home)[1]

    
    withy = df_with_player(df,player,home)[0]
    
#df = pd.read_csv('333.csv')
#player_comp(df,'NATHAN MACKINNON',home=2)

#def isolated_impact(player1,teammates):
#df_with_player(create_player_df('JONATHAN DROUIN'),'NATHAN MACKINNON',home=2)[1].to_csv('2asdfds.csv')


def get_teammates(df, player):
    
    #df = create_player_df(player)
    
    home_mask = ((df['home_on_1'] == player) | (df['home_on_2'] == player) | (df['home_on_3'] == player) | (df['home_on_4'] == player) | (df['home_on_5'] == player) | (df['home_on_6'] == player))
        
    home_tms = []
    home_opps = []
    away_tms = []
    away_opps = []

    for col in home_cols:
        home_opps.append(df[~home_mask][col].unique().tolist())
        home_tms.append(df[home_mask][col].unique().tolist())

    for col in away_cols:
        away_tms.append(df[~home_mask][col].unique().tolist())
        away_opps.append(df[home_mask][col].unique().tolist())
        
    return [set_ls2(home_tms),set_ls2(home_opps),set_ls2(away_tms),set_ls2(away_opps)]

def with_or_without(player, player2):
    df = create_player_df(player)

'''
def create_mask(player,home):
    if home == True:
        
    if home == False:
        
'''        

#home0 = p1 home
#home1 = p1 away
def against_player(df,player1, player2, home):

    p1_home_mask = ((df['home_on_1'] == player1) | (df['home_on_2'] == player1) | (df['home_on_3'] == player1) | (df['home_on_4'] == player1) | (df['home_on_5'] == player1) | (df['home_on_6'] == player1))
    p1_away_mask = ((df['away_on_1'] == player1) | (df['away_on_2'] == player1) | (df['away_on_3'] == player1) | (df['away_on_4'] == player1) | (df['away_on_5'] == player1) | (df['away_on_6'] == player1))    

    p2_home_mask = ((df['home_on_1'] == player2) | (df['home_on_2'] == player2) | (df['home_on_3'] == player2) | (df['home_on_4'] == player2) | (df['home_on_5'] == player2) | (df['home_on_6'] == player2))
    p2_away_mask = ((df['away_on_1'] == player2) | (df['away_on_2'] == player2) | (df['away_on_3'] == player2) | (df['away_on_4'] == player2) | (df['away_on_5'] == player2) | (df['away_on_6'] == player2))    


    if home==True:
        p1_mask = ((df['home_on_1'] == player1) | (df['home_on_2'] == player1) | (df['home_on_3'] == player1) | (df['home_on_4'] == player1) | (df['home_on_5'] == player1) | (df['home_on_6'] == player1))
        p2_mask = ((df['away_on_1'] == player2) | (df['away_on_2'] == player2) | (df['away_on_3'] == player2) | (df['away_on_4'] == player2) | (df['away_on_5'] == player2) | (df['away_on_6'] == player2))

    if home==False:
        p1_mask = ((df['away_on_1'] == player1) | (df['away_on_2'] == player1) | (df['away_on_3'] == player1) | (df['away_on_4'] == player1) | (df['away_on_5'] == player1) | (df['away_on_6'] == player1))
        p2_mask = ((df['home_on_1'] == player2) | (df['home_on_2'] == player2) | (df['home_on_3'] == player2) | (df['home_on_4'] == player2) | (df['home_on_5'] == player2) | (df['home_on_6'] == player2))

    
    return df[~p2_mask]#pd.concat([df[~p2_mask], df[p2_mask]], axis=1)



    #df_away_with_both = df[(p1_away_mask & p2_home_mask)]
    #df_away_without_p2 = df[~p2_home_mask]


    #df_dict = {'df_home_with_both' : df_home_with_both, 'df_home_without_p2' : df_home_without_p2}
    #return pd.concat([df_away_with_both, df_away_without_p2]).to_csv('jdhmh.csv')


    
    #df where "home_on1" etc.. = player & away_on_1 = player2
    #compare to
    #df where "home_on1" etc.. ~= player & away_on_1 = player2

    #df where "away_on1" etc.. = ```
    #compare to
    #``` "away on1" = player

#against_player('JONATHAN DROUIN', 'MIRO HEISKANEN',home=True).to_csv('2combined.csv')
    
    #mates = get_teammates(df,player)[0]
    #opps = get_teammates(df,player)[3]
    
#df = create_player_df('JONATHAN DROUIN')
#print(get_teammates(df,'JONATHAN DROUIN'))

def create_player_chart(df,player):
    ls = get_teammates(df,'JONATHAN DROUIN')
    for i in range(3):
        while ' ' in ls[i]:
            ls[i].remove(' ')
            
    #for each in ls[0] 
  

#df = pd.read_csv('333.csv')
#create_player_chart(df,'JONATHAN DROUIN')


def replace_name(df):
    x = []
    for each in df['name'].tolist():
        x.append(each['default'].split(' ', 1)[1])
                   
    df.drop('name',axis = 1, inplace = True)
    df['name'] = x
                   
    return df

def boxscore(game):
    url = 'https://api-web.nhle.com/v1/gamecenter/' + str(game) + '/boxscore'
    jxn = use_api(url)    
    ls = []
    for each in ['awayTeam','homeTeam']:
        currentTeam = jxn[each]['abbrev']
        ros = jxn["playerByGameStats"][each]
        df = replace_name(pd.DataFrame.from_dict(ros['forwards'] + ros['defense']))     
        ls.append(df)
        ls[0]['awayTeam'] = jxn['awayTeam']['abbrev']
        ls[0]['homeTeam'] = jxn['homeTeam']['abbrev']
        ls[0]['game_id'] = game
        
    return pd.concat(ls,axis=1)

#gen list of gameids
def team_games(team):   
    query = '''select game_id from shots where  ''' + "'" + team + "'" + ''' in ("home_team","away_team");'''
    games = pd.read_sql_query(query,engine)['game_id'].tolist()
    new_games = list(dict.fromkeys(games).keys())
    return new_games

#return list of df
def roster_game(team, game):
    url = 'https://api-web.nhle.com/v1/gamecenter/' + str(game) + '/boxscore'
    jxn = use_api(url)
    ls = []
    for each in ['awayTeam','homeTeam']:
        currentTeam = jxn[each]['abbrev']
        ros = jxn["playerByGameStats"][each]
        df = replace_name(pd.DataFrame.from_dict(ros['forwards'] + ros['defense']))     
        df['team'] = currentTeam
        if currentTeam != team:
            df = df.add_prefix('opp_')
        ls.append(df)
    ls[0]['awayTeam'] = jxn['awayTeam']['abbrev']
    ls[0]['homeTeam'] = jxn['homeTeam']['abbrev']
    ls[0]['game_id'] = game
    #print(df)
    #df['url'] = 'https://www.nhl.com/player/' + df['playerId']
    return pd.concat(ls,axis=1)


#possible cols:
    #{'id': 8481020, 'headshot': 'https://assets.nhle.com/mugs/nhl/20232024/COL/8481020.png', 'firstName': {'default': 'Justus'}, 'lastName': {'default': 'Annunen'},
    #'sweaterNumber': 60, 'positionCode': 'G', 'shootsCatches': 'L', 'heightInInches': 76, 'weightInPounds': 210, 'heightInCentimeters': 193, 'weightInKilograms': 95,
    #'birthDate': '2000-03-11', 'birthCity': {'default': 'Kempele'}, 'birthCountry': 'FIN'}

#cols = ['id','firstName','lastName','sweaterNumber','shootsCatches']

def team_goalies(team):
    return pd.DataFrame.from_dict(use_api('https://api-web.nhle.com/v1/roster/' + team + '/20232024')['goalies'])

def goalies():
    return pd.DataFrame.from_dict(use_api('https://api.nhle.com/stats/rest/en/goalie

def nhl_puck(season):
    return pd.DataFrame.from_dict(use_api('https://api.nhle.com/stats/rest/en/skater/puckPossessions?cayenneExp=seasonId=' + season + '&limit=-1&start=0')['data'])
                   
#goalies('COL').to_csv('psdd.csv')
#nhl_puck('20232024').to_csv('puck.csv')
                   
def complete_df(team):
    gamelis = team_games(team)
    dfls = []
    for game in gamelis:
        dfls.append(roster_game(team, game))
    return pd.concat(dfls,axis=0)


nhl_dict = {24: 'ANA', 53: 'ARI', 6: 'BOS', 7: 'BUF', 20: 'CGY', 12: 'CAR', 16: 'CHI', 21: 'COL', 29: 'CBJ', 25: 'DAL', 17: 'DET', 22: 'EDM', 13: 'FLA', 26: 'LAK', 30: 'MIN',
8: 'MTL', 18: 'NSH', 1: 'NJD', 2: 'NYI', 3: 'NYR', 9: 'OTT', 4: 'PHI', 5: 'PIT', 28: 'SJS', 55: 'SEA', 19: 'STL', 14: 'TBL', 10: 'TOR', 23: 'VAN', 54: 'VGK', 52: 'WPG', 15: 'WSH'}
    
def fix_period_seconds(df):
    cols = ['startSec','endSec']
    grouped = df.groupby(['period'])
    i=0
    ls = []
    for each in list(grouped.groups.keys()):
        df2 = grouped.get_group(each)
        df2[cols] = df2[cols].apply(lambda x: x + i)
        i += 1200
        ls.append(df2)

    return pd.concat(ls)
    
    
    
def shift_chart(game):
    url = 'https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId=' + game + '%20and%20((duration%20!=%20%2700:00%27%20and%20typeCode%20=%20517)%20or%20typeCode%20!=%20517%20)&exclude=detailCode&exclude=duration&exclude=eventDetails&exclude=teamAbbrev&exclude=teamName'
    jxn = use_api(url)
    df = pd.DataFrame.from_dict(jxn['data'])
    cols = ['teamId','firstName','lastName','startTime','endTime','playerId','period','typeCode','gameId','startSec','endSec','team']
    df['startSec'] = df['startTime'].apply(lambda x: float(x.split(':')[0])*60+float(x.split(':')[1]))
    df['endSec'] = df['endTime'].apply(lambda x: float(x.split(':')[0])*60+float(x.split(':')[1]))
    df['endSec'] = df['endSec'].astype(int)
    df['startSec'] = df['startSec'].astype(int)    
    df['team'] = df['teamId'].map(nhl_dict)
    return fix_period_seconds(df[cols]).sort_values(by=['playerId','period'])

#shift_chart('2023020101').to_csv('ppppp.csv')

def sort_players_teams(game):
    df=pd.read_csv('2test222.csv')
    dfsec=pd.read_csv('fads.csv')
    
    teamgroup = df.groupby('team')
    teamlis = list(teamgroup.groups.keys())

    playergroup1 = list(teamgroup.get_group(teamlis[0]).groupby('playerId').groups.keys())
    team1dic = {teamlis[0]:playergroup1}
    #print(team1dic)
    
    playergroup2 = list(teamgroup.get_group(teamlis[1]).groupby('playerId').groups.keys()) 
    team2dic = {teamlis[1]:playergroup2}
    #print(team2dic)


#inputs: dic = {0:1,1:2,2:3}, key=1
#output: 2
def get_next_key(dic,key):
    # prepare additional dictionaries
    ki = dict()
    ik = dict()
    offset = 1
    for i, k in enumerate(dic):
        ki[k] = i   # dictionary index_of_key
        ik[i] = k     # dictionary key_of_index

    index_of_key = ki[key]
    index_of_next_key = index_of_key + offset
    res = ik[index_of_next_key] if index_of_next_key in ik else None
    return res

#dic = {0:1,1:2,2:3}

#print('next key is: ' + str(get_next_key(dic,1)))
        
    
def find_shift_overlaps(game):
    
    df = shift_chart(game)      
    teamgroup = df.groupby('team')
    teamlis = list(teamgroup.groups.keys())

    end = df['endSec'].max()
    
    gameseconds = list(range(1,end+1))
    

    glis = {}
    for team in teamlis:
        g = goalies(team)['id'].tolist()
        glis.update({team: g})
        playershifts = {}
        goalieshifts = {}
        for player in list(teamgroup.get_group(team).groupby('playerId').groups.keys()):
            values = df.loc[np.where(df['playerId'] == player)]['endSec'].tolist()
            keys = df.loc[np.where(df['playerId'] == player)]['startSec'].tolist()
            res = dict(map(lambda i,j : (i,j) , keys,values))
            
            if player not in glis[team]:
                playershifts.update({player:res})
                
            if player in glis[team]:
                goalieshifts.update({player:res})
        
        for g in goalieshifts.values():
            gpull = {}
            for key,val in g.items():
                
                #if current value != next key then goalie was pulled, it was not period change
                
                if get_next_key(g,key) == None:
                    next_key=end
                    next_val=end
                else:
                    next_key = get_next_key(g,key)
                    next_val = g[next_key]
                    
                if val != next_key:

                    gpull.update({val:next_key})


                if (val not in [1200,2400,3600,3900]) and (next_key != end):
                    gpull.update({val: next_key})
                    

            #print(team)
            #print(gpull)                
        d = {}
        
        for each in gameseconds:
            templs = []
            for player,shifts in playershifts.items():
                for ke,val in shifts.items():
                    if ke <= each <= val:
                        templs.append(player)
                    d[each] = templs
         
        pd.DataFrame.from_dict(d).to_csv('p12.csv')
#shift_chart('2023020102').to_csv('ppppp.csv')
find_shift_overlaps('2023020102')#.to_csv('ppp.csv')

    
def plot_chart(game,team):
    df = shift_chart(game)
    fig,ax = plt.subplots()
    
    grouped = df.groupby(['playerId'])
    m=-1
    
    ls = []
    ms = []
    
    for pid,group in grouped:
        name = str(group.iat[0, 1]) + ' ' + str(group.iat[0, 2])
        x = group['startSec']
        y = group['endSec'] - x
        m += 1.5
        coord = []
        for a,b in zip(x,y):
            coord.append([a,b])
        
        ls.append(name)
        ms.append(m + 0.5)
        
        ax.broken_barh(coord, (m, 1))

    ax.set_xlabel('period')
    ax.set_xticks([0,1200,2400,3600],['1st','2nd','3rd','end'])
    ax.set_yticks(ms,labels=ls)
    plt.xlim(0,3600)
    plt.ylim(0,m)
    #plt.
    plt.show()

#plot_chart('2022030415','FLA')
#shift_chart('2023020928').to_csv('50101.csv')
#.to_csv('50102.csv')
    

    #print(type(each))
    
#roster_df('VGK',home=True).to_csv('VGK2home.csv')

#create_player_charts('COL')

def apply_score_effects(df):
    #https://evolving-hockey.com/glossary/score-adjustments/
    #home team on left ex. '0v3' home =0,away=3
    
    fenwick_sva = {'5v5' :
                {'home' : {'0v3' : .859, '0v2': .881, '0v1' : .909, '0v0': .968, '1v0' : 1.037, '2v0' : 1.078, '3v0': 1.109},
                'away' : {'0v3' : 1.197, '0v2': 1.155, '0v1' : 1.111, '0v0': 1.034, '1v0' : 0.966, '2v0' : 0.933, '3v0': .911}
                }
        ,'4v4' :
                {'home' : {'0v3' : .933, '0v2': .931, '0v1' : .938, '0v0': .973, '1v0' : 1.027, '2v0' : 1.040, '3v0': 1.060},
                'away' : {'0v3' : 1.077, '0v2': 1.079, '0v1' : 1.071, '0v0': 1.029, '1v0' : 0.975, '2v0' : 0.963, '3v0': .947}
                }
        ,'3v3' :
                {'home' : {'0v3' : .991, '0v2': .991, '0v1' : .991, '0v0': .991, '1v0' : .991, '2v0' : .991, '3v0': .991},
                'away' : {'0v3' : 1.009, '0v2': 1.009, '0v1' : 1.009, '0v0': 1.009, '1v0' : 1.009, '2v0' : 1.009, '3v0': 1.009}
                }
        ,'5v4' :
                {'home' : {'0v3' : 0.843, '0v2': 0.843, '0v1' : 0.843, '0v0': 0.926, '1v0' : 1.039, '2v0' : 1.039, '3v0': 1.039},
                'away' : {'0v3' : 1.229, '0v2': 1.229, '0v1' : 1.229, '0v0': 1.087, '1v0' : 0.964, '2v0' : 0.964, '3v0': 0.964}
                }
        ,'5v3' :
                {'home' : {'0v3' : 0.798, '0v2': 0.798, '0v1' : 0.798, '0v0': 0.906, '1v0' : 0.932, '2v0' : 0.932, '3v0': 0.932},
                'away' : {'0v3' : 1.340, '0v2': 1.340, '0v1' : 1.340, '0v0': 1.115, '1v0' : 1.078, '2v0' : 1.078, '3v0': 1.078}
                }
        ,'4v3' :
                {'home' : {'0v3' : 0.814, '0v2': 0.814, '0v1' :0.814, '0v0': 0.921, '1v0' : 0.941, '2v0' : 0.941, '3v0': 0.941},
                'away' : {'0v3' : 1.297, '0v2': 1.297, '0v1' : 1.297, '0v0': 1.093, '1v0' : 1.066, '2v0' : 1.066, '3v0': 1.066}
                }
        }
    

    print(fenwick_sva['5v5']['home']['0v0'])
    
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


'''
def create_team_rosters():
    df = pd.read_sql_table('shots',engine).replace(u'\xa0', ' ')
    players = []
    for col in cols:
        players.append(df[col].unique().tolist())
    return players

#print(create_team_rosters())

def team_roster():
    homecols = SQL_in(home_cols)
    awaycols = SQL_in(away_cols)
    SQLcols = SQL_in(cols)
'''    
   # for team in nhl_teams:
       #query = '''select ''' + SQLcols + ''' from "shots" where "home_team" == ''' + "'" + team + "'"
'''
nhl_team_dictionary = {
    "ANA": "24",
    "ARI": "53", 
    "BOS": "6",
    "BUF": "7",
    "CGY": "20",
    "CAR": "12",
    "CHI": "16",
    "COL": "21",
    "CBJ": "29", 
    "DAL": "25",
    "DET": "17",
    "EDM": "22",
    "FLA": "13",
    "LAK": "26",
    "MIN": "30",
    "MTL": "8",
    "NSH": "18",
    "NJD": "1",
    "NYI": "2",
    "NYR": "3",
    "OTT": "9",
    "PHI": "4",
    "PIT": "5",
    "SJS": "28",
    "SEA": "55",
    "STL": "19",
    "TBL": "14",
    "TOR": "10",
    "VAN": "23",
    "VGK": "54",
    "WPG": "52",
    "WSH": "15"
    }

inv_dict = {v: k for k,v in nhl_team_dictionary.items()}
   
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
    
def multi_players(team, playerlis):
#    query = select * from "_shots_home" where "event_zone" = 'Off';
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


from dataclasses import dataclass
@dataclass
class score_effects:
    homevaway: double,
    strength: string,
    event_team_home:

    
def LabelShotRebound(team):
    #query = select * from " + team + '_shots_home"' + where "event_zone" = 'Off';
    df.loc[np.where(df['event_team'] == team)]


#find if shot was a followup to a hit,takeaway,block,miss,shot
def game_rebounds(df):
    df['prior_event'] = np.nan
    grouped = df.groupby(['game_id'])
    gamelis = {}
    indices = []
    for x, group in grouped:
        pergrp = group.groupby(['game_period'])
        
        d = {}
         
        for each in [1,2,3]:
            grpdf = pergrp.get_group(each)             
            ls = sorted(grpdf['game_seconds'].tolist())
            pair = {}
                
            for previous, current in zip(ls, ls[1:]):
                
                if (previous + 3.1 > current):
                    pair.update({grpdf.loc[(grpdf['game_seconds'] == previous)].index[0]:grpdf.loc[(grpdf['game_seconds'] == current)].index[0]})
                    
                    prev_ind = grpdf.loc[(grpdf['game_seconds'] == previous)].index[0]
                    curr_ind = grpdf.loc[(grpdf['game_seconds'] == current)].index[0]
                    
                    prior = df.loc[prev_ind]['event_type']
                    curr = df.loc[curr_ind]['event_type']

                    priorTeam = df.loc[prev_ind]['event_team']
                    currTeam = df.loc[curr_ind]['event_team']
                    

                    if (curr=='BLOCK') or (curr=='SHOT') or (curr=='MISS') or (curr=='GOAL'):

                        if (prior=='GIVE') or (prior=='HIT'):
                            if currTeam != priorTeam:
                                df.at[curr_ind,'prior_event'] = prior 

                        if (prior=='TAKE') or (prior=='HIT'):
                            if currTeam == priorTeam:
                                df.at[curr_ind,'prior_event'] = prior
            
                                
                        df.at[curr_ind,'prior_event'] = prior
                        print(df.loc[curr_ind]['prior_event'])

                
                    #d.update({each:pair})
                    #indices.append(pair)
        
        
    
            
        gamelis.update({x:d})
    df.to_csv('303.csv')      
                 
    #print(gamelis)
    #print(indices)
    #return df.where(df['game_seconds'].isin(newlis.items())).dropna(how='all').sort_values(by=['game_id','game_seconds'])

#df = pd.read_csv('333.csv')

#game_rebounds(df).to_csv('054.csv')

'''       
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
           #playergroup1 = list(teamgroup.get_group(teamlis[0]).groupby('playerId').groups.keys())
    #playergroup2 = list(teamgroup.get_group(teamlis[1]).groupby('playerId').groups.keys())
    #teamgroups = []
    #teamgroups.append(playergroup1)
    #teamgroups.append(playergroup2)

'''
for i in range(len(teamlis)):
    playershifts = {}        
    for player in teamgroups[i]:
        values = df.loc[np.where(df['playerId'] == player)]['endSec'].tolist()
        keys = df.loc[np.where(df['playerId'] == player)]['startSec'].tolist()
        res = dict(map(lambda i,j : (i,j) , keys,values))
        playershifts.update({player:res})

    d = {}
    
    
    for each in gameseconds:
        templs = []
        for player,shifts in playershifts.items():
            for ke,val in shifts.items():
                if ke <= each <= val:
                    templs.append(player)
                d[each] = templs

    for x in d.values():
        if len(x) >= 6:                    

'''
                    
'''
if val not in [1200,2400,3600]:#,3900]:
    print(team + ' goalie pulled at: ' + str(val))
    
    next_key = get_next_key(g,key)
    
    if get_next_key(g,key) == None:
        next_key = 3600
        next_val = 3600
    else:
        next_key = get_next_key(g,key)
        next_val = g[next_key]
    
    #print(next_key)
    #print(next_val)

        
    print(team + ' goalie pulled at: ' + str(next_key))
    print(team + ' goalie returned at: ' + str(next_val))
    #else:
        #print(team + ' goalie returned at: ' + str(g[next_key]))
if key not in [0,1200,2400,3600]:
    print(team + ' goalie returned at: ' + str(key))

'''
