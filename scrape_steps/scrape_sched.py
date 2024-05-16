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
import ast

nhl_dict = {24: 'ANA', 53: 'ARI', 6: 'BOS', 7: 'BUF', 20: 'CGY', 12: 'CAR', 16: 'CHI', 21: 'COL', 29: 'CBJ', 25: 'DAL', 17: 'DET', 22: 'EDM', 13: 'FLA', 26: 'LAK', 30: 'MIN',
8: 'MTL', 18: 'NSH', 1: 'NJD', 2: 'NYI', 3: 'NYR', 9: 'OTT', 4: 'PHI', 5: 'PIT', 28: 'SJS', 55: 'SEA', 19: 'STL', 14: 'TBL', 10: 'TOR', 23: 'VAN', 54: 'VGK', 52: 'WPG', 15: 'WSH'}

def use_api(url):
    return requests.get(url).json()

def datelis_to_str(datelis):
    
    return [str(date) for date in datelis]

def lis_to_date(lis):
    return [x.strftime("%Y-%m-%d") for x in lis]
    
def str_to_date(date):
    return datetime.strptime(date, "%Y-%m-%d").date()

def sched(first, last):
    return lis_to_date(pd.date_range(first,last,freq='W-SUN'))#.str[0]

#print(sched('2020-10-12','2021-04-29'))


#return df
def scrape_date(sunday):
    
    if type(sunday) != str:
        sunday = str(sunday)
        
    cols = ['id','season','gameType','awayTeam','homeTeam', 'gameOutcome']
     
    jxn = use_api('https://api-web.nhle.com/v1/schedule/' + sunday)    
    info = pd.json_normalize(json.loads(json.dumps(jxn))['gameWeek'])['games']

    games = []      
 
    for day in info.tolist():
        if len(day) == 0:
            return pd.DataFrame()
        for game in day:
            new_d = {key: game[key] for key in cols}
            
            for each in ['awayTeam','homeTeam']:
                
                new_d[each + '_abbrev'] = new_d[each]['abbrev']
                new_d[each + '_score'] = new_d[each]['score']
                del new_d[each]
                new_d[each] = new_d[each + '_abbrev']
                new_d[each[0:4] + 'Score'] = new_d[each + '_score']
                del new_d[each + '_abbrev']
                del new_d[each + '_score']

            new_d['lastPeriod'] = new_d['gameOutcome']['lastPeriodType']
            del new_d['gameOutcome']
            
            df = pd.DataFrame.from_dict(new_d,orient='index').T
            df['date'] = day[0]['startTimeUTC'][:10]
            games.append(df)

    return pd.concat(games,ignore_index=True)
            

#scrape_date('2020-03-15')#.to_csv('19091.csv')


        
    
def scrape_sch_csv(start_yr):

    if start_yr == '2023':
        sch = ['2023-10-11','2024-04-18']
        
    if start_yr == '2022':
        sch = ['2022-10-11', '2023-04-13']
        
    if start_yr == '2021':
        sch = ['2021-10-12', '2022-04-29']
        
    if start_yr == '2020':
        sch = ['2021-01-13', '2021-05-08']

    if start_yr == '2019':
        sch = ['2019-10-02','2020-03-12']
        
    if start_yr == '2018':
        sch = ['2018-10-03','2019-04-06']

    if start_yr == '2017':
        sch = ['2017-10-04','2018-04-07']

    if start_yr == '2016':
        sch = ['2016-10-12','2017-04-09']

    if start_yr == '2015':
        sch = ['2015-10-07','2016-04-09']
       
    if int(start_yr) < 2015:
        print('sorry i only do 2015 and above!')
        
    if int(start_yr) > 2023:
        print('this is a date in the future')

    sundays = sched(sch[0],sch[1])
    ls = []
    for day in sundays:
        ls.append(scrape_date(day))
        #scrape_date(day).to_csv('2019_' + day + '.csv')

    pd.concat(ls,ignore_index=True).to_csv(start_yr + '_scrape.csv')
    print(start_yr + ' successfully scraped!')

from ast import literal_eval

def team_goalies(team):
    return pd.DataFrame.from_dict(use_api('https://api-web.nhle.com/v1/roster/' + team + '/20232024')['goalies'])

def goalies():
    return pd.DataFrame.from_dict(use_api('https://api.nhle.com/stats/rest/en/goalie/savesByStrength?isAggregate=false&isGame=false&sort=%5B%7B%22property%22:%22evSavePct%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=-1&factCayenneExp=gamesPlayed%3E=1&cayenneExp=gameTypeId=2%20and%20seasonId%3C=20232024%20and%20seasonId%3E=20232024')['data'])

def league_usat():
    return pd.DataFrame.from_dict(use_api('https://api.nhle.com/stats/rest/en/team/percentages')['data'])

#league_usat().to_csv('l2l2.csv')
#goalies().to_csv('goalies.csv')

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

def scrape_shifts(game):
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

#scrape_shifts('2023020466').to_csv('torpit.csv')

#{8477465: {0: 754, 786: 1200, 1200: 1348}, 8477968: {1348: 1627, 1633: 2400, 2400: 2503, 2528: 3600}}
def find_goalie_pulled(shift_dic,end):

    
    gpull = {}
    for k,g in shift_dic.items():
        gpulltemp = {}

        for key,val in g.items():

            #if current value != next key then goalie was pulled, it was not period change
            
            next_key = get_next_key(g,key)
            if (val != next_key) and (next_key):
                gpulltemp.update({val:next_key})

        gpull.update({k:gpulltemp})
    return gpull


def find_shift_overlaps(game):

    
    df = scrape_shifts(game)
    #df= pd.read_csv('torpit.csv')
    teamgroup = df.groupby('team')
    
    teamlis = list(teamgroup.groups.keys())

    end = df['endSec'].max()
    
    gameseconds = list(range(1,end+1))

    glis = {}
    gdf = goalies()[['playerId','teamAbbrevs']]
    
    g_pId = set(gdf['playerId'].tolist())
    game_pId = set(df['playerId'].tolist())
    goalis = list(g_pId.intersection(game_pId))
    
    
    for goalie in goalis:
        x = list(set(df.loc[np.where(df['playerId'] == goalie)]['team']))[0]
        glis[goalie] = x
        

    rglis = {}
    for key, value in glis.items():
        rglis.setdefault(value, [])
        rglis[value].append(key)
    
    #print(reversed_dict)

    team_goalie_gaps = {}
    
    for team in teamlis:

        playershifts = {}
        goalieshifts = {}
        for player in list(teamgroup.get_group(team).groupby('playerId').groups.keys()):
            values = df.loc[np.where(df['playerId'] == player)]['endSec'].tolist()
            keys = df.loc[np.where(df['playerId'] == player)]['startSec'].tolist()
            res = dict(map(lambda i,j : (i,j) , keys,values))
            
            if player not in rglis[team]:
                playershifts.update({player:res})
                
            if player in rglis[team]:
                goalieshifts.update({player:res})
                
        team_goalie_gaps.update({team:find_goalie_pulled(goalieshifts,end)})

        d = {}
        for each in gameseconds:
            templs = []
            for player,shifts in playershifts.items():
                for ke,val in shifts.items():
                    if ke <= each <= val:
                        templs.append(player)
                    d[each] = templs
        print(d)

    print(team_goalie_gaps)
    
    #return #pd.DataFrame.from_dict(d)

#find_shift_overlaps('2023020466')#.to_csv('overlaps.csv')

#scrape_shifts('2023020466').to_csv('shifty2.csv')

def team_lookup(x):
    if pd.isna(x):
        return ''
    else:
        return nhl_dict[x]
    
    #return lambda x: nhl_dict[x]
cols_all = ['zoneCode','xCoord','yCoord','eventOwnerTeamId']
no_deets = ['eventId', 'timeInPeriod', 'timeRemaining', 'situationCode', 'homeTeamDefendingSide', 'typeCode', 'event_type', 'sortOrder', 'period', 'period_type']


def sep_deets(df,deet_map):
    cols = list(deet_map.keys()) + cols_all
    deet_df = pd.DataFrame(df['details'].tolist(),columns=cols,index=df.index)

    #deet_df.rename(columns={'eventOwnerTeamId':'event_team'},inplace=True)
    #deet_df['event_team'] = deet_df['event_team'].apply(lambda x: team_lookup(x))
    
    final_df = pd.concat([df[no_deets],deet_df],axis=1)
    final_df.rename(columns=deet_map,inplace=True)
    
    return final_df                

#sep_deets(pd.read_csv('2asddd.csv'), {'blockingPlayerId': 'event_player_1', 'shootingPlayerId': 'event_player_2'}).to_csv('WAHOO2.csv')
#bs_df(pd.read_csv('2asddd.csv')).to_csv('WAHOO.csv')
                           
def scrape_pbp(game):
    jxn = use_api('https://api-web.nhle.com/v1/gamecenter/' + game + '/play-by-play')
    
    away = jxn['awayTeam']['abbrev']
    home = jxn['homeTeam']['abbrev']

    pbp = jxn['plays']
    df = pd.DataFrame.from_records(pbp)#,index=['eventId'])
    #df.to_csv('pspsp.csv')

    #df = pd.read_csv('torpit_2.csv')
    mapping = {'typeDescKey':'event_type'}
    df.rename(columns=mapping, inplace = True)       

    df = df.dropna(axis=0, subset=['details'])
    #df.situationCode = df.situationCode.astype(str)
    #print(df.situationCode.unique())
    c=0
    while c < 2:
        for x in df['details']:
            if type(x)==str:
                df['details'] = df['details'].apply(literal_eval)
            c+= 1
        for x in df['periodDescriptor']:
            if type(x)==str:
                df['periodDescriptor'] = df['periodDescriptor'].apply(literal_eval)
            c+=1   
    
    
    per_df = pd.DataFrame(df['periodDescriptor'].tolist(),index=df.index)
    per_df.rename(columns={'number':'period','periodType':'period_type'},inplace=True)

    df = pd.concat([df[[i for i in list(df) if i != 'periodDescriptor']],per_df],axis=1)
    
    event_coords = ['blocked-shot','giveaway','goal','hit','missed-shot','shot-on-goal','takeaway','penalty','stoppage','delayed-penalty']
    
    
    grouped = df.groupby('event_type',as_index=False)

    all_events = ['blocked-shot', 'delayed-penalty', 'faceoff', 'game-end', 'giveaway', 'goal', 'hit', 'missed-shot', 'penalty', 'period-end', 'period-start', 'shot-on-goal', 'stoppage', 'takeaway']
    non_events = [x for x in all_events if x not in event_coords]
    #print(non_events)

    #event_detail = shotType(shots), reason(stoppage,missed shot), descKey(penalty)

    cols_all = ['zoneCode','xCoord','yCoord','eventOwnerTeamId']

    dp_map = {'event_team': 'eventOwnerTeamId'}
    
    master_map = {'blocked-shot': {'blockingPlayerId': 'event_player_1', 'shootingPlayerId': 'event_player_2'},
    'giveaway': {'playerId': 'event_player_1', '' : 'event_player_2'},
    'goal': {'shootingPlayerId': 'event_player_1', 'assist1PlayerId': 'event_player_2', 'assist2PlayerId': 'event_player_3', 'goalieInNetId' : 'event_goalie'},
    'hit': {'hittingPlayerId': 'event_player_1', 'hitteePlayerId': 'event_player_2'},
    'missed-shot': {'shootingPlayerId': 'event_player_1', 'shotType': 'event_detail', 'goalieInNetId' : 'event_goalie', '' : 'event_player_2', 'reason' : 'description'},
    'shot-on-goal': {'shootingPlayerId': 'event_player_1', 'shotType': 'event_detail', 'goalieInNetId' : 'event_goalie', '' : 'event_player_2'},
    'takeaway': {'playerId': 'event_player_1', '' : 'event_player_2'},
    'penalty': {'committedByPlayerId': 'event_player_1', 'drawnByPlayerId': 'event_player_2'},
    'stoppage' : {'reason' : 'description'},
    'delayed-penalty' : {}}

    xtra_map ={'delayed-penalty' : {'eventOwnerTeamId' : 'event_team'},
    'stoppage' : {'reason' : 'description'}}

    #ev = 'shot-on-goal'
    #sep_deets(grouped.get_group(ev),master_map[ev]).to_csv('10101.csv')
    
    ls = []
    for each in event_coords:
        #print(each)
        pp = grouped.get_group(each)
        #print(pp)
        #print(each)
        #print(master_map[each])
        df = sep_deets(pp,master_map[each])
       # print(each)
        ls.append(df)
        
    f_df = pd.concat(ls,axis=0)
    
    f_df['away_team'] = away
    f_df['home_team'] = home
    f_df.rename(columns={'eventOwnerTeamId':'event_team'},inplace=True)
    f_df['event_team'] = f_df['event_team'].map(lambda x: team_lookup(x))

    inv_map = {'1551': '5v5', '1541': '5v4', '1451': '4v5', '1531': '5v3', '1351': '3v5', '1441': '4v4', '1431': '4v3', '1341': '3v4',
               '1331': '3v3', '6031': 'Ev3', '1360': '3vE', '0641': 'Ev4', '0651': 'Ev5', '1560': '5vE', '1631': '6v3', '1361': '3v6'}
    
    f_df['game_strength'] = f_df['situationCode'].map(lambda x: inv_map[x])

    return f_df
    
    
#scrape_pbp('2023020466').to_csv('torpit8.csv')

def game_seconds(df):
    df['gameSec'] = df['timeInPeriod'].apply(lambda x: float(x.split(':')[0])*60+float(x.split(':')[1]))
    df['gameSec'] = df['gameSec'].astype(int)
    
    grouped = df.groupby(['period'])
    i=0
    ls = []
    for each in list(grouped.groups.keys()):
        df2 = grouped.get_group(each)
        df2['gameSec'] = df2['gameSec'].apply(lambda x: x + i)
        i += 1200
        ls.append(df2)
    return pd.concat(ls)

def shift_and_pbp(game):
    shifty = pd.read_csv('shifty2.csv')
    
    df = pd.read_csv('torpit8.csv',index_col=0,converters={'situationCode':str})
    
    
    away_cols = ['away_p1','away_p2','away_p3','away_p4','away_p5']
    home_cols = ['home_p1','home_p2','home_p3','home_p4','home_p5']

    teams = shifty.team.unique()
    home = df.home_team.unique()
    away = df.away_team.unique()
    pbp = game_seconds(df)        

    #FIND HOME + AWAY SKATERS ON ICE FOR EACH PBP EVENT
    #ALSO USE FOR FINDING SHIFT CHARTS PER GAME
    
    #print(pbp.situationCode.unique())

    
    #print(pbp['gameSec'])
    
    #pbp.to_csv('paspspdp.csv')
    
    #GET GAME SECONDS OF PBP
    
    #for each in [home,away]:
        
shift_and_pbp('2023020466')
    



    #situation_map = {'5v5': '1551', '5v4': '1541', '4v5': '1451', '5v3': '1531', '3v5': '1351', '4v4' : '1441', '4v3': '1431', '3v4': '1341', '3v3': '1331',
    #'Ev3' : '6031', '3vE' : '1360', 'Ev4': '0641', 'Ev5': '0651', 'Ev5': '0651', '5vE': '1560', '6v3' : '1631', '3v6' : '1361'}

    
    #situation_map = {'5v5': '1551', '5v4': '1541', '4v5': '1451', '5v3': '1531', '3v5': '1351', '4v4' : '1441', '4v3': '1431', '3v4': '1341', '3v3': '1331',
    #'Ev3' : '6031', '3vE' : '1360', 'Ev4': '0641', 'Ev5': '0651', 'Ev5': '0651', '5vE': '1560', '6v3' : '1631', '3v6' : '1361'}
    
    #inv_map = {v: k for k, v in situation_map.items()}
    #print(inv_map)
    
'''
        
    take_map = {'playerId': 'event_player_1'}#, '': 'goalie'}
    face_map = {'winningPlayerId': 'event_player_1', 'losingPlayerId': 'event_player_2'}#, '': 'goalie'}
    hit_map = {'hittingPlayerId': 'event_player_1', 'hitteePlayerId': 'event_player_2'}#, '': 'goalie'}
    bs_map = {'blockingPlayerId': 'event_player_1', 'shootingPlayerId': 'event_player_2'}#@, '': 'goalie'}
    pen_map = {'committedByPlayerId' : 'event_player_1', 'drawnByPlayerId' : 'event_player_2'}
    give_map = {'playerId': 'event_player_1'}
    g_map = {'shootingPlayerId' : 'event_player_1', 'assist1PlayerId' : 'event_player_2', 'assist2PlayerId' : 'event_player_3', 'goalie' : 'goalieInNetId'}
    ms_map = {'shootingPlayerId' : 'event_player_1', 'description' : 'event_detail'}
    s_map = {'shootingPlayerId' : 'event_player_1', 'shotType' : 'event_detail', 'goalie' : 'goalieInNetId'}

    master_map = {}
    for each in events_coords:
        if each == 'blocked-shot':
            master_map[each] = bs_map
        if each == 'missed-shot':
            master_map[each] = ms_map
        if each == 'shot-on-goal':
            master_map[each] = s_map
        if each == 'giveaway':
            master_map[each] = give_map
        if each == 'goal':
            master_map[each] = g_map
        if each == 'hit':
            master_map[each] = hit_map
        if each == 'takeaway':
            master_map[each] = take_map
        if each == 'penalty':
            master_map[each] = pen_map
        if each == 'faceoff':
            master_map[each] = face_map                
    print(master_map)    

    #bs_grp.to_csv('bs.csv')
    cols = list(bs_map.keys()) + cols_all
    deet_df = pd.DataFrame(bs_grp['details'].tolist(),columns=cols,index=bs_grp.index)#.to_frame()

    per_df = pd.DataFrame(bs_grp['periodDescriptor'].tolist(),index=bs_grp.index)
    per_df.rename(columns={'number':'period','periodType':'period type'},inplace=True)
    
    grpcols = list(df)
    grpcols = [i for i in grpcols if i not in ['details','periodDescriptor']]
    #grpcols = ['eventId', 'timeInPeriod', 'timeRemaining', 'situationCode', 'homeTeamDefendingSide', 'typeCode', 'event_type', 'sortOrder']

    final_df = pd.concat([bs_grp[grpcols],per_df,deet_df],axis=1)
    final_df.rename(columns=bs_map,inplace=True)
    #final_df.to_csv('bs2.csv')
'''

   # ,per_df













    #print(grpcols)
    #print(bs_grp)
    
    #print(pd.concat([deet_df2,bs_grp]))
    #for line in bs_grp['details']:
        #bs_grp[line.keys()] = #print(line.keys())
        #break
    #np.where(bs_grp['details'])
    
    #for each in events_coords:
        #events = grouped.get_group(each)
        #x=0
        #for detail in events['details']:
            #print(detail)

            #while x == 0:
                #x+=1
                #event_mapping[each] = list(detail.keys())
                #lscols.append(list(detail.keys()))    
           # events['xCoord'] = x['xCoord']
           # print(x)
            
        #break
            
            #for col in event_mapping[each]:
                #print(list(rows['details'].items()))
                #events[col] == events['details'][col]
 
    #df.to_csv('pspspspsps.csv')
            
                    
            #df[event_mapping[each].values()]
               # print(df[k])
  
        
   # print(event_mapping)
    #print(lscols)

    #print(df2)

#    print(event_mapping)

    #events_shfits = ['faceoff','game-end'

    #df2 = pd.concat([grouped.get_group(event) for event in events_coords])
    
    #df2.to_csv('papsp.csv')

    #df3 = pd.concat([grouped.get_group(event) for event in events_coords])
    #df2['details']
        #ls.append(row.keys())
        
    #event_mapping['blocked-shot'] = np.unique(ls)
    #print(event_mapping)

    #event_mapping = {'blocked-shot' :
    #ls = []
                     
    #grouped.get_group('blocked-shot')['details']#df2['blocked-shot']
        
        #df2 = grouped.get_group(each)
        
               
    #df['xC'] = df['details']['
    
    #return df
         
#df.rename(columns=mapping, inplace = True)
#df.to_csv('asddd.csv')
#for row in df['details'].iterrows():

    #row = ast.literal_eval(row)
#print(df['details'].dtype)
#print(df['event_type'])
#pd.json_normalize(df['details'])
#print(type(literal_eval(df['details'][3])))
#for x in range(1,10):
    #for each in df['details'].tolist():
        #print(type(each))
        #print(each)
       
  
#df['period'] = df['periodDescriptor']['number']
#df['details'] = df['details'].apply(literal_eval)
#print(df['details'].dtype

# for k,v in events['details'].items():
    #for k2,v2 in v:
        #if k2 == col:
           # df.at[k][col] = v2
    #print('key: ' + str(k))
    #print(v)
    
    #df[col] = 

'''
def bs_df(bsgrp):
    
    bs_map = {'blockingPlayerId': 'event_player_1', 'shootingPlayerId': 'event_player_2'}

    cols = list(bs_map.keys()) + cols_all
    deet_df = pd.DataFrame(bsgrp['details'].tolist(),columns=cols,index=bsgrp.index)

    final_df = pd.concat([bsgrp[no_deets],deet_df],axis=1)
    final_df.rename(columns=bs_map,inplace=True)
    return final_df

def s_df(sgrp):
    
    s_map = {'shootingPlayerId' : 'event_player_1', 'shotType' : 'event_detail', 'goalieInNetId' : 'goalie'}

    cols = list(s_map.keys()) + cols_all
    deet_df = pd.DataFrame(sgrp['details'].tolist(),columns=cols,index=sgrp.index)

    final_df = pd.concat([sgrp[no_deets],deet_df],axis=1)
    final_df.rename(columns=s_map,inplace=True)
    return final_df  

def ms_df(msgrp):
    ms_map = {'shootingPlayerId' : 'event_player_1', 'description' : 'event_detail'}
    cols = list(ms_map.keys()) + cols_all
    deet_df = pd.DataFrame(msgrp['details'].tolist(),columns=cols,index=msgrp.index)

    final_df = pd.concat([msgrp[no_deets],deet_df],axis=1)
    final_df.rename(columns=ms_map,inplace=True)
    return final_df
'''
