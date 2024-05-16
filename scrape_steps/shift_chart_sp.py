from datetime import date, timedelta
#import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
from html.parser import HTMLParser
import sqlalchemy as db
import sqlalchemy.orm
import psycopg2
from sqlalchemy import URL, create_engine, select
from psycopg2 import sql
import numpy as np
import matplotlib.pyplot as plt

yesterday  = str(date.today() - timedelta(days = 1))

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

def use_api(url):
    #url = string_1 + value + string_2
    data = requests.get(url).json()
    return data

def get_roster(team):
    rosters = []
    playerType = ["forwards", "defensemen", "goalies"]
    data = use_api('https://api-web.nhle.com/v1/roster/'+ team + '/20232024')
    for index in range(len(playerType)):
        playerAttrs = data[playerType[index]]
        #get_line_stats_sva for forwards
        #get_d_zone_pair stats for defensemen
        #get goalie stats for goalies
        for index in range(len(playerAttrs)):
            rosters.append((playerAttrs[index]["id"]))
    return rosters

#playerType = "skaters" or "goalies"
def get_team_stats(team, playerType):
    if playerType == "skaters" or "goalies":
        df = pd.DataFrame.from_dict(use_api("https://api-web.nhle.com/v1/club-stats/" + team + "/20232024/2")[playerType])
        names = ['firstName', 'lastName']
        for name in names:
            new_df = pd.DataFrame(list(df[name]))
            df[name] = new_df['default']
        return df
    else:
        print('skaters or goalies only paramter')
    
    
#get_team_skater_stats('BOS').to_csv('BOStest.csv')

#[2023020982, 2023020983, 2023020981, 2023020984, 2023020985, 2023020986, 2023020987, 2023020988, 2023020989]
def get_games_df(date):   
    game_list = use_api('https://api-web.nhle.com/v1/score/' + date)["games"]
    
    df = pd.DataFrame()
    df['gameid'] = [d['id'] for d in game_list]
    df['awayTeam'] = [d['awayTeam']['abbrev'] for d in game_list]
    df['homeTeam'] = [d['homeTeam']['abbrev'] for d in game_list]
    df['awayShots'] = [d['awayTeam']['sog'] for d in game_list]
    df['homeShots'] = [d['homeTeam']['sog'] for d in game_list]
    df['awayScore'] = [d['awayTeam']['score'] for d in game_list]
    df['homeScore'] = [d['homeTeam']['score'] for d in game_list]
    df.set_index('gameid', inplace=True, drop=False)
    return df

'''    
def player_shift_chart(shift_df, player_id, period):
    startlis = []
    endlis = []
    for shift in shift_df:
        if shift['period'] == period and shift['playerId'] == int(player_id):
            startlis.append((int(shift['startTime'][0:2])*60) + (int(shift['startTime'][3:5])))
            endlis.append((int(shift['endTime'][0:2])*60) + (int(shift['endTime'][3:5])))
    testdic = {}
    for key in startlis:
        for value in endlis:
            testdic[key] = value
            endlis.remove(value)
            break
    return testdic

def math_shifts(shiftdf):
    second_per_mult = '1200'
    third_per_mult = '2400'
    times = ['startTime','endTime']
             
    for time in times:
        shiftdf[time] = 
        periodlist = shiftdf['period'].items()
        
        seconds = (int(item[0:2])*60)) + (int(item[3:5]))
            
    for key in timelist:
        for value in periodlist:
            testdic[key] = value
            awaylist.remove(value)
            break
        
    return shiftdf
'''

def min_seconds(item):
    item = (int(item[0:2])*60) + (int(item[3:5]))
    return item

#team: away or home
def create_shift_chart(game_id):
    shift_data = use_api("https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId=" + game_id + "%20and%20((duration%20!=%20%2700:00%27%20and%20typeCode%20=%20517)%20or%20typeCode%20!=%20517%20)&exclude=detailCode&exclude=duration&exclude=eventDetails&exclude=teamAbbrev&exclude=teamName")["data"]
    shiftdf = pd.DataFrame(shift_data, columns = ['playerId', 'gameId', 'startTime', 'endTime', 'teamId', 'period', 'firstName', 'lastName'])
    return shiftdf

#game_df = get_games_df(yesterday)
    #df = pd.DataFrame(get_team_stats(team, 'skaters'))
    #for period in periods:
        #for playerId in df['playerID']:
            #playerlistname = str(playerId) + 'shiftlis'
            #for shift in shift_data:
                #if shift['period'] == period and shift['playerId'] == int(player_id):
                    
                    #newdf = pd.DataFrame(df['playerId'], df['lastName'], df['firstName'],

def correct_times(new_df):
    g=0
    times = ['startTime','endTime']

    while g < len(new_df.index):
        shiftdf = create_shift_chart(str(new_df.iloc[g]['gameid'])) 
        for time in times:
            shiftdf[time] = shiftdf[time].astype(str).apply(min_seconds)
            
            for item in shiftdf.index[shiftdf['period']==2].tolist():
                shiftdf.at[item,time] = np.int64(int(shiftdf.at[item,time]) + 1200)
                
            for item in shiftdf.index[shiftdf['period']==3].tolist():
                shiftdf.at[item,time] = np.int64(int(shiftdf.at[item,time]) + 2400)
        g+=1
        shiftdf.to_csv('2023020061.csv', mode = 'a')

correct_times(get_games_df(yesterday)[['gameid', 'awayTeam', 'homeTeam']])

    #inv_df= shiftdf.transpose()
    #print(inv_df)
        #if shiftdf['period'].isin(['2']) == True:
        #df = shiftdf['period'].isin(['2'])
    #for ind, row in shiftdf.iterrows():
        #if shiftdf.loc[row] == 2:
            #shiftdf.loc[row]['startTime'] += 1200
    #for ind, row in shiftdf.iterrows():
        #if row['period'] == 2:
            #shiftdf.loc[ind]['startTime'].apply(lambda x: x + 1200)
    #booldf = pd.DataFrame(shiftdf['period'].isin([2,3]))
    #for item in booldf['period']:
        #print(item)
        #if item == True:
            #shiftdf.loc[index
    #print(shiftdf[time])
            #shiftdf[time].iloc(ind).apply(lambda x: x + 1200)
            #if shiftdf['period'].iloc(ind) > 1:
            
        #if int(shiftdf['period']) > 2 and < 4:
            #shiftdf[time].apply(lambda x: x + 2400)
            
    #awaydf[['startTime','endTime','period']]
    #print(type(awaydf['startTime']))
    #math_shifts(awaydf)
    #math_shifts(awaydf).to_csv('testtt.csv')
    #for player in get_team_stats(new_df.iloc[g]['awayTeam'], "skaters"):
        #print(player_shift_chart(awaydf, str(player), 1))
        
    #homedf = create_shift_chart(str(new_df.iloc[g]['gameid']), new_df.iloc[g]['homeTeam'])


'''    
#shift_df = create_shift_chart('2023020900', 'BUF')


    #for period in periods:
        #player_shift_chart(game_id, awaydf['playerId'], period)
        
#{0: 25, 159: 174, 287: 321, 361: 390, 469: 510, 705: 783, 806: 857, 895: 901,
#141: 189, 275: 321, 440: 481, 661: 697, 793: 853, 926: 988, 1109: 1151,
#147: 198, 313: 333, 694: 776, 820: 845, 889: 916, 939: 951, 1043: 1095, 1192: 1200}

#shiftdic = player_shift_chart('2023020982', '8474090', 1)
#df = get_games_df(yesterday)

#list_away = df['awayTeam'].to_list()
#list_games = df['gameid']
#list_home = df['homeTeam'].to_list()

#new_df = pd.DataFrame(list_away, columns = df['gameid'])
#print(new_df)
#print(df['awayTeam'].to_list())
#print(df['awayTeam'])

#x= string column names, {x} = set, same data
#y = pandas series of data in each column sorted by index. {y} unhashable
#dfaway = get_roster(df['awayTeam'])
#for x,y in df.items():
    #print(df[x])

for index,row in df.iterrows():
    away = row['awayTeam']
    home = row['homeTeam']
    gameid = row['gameid']
    print(create_shift_chart(gameid, away, home))
    
 

    

#print(teamis)
#print(shiftdic)

#6for k in shiftdic:
    #print(shiftdic[k] - k)
#plt.show()



       
#plt.plot(brendan_array)
#plt.show()
#print(shiftdic)

periods = [1,2,3,4]
for each in periods:
    df[period] = period
    df['
for each in listy:
    if listy.index(n) < listy[ind-1]
i=0

for k in shiftdic:
    #plt.plot(np.array(k,shiftdic[k]))
    if k > shiftdic[k]:
        i += 1200
    print(i + k)
    print(i + shiftdic[k])# + ' ' + shiftdic[k])
print(lis)
        
#plt.show()

def opponent(df, team_name):
    df['

url_hky = URL.create(
    "postgresql+psycopg2",
    username="postgres",
    password="p33Gritz!!",  
    host="localhost",
    port="5432",
    database="playbyplay",
)
try:
    engine = db.create_engine(url_hky)
except:
    print("I am unable to connect to the engine")

try:
    conn = psycopg2.connect(
        host="localhost",
        database="playbyplay",
        user="postgres",
        password="p33Gritz!!",
        port="5432"
    )
except:
    print("I am unable to connect to the psycopg2")
'''
#cur = conn.cursor()
#cur.execute('''select "game_id" from "2024-03-05"''')
            #lis.append(start)
            #lis.append(end)
    #for i,e in enumerate(startlis):
        #if startlis[i] >
    #print(lis)
'''
    for i in range(len(lis)):
        if lis[i] > lis[i-1]:
            x+=1200
            print(lis[i] + x)
            print(lis[i-1] + x)
        lis[i] += x
        lis[i-1] += x#prev_ele += x
    print(lis)
      x=0
    i=1
    for i in range(len(startlis)):
        if startlis[i] < endlis[i-1]:
            #print('yippe')
            x+=1200
            startlis[i] = startlis[i] + x# += x
            endlis[i] = endlis[i] + x# += x
    print(startlis)
    print(endlis)
    #print(startlis[i])
    #print(endlis[i])
    #print(startlis)
    #print(endlis)


    def get_matchups(date):
    game_list = use_api('https://api-web.nhle.com/v1/score/' + date)["games"]

    awaylist = []
    homelist = []
    for d in game_list:
        awaylist.append( [ d['awayTeam']['abbrev'],d['awayTeam']['sog'],d['awayTeam']['score'] ] )
        homelist.append( [ d['homeTeam']['abbrev'],d['homeTeam']['sog'],d['homeTeam']['score'] ] )
    testdic = {}#,d['awayTeam']['sog'],d['awayTeam']['score']
 
    for key in awaylist:
        for value in homelist:
            testdic[key] = value
            awaylist.remove(value)
            break

    return {d['awayTeam']['abbrev'] for d in game_list : d['homeTeam']['abbrev'] for d in game_list} #[d['awayTeam']['abbrev'] for d in game_list]

'''
   
#sqlquery = '''select 
