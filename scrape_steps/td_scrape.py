import requests
import json
import pandas as pd
import os
import sqlalchemy as db
from bs4 import BeautifulSoup as bs
import sqlalchemy.orm
import psycopg2
from sqlalchemy import URL, create_engine, select, text
from psycopg2 import sql
from datetime import date, datetime, timedelta
#import TopDownHockey_Scraper.TopDownHockey_NHL_Scraper as tdhnhlscrape
from functools import reduce

yesterday  = date.today() - timedelta(days = 1)
today = date.today()
COL_list = ['2023020008', '2023020030', '2023020046', '2023020090', '2023020105', '2023020126', '2023020173', '2023020228', '2023020264', '2023020275', '2023020306', '2023020350', '2023020366', '2023020373', '2023020467', '2023020492', '2023020537', '2023020552', '2023020593', '2023020662', '2023020678', '2023020682', '2023020692', '2023020707', '2023020781', '2023020787', '2023020799', '2023020809', '2023020828', '2023020842', '2023020890', '2023020949', '2023020959', '2023021039', '2023021044', '2023021070', '2023021089', '2023021181', '2023021207', '2023021214', '2023021284']

def use_api(url):
    #url = string_1 + value + string_2
    return requests.get(url).json()

def get_games(date):   
    game_list = use_api('https://api-web.nhle.com/v1/score/' + date)["games"]
    return [str(d['id']) for d in game_list]

'''
def get_games(date):   
    df = pd.DataFrame.from_dict(use_api('https://api-web.nhle.com/v1/score/' + date)["games"],orient='index',columns=['id','awayTeam.abbrev','homeTeam.abbrev'])
    return df
'''

def matchup_df(date):   
    game_list = use_api('https://api-web.nhle.com/v1/score/' + date)["games"]
    gamelis = []
    awaylis = []
    homelis = []
    for d in game_list:
        gamelis.append(str(d['id']))
        awaylis.append(d['awayTeam']['abbrev'])
        homelis.append(d['homeTeam']['abbrev'])
    return pd.DataFrame({'game_id' : gamelis, 'away' : awaylis, 'home': homelis})

#df = matchup_df('2024-03-22')

#return rosterdict
#pipe deliminated (easily reversed)
def get_roster_by_game(game_id):
    url = 'https://www.nhl.com/scores/htmlreports/20232024/RO' + game_id[4:] + '.HTM'
    header = {
      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
      "X-Requested-With": "XMLHttpRequest"
    }
    r = requests.get(url, headers=header)
    soup = bs(r.content, 'lxml')
    li = soup.prettify().split('\n')
    
    homelis = []
    awaylis = []
    x=234
    y=469
 
    while x < 454:
        dastring = str(li[x]).replace("(A)","").replace("(C)","").strip()
        homelis.append(dastring)
        x+=11
    while y < 689:
        dastring = str(li[y]).replace("(A)","").replace("(C)","").strip()
        awaylis.append(dastring)
        y+=11
    delim='|'
    #print(type(reduce(lambda x, y: str(x) + delim + str(y), homelis)))
    #print(reduce(lambda x, y: str(x) + delim + str(y), awaylis))
    return {'home_roster' : reduce(lambda x, y: str(x) + delim + str(y), homelis), 'away_roster' : reduce(lambda x, y: str(x) + delim + str(y), awaylis)}

#print(get_roster_by_game('2023021109'))

#required: get_roster_by_game, matchup_df
def combine_matchup_roster_df(date):
#print(df)
    df = matchup_df(date)
    teamlis = ['home_roster','away_roster']
    for team in teamlis:
        lis = []
        for each in get_games(date):
            lis.append(get_roster_by_game(each)[team])
        df[team] = lis
    return df

combine_matchup_roster_df('2024-03-22').to_csv('asdfds.csv')

#print(len(lis))
#print(lis)

#print(df)
#lis = []
#for each in get_games('2024-03-22'):
    #print(each)
    #print(get_roster_by_game(each))
    #break
#print(lis)
    

#print(get_roster_by_game('2023020842')['home'])
      
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

def current_sched_tablenames(datelis):
    #datelis = pd.read_sql("fullschedule", engine)["date"].tolist()
    tablenames = []
    for date in datelis:
        dater = datetime.strptime(date, '%Y-%m-%d').date()
        dater = dater.strftime('%B' '%d' '%y')
        if date == str(today):
            break
        else:
            tablenames.append(dater)
    return list(dict.fromkeys(tablenames).keys())

#ex '2023-10-10' -> datetime.datetime(2023, 10, 10)
def lis_str_to_date(datelis):
    return [datetime.strptime(date, "%Y-%m-%d").date() for date in datelis]

def tablenames(datelis):
    return [date.strftime('%B' '%d' '%y') for date in lis_str_to_date(datelis)]

def single_table(date):
    return str_to_date(date).strftime('%B' '%d' '%y')

#polis = ['2023-10-10', '2023-10-11', '2023-10-12', '2023-10-13', '2023-10-14', '2023-10-15', '2023-10-16', '2023-10-17', '2023-10-18', '2023-10-19', '2023-10-20', '2023-10-21', '2023-10-22', '2023-10-23', '2023-10-24', '2023-10-25', '2023-10-26', '2023-10-27', '2023-10-28', '2023-10-29', '2023-10-30', '2023-10-31', '2023-11-01', '2023-11-02', '2023-11-03', '2023-11-04', '2023-11-05', '2023-11-06', '2023-11-07', '2023-11-08', '2023-11-09', '2023-11-10', '2023-11-11', '2023-11-12', '2023-11-13', '2023-11-14', '2023-11-15', '2023-11-16', '2023-11-17', '2023-11-18', '2023-11-19', '2023-11-20', '2023-11-22', '2023-11-24', '2023-11-25', '2023-11-26', '2023-11-27', '2023-11-28', '2023-11-29', '2023-11-30', '2023-12-01', '2023-12-02', '2023-12-03', '2023-12-04', '2023-12-05', '2023-12-06', '2023-12-07', '2023-12-08', '2023-12-09', '2023-12-10', '2023-12-11', '2023-12-12', '2023-12-13', '2023-12-14', '2023-12-15', '2023-12-16', '2023-12-17', '2023-12-18', '2023-12-19', '2023-12-20', '2023-12-21', '2023-12-22', '2023-12-23', '2023-12-27', '2023-12-28', '2023-12-29', '2023-12-30', '2023-12-31', '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-06', '2024-01-07', '2024-01-08', '2024-01-09', '2024-01-10', '2024-01-11', '2024-01-12', '2024-01-13', '2024-01-14', '2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-19', '2024-01-20', '2024-01-21', '2024-01-22', '2024-01-23', '2024-01-24', '2024-01-25', '2024-01-26', '2024-01-27', '2024-01-28', '2024-01-29', '2024-01-30', '2024-01-31', '2024-02-05', '2024-02-06', '2024-02-07', '2024-02-08', '2024-02-09', '2024-02-10', '2024-02-11', '2024-02-12', '2024-02-13', '2024-02-14', '2024-02-15', '2024-02-16', '2024-02-17', '2024-02-18', '2024-02-19', '2024-02-20', '2024-02-21', '2024-02-22', '2024-02-23', '2024-02-24', '2024-02-25', '2024-02-26', '2024-02-27', '2024-02-28', '2024-02-29', '2024-03-01', '2024-03-02', '2024-03-03', '2024-03-04', '2024-03-05', '2024-03-06', '2024-03-07', '2024-03-08', '2024-03-09', '2024-03-10', '2024-03-11', '2024-03-12', '2024-03-13', '2024-03-14', '2024-03-15', '2024-03-16', '2024-03-17', '2024-03-18', '2024-03-19']

#print(tablenames(polis))

def date_to_str(datelis):
    return [str(date) for date in datelis]

def single_date_to_str(date):
    return str(date)

def str_to_date(date):
    return datetime.strptime(date, "%Y-%m-%d").date()

def is_date_in_future(date):
    return date > today

def is_date_in_past(date):
    return date < today

def dates_in_future(datelis):
    newlis = list(filter(is_date_in_future, str_to_date(datelis)))
    return [date.strftime("%Y-%m-%d") for date in newlis]

def dates_in_past(datelis):
    newlis = list(filter(is_date_in_past, str_to_date(datelis)))
    return [date.strftime("%Y-%m-%d") for date in newlis]

#return df
def get_team_dates(team, home):
    df = pd.read_sql("fullschedule", engine)[["date", "ID", "home_team", "away_team"]]
    if home == True:
        grouped = df.groupby(df.home_team)
    if home == False:
        grouped = df.groupby(df.away_team)
    return grouped.get_group(team)

#return list of strings
def use_team_dates_to_date(team, home):
    lis = get_team_dates(team, home)["date"].tolist()
    return dates_in_past(lis)

#return list of strings
def full_sched_to_date(team,home):
    return use_team_dates_to_date(team,home) + use_team_dates_to_date(team,home)

#col_home = full_sched_to_date('COL', True)
#col_away = full_sched_to_date('COL', True)
#print(current_sched_tablenames(col_home) + current_sched_tablenames(col_away))

def scrape_till_yesterday():
    dates = pd.read_sql("fullschedule", engine)["date"].tolist()
    #tablenames = []
    #daters = []
    for date in dates:
        if is_date_in_past(str_to_date(date)):
            dater = datetime.strptime(date, '%Y-%m-%d').date()
            #tablenames.append(dater.strftime('%B' '%d' '%y'))
            #daters.append(date)
    #set_table = list(dict.fromkeys(tablenames).keys())
    #set_daters = list(dict.fromkeys(daters).keys())
    #print(set_table)
    #print(set_daters)
    #res = [{set_table[i]: set_daters[i]} for i in range(len(set_daters))]
    #print(res)

            df = tdhnhlscrape.full_scrape(get_games(dater.strftime("%Y-%m-%d")),  shift = True)
            df.to_sql(dater.strftime('%B' '%d' '%y'), engine, if_exists='replace')
            print(str(dater) + "success!")

def get_full_schedule():      
    tdhnhlscrape.scrape_full_schedule().to_sql("fullschedule", engine)
    
#scrape_till_yesterday()

#tdhnhlscrape.scrape_full_schedule().to_sql("fullschedule", engine)

def all_game_ids():
    return pd.read_sql("fullschedule",engine)["ID"].tolist()

def scrape_everything():
    tdhnhlscrape.full_scrape(all_game_ids(),shift=True).to_sql('data',engine,if_exists='replace')
    print("Success!")

#scrape_everything()
'''
def scrape_shots():
    tdhnhlscrape.full_scrape(all_game_ids(),shift=False).to_sql('all_data_no_shifts',engine,if_exists='replace')
    print("Success! no shifts")
'''
#scrape_shots()


#print(len(all_game_ids()))
'''
def get_yesterdays_games():
    game_list = use_api('https://api-web.nhle.com/v1/score/' + str(date.today() - timedelta(days = 1)))["games"]
    for each in game_list:
        print(each["id"])
    #return game_list
'''
#print(get_games(str(yesterday)))

def scrape_somethin():
    datelis = ['03-11-24', '03-12-24', '03-13-24', '03-14-24', '03-15-24', '03-16-24']   
    tablenames = []
    for date in datelis:
        dater = datetime.strptime(date, '%m-%d-%y').date()
        
        tablenames.append(dater.strftime('%B' '%d' '%y'))
        #print(get_games(dater.strftime("%Y-%m-%d")))
        df = tdhnhlscrape.full_scrape(get_games(dater.strftime("%Y-%m-%d")),  shift = True)
        df.to_sql(dater.strftime('%B' '%d' '%y'), engine, if_exists='replace')
'''
    if home = True:
        x=234
        y=454
    if home = False:
        x=469
        y=689
'''       
def update_second_period_coords():
    query = '''update {}
set "coords_x"="coords_x"*-1
where "game_period" in ('2', '4')
and "coords_x" is Not NULL
and "coords_x" != 0;'''

    dates = pd.read_sql("fullschedule", engine)["date"].tolist()
    tablenames = []
    for date in dates:
        dater = datetime.strptime(date, '%Y-%m-%d').date()
        dater = dater.strftime('%B' '%d' '%y')
        if date == str(today):
            break
        else:
            tablenames.append(dater)
    set_table = list(dict.fromkeys(tablenames).keys())
    print(set_table)   
    for table in set_table:
        cur.execute(sql.SQL(query).format(sql.Identifier(table)))
        print("updating values for " + table)
        conn.commit()

def new_date_col():
    query = '''ALTER TABLE fullschedule 
    ADD new_date DATE;'''

    query2 = '''update fullschedule 
    SET "new_date" = to_date("date", 'YYYY-MM-DD');'''

    cur.execute(query)
    cur.execute(query2)
    conn.commit()

