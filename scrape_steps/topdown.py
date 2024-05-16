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

nhl_teams = ["ANA", "ARI", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ",
    "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", "NYI", "NYR",
    "OTT", "PHI", "PIT", "SJS", "SEA", "STL", "TBL", "TOR", "VAN", "VGK", "WPG", "WSH"]

yesterday  = date.today() - timedelta(days = 1)
today = date.today()
#df = tdhnhlscrape.scrape_full_schedule()

def use_api(url):
    #url = string_1 + value + string_2
    return requests.get(url).json()

def get_games(date):   
    game_list = use_api('https://api-web.nhle.com/v1/score/' + date)["games"]
    return [d['id'] for d in game_list]

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
        awaylis.append(dastring)
        x+=11
    while y < 689:
        dastring = str(li[y]).replace("(A)","").replace("(C)","").strip()
        homelis.append(dastring)
        y+=11
    return {'home_team' : homelis, 'away_team' : awaylis}

#print(get_roster_by_game('2023020008'))

#df of team stats
def team_skaters(team):
    names = ['firstName', 'lastName']
    response = json.loads(requests.get("https://api-web.nhle.com/v1/club-stats/" + team + "/20232024/2").text)
    df = pd.DataFrame.from_dict(response["skaters"])
    for name in names:
        new_df = pd.DataFrame(list(df[name]))
        df[name] = new_df['default']
    return df

#team_skaters('COL').to_csv('coldata.csv')
'''
def all_data_returned(date, team, ID, home):
    names = ['firstName', 'lastName']
    response = json.loads(requests.get("https://api-web.nhle.com/v1/club-stats/" + team + "/20232024/2").text)
    df = pd.DataFrame.from_dict(response["skaters"])
    for name in names:
        new_df = pd.DataFrame(list(df[name]))
        df[name] = new_df['default']
    #namestring = df['firstName'].upper() + " " + df['lastName'].upper()
    #player_ind = df['playerID'].tolist().index(ID)
    return df, namestring, player_ind
'''

#team playernames list
def player_names(team):
    return team_skaters(team)['firstName'] + " " + team_skaters(team)['lastName']

#team playerids list
def player_ids(team):
    return team_skaters(team)['playerId']

#team [first,last,id] df 
def ids_names(team):
    return team_skaters(team)[['firstName','lastName','playerId']]

#get row # of player
def ind_player(team, ID):
    strlis = list(map(str, ids_names(team)['playerId'].tolist()))
    return strlis.index(ID)

def name_player(team, rowind):
    return team_skaters(team).iloc[rowind]['firstName'] + " " + team_skaters(team).iloc[rowind]['lastName']

def player_use_df(df, Id):
    strlis = list(map(str, df['playerId'].tolist()))
    player_index = strlis.index(Id)
    namestring = df['firstName'].iloc[player_index].upper() + " " + df['lastName'].iloc[player_index].upper()
    #namestring = df['firstName'].upper() + " " + df['lastName'].upper()
    return [namestring, player_index]

#def get_playername(player_id):
    
    
'''
def loop_thru_df(df):
    strlis = list(map(str, df['playerId'].tolist()))
    #player_index = strlis.index(Id)
    for player in strlis:
        #CALL SQL BY PLAYER ID
        #EXPORT DF TO R FOR CHARTING
'''        
#print(player_use_df(team_skaters('COL'), '8480069'))
    
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
    cur=conn.cursor()
except:
    print("I am unable to connect to the psycopg2")
 
#string to 'October 10 23'
def date_to_tablename(date):
    return datetime.strptime(date, "%Y-%m-%d").date().strftime('%B' '%d' '%y')

def datelis_to_str(datelis):
    return [str(date) for date in datelis]

def str_to_date(date):
    return datetime.strptime(date, "%Y-%m-%d").date()

def tablename_to_date(tablename):
    return datetime.strptime(tablename,'%B' '%d' '%y').date()

#print(type(tablename_to_date('October1023'))

def is_date_in_future(date):
    return date > today

def is_date_in_past(date):
    return date < today

def str_date_in_past(date):
    return datetime.strptime(date, "%Y-%m-%d").date() < today

def str_date_in_future(date):
    return datetime.strptime(date, "%Y-%m-%d").date() > today
'''
#return list of 'October1423'
def tables_between_two_dates(start_date, end_date):
    sdate = str_to_date(start_date)
    edate = str_to_date(end_date)
    returnlis = []
    #start_date = date_to_tablename(start_date)
    #end_date = date_to_tablename(end_date)
    for each in get_current_database():
        if tablename_to_date(each) >= sdate and tablename_to_date(each) <= edate:
            returnlis.append(each)
    return returnlis
'''
#print(tables_between_two_dates('2023-10-11','2023-10-15'))

def dates_to_skip():
    return ['November2123','November2323','February0124','February0224','February0324','February0424','December2423','December2523','December2623']

def all_tables_to_date():
    sql = '''select distinct "date" from "fullschedule"
    order by "date";'''
    return pd.read_sql_query(sql,engine)["date"].tolist()

#print(all_tables_to_date())

#print([tablename_to_date(each) for each in dates_to_skip()])

#takes str date, return str date
def remove_nongamedays():
    x =[str_to_date(each) for each in all_tables_to_date()]
    y =[tablename_to_date(each) for each in dates_to_skip()]
    return [str(p) for p in x if p not in y]

#print(remove_nongamedays())

homelis = ['home_team', 'away_team']
homeplayers = '''("home_on_2","home_on_3","home_on_4","home_on_5","home_on_6")'''
awayplayers = '''("away_on_2","away_on_3","away_on_4","away_on_5","away_on_6")'''


def fullschedule():
    return pd.read_sql("fullschedule", engine)["date"].tolist()

def df_schedule_past():
    query = '''select * from fullschedule where "new_date" < CURRENT_DATE
order by "new_date" ASC;'''
    return pd.read_sql_query(query,engine)

#df_schedule_past().to_csv('zcvo.csv')

#ONLY PLAYED GAMES! WOOPIE
def team_df_sched(team, home, away):
    query = '''select "index","ID","season","home_team","away_team","new_date" from fullschedule 
where "new_date" < CURRENT_DATE '''
    lis = [team]
    
    if (away == True) and (home == False):
        query += '''and "away_team" = %(xyz)s'''
        
        
    if (away == True) and (home == True):
        query += '''and ("home_team" = %(xyz)s or "away_team" = %(xyz)s)'''
        lis = [team,team]
        
    if (away == False) and (home == True):
        query += '''and "home_team" = %(xyz)s'''
        
    if (away == False) and (home == False):
        query += 'STOP'
        print('ERROR: NO GAMES FOUND')
    query += ''' order by "new_date" ASC;'''  
    return pd.read_sql(query,engine,params={'xyz' : team})




#ONLY PLAYED GAMES! WOOPIE
def games_played(team):
    return team_df_sched(team,home=True,away=True)["ID"].tolist()

#ONLY PLAYED GAMES! WOOPIE
def home_games_played(team):
    return team_df_sched(team,home=True,away=False)["ID"].tolist()

#ONLY PLAYED GAMES! WOOPIE
def away_games_played(team):
    return team_df_sched(team,home=False,away=True)["ID"].tolist()

def create_gameday_rosters():
    delim = ";"
    for team in nhl_teams:
        with open(team + '_gameday_rosters.csv', 'a') as file:
            file.write('game_id,roster,game_type' + '\n')
            for each in home_games_played(team):
                if each in special:
                    lis = special_gameday(str(each))['home_team']
                else:
                    lis = get_roster_by_game(str(each))["home_team"]
                res = reduce(lambda x, y: str(x) + delim + str(y), lis)
                file.write(str(each) + ',"' + res + '",home' + '\n')

            for each in away_games_played(team):
                if each in special:
                    lis = special_gameday(str(each))['home_team']
                else:                
                    lis = get_roster_by_game(str(each))["away_team"]
                res = reduce(lambda x, y: str(x) + delim + str(y), lis)
                file.write(str(each) + ',"' + res + '",away' + '\n')

            file.close()

#create_gameday_rosters()
    
'''
#print([str(x) for x in team_games_played('COL')])
def away_game_dict(team):
    for each game in away_games_played('COL'):
        {key: val for key, 
            val in x.items() if str_date_in_past(key) == True}
'''
'''
df = team_df_sched('COL',home=True,away=True)
lis = games_played('COL')
#print(lis)
#newlis = []
for ele in lis:
    get_roster_by_game(
    #print(df.loc[df['ID'] == ele])
    #print(type(df.loc[df['ID'] == ele]))
    #break

def add_roster_cols(df):
    #df["home_roster"]#.tolist()
    #df["away_roster"]#.tolist()
    for each in df['game_id']:
         df.iloc['game_id']
'''
        #home_team_column.df.loc(each).insert(get_roster_by_game(each)["home_team"])
        #away_roster_column.df.loc(each).insert(get_roster_by_game(each)["away_team"])
#for each in team_games_played('COL'):
    
#print(team_games_played('COL'))

#def roster_df(df,team):
    
    
#roster_df(team_df_sched('COL',home=True,away=True))
    
#df_schedule_past().to_csv('sdfsdffdsfs.csv')
'''
def get_current_database():
    dates = pd.read_sql("fullschedule", engine)["date"].tolist()
    tablenames = []
    res = []
    for date in dates:
        dater = datetime.strptime(date, '%Y-%m-%d').date()
        dater = dater.strftime('%B' '%d' '%y')
        if date == str(today):
            break
        else:
            tablenames.append(dater)
    return [i for n, i in enumerate(tablenames) if i not in tablenames[:n]]
'''
#print(get_current_database())

def update_second_period_coords():
    query = '''update {}
set "coords_x"="coords_x"*-1
where "game_period" = '2'
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
    set_table = set(tablenames)
    print(set_table)   
    for table in set_table:
        cur.execute(sql.SQL(query).format(sql.Identifier(table)))
        print("updating values for " + table)
        conn.commit()

def games_by_team(team, home):
    query = '''select "ID" from fullschedule 
 {} = %s
order by "new_date" ASC;'''
    newQ = sql.SQL(query).format(sql.Identifier(home))
    cur.execute(newQ, (team,))
    results = cur.fetchall()
    results = [str(i[0]) for i in results]
    return results
#print(games_by_team('COL',"home_team"))

#returns list of dates (as string) for every games in past
def dates_by_team(team, home):
    query = '''select "date" from "fullschedule" where "new_date" < CURRENT_DATE and {} = %s;'''
    newQ = sql.SQL(query).format(sql.Identifier(home))
    cur.execute(newQ, (team,))
    results = cur.fetchall()
    results = [str(i[0]) for i in results]
    return list(filter(str_date_in_past, results))

#print(dates_by_team('COL',"home_team"))

#return dict {'2023-10-11': '2023020008', '2023-10-14': '2023020030', etc..} only for past games
def dates_games_by_team(team, home, away):
    query = '''select "date", "ID" from "fullschedule" '''
    lis = [team]
    
    if (away == True) and (home == False):
        query += '''where "away_team" = %s;'''
        
        
    if (away == True) and (home == True):
        query += '''where "home_team" = %s or "away_team" = %s;'''
        lis = [team,team]
        
    if (away == False) and (home == True):
        query += '''where "home_team" = %s;'''
        
    if (away == False) and (home == False):
        query += ';'
        
    cur.execute(query,lis)

    results = cur.fetchall()
    dates = [str(i[0]) for i in results]
    games = [str(i[1]) for i in results]
    x = dict(zip(dates,games))
    
    return {key: val for key, 
            val in x.items() if str_date_in_past(key) == True}

#col_games = dates_games_by_team('COL', home=True, away=True)
#print(col_games)

'''
def games_rosters_by_team(team,home):
    row 
    if home == True:
        stringy = "home_team"
    if home == False:
        stringy = "away_team"                   
    lis = games_by_team(team, stringy)
    for game in lis:
        x = get_roster_by_game(game)[stringy]
'''

#home = "home_team"
def did_player_play(player, game_id, home):
    #games = games_by_team(team,home)
    print(get_roster_by_game(game_id)[home])
    if player in get_roster_by_game(game_id)[home]:
        return True
    else:
        return False
'''
#print(did_player_play('CALE MAKAR', '2023020076', "home_team"))
def game_list_if_played(team, player):
    games = games_by_team(team,"home_team") + games_by_team(team,"away_team")
    for each 
 '''   
#df = team_df_sched('COL',home=True,away=True)
#for k,v, in col_games:
    


def game_id_roster(team, homey):
    #rosterlis = []
    returnlis = []
    if homey == True:
        string = "home_team"
        x = dates_games_by_team(team,home=True,away=False)
    if homey == False:
        string = "away_team"
        x = dates_games_by_team(team,home=False,away=True)
    for k,v in x.items():
        returnlis.append({v: get_roster_by_game(v)[string]})# for key,val in x.items()]
        #print(get_roster_by_game(game)[home])
        #returnlis.append({game: get_roster_by_game(game)[home]})
    return returnlis
'''
for each in game_id_roster('COL',homey=True) + game_id_roster('COL',homey=False):
    each
'''
#big_data = [{'2023020076': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'ARTTURI LEHKONEN', 'TOMAS TATAR', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020143': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'RILEY TUFTE', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'KURTIS MACDERMID', 'ARTTURI LEHKONEN', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020188': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', "LOGAN O'CONNOR", 'ONDREJ PAVEL', 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'ARTTURI LEHKONEN', 'TOMAS TATAR', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020201': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'RILEY TUFTE', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'ARTTURI LEHKONEN', 'TOMAS TATAR', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020219': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'RILEY TUFTE', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'KURTIS MACDERMID', 'TOMAS TATAR', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020240': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'SAMUEL GIRARD', 'CALEB JONES', 'TOMAS TATAR', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020291': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'RILEY TUFTE', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'CALEB JONES', 'TOMAS TATAR', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020313': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'CALEB JONES', 'TOMAS TATAR', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020324': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'CALEB JONES', 'TOMAS TATAR', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020386': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'RYAN JOHANSEN', 'ROSS COLTON', 'FREDRIK OLOFSSON', 'OSKAR OLAUSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'KURTIS MACDERMID', 'SAM MALINSKI', 'CALEB JONES', 'TOMAS TATAR', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020402': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'KURTIS MACDERMID', 'SAM MALINSKI', 'TOMAS TATAR', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020417': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'KURTIS MACDERMID', 'SAM MALINSKI', 'TOMAS TATAR', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020431': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'BEN MEYERS', 'SAM MALINSKI', 'TOMAS TATAR', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020445': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'BEN MEYERS', 'SAM MALINSKI', 'TOMAS TATAR', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020477': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'KURTIS MACDERMID', 'SAM MALINSKI', 'CALEB JONES', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020507': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'KURTIS MACDERMID', 'BEN MEYERS', 'SAM MALINSKI', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020525': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'KURTIS MACDERMID', 'SAM MALINSKI', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020571': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'KURTIS MACDERMID', 'BEN MEYERS', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020582': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'BEN MEYERS', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020606': ['JACK JOHNSON', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'KURTIS MACDERMID', 'BEN MEYERS', 'CALEB JONES', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020624': ['JACK JOHNSON', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'NATHAN MACKINNON', 'JASON POLIN', 'SAMUEL GIRARD', 'KURTIS MACDERMID', 'SAM MALINSKI', 'CALEB JONES', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020637': ['JACK JOHNSON', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'NATHAN MACKINNON', 'JASON POLIN', 'SAMUEL GIRARD', 'KURTIS MACDERMID', 'SAM MALINSKI', 'CALEB JONES', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020742': ['BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'KURTIS MACDERMID', 'ARTTURI LEHKONEN', 'SAM MALINSKI', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020757': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'KURTIS MACDERMID', 'ARTTURI LEHKONEN', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}, {'2023020864': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ZACH PARISE', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'CHRIS WAGNER', 'ROSS COLTON', 'FREDRIK OLOFSSON', 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'ARTTURI LEHKONEN', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'JUSTUS ANNUNEN']}, {'2023020881': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ZACH PARISE', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'CHRIS WAGNER', 'ROSS COLTON', 'FREDRIK OLOFSSON', 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'ARTTURI LEHKONEN', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'JUSTUS ANNUNEN']}, {'2023020908': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ZACH PARISE', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'CHRIS WAGNER', 'ROSS COLTON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'ARTTURI LEHKONEN', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'JUSTUS ANNUNEN']}, {'2023020936': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ZACH PARISE', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'CHRIS WAGNER', 'ROSS COLTON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'ARTTURI LEHKONEN', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'JUSTUS ANNUNEN']}, {'2023020979': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ZACH PARISE', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'CHRIS WAGNER', 'ROSS COLTON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'SAMUEL GIRARD', 'ARTTURI LEHKONEN', 'CALEB JONES', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'JUSTUS ANNUNEN']}, {'2023020991': ['JACK JOHNSON', 'DEVON TOEWS', 'CALE MAKAR', 'ZACH PARISE', 'ANDREW COGLIANO', 'CHRIS WAGNER', 'ROSS COLTON', 'ONDREJ PAVEL', 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'ARTTURI LEHKONEN', 'CALEB JONES', 'JEAN-LUC FOUDY', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'JUSTUS ANNUNEN']}, {'2023021006': ['JACK JOHNSON', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'BRANDON DUHAIME', 'VALERI NICHUSHKIN', 'YAKOV TRENIN', 'ROSS COLTON', 'SEAN WALKER', 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'CASEY MITTELSTADT', 'JOSH MANSON', 'SAMUEL GIRARD', 'ARTTURI LEHKONEN', 'JOEL KIVIRANTA', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'JUSTUS ANNUNEN']}, {'2023020061': ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'ARTTURI LEHKONEN', 'TOMAS TATAR', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV']}]

#print(big_data['2023020076'])
'''
def roster_lookup_by_game(game_id):
    for each in big_data:
        print(each.keys())

rows = []
for each in big_data:
    print(each)
    for key in each.keys():
        print(key)
        break
    for value in each.values():
        print(value)
        break
    break
'''    
#newdict = [{'game_id': 'roster'}]
#pp = newdict + big_data
#df=pd.DataFrame(big_data)#, columns = ['game_id','roster'])
#df=pd.json_normalize(pp)
#df.to_sql('col_test',engine,if_exists='replace')

#def team_roster_history(team, homey):
    
#if 'CALE MAKAR' in game_id_roster('COL', "away_team"))

#print(did_player_play('COL', 'CALE MAKAR', homey=True))
#print(did_player_play('COL', 'CALE MAKAR', homey=False))
    


#for each in col_away_games:
    


 #get_roster_by_game(game_id)
#same as dates_by_team but 'October0523' etcc
def get_tablenames(datelis):
    daterlis = []
    for i in datelis:
        dater = datetime.strptime(i,'%Y-%m-%d').date()
        month = dater.strftime("%B")
        day = dater.strftime("%d")
        year = dater.year
        daterlis.append(month + day + str(year)[2:])
    return daterlis

#print(get_tablenames(dates_by_team('COL',homelis[1])))

def all_team_data(date,team,home):
    query = '''select * from {table} where {homey} = %s;'''
    sdf = sql.SQL(query).format(table=sql.Identifier(date),
    homey=sql.Identifier(home))
    cur.execute(sdf, (team,))
    cols = cur.execute(sdf,(team,))
    df = pd.DataFrame(cur.fetchall(), columns = [desc[0] for desc in cur.description])
    return df

#df = all_team_data('October1923','COL',homelis[0])

def group_by_strength(df):
    return df.groupby(df.game_strength_state)

def PK_of(grouped):
    return grouped.get_group('4v5')

def PP_of(grouped):
    return grouped.get_group('5v4')

def EV_of(grouped):
    return grouped.get_group('5v5')

#grouped = df.groupby(df.game_strength_state)

def multi_players(home, team, playerlis):
    values = [team, 'SHOT', 'MISS', 'GOAL']
    
    query = '''select {event_var}, {gamer},{x_coord},{y_coord},{team},{event_player_1},{event_player_2},{strength},{home_one},{home_two},{home_three},{home_four},{home_five},
    {away_one},{away_two},{away_three},{away_four},{away_five}
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
    #print(newquery)
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
            home_one=sql.Identifier("home_on_2"),
            home_two=sql.Identifier("home_on_3"),
            home_three=sql.Identifier("home_on_4"),
            home_four=sql.Identifier("home_on_5"),
            home_five=sql.Identifier("home_on_6"),
            away_one=sql.Identifier("away_on_2"),
            away_two=sql.Identifier("away_on_3"),
            away_three=sql.Identifier("away_on_4"),
            away_four=sql.Identifier("away_on_5"),
            away_five=sql.Identifier("away_on_6"))
    
    cur.execute(sdf,values)
    cols = cur.execute(sdf,values)
    df = pd.DataFrame(cur.fetchall(), columns = [desc[0] for desc in cur.description])
    return df

multi_players("home_team", 'BUF', ['JJ PETERKA, ALEX TUCH']).to_csv('a2sdfds.csv')
print('succ')
#print(tables_between_two_dates('2023-10-01','2023-10-31'))

#return list of '2023-10-14'
def did_team_play(team, start_date, end_date):
    tablenames = [tablename_to_date(each) for each in tables_between_two_dates(start_date, end_date)]
    full_team = [str_to_date(each) for each in dates_by_team(team,"home_team") + dates_by_team(team,"away_team")]
    return [str(x) for x in tablenames if x in full_team]

#return list of '2023-10-14'
#home="home_team" or "away_team"
def did_team_play_home_or_away(team, start_date, end_date, home):
    tablenames = [tablename_to_date(each) for each in tables_between_two_dates(start_date, end_date)]
    full_team = [str_to_date(each) for each in dates_by_team(team,home)]# + dates_by_team(team,"away_team")]
    return [str(x) for x in tablenames if x in full_team]

#print(did_team_play('ANA', '2023-10-11', '2024-01-01'))


#print(aggregate_team_data('ANA', '2024-01-01', '2024-01-11', "home_team"))


#def check_player_played(date, team, playername):
    #sql = '''select "game_id" from {};'''
       # if pd.read_sql_query("fullschedule", engine)

        
#ONLY WORKS FOR A SINGLE TEAM
def aggregate_players_dates():
    #game_id_lis = pd.read_sql("fullschedule", engine)["ID"].tolist()
    for each in datelis:
        multi_players(date_to_tablename(each), home, team, playerlis)
        print(get_roster_by_game('2023020842')['home'])

#def multi_dates_and_players(team, start_date, end_date, playerlis):
    
#multi_players(date, home, team, playerlis)
    


#(team, 'SHOT', 'MISS', 'GOAL','CALE MAKAR','NATHAN MACKINNON')
#multi_players('October1923', homelis[0], 'COL', ['CALE MAKAR', 'JONATHAN DROUIN', 'DEVON TOEWS']).to_csv('nate_mack_oct19.csv')
#multi_players('November1323', homelis[1], 'COL', ['CALE MAKAR', 'DEVON TOEWS']).to_csv('nate_mack_nov13.csv')


#df=pd.read_sql_query(sql, engine)
#df[['coords_x', 'coords_y', 'event_team']].to_csv('wpg13.csv')
'''
col_home = ['October1923', 'October2123', 'November0123', 'November0723', 'November0923', 'November1123', 'November1523', 'November2223', 'November2523', 'November2723', 'December0523', 'December0723', 'December0923', 'December1123', 'December1323', 'December1723', 'December2123', 'December2323', 'December3123', 'January0224', 'January0624', 'January0824', 'January1024', 'January2424', 'January2624', 'February1824', 'February2024', 'February2424', 'February2724', 'March0424', 'March0624', 'March0824']
df = pd.DataFrame()
for ele in col_home:
    df2=(multi_players(ele, homelis[0], 'COL', ['CALE MAKAR', 'JONATHAN DROUIN', 'DEVON TOEWS']))
    if df2.empty:
        print('no df for ' + ele)
    else:
        df = pd.concat([df,df2],ignore_index=True)
#df.to_csv('sdfs.csv')

for each in df['event_team']:
    if each == 'COL':
        print('COL')
    else:
        df.loc[df['event_team'] == each, 'event_team'] = 'enemy'

df[['coords_x', 'coords_y','event_team']].to_csv('testsd.csv')

#take df, find playername
def player_pandas(df, playername):

#take df, find faceoffs
def fac_pandas(df):

#take df, find 'shot' and 'miss'
def shots_of(df):

#SQL FOR BIGGER DATA
    
#df joins thru date range
def sql_joins([dates])

def sql_players_2(playerOne,playerTwo):
'''

def shot_data_by_player(date, team, playername, home):
    query = '''select "event_team", "coords_x", "coords_y" from {table} where {homey} = %s and
    ("event_type" = %s or "event_type" = %s)
    and %s in ({home_one}, {home_two}, {home_three}, {home_four}, {home_five})
    and "event_zone" = 'Off';'''
    sdf = sql.SQL(query).format(table=sql.Identifier(date),
            homey=sql.Identifier(home),
            home_one=sql.Identifier("home_on_2"),
            home_two=sql.Identifier("home_on_3"),
            home_three=sql.Identifier("home_on_4"),
            home_four=sql.Identifier("home_on_5"),
            home_five=sql.Identifier("home_on_6"))
    cur.execute(sdf, (team,'SHOT','MISS',playername))
    cols = cur.execute(sdf,(team,'SHOT','MISS',playername))
    df = pd.DataFrame(cur.fetchall(), columns = [desc[0] for desc in cur.description])
    return df

#shot_data_by_player('October1923', 'COL', 'CALE MAKAR', homelis[0]).to_csv('makar_oct19.csv')

def faceoff_starts(date,team,playername,home):
    query = '''select "event_team", "coords_x", "coords_y" from {table} where {homey} = %s and
    "event_type" = %s
    and %s in ({home_one}, {home_two}, {home_three}, {home_four}, {home_five});'''
    sdf = sql.SQL(query).format(table=sql.Identifier(date),
            homey=sql.Identifier(home),
            home_one=sql.Identifier("home_on_2"),
            home_two=sql.Identifier("home_on_3"),
            home_three=sql.Identifier("home_on_4"),
            home_four=sql.Identifier("home_on_5"),
            home_five=sql.Identifier("home_on_6"))
    cur.execute(sdf, (team,'FAC',playername))
    cols = cur.execute(sdf,(team,'FAC',playername))
    df = pd.DataFrame(cur.fetchall(), columns = [desc[0] for desc in cur.description])
    return df

#faceoff_starts('October1923', 'COL', 'CALE MAKAR', homelis[0]).to_csv('makar_oct19.csv')

def team_shot_without_player(date, team, playername, home):
    query = '''select "event_team", "coords_x", "coords_y" from {table} where {homey} = %s and
    ("event_type" = %s or "event_type" = %s)
    and %s not in ({home_one}, {home_two}, {home_three}, {home_four}, {home_five})
    and "event_zone" = 'Off';'''
    sdf = sql.SQL(query).format(table=sql.Identifier(date),
            homey=sql.Identifier(home),
            home_one=sql.Identifier("home_on_2"),
            home_two=sql.Identifier("home_on_3"),
            home_three=sql.Identifier("home_on_4"),
            home_four=sql.Identifier("home_on_5"),
            home_five=sql.Identifier("home_on_6"))
    cur.execute(sdf, (team,'SHOT','MISS',playername))
    cols = cur.execute(sdf,(team,'SHOT','MISS',playername))
    df = pd.DataFrame(cur.fetchall(), columns = [desc[0] for desc in cur.description])
    return df

#team_shot_without_player('October1923', 'COL', 'CALE MAKAR', homelis[0]).to_csv('no_makar_oct19.csv')



'''
#homelis = ['home_team', 'away_team']
def get_schedule_team(team, home):
    lis = []
    query = select distinct {home}, "game_id" from {table};
    sql.SQL(query).format(table=sql.Identifier(date),
    for x in get_current_database():

def sql_shot_data_by_player(team, ID):
'''
#= sql select * from "December0723"
#where "event_type" in ('SHOT' , 'MISS' , 'GOAL')
#and "game_id" = '2023020402'
#and "game_period" in ('1', '3');
'''    
    
#shot_data_by_player_id('October1923', 'COL', '8480069', homelis[0]).to_csv('makar_data.csv')

def all_blocked_shots(date,game):
    event_player_2 = 'the blocker'
    event_player_1 = 'the shooter'
    return 'tbd'
'''
#print(dates_by_team('COL'))
#testlis = ['2023-10-11', '2023-10-14', '2023-10-17', '2023-10-19', '2023-10-21', '2023-10-24', '2023-10-26', '2023-10-29', '2023-11-01', '2023-11-04', '2023-11-07', '2023-11-09', '2023-11-11', '2023-11-13', '2023-11-15', '2023-11-18', '2023-11-20', '2023-11-22', '2023-11-24', '2023-11-25', '2023-11-27', '2023-11-30', '2023-12-02', '2023-12-03', '2023-12-05', '2023-12-07', '2023-12-09', '2023-12-11', '2023-12-13', '2023-12-16', '2023-12-17', '2023-12-19', '2023-12-21', '2023-12-23', '2023-12-27', '2023-12-29', '2023-12-31', '2024-01-02', '2024-01-04', '2024-01-06', '2024-01-08', '2024-01-10', '2024-01-13', '2024-01-15', '2024-01-16', '2024-01-18', '2024-01-20', '2024-01-24', '2024-01-26', '2024-02-05', '2024-02-06', '2024-02-08', '2024-02-10', '2024-02-13', '2024-02-15', '2024-02-18', '2024-02-20', '2024-02-22', '2024-02-24', '2024-02-27', '2024-02-29', '2024-03-02', '2024-03-04', '2024-03-06', '2024-03-08', '2024-03-12', '2024-03-13', '2024-03-16', '2024-03-19', '2024-03-22', '2024-03-24', '2024-03-26', '2024-03-28', '2024-03-30', '2024-04-01', '2024-04-04', '2024-04-05', '2024-04-07', '2024-04-09', '2024-04-13', '2024-04-14', '2024-04-18']

#datelis = []
#for t in testlis:
    #tr=datetime.strptime(t, '%Y-%m-%d').date()
    #datelis.append(tr)
#print(datelis)

#cur.fetchall('''select "date" from "fullschedule" where "home_team" = %s or "away_team" = %s;''', ('COL','COL'))
   
#dates = pd.read_sql("fullschedule", engine)["date"].tolist()
#to_yesterday = '''select * from "fullschedule" where date < ''' + "'" + str(date.today()) + "';"
#df = pd.read_sql_query(to_yesterday, engine)[["ID", "away_team", "home_team", "date"]]
#print(df)#[["away_team", "home_team"]])
#dates = df["date"].tolist()


#JUST PLAYERID, NO GOALIES
def get_playerlist(team):
    rosters = []
    playerType = ["forwards", "defensemen"]
    data = use_api('https://api-web.nhle.com/v1/roster/'+ team + '/20232024')
    for index in range(len(playerType)):
        playerAttrs = data[playerType[index]]
        #get_line_stats_sva for forwards
        #get_d_zone_pair stats for defensemen
        #get goalie stats for goalies
        for index in range(len(playerAttrs)):
            rosters.append((playerAttrs[index]["id"]))
    return rosters



'''   
to_date = []
for date in dates:
    if date == str(today):
        break
    else:
        to_date.append(date)
set_date = set(to_date)

for date in set_date:
    #dater = datetime
    #date = string
    dater = datetime.strptime(date, '%Y-%m-%d').date()
    df2 = tdhnhlscrape.full_scrape(get_games(date), shift = True)
    df2.to_sql(dater.strftime('%B' '%d' '%y'), engine)
    df2.to_csv(os.getcwd() + "/gamedata_csv/" + dater.strftime('%B' '%d' '%y') + ".csv")
'''
#tdhnhlscrape.scrape_full_schedule().to_sql('fullschedule', engine)
#df.to_csv('pbpbp.csv')
#path = 
#newPath = path.replace(os.sep, '/')
#print(newPath) 
#sys.path.append('C:/Users/pgrts/AppData/Local/Packages/PythonSoftwareFoundation.Python.3.12_qbz5n2kfra8p0/LocalCache/local-packages/Python312/site-packages')
#yesterday_game = use_api('https://api-web.nhle.com/v1/score/' + str(date.today() - timedelta(days = 1)))["games"]
#PKdf = df.where(df["game_strength_state"] == "4v5")
#PPdf = df.where(df["game_strength_state"] == "5v4")
#PK2df = df.where(df["game_strength_state"] == "3v5")
#PP2df = df.where(df["game_strength_state"] == "5v3")
#extra = df.where(df["game_strength_state"].starts
#print(df.where(df["game_strength_state"] == '5v5'))
#print(df.info())
#df.sort_values("game_strength_state", inplace=True)
'''
#EV_filter = (df["game_strength_state"] == '5v5')
for p in df["game_strength_state"]:
    if p == '5v5':
        print('EVEN ' + p)
    if p == '4v5':
        print('4v5 ' + p)
'''


#upByOne_filter = df["game_score_state"] == '0v0' or '1v0' or '2v1' '3v2' or '4v3' or '5v4' or '6v5'

