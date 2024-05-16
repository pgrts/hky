from bs4 import BeautifulSoup as bs
import time
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

nhl_teams = ["ANA", "ARI", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ", "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", "NYI", "NYR", "OTT", "PHI", "PIT", "SJS", "SEA", "STL", "TBL", "TOR", "VAN", "VGK", "WPG", "WSH"]

#nhl_tablenames = ['only_ANA_shots_home', 'only_ANA_shots_away', 'only_ARI_shots_home', 'only_ARI_shots_away', 'only_BOS_shots_home', 'only_BOS_shots_away', 'only_BUF_shots_home', 'only_BUF_shots_away', 'only_CGY_shots_home', 'only_CGY_shots_away', 'only_CAR_shots_home', 'only_CAR_shots_away', 'only_CHI_shots_home', 'only_CHI_shots_away', 'only_COL_shots_home', 'only_COL_shots_away', 'only_CBJ_shots_home', 'only_CBJ_shots_away', 'only_DAL_shots_home', 'only_DAL_shots_away', 'only_DET_shots_home', 'only_DET_shots_away', 'only_EDM_shots_home', 'only_EDM_shots_away', 'only_FLA_shots_home', 'only_FLA_shots_away', 'only_LAK_shots_home', 'only_LAK_shots_away', 'only_MIN_shots_home', 'only_MIN_shots_away', 'only_MTL_shots_home', 'only_MTL_shots_away', 'only_NSH_shots_home', 'only_NSH_shots_away', 'only_NJD_shots_home', 'only_NJD_shots_away', 'only_NYI_shots_home', 'only_NYI_shots_away', 'only_NYR_shots_home', 'only_NYR_shots_away', 'only_OTT_shots_home', 'only_OTT_shots_away', 'only_PHI_shots_home', 'only_PHI_shots_away', 'only_PIT_shots_home', 'only_PIT_shots_away', 'only_SJS_shots_home', 'only_SJS_shots_away', 'only_SEA_shots_home', 'only_SEA_shots_away', 'only_STL_shots_home', 'only_STL_shots_away', 'only_TBL_shots_home', 'only_TBL_shots_away', 'only_TOR_shots_home', 'only_TOR_shots_away', 'only_VAN_shots_home', 'only_VAN_shots_away', 'only_VGK_shots_home', 'only_VGK_shots_away', 'only_WPG_shots_home', 'only_WPG_shots_away', 'only_WSH_shots_home', 'only_WSH_shots_away']

def use_api(url):
    #url = string_1 + value + string_2
    return requests.get(url).json()

def get_games(date):   
    game_list = use_api('https://api-web.nhle.com/v1/score/' + date)["games"]
    return [d['id'] for d in game_list]


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
        print('ERROR: NO GAMES FOUND')
        return 'NOTHING'
    query += ''' order by "new_date" ASC;'''  
    return pd.read_sql(query,engine,params={'xyz' : team})


#ONLY PLAYED GAMES! WOOPIE
def games_played(team):
    return team_df_sched(team,home=True,away=True)["ID"].tolist()

#ONLY PLAYED GAMES! WOOPIE
def home_games_played(team):
    return team_df_sched(team,home=True,away=False)["ID"].tolist()

#print(home_games_played('COL'))

#ONLY PLAYED GAMES! WOOPIE
def away_games_played(team):
    return team_df_sched(team,home=False,away=True)["ID"].tolist()

specials = [2023020251,2023020129,2023020267,2023020254,2023020242,2023020863,2023020859,2023020573]

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
    delim = "|" 
    while x < 454:
        dastring = str(li[x]).replace("(A)","").replace("(C)","").strip()
        if '<' in dastring:
            dastring = 'NULL'
        if 'KANPÄ' in dastring:
            dastring = 'JANI HAKANPAA'
        if 'STÜTZLE' in dastring:
            dastring = 'TIM STUTZLE'
        if 'YLÖNEN' in dastring:
            dastring = 'JESSE YLONEN'
        awaylis.append(dastring)
        x+=11
    while y < 689:
        dastring = str(li[y]).replace("(A)","").replace("(C)","").strip()
        if '<' in dastring:
            dastring = 'NULL'
        if 'KANPÄ' in dastring:
            dastring = 'JANI HAKANPAA'
        if 'STÜTZLE' in dastring:
            dastring = 'TIM STUTZLE'
        if 'YLÖNEN' in dastring:
            dastring = 'JESSE YLONEN'
        homelis.append(dastring)
        y+=11
    return {'home_team' : reduce(lambda x, y: str(x) + delim + str(y), homelis),
            'away_team' : reduce(lambda x, y: str(x) + delim + str(y), awaylis)}

def special_gameday(game_id):
    url = 'https://www.nhl.com/scores/htmlreports/20232024/RO' + game_id[4:] + '.HTM'
    header = {
      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
      "X-Requested-With": "XMLHttpRequest"
    }
    r = requests.get(url, headers=header)
    soup = bs(r.content, 'lxml')
    #print(soup)
    li = soup.prettify().split('\n')
    #print(li)
    homelis = []
    awaylis = []
    x=235
    y=470
    delim='|'
    while x < 455:
        dastring = str(li[x]).replace("(A)","").replace("(C)","").strip()
        if '<' in dastring:
            dastring = 'NULL'
        if 'KANPÄ' in dastring:
            dastring = 'JANI HAKANPAA'
        if 'STÜTZLE' in dastring:
            dastring = 'TIM STUTZLE'
        if 'YLÖNEN' in dastring:
            dastring = 'JESSE YLONEN'
        awaylis.append(dastring)
        x+=11
    while y < 690:
        dastring = str(li[y]).replace("(A)","").replace("(C)","").strip()
        if '<' in dastring:
            dastring = 'NULL'
        if 'KANPÄ' in dastring:
            dastring = 'JANI HAKANPAA'
        if 'STÜTZLE' in dastring:
            dastring = 'TIM STUTZLE'
        if 'YLÖNEN' in dastring:
            dastring = 'JESSE YLONEN'
        homelis.append(dastring)
        y+=11
    return {'home_team' : reduce(lambda x, y: str(x) + delim + str(y), homelis),
            'away_team' : reduce(lambda x, y: str(x) + delim + str(y), awaylis)}

#print(get_roster_by_game('2023020614'))
#print(get_roster_by_game('2023020009'))
#print(os.getcwd())

def create_gameday_rosters():
    for team in nhl_teams:
        with open(team + '_gameday_rosters.csv', 'a') as file:
            file.write('game_id,roster,game_type' + '\n')
            for each in home_games_played(team):
                if each in specials:
                    lis = special_gameday(str(each))['home_team']
                    #print('special home')
                else:
                    lis = get_roster_by_game(str(each))["home_team"]
                    #print(lis)
                #res = reduce(lambda x, y: str(x) + delim + str(y), lis)
                file.write(str(each) + ',"' + lis + '",home' + '\n')

            for each in away_games_played(team):
                if each in specials:
                    lis = special_gameday(str(each))['away_team']
                    #print('special away')
                else:                
                    lis = get_roster_by_game(str(each))["away_team"]
                    #print(lis)
                #res = reduce(lambda x, y: str(x) + delim + str(y), lis)
                file.write(str(each) + ',"' + lis + '",away' + '\n')
            #pd.read_csv(file).to_sql(team + '_gameday_rosters', engine)
            print(team + ' success!')           
            file.close()
            
def rosters_sql(team):
    folder = 'C:/Users/pgrts/Desktop/python/scrape_steps/' + team + '_gameday_rosters.csv'
    pd.read_csv(folder,encoding='cp1252').to_sql(team + '_gameday_rosters', engine, if_exists='replace')
    return print(team + '_gameday_rosters')

def get_every_player(team):
    ls = pd.read_sql_table(team + '_gameday_rosters', engine)['roster'].tolist()
    bigls = []
    for each in ls:
        eachls = each.split("|")
        bigls.extend(eachls)
    returnlis = list(dict.fromkeys(bigls).keys())
    if 'NULL' in returnlis:
        returnlis.remove('NULL')
    if 'roster' in returnlis:
        returnlis.remove('roster')
    return returnlis

#print(get_every_player('ANA'))

def roster_df(team):
    playerType = ["forwards", "defensemen", "goalies"]
    data = use_api('https://api-web.nhle.com/v1/roster/'+ team + '/20232024')
    cols = ['playerID','firstName','lastName','number','mugshot','pos','height','weight','url']
    
    id_lis = []
    first_lis = []
    last_lis = []
    mug_lis = []
    url_lis = []
    pos_lis = []
    height_lis = []
    weight_lis = []
    num_lis = []
    
    for each in playerType:
        for ele in data[each]:          
            id_lis.append(ele['id'])
            url_lis.append('https://www.nhl.com/player/' + str(ele['id']))
            pos_lis.append(ele['positionCode'])
            first_lis.append(ele['firstName']['default'])
            last_lis.append(ele['lastName']['default'])
            mug_lis.append(ele['headshot'])
            height_lis.append(ele['heightInInches'])
            weight_lis.append(ele['weightInPounds'])
            try:
                num_lis.append(ele['sweaterNumber'])
            except:
                num_lis.append('NULL')
            
    return pd.DataFrame(list(zip(id_lis,first_lis,last_lis,num_lis,mug_lis,pos_lis,height_lis,weight_lis,url_lis)),columns = cols)

#roster_df('CAR').to_csv('asdfsd.csv')

#return ['BOWEN BYRAM', 'RYAN JOHANSEN', 'FREDRIK OLOFSSON', 'TOMAS TATAR', 'IVAN PROSVETOV', 'RILEY TUFTE', 'KURTIS MACDERMID'
def find_nonroster(team):
    everyplayer = get_every_player(team)
    y = roster_df(team)['firstName'].tolist()
    fn = [x.upper() for x in y]
    bls=[]
    for each in everyplayer:
        newy = each.split(' ')
        if newy[0] not in fn:
            bls.append(each)
    return(bls)


print(find_nonroster('CAR'))

def get_urls_of_nonroster(lis):
    try:
        from googlesearch import search
    except ImportError: 
        print("No module named 'google' found")
    urls = []
    for x in lis:
        for j in search(x + ' nhl.com'):
            urls.append(j)
            break
        time.sleep(3)
    return urls

#print(get_urls_of_nonroster(['ANDREW PEEKE', 'PATRIK LAINE', 'ERIC ROBINSON', 'EMIL BEMSTROM', 'JACK ROSLOVIC', 'SPENCER MARTIN', 'LIAM FOUDY', 'DAVID JIRICEK', 'JET GREAVES', 'TREY FIX-WOLANSKY', 'NICK BLANKENBURG', 'ALEX NYLANDER']))

#for team in nhl_teams:
#for each in ['JUSTIN SOURDIF', 'UVIS BALINSKIS', 'MACKIE SAMOSKEVICH', 'WILLIAM LOCKWOOD', 'MIKE REILLY']:
    #print(get_urls_of_nonroster([each]))
#print(get_urls_of_nonroster(find_nonroster('FLA')))
    
#ls = ['BOWEN BYRAM', 'RYAN JOHANSEN', 'FREDRIK OLOFSSON', 'TOMAS TATAR', 'IVAN PROSVETOV', 'RILEY TUFTE', 'KURTIS MACDERMID', 'ONDREJ PAVEL', 'OSKAR OLAUSSON', 'SAM MALINSKI', 'BEN MEYERS', 'JASON POLIN', 'CHRIS WAGNER', 'JEAN-LUC FOUDY']
#url = ['https://www.nhl.com/sabres/player/bowen-byram-8481524', 'https://www.nhl.com/flyers/player/ryan-johansen-8475793', 'https://www.nhl.com/avalanche/player/fredrik-olofsson-8478028', 'https://www.nhl.com/kraken/player/tomas-tatar-8475193', 'https://www.nhl.com/avalanche/player/ivan-prosvetov-8481031', 'https://www.nhl.com/avalanche/player/riley-tufte-8479362', 'https://www.nhl.com/devils/player/kurtis-macdermid-8477073', 'https://www.nhl.com/avalanche/player/ondrej-pavel-8484259', 'https://www.nhl.com/avalanche/player/oskar-olausson-8482712', 'https://www.nhl.com/avalanche/player/sam-malinski-8484258', 'https://www.nhl.com/ducks/player/ben-meyers-8483570', 'https://www.nhl.com/avalanche/player/jason-polin-8484255', 'https://www.nhl.com/avalanche/player/chris-wagner-8475780', 'https://www.nhl.com/avalanche/player/jean-luc-foudy-8482147']

def df_nonroster(team):
    
    ls = find_nonroster(team)
    urls = get_urls_of_nonroster(ls)
    
    id_lis = []
    first_lis = []
    last_lis = []
    mug_lis = []
    pos_lis = []

    heightlis = []
    weightlis = []
    num_lis = []
    for each in urls:
        atts = use_api('https://api-web.nhle.com/v1/player/' + each[-7:] + '/landing')
        id_lis.append(atts['playerID'])
        first_lis.append(atts['firstName']['default'])
        last_lis.append(atts['lastName']['default'])
        mug_lis.append(atts['headshot'])
        heightlis.append(atts['heightInInches'])
        weightlis.append(atts['weightInPounds'])
        pos_lis.append(atts['pos'])
        try:
            num_lis.append(atts['sweaterNumber'])
        except:
            num_lis.append('NULL')

    return pd.DataFrame({'name': ls, 'playerId': id_lis, 'position':pos_lis, 'height':heightlis,'weight':weightlis, 'mugshot':mug_lis,'firstName':first_lis,'lastName':last_lis,'url': urls})

#df_nonroster('CAR').to_csv('fsdf.csv')

def test_nonroster_allteams():
    for team in nhl_teams:
        df_nonroster(team).to_csv(team + '_nonroster.csv')

#test_nonroster_allteams()

def game_ids_by_player(team,playername):
    df = pd.read_sql_table(team + '_gameday_rosters', engine)
    df[df.apply(lambda r: r.str.contains(playername, case=True).any(), axis=1)].drop(columns=['index'])
    return df['game_id'].tolist()

#print(game_ids_by_player('COL','BOWEN BYRAM'))

'''
def nonroster_rename_sql():
    for team in nhl_teams:
        pd.read_csv(team + '_nonroster.csv').rename(columns={'position':'pos','playerId':'playerID'}).drop(columns=['Unnamed: 0']).to_sql(team + '_nonroster', engine,if_exists='replace')

'''
#nonroster_rename_sql()
    
def gen_roster_sql(team):
    cols = ['playerID','firstName','lastName','mugshot','pos']
    df1 = pd.read_csv(team + '_roster_2024-03-27.csv')[cols]
    #print(df1)
    df2 = pd.read_sql_table(team + '_nonroster', engine)[cols]
    #print(df2)
    return pd.concat([df1,df2])

def all_roster_sql():
    for team in nhl_teams:
        gen_roster_sql(team).to_sql(team + '_fullroster', engine, if_exists = 'replace')
        print(team + ' success!')

#all_roster_sql()

#gen_roster_sql('CAR')






    #print(flatten(ls))
#print(every_player_by_team('COL'))
#['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN',
#'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN',
#'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'ARTTURI LEHKONEN',
#'TOMAS TATAR', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV', 'RILEY TUFTE', 'KURTIS MACDERMID',
#'ONDREJ PAVEL', 'CALEB JONES', 'JOEL KIVIRANTA', 'OSKAR OLAUSSON', 'SAM MALINSKI',
#'BEN MEYERS', 'JASON POLIN', 'ZACH PARISE', 'CHRIS WAGNER', 'JUSTUS ANNUNEN', 'JEAN-LUC FOUDY',
#'BRANDON DUHAIME', 'YAKOV TRENIN', 'SEAN WALKER', 'CASEY MITTELSTADT']

#create_gameday_rosters()
#for team in nhl_teams:
 #   rosters_sql(team)
            
#x = team_df_sched('COL',home=True,away=False)["ID"].tolist()
#col_home = list(map(str,x))
#print(col_home)
#col_home = ['2023020061', '2023020076', '2023020143', '2023020188', '2023020201', '2023020219', '2023020240', '2023020291', '2023020313', '2023020324', '2023020386', '2023020402', '2023020417', '2023020431', '2023020445', '2023020477', '2023020507', '2023020525', '2023020571', '2023020582', '2023020606', '2023020624', '2023020637', '2023020742', '2023020757', '2023020864', '2023020881', '2023020908', '2023020936', '2023020979', '2023020991', '2023021006', '2023021111', '2023021125']

#def player_shots(playername,team,home):

def team_shots(team,home):
    #tablename = 'only_' + team + '_shots_' + home
    query = '''create table {tablename} as
    select * from "only_shots" where {homey} = %s;'''
    newQ = sql.SQL(query).format(homey=sql.Identifier(home),
        tablename=sql.Identifier(team + '_shots_' + home[:4]))
    cur.execute(newQ, (team,))
    print(newQ.as_string(conn))
    conn.commit()
    #results = [str(i[0]) for i in results]
    #return results

#team_shots('COL',"home_team")

#scraping function from big database, splitting shot partition by team
def create_team_shots():
    #lis = []
    for team in nhl_teams:
        for each in ["home_team","away_team"]:
           # lis.append(team + '_shots_' + each[:4])
   # print(lis)
    #return lis
            team_shots(team,each)
            
def del_tables():
    for each in nhl_tablenames:
        query = '''drop table if exists {table};'''
        newQ = sql.SQL(query).format(table=sql.Identifier(each))
        #print(query)
        cur.execute(newQ)
        conn.commit()
        
#create_team_shots()
