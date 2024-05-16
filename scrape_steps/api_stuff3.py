from bs4 import BeautifulSoup as soup
import requests
import json
import pandas as pd
import os
#import time
#from html.parser import HTMLParser
from datetime import date, timedelta
from io import StringIO
#from scrape_nst_from_playerid import function2#, function1
#from scrape_team_line_combos import sc

'''
nhl_team_array = ["ANA", "ARI", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ",
    "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", "NYI", "NYR",
    "OTT", "PHI", "PIT", "SJS", "SEA", "STL", "TBL", "TOR", "VAN", "VGK", "WPG", "WSH"]
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

def use_api(url):
    #url = string_1 + value + string_2
    data = requests.get(url).json()
    return data

def standings(team):
    x = use_api('https://api-web.nhle.com/v1/standings/' + str(date.today()))["standings"]
    #x                                                                       
    #team_dictionary = json.loads(x)
    #df = pd.DataFrame.from_dict(use_api(url) + str(date.today()["standings"]))
    #df.to_csv('test.csv')

#list of game ids
def get_yesterdays_games():
    game_list = use_api('https://api-web.nhle.com/v1/score/' + str(date.today() - timedelta(days = 1)))["games"] #list of dict
    #d['awayTeam']['default']
    #final_dic = {'id' : dic1}
    lis = []
    awaydf = pd.DataFrame(columns=['name','sog','score','logo'])
    homedf = pd.DataFrame(columns=['name','sog','score','logo'])
    for d in game_list:
        idlist = [ d['id']]
        awaylist.append( [ d['awayTeam']['abbrev'],d['awayTeam']['sog'],d['awayTeam']['score'] ] )
        homelist.append( [ d['homeTeam']['abbrev'],d['homeTeam']['sog'],d['homeTeam']['score'] ] )
        

    for d in game_list:
        #awaydf = pd.DataFrame(data=[
        lis.append(

            {d['id'] :
                     
                [
                    {'away' :
                        [
                            {'team' : d['awayTeam']['abbrev']},
                            {'sog' : d['awayTeam']['sog']},
                            {'score' : d['awayTeam']['score']}#,
                            #{'id' : d['awayTeam']['id']}
                        ]
                    }


                    ,
                    {'home' :
                        [
                            {'team' : d['homeTeam']['abbrev']},
                            {'sog' : d['homeTeam']['sog']},
                            {'score' : d['homeTeam']['score']}#,
                            #{'id' : d['homeTeam']['id']}
                        ]
                    }
                ]
            }

                    )

    #df = pd.DataFrame.from_dict(lis[0]), columns=['team','sog',)
    #lis.append( {'id' : d['id']} )
    #lis.append( {'away' : d['awayTeam']['abbrev']} )
    #lis.append( {'home' : d['homeTeam']['abbrev']} )
    #df = pd.DataFrame(data=lis, columns=['id','away','home'])
    #lis.append( d['id'] : {d["awayTeam"]["abbrev"]: d["homeTeam"]["abbrev"]})
    #dic_matchups = {d["awayTeam"]["abbrev"]: d["homeTeam"]["abbrev"] for d in game_list}
    #list_gameid = [d['id'] for d in game_list]
    
    return df #lis['away']

#print(get_yesterdays_games())

'''    
def scrape_line_stats_sva(team_array):
    for team in team_array:
    team_lowercase = team.lower()
    csv_filename = team_lowercase  + '_line_stats_sva.csv'
    the_table = team_lowercase + '_lines_sva'
    url = "https://www.naturalstattrick.com/teamreport.php?season=20232024&team=" + team + "&stype=2"

 for d in game_list:
        dic1 = ['id' : [d['id'] for d in game_list if 'id' in d]
        dic1 = {'away' : d['awayTeam']['abbrev']}
        
    req = requests.get(url)
    soup=BeautifulSoup(req.content, 'html.parser')
    tables = soup.find_all("table")

    df = pd.read_html(StringIO(str(tables[20])), header=0, index_col = 0, na_values=[""])[0]
    df.to_csv(csv_filename)
    df.to_sql(the_table, engine, if_exists='replace')
'''
'''
#take list of games
#generate list of team abbrevs
#next step: get rosters
def get_teams(list_gameid):
    teams = []
    #list_of_games = [2023020876, 2023020877, 2023020878, 2023020879, 2023020880, 2023020881, 2023020882, 2023020883]
    for each in list_gameid:
        data = use_api('https://api-web.nhle.com/v1_1/gamecenter/'+ each + '/landing')
        teams.append({'game_id' : each})
        teams.append({'away' : data["awayTeam"]["abbrev"]})
        teams.append({'home' : data["homeTeam"]["abbrev"]})
    return teams
'''
lis = ['2023021109', '2023021110', '2023021111', '2023021112']
#for each in lis:
#print(get_teams(lis))




def get_matchups(lis):
    matchups = []
    for each in lis:
        #game_id = str(list_of_games[index])
        data = use_api('https://api-web.nhle.com/v1_1/gamecenter/' + game_id + '/landing')
        #tempdict =  {'game_id' : game_id, 'away' : data["awayTeam"]["abbrev"], 'home' : data["homeTeam"]["abbrev"]}
        #matchup_dictionary.update(tempdict)
        
        get_roster(data["awayteam"]["abbrev"]        
        awaydict = {"away" : data["awayTeam"]["abbrev"], "awayRoster" = { "First Name" : "Jack", "Last Name" : "Hughes", "Position" : "C", "Player ID" : "824388",}
                                                                        
                                                                    
        matchups.append([{"game_id" : game_id, {"away" : data["awayTeam"]["abbrev"]}, {"home" : data["homeTeam"]["abbrev"]}}])
    return matchups


'''
#generate each team's roster's playerid
#team = index in team array
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
'''
#take team list
#generate player ids
#return every player id
def get_every_player(teams):
    #teams = ['OTT', 'FLA', 'DAL', 'NYR', 'NYI', 'PIT', 'NJD', 'WSH', 'MIN', 'WPG', 'VAN', 'COL', 'NSH', 'VGK', 'CBJ', 'LAK']
    rosters = []
    playerType = ["forwards", "defensemen", "goalies"]
    for index in range(len(teams)):
        data = use_api('https://api-web.nhle.com/v1/roster/'+ teams[index]+ '/20232024')
        for index in range(len(playerType)):
            playerAttrs = data[playerType[index]]
            #get_line_stats_sva for forwards
            #get_d_zone_pair stats for defensemen
            #get goalie stats for goalies
            for index in range(len(playerAttrs)):
                rosters.append((playerAttrs[index]["id"]))
    return rosters

#gameList = get_yesterdays_games()
#print(gameList)
#teamList = get_teams(get_yesterdays_games())
#print(teamList)
#print(get_every_player(get_teams(get_yesterdays_games())))
#function2(get_every_player(get_teams(get_yesterdays_games())))
#function1(get_every_player(get_teams(get_yesterdays_games())))
#print(every_player)
#print(get_matchups([2023020876, 2023020877]))

'''
for key, value in game_info:
        
    print(f"\nKey: {key}")
    print(f"Value: {value}\n")
    #for key = 

#df = pd.DataFrame(data["games"], columns = ["id","awayTeam","homeTeam"])
#df.to_csv("C:/Users/pgrts/Desktop/python/scrape_steps/test.csv")

game_id = '2023020879'
gamecenter_url = 'https://api-web.nhle.com/v1_1/gamecenter/' + \
game_id + '/landing'
game_id = '2023020883'
gamecenter_url = 'https://api-web.nhle.com/v1_1/gamecenter/' + game_id + '/landing'
r1 = requests.get(gamecenter_url)
data = r1.json()
teams = []
teams.append(data["awayTeam"]["abbrev"])
teams.append(data["homeTeam"]["abbrev"])
away_url = 'https://api-web.nhle.com/v1/roster/' + teams[0] + '/20232024'
away = requests.get(away_url)
away_roster = away.json()
home_url = 'https://api-web.nhle.com/v1/roster/' + teams[1] + '/20232024'
home = requests.get(home_url)
home_roster = home.json()


''' 
