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

gameid = '2023020900'
test = 'https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId=' + gameid + \
'%20and%20((duration%20!=%20%2700:00%27%20and%20typeCode%20=%20517)%20or%20typeCode%20!=%20517%20)&exclude=detailCode&exclude=duration&exclude=eventDetails&exclude=teamAbbrev&exclude=teamName'

def use_api(url):
    #url = string_1 + value + string_2
    r = requests.get(url)
    data = r.json()
    return data


#def shift_charts(): #(json):
data_games = use_api(test)["data"] #type: list of dicts
#print(data_games)
df = pd.DataFrame(data_games)
print(df)
#df.to_csv(etc..)
#df.to_sql(etc..)





'''
#row by row, dict
for index in range(len(data_games)):
    #print(type(data_games[index])) #dict
    for key in data_games[index]:
        print(key, "->", data_games[index][key])
        
        #print(key)
        #print(value)
#df = pd.DataFrame()
#print(type(data_games))

x=0
while x < 5:
    for ind in data_games:
        print(data_games[str(ind)])
        for key,value in data_games[str(ind)]:
            print(key)
            print(value)
        
        x += 1


#take today's date
#generate game info from yesterday
#get a list of each game id
#next step: get_teams
def get_yesterdays_games():
    yesterday = date.today() - timedelta(days = 1)
    data = use_api('https://api-web.nhle.com/v1/score/' + str(yesterday))
    game_list = data["games"]
    list_of_games = []
    for index in range(len(game_list)):
        for key in game_list[index]:
            if key == "id":
                list_of_games.append(game_list[index][key])
    return list_of_games
  
def scrape_line_stats_sva(team_array):
    for team in team_array:
    team_lowercase = team.lower()
    csv_filename = team_lowercase  + '_line_stats_sva.csv'
    the_table = team_lowercase + '_lines_sva'
    url = "https://www.naturalstattrick.com/teamreport.php?season=20232024&team=" + team + "&stype=2"


    req = requests.get(url)
    soup=BeautifulSoup(req.content, 'html.parser')
    tables = soup.find_all("table")

    df = pd.read_html(StringIO(str(tables[20])), header=0, index_col = 0, na_values=[""])[0]
    df.to_csv(csv_filename)
    df.to_sql(the_table, engine, if_exists='replace')

#take list of games
#generate list of team abbrevs
#next step: get rosters
def get_teams(list_of_games):
    teams = []
    #list_of_games = [2023020876, 2023020877, 2023020878, 2023020879, 2023020880, 2023020881, 2023020882, 2023020883]
    for index in range(len(list_of_games)):
        data = use_api('https://api-web.nhle.com/v1_1/gamecenter/'+ str(list_of_games[index]) + '/landing')
        #process_games(list_of_games[index])
        #game_url = 'https://api-web.nhle.com/v1_1/gamecenter/' + str(list_of_games[index]) + '/landing'
        #r1 = requests.get(game_url)
        #print(type(data))
        teams.append(data["awayTeam"]["abbrev"])
        teams.append(data["homeTeam"]["abbrev"])
        #print(type(teams))
    return teams

#each team
#generate roster player ids from last nite
def get_roster(team):
    rosters = []
    playerType = ["forwards", "defensemen", "goalies"]
    data = use_api('https://api-web.nhle.com/v1/roster/'+ teams[index]+ '/20232024')
    for index in range(len(playerType)):
        playerAttrs = data[playerType[index]]
        #get_line_stats_sva for forwards
        #get_d_zone_pair stats for defensemen
        #get goalie stats for goalies
        for index in range(len(playerAttrs)):
            rosters.append((playerAttrs[index]["id"]))
    return rosters

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
function2(get_every_player(get_teams(get_yesterdays_games())))
#function1(get_every_player(get_teams(get_yesterdays_games())))
#print(every_player)


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
