from bs4 import BeautifulSoup as soup
import requests
import json
import pandas as pd
import os
from datetime import date, timedelta
from io import StringIO

#take today's date
#generate game info from yesterday
#get a list of each game id
#next step: get_teams
def get_yesterdays_games():
    yesterday = date.today() - timedelta(days = 1)
    url = 'https://api-web.nhle.com/v1/score/' + str(yesterday)
    r = requests.get(url)
    data = r.json() #dict
    #list_of_games = json.dumps(data["games"])
    #print(type(data["games"])) #list
    #print(type(list_of_games)) #str
    #json_data = json.loads(games) #list
    #print(type(json_data))
    game_info = data["games"]
    list_of_games = []
    for index in range(len(game_info)):
        for key in game_info[index]:
            if key == "id":
                list_of_games.append(game_info[index][key])
    return list_of_games


#take list of games
#generate list of team abbrevs
#next step: get rosters
def get_teams(list_of_games):
    teams = []
    #list_of_games = [2023020876, 2023020877, 2023020878, 2023020879, 2023020880, 2023020881, 2023020882, 2023020883]
    for index in range(len(list_of_games)):
        #process_games(list_of_games[index])
        game_url = 'https://api-web.nhle.com/v1_1/gamecenter/' + str(list_of_games[index]) + '/landing'
        r1 = requests.get(game_url)
        data = r1.json()
        #print(type(data))
        teams.append(data["awayTeam"]["abbrev"])
        teams.append(data["homeTeam"]["abbrev"])
        #print(type(teams))
    return teams

#take team list
#generate player ids
#return every player id
def get_every_player(teams):
    #teams = ['OTT', 'FLA', 'DAL', 'NYR', 'NYI', 'PIT', 'NJD', 'WSH', 'MIN', 'WPG', 'VAN', 'COL', 'NSH', 'VGK', 'CBJ', 'LAK']
    rosters = []
    playerType = ["forwards", "defensemen", "goalies"]
    for index in range(len(teams)):
        roster_url = 'https://api-web.nhle.com/v1/roster/' + teams[index] + '/20232024'
        r = requests.get(roster_url)
        data = r.json()
        for index in range(len(playerType)):
            playerAttrs = data[playerType[index]]
            #print(playerAttrs)
            for index in range(len(playerAttrs)):
                each_player = playerAttrs[index]
                rosters.append((each_player["id"]))
    return rosters

gameList = get_yesterdays_games()
teamList = get_teams(gameList)
every_player = get_every_player(teamList)
print(every_player)

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
