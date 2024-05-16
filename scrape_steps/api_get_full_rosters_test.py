import requests
import json
import csv
import pandas as pd
import os

nhl_team_array = ["ANA", "ARI", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ",
    "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", "NYI", "NYR",
    "OTT", "PHI", "PIT", "SJS", "SEA", "STL", "TBL", "TOR", "VAN", "VGK", "WPG", "WSH"]

nhl_team_array2 = ["ANA", "ARI"]

glossary = "https://api.nhle.com/stats/rest/en/glossary"
game_id = "gameId=2021020001"
shift_chart = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=" + game_id
idk_stats = "https://api.nhle.com/stats/rest/en/skater/puckPossessions?cayenneExp=seasonId=20232024&limit=-1&start=0"
idk_stats_2022 = "https://api.nhle.com/stats/rest/en/skater/puckPossessions?cayenneExp=seasonId=20222023&limit=-1&start=0"
player_stats = "https://api-web.nhle.com/v1/skater-stats-leaders/20232024/2?categories=" + stat_category + "&limit=-1"
#csv_filename = "C:/Users/pgrts/Desktop/python/scrape_steps/" + team + "full_team_rosters.csv"
the_table = "full_team_rosters"

#with open(csv_filename, "w") as my_file:
    #my_file.write(header)
    #my_file.write("\n")
for team in nhl_team_array2:
    API_players = "https://api-web.nhle.com/v1/club-stats/" + team + "/20232024/2"
    response = requests.get(API_players)
    data = response.json()
    
    df = pd.DataFrame(data["skaters"])
    print(df)
'''
    for x in data["skaters"]:
        my_file.write((str(x["playerId"])))
        my_file.write(",")
        my_file.write(team)
        my_file.write(",")
        my_file.write(str(x["firstName"]["default"]))
        my_file.write(",")            
        my_file.write(str(x["lastName"]["default"]))
        my_file.write(",")
        my_file.write(str(x["positionCode"]))
        my_file.write(",")
        my_file.write(str(x["gamesPlayed"]))
        my_file.write(",")
        my_file.write(str(x["goals"]))
        my_file.write(",")
        my_file.write(str(x["assists"]))
        my_file.write(",")
        my_file.write(str(x["points"]))
        my_file.write(",")
        my_file.write(str(x["plusMinus"]))
        my_file.write("\n")
'''

    

