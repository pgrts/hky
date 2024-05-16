import requests
import json
import csv
import pandas as pd
import os

stat_category = "assists"

player_stats_array = ["playerId", "firstName", "lastName", "positionCode", "gamesPlayed", "goals",
    "assists", "points", "plusMinus"]
#firstName[default]

header = "playerId,firstName,lastName,positionCode,gamesPlayed,goals,assists,points,plusMinus"
#for x in player_stats_array:
    #header += x + ","

nhl_team_array = ["ANA", "ARI", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ",
    "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", "NYI", "NYR",
    "OTT", "PHI", "PIT", "SJS", "SEA", "STL", "TBL", "TOR", "VAN", "VGK", "WPG", "WSH"]

nhl_team_array2 = ["ANA"]

glossary = "https://api.nhle.com/stats/rest/en/glossary"
game_id = "gameId=2021020001"
shift_chart = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=" + game_id
idk_stats = "https://api.nhle.com/stats/rest/en/skater/puckPossessions?cayenneExp=seasonId=20232024&limit=-1&start=0"
idk_stats_2022 = "https://api.nhle.com/stats/rest/en/skater/puckPossessions?cayenneExp=seasonId=20222023&limit=-1&start=0"
player_stats = "https://api-web.nhle.com/v1/skater-stats-leaders/20232024/2?categories=" + stat_category + "&limit=-1"

'''
response=requests.get(player_stats)
API_Data=response.json()

for key in API_Data:{
    print(key,":", API_Data[key])
}
'''
def get_all_keys(d):
    for key1, value1 in d.items():
        if isinstance(value1, dict):
            for key2, value2 in value1.items():
                print(key2, ":", value2)
        else:
            print(key1, ":", value1)

for team in nhl_team_array:
    csv_filename = "C:/Users/pgrts/Desktop/python/scrape_steps/player_stats_by_team/" + team + "playerstats.csv"
    API_players = "https://api-web.nhle.com/v1/club-stats/" + team + "/20232024/2"
    response = requests.get(API_players)
    data = response.json()
    with open(csv_filename, "w") as my_file:
        my_file.write(header)
        my_file.write("\n")
        for x in data["skaters"]:
            my_file.write((str(x["playerId"])))
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

        
        
    def jprint(obj):
    # create a formatted string of the Python JSON object
        text = json.dumps(df, sort_keys=True, indent=4)
        df.to_csv(csv_filename, encoding='utf-8', index=False)



    df = pd.DataFrame.from_dict(data, orient='index')    
    text = json.dumps(data, sort_keys=True, indent=4)
    print(text)
    df.to_csv(csv_filename, encoding='utf-8', index=False)


def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)

jprint(response.json())
def flatten_json(nested_json, exclude=['']):
    """Flatten json object with nested keys into a single level.
        Args:
            nested_json: A nested json object.
            exclude: Keys to exclude from output.
        Returns:
            The flattened json object if successful, None otherwise.
    """
    out = {}

    def flatten(x, name='', exclude=exclude):
        if type(x) is dict:
            for a in x:
                if a not in exclude: flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out
'''

