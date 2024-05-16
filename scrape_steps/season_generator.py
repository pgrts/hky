current_season_start = 2023
num_seasons = 24
seasons = [""] * num_seasons
start = current_season_start - num_seasons + 1 #2000

while start < current_season_start:
    for i in range(num_seasons):
        seasons[i] = str(start) + str(start+1)
        start += 1
print(seasons[num_seasons-1])

