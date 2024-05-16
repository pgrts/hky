import requests
from bs4 import BeautifulSoup
import pandas as pd
from html.parser import HTMLParser
import sqlalchemy as db
import sqlalchemy.orm
import psycopg2
from sqlalchemy import URL, create_engine, select
from psycopg2 import sql
import psycopg2.extras as extras

#w1, 50toi
sva_url = \
"https://www.naturalstattrick.com/playerteams.php?fromseason=20232024&thruseason=20232024&stype=2&sit=sva&score=w1&stdoi=oi&rate=r&team=ALL&pos=S&loc=B&toi=50&gpfilt=none&fd=&td=&tgp=410&lines=single&draftteam=ALL"
#all scores, 10toi
pk_url = \
"https://www.naturalstattrick.com/playerteams.php?fromseason=20232024&thruseason=20232024&stype=2&sit=4v5&score=all&stdoi=oi&rate=r&team=ALL&pos=S&loc=B&toi=10&gpfilt=none&fd=&td=&tgp=410&lines=single&draftteam=ALL"
#all scores, 10toi
pp_url = \
"https://www.naturalstattrick.com/playerteams.php?fromseason=20232024&thruseason=20232024&stype=2&sit=5v4&score=all&stdoi=oi&rate=r&team=ALL&pos=S&loc=B&toi=10&gpfilt=none&fd=&td=&tgp=410&lines=single&draftteam=ALL"

sva_filename = "sva_relative_all_players.csv"
sva_table = "sva_relative_all_players"

pk_filename = "pk_relative_all_players.csv"
pk_table = "pk_relative_all_players"

pp_filename = "pp_relative_all_players.csv"
pp_table = "pp_relative_all_players"

url_hky = URL.create(
    "postgresql+psycopg2",
    username="postgres",
    password="p33Gritz!!",  
    host="localhost",
    port="5432",
    database="hockey",
)
try:
    engine = db.create_engine(url_hky)
except:
    print("I am unable to connect to the engine")

try:
    conn = psycopg2.connect(
        host="localhost",
        database="hockey",
        user="postgres",
        password="p33Gritz!!",
        port="5432"
    )
except:
    print("I am unable to connect to the psycopg2")

df = pd.read_html(sva_url, header=0, index_col = 0, na_values=[""])[0]
df.to_csv(sva_filename)
df.to_sql(sva_table, engine, if_exists='replace')

df2 = pd.read_html(pk_url, header=0, index_col = 0, na_values=[""])[0]
df2.to_csv(pk_filename)
df2.to_sql(pk_table, engine, if_exists='replace')

df3 = pd.read_html(pp_url, header=0, index_col = 0, na_values=[""])[0]
df3.to_csv(pp_filename)
df3.to_sql(pp_table, engine, if_exists='replace')
