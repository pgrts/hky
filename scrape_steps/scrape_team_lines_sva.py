import pandas as pd
import requests
import psycopg2
from bs4 import BeautifulSoup
from html.parser import HTMLParser
import sqlalchemy as db
import sqlalchemy.orm
from sqlalchemy import URL, create_engine, select
from psycopg2 import sql
import psycopg2.extras as extras
from io import StringIO


#tables[19] = lines5v5
#sva = tables[20]

url_hky = URL.create(
    "postgresql+psycopg2",
    username="postgres",
    password="p33Gritz!!",  
    host="localhost",
    port="5432",
    database="hockey_lines",
)
try:
    engine = db.create_engine(url_hky)
except:
    print("I am unable to connect to the engine")

try:
    conn = psycopg2.connect(
        host="localhost",
        database="hockey_lines",
        user="postgres",
        password="p33Gritz!!",
        port="5432"
    )
except:
    print("I am unable to connect to the psycopg2")
    
team_array = ["ANA", "ARI"]


#filename = 'soupy_' + team + '.html'


for team in team_array:
    team_lowercase = team.lower()
    csv_filename = team_lowercase  + '_line_stats_sva.csv'
    the_table = team_lowercase + '_lines_sva'
    url = "https://www.naturalstattrick.com/teamreport.php?season=20232024&team=" + team + "&stype=2"


    req = requests.get(url)
    soup=BeautifulSoup(req.content, 'html.parser')
    tables = soup.find_all("table")[20]
    #print(tables)
    #print(str(tables[20]))
    df = pd.read_html(StringIO(str(tables)), header=0, index_col = 0, na_values=[""])[0]
    #df.to_csv(csv_filename)
    #df.to_sql(the_table, engine, if_exists='replace')


