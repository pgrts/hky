import os
import requests
import psycopg2
from bs4 import BeautifulSoup
from html.parser import HTMLParser
import sqlalchemy as db
import sqlalchemy.orm
from sqlalchemy import URL, create_engine, select
from psycopg2 import sql
import psycopg2.extras as extras

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
team_array = ["ANA", "ARI", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ", "DAL", "DET",
              "EDM", "FLA", "L.A", "MIN", "MTL", "NSH", "N.J", "NYI", "NYR", "OTT",
              "PHI", "PIT", "S.J", "SEA", "STL", "T.B", "TOR", "VAN", "VGK", "WPG", "WSH"]

team_lowercase = team.lower()
csv_filename = team_lowercase  + '_line_stats_sva.csv'


select "TOI", "TOI/GP", "Team",  "Player","xGF/60 Rel", "xGA/60 Rel" ,"HDCF/60 Rel", "xGF% Rel",  "xGA/60 Rel"  from "pp_relative_all_players"
where "TOI" > '50' AND "TOI/GP" > '2'
order by "xGF% Rel" desc;
