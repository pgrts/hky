from datetime import date, timedelta
from bs4 import BeautifulSoup
import pandas as pd
from html.parser import HTMLParser
import sqlalchemy as db
import sqlalchemy.orm
import psycopg2
from sqlalchemy import URL, create_engine, select
from psycopg2 import sql
import hockey_scraper
 
#yesterday = str(date.today() - timedelta(days = 1))

#sched_df = hockey_scraper.scrape_schedule("2023-10-01", "2024-06-01")
#sched_df.to_csv('sdfs.csv')

url_hky = URL.create(
    "postgresql+psycopg2",
    username="postgres",
    password="p33Gritz!!",  
    host="localhost",
    port="5432",
    database="playbyplay2",
)
try:
    engine = db.create_engine(url_hky)
except:
    print("I am unable to connect to the engine")

try:
    conn = psycopg2.connect(
        host="localhost",
        database="playbyplay2",
        user="postgres",
        password="p33Gritz!!",
        port="5432"
    )
except:
    print("I am unable to connect to the psycopg2")

#hockey_scraper.scrape_date_range('2024-03-11', '2024-03-12', True, data_format='Pandas')
#pd.read_csv('nhl_pbp_2024-03-12--2024-03-13.csv').to_sql('test', engine)
hockey_scraper.scrape_date_range('2023-10-10', '2024-03-12', True, data_format='Pandas').to_sql('seasondata', engine)
