from bs4 import BeautifulSoup as bs
import requests
import json
import pandas as pd
import os
import sqlalchemy as db
import sqlalchemy.orm
import psycopg2
from sqlalchemy import URL, create_engine, select, text
from psycopg2 import sql
from datetime import date, datetime, timedelta
from io import StringIO
import matplotlib.pyplot as plt
import numpy as np
import time
from time import perf_counter
import math

nhl_dict = {24: 'ANA', 53: 'ARI', 6: 'BOS', 7: 'BUF', 20: 'CGY', 12: 'CAR', 16: 'CHI', 21: 'COL', 29: 'CBJ', 25: 'DAL', 17: 'DET', 22: 'EDM', 13: 'FLA', 26: 'LAK', 30: 'MIN',
8: 'MTL', 18: 'NSH', 1: 'NJD', 2: 'NYI', 3: 'NYR', 9: 'OTT', 4: 'PHI', 5: 'PIT', 28: 'SJS', 55: 'SEA', 19: 'STL', 14: 'TBL', 10: 'TOR', 23: 'VAN', 54: 'VGK', 52: 'WPG', 15: 'WSH'}

url_hky = URL.create(
    "postgresql+psycopg2",
    username="postgres",
    password="p33Gritz!!",  
    host="localhost",
    port="5432",
    database="pbp5",
)
try:
    engine = db.create_engine(url_hky)
except:
    print("I am unable to connect to the engine")

try:
    conn = psycopg2.connect(
        host="localhost",
        database="pbp5",
        user="postgres",
        password="p33Gritz!!",
        port="5432"
    )
except:
    print("I am unable to connect to the psycopg2")

cur=conn.cursor()



# DATAFRAME OF SHOTS
# X-AXIS: LIST OF PLAYERS BY SWEATER NUMBER [home_team]
# Y-AXIS: LIST OF PLAYERS BY SWEATER NUMBER [away_team]

