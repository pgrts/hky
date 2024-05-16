import requests
import pandas as pd
import html.parser
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

from bs4 import BeautifulSoup
import urllib.request
from html.parser import HTMLParser

team = 'BOS'
team_lowercase = 'bos'
filename = 'soupy_' + team_lowercase + '.html'
csv = team_lowercase + '_line_stats_sva.csv'

headers = {
'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.79 Safari/537.36'
}

link  = 'https://www.naturalstattrick.com/teamreport.php?team=' + team


driver = webdriver.Chrome()
driver.maximize_window()
driver.get(link)
SVA_button = driver.find_element(By.ID,"rd" + team + "flsva")
tables = driver.find_element(By.ID,team + "fllb")

driver.implicitly_wait(5)

driver.execute_script("arguments[0].click();", SVA_button)

driver.implicitly_wait(5)
page = requests.get(link)
soup = BeautifulSoup(page.content, 'lxml')
table1 = soup.find('table',id='tb' + team + 'flsva')

with open(filename,"w",encoding='utf-8') as file:
    file.write(str(table1))
    
driver.close()
