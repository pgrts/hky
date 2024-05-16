import requests
import pandas as pd
import html.parser
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from bs4 import BeautifulSoup
import urllib.request
from html.parser import HTMLParser

headers = {
'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.79 Safari/537.36'
}

link  = 'https://www.naturalstattrick.com/playerteams.php?fromseason=20232024&thruseason=20232024&stype=2&sit=sva&score=w1&stdoi=oi&rate=r&team=ALL&pos=S&loc=B&toi=100&gpfilt=none&fd=&td=&tgp=410&lines=single&draftteam=ALL'


driver = webdriver.Chrome()
driver.maximize_window()
driver.get(link)
csv_button = driver.find_element(By.LINK_TEXT,"CSV (All)")

driver.implicitly_wait(5)

driver.execute_script("arguments[0].click();", csv_button)

driver.implicitly_wait(3)
    
driver.close()
