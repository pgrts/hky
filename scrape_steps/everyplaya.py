from selenium import webdriver
import requests
from bs4 import BeautifulSoup
import sys 
import google
#sys.path.append('C://Users/pgrts/AppData/Local/Packages/PythonSoftwareFoundation.Python.3.12_qbz5n2kfra8p0/LocalCache/local-packages/Python312/site-packages/')
ls = ['JACK JOHNSON', 'BOWEN BYRAM', 'DEVON TOEWS', 'CALE MAKAR', 'ANDREW COGLIANO', 'RYAN JOHANSEN', 'VALERI NICHUSHKIN', 'ROSS COLTON', 'FREDRIK OLOFSSON', "LOGAN O'CONNOR", 'JONATHAN DROUIN', 'MILES WOOD', 'NATHAN MACKINNON', 'JOSH MANSON', 'SAMUEL GIRARD', 'ARTTURI LEHKONEN', 'TOMAS TATAR', 'MIKKO RANTANEN', 'ALEXANDAR GEORGIEV', 'IVAN PROSVETOV', 'RILEY TUFTE', 'KURTIS MACDERMID', 'ONDREJ PAVEL', 'CALEB JONES', 'JOEL KIVIRANTA', 'OSKAR OLAUSSON', 'SAM MALINSKI', 'BEN MEYERS', 'JASON POLIN', 'ZACH PARISE', 'CHRIS WAGNER', 'JUSTUS ANNUNEN', 'JEAN-LUC FOUDY', 'BRANDON DUHAIME', 'YAKOV TRENIN', 'SEAN WALKER', 'CASEY MITTELSTADT']

#rl = '''C:\Users\pgrts\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.12_qbz5n2kfra8p0\LocalCache\local-packages\Python312\site-packages\googlesearch'''
##url.replace('\\','/')
#print(url)


try:
    from googlesearch import search
except ImportError: 
    print("No module named 'google' found")
    
query = "jean luc foudy nhl"

for j in search(query):
    print(type(j))
    break
