import requests
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup as soup
from bs4 import SoupStrainer as strainer
from html.parser import HTMLParser
from urllib.parse import urlparse
import pandas as pd
from io import StringIO

'''
index = 0

the_url = []
links = []
html_tables = []

messages = [{'title': the_url[index],
             'tables': html_tables[index],
             'links' : links[index]}
            ]
'''
URL = input("Enter URL:")
o = urlparse(URL)
session = requests.Session()
response = session.get(URL, headers={'User-Agent': 'Mozilla/5.0'})

page_soup = soup(response.content, 'html.parser')

links = []
self_ref_links = []
tables = []

#for table in page_soup.find_all("
#df2 = pd.read_html(URL, header=0, index_col = 0, na_values=["-"])[0]
#df2.head()

'''
for link in page_soup.find_all('a'):
    link_text = link.get('href')
    if '#' in link_text:
        self_ref_links.append(link_text)
    if link_text.startswith('https'):
        links.append(link_text)
'''

num_tables = 0
for table in page_soup.find_all("table"):
    num_tables += 1
    tables.append(table)
#for i in range(num_tables):
        
print(tables)
'''
#print(links)
#print(self_ref_links)



@app.route('/decide/', methods=('GET', 'POST'))
def preview_table(table):
    df = pd.read_html(StringIO(str(table)), header=0, index_col = 0, na_values=[""])[0]
    df.to_h

    return render_template('decide.html')

for table in page_soup.find_all("table"):
    preview_table(table)


    

if response.ok:
    messages.append({'title': URL, 'content': soup})
    print(messages)
'''
