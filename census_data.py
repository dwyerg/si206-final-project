from bs4 import BeautifulSoup
import requests
import re
import os
import sqlite3

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def setStatesTable(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS States (id INTEGER PRIMARY KEY, state TEXT)")
    conn.commit()

def setCensusTable(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Census (white PERCENT, black PERCENT, native PERCENT, asian PERCENT, islander PERCENT, multiracial PERCENT, hispanic PERCENT, female PERCENT)")
    conn.commit()


"""scrapes a website to get data for a dictionary of the states
to get a number id for each state in alphabetical order
https://www.owogram.com/us-states-alphabetical-order/"""
def numbered_states():
    url = 'https://www.owogram.com/us-states-alphabetical-order/'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    tags = soup.find_all('h3', class_="ftwp-heading")

    states = {}
    
    for tag in tags:
        header = tag.text.split('.')
        id = header[0]
        #end after all 50 are collected
        if id == 'US States and Capitals Chart':
            break
        state = header[1].split()[-1][1:3]
        
        #website messed up for wisconsin and tennessee.
        if state == 'is':
            state = 'WI'
        if state == 'en':
            state = 'TN'
        
        states[state] = id
        
    return states

"""
function to collect data into a dictionary from quick facts page of census for each state
in list of tuples
[(DEMOGRAPHIC, PERCENT), (DEMOGRAPHIC, PERCENT), (DEMOGRAPHIC, PERCENT), (DEMOGRAPHIC, PERCENT)]
https://www.census.gov/quickfacts/fact/table/[STATE]/PST045219
"""
def state_data_from_search(state):
    url = f'https://www.census.gov/quickfacts/fact/table/{state}/PST045219'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')

    table = soup.find('div', class_='qf-facttable')
    race_section = table.find("tbody", {"data-topic" : "Race and Hispanic Origin"})
    rows = race_section.find_all('tr')
    race_data = []

    for row in rows[2:]:
        info = row.find_all('td')
        
        for i in range(len(info)):

            #check for positive index for title name
            if (i % 2 == 0):
                title = info[i].find('span').text.split(',')[0]

            #check for negative index for percent value
            else:
                percent = info[i].text.split('\n')[-1]
                race_data.append((title,percent))
   
    return race_data


states_dict = numbered_states()
states = list(states_dict.keys())

print(state_data_from_search(states[0]))
print('\n')
print(state_data_from_search('AL'))