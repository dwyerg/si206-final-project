from bs4 import BeautifulSoup
import requests
import re
import csv
import os
import sqlite3

"""set up database"""
def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn



"""sets up the table with the census data"""
def set_states_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS States (stateid INTEGER PRIMARY KEY, abbreviation TEXT, type TEXT)")
    conn.commit()
    
"""scrapes a website to get data for a dictionary of the states
to get a unique number id for each state in alphabetical order
ids 1 - 50 are for population data, 51 - 100 are for poverty data
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
        
        states[int(id)] = state

    for i in range(51,101):
        id = i - 50
        states[i] = states.get(id)
        
    return states

"""add state id and abbreviations to database"""
def add_states(cur, conn, states):
    for id in states.keys():
        if id < 51:
            label = 'population stats'
        else:
            label = 'poverty stats'
        
        cur.execute("INSERT OR REPLACE INTO States (stateid, abbreviation, type) VALUES (?, ?, ?)", (id, states[id], label))

    conn.commit()



"""sets up the table with the census data"""
def set_census_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Census (stateid INTEGER PRIMARY KEY,state TEXT, black PERCENT, native PERCENT, asian PERCENT, islander PERCENT, multiracial PERCENT, hispaniclatino PERCENT, white PERCENT)")
    conn.commit()

"""
function to collect data into a dictionary from quick facts page of census for each state
in list of tuples
[(DEMOGRAPHIC, PERCENT), (DEMOGRAPHIC, PERCENT), (DEMOGRAPHIC, PERCENT), (DEMOGRAPHIC, PERCENT)]
https://www.census.gov/quickfacts/fact/table/[STATE]/PST045219
"""
def population_data(state):
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

"""
takes in list of state abbreviations
function to use csv file of data on poverty rates
returns a list of dicts for each state
[state:{label:%, label:%}, state:{label:%, label:%}, state:{label:%, label:%}, state:{label:%, label:%}]
"""
def poverty_data_from_csv(states):
    out = []
    i = 0
    labels = ['location', 'white', 'black', 'hispaniclatino', 'asian', 'native', 'multiracial']

    with open('poverty_data.csv', 'r', encoding='utf-8-sig') as csv_file:
        csv_reader = csv.reader(csv_file)
        header = next(csv_reader)

        for row in csv_reader:
            state = states[i]
            i = i + 1

            stats = {}
            for t in range(1, len(labels)):
                stats[labels[t]] = row[t]

            state_dict = {}
            state_dict[state] = stats
            out.append(state_dict)

    return out


    #states[i] : {white:%, black:%, hispanic:%, asian:%, native:%, multiracial:%}"""

"""TO DO: add stats to database"""

def main():
    states_dict = numbered_states()
    states_names = list(states_dict.values())[:50]
    poverty_list = poverty_data_from_csv(states_names)

    cur, conn = setUpDatabase("census_data.db")
    set_census_table(cur, conn)
    set_states_table(cur, conn)
    add_states(cur, conn, states_dict)

main()