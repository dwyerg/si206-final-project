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
def set_census_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Census (stateid INTEGER PRIMARY KEY,state TEXT, black PERCENT, native PERCENT, asian PERCENT, islander PERCENT, multiracial PERCENT, hispaniclatino PERCENT, white PERCENT)")
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
        
        states[state] = int(id)
        
    return states

"""uses states dictionary to create another dictionary for
the state poverty data to use as primary key"""
def poverty_state_ids(states):
    out = {}

    for key in states:
        out[key] = states[key] + 50

    return out

"""
function to collect data into a dictionary from quick facts page of census for each state
in list of tuples
[(DEMOGRAPHIC, PERCENT), (DEMOGRAPHIC, PERCENT), (DEMOGRAPHIC, PERCENT), (DEMOGRAPHIC, PERCENT)]
https://www.census.gov/quickfacts/fact/table/[STATE]/PST045219
"""
def race_data_from_search(state):
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

"""TO DO: add states + ids to database"""

"""TO DO: add data to database"""
def add_criminal(cur, conn, totalrates, povertyrates):
    response = requests.get('https://api.fbi.gov/wanted/v1/list')
    data = json.loads(response.content)
    criminal_id = 0

    for item in data['items']:
        criminal_id += 1
        name = item['title']
        if item['dates_of_birth_used'] == None:
            dob_used = "no dob"
        else:
            dob_used = item['dates_of_birth_used'][0]
        race = item['race']
        if item['field_offices'] == None:
            field_office = "no location"
        else:
            field_office = item['field_offices'][0]
        sex = item['sex']
        crimes = item['description']
        reward = item['reward_text']
   
        cur.execute("INSERT INTO Criminals (criminal_id, name, dob_used, race, field_office, sex, crimes, reward) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (criminal_id, name, dob_used, race, field_office, sex, crimes, reward))

    conn.commit()

def main():
    cur, conn = setUpDatabase("census_data.db")
    set_census_table(cur, conn)

    states_dict = numbered_states()
    poverty_states_dict = poverty_state_ids(states_dict)
    states = list(states_dict.keys())
    poverty_list = poverty_data_from_csv(states)

main()