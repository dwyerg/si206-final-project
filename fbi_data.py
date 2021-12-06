# Getting FBI Criminal API Data

import requests
import json
import re
import os
import csv
import sqlite3

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def create_criminal_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Criminals (name TEXT PRIMARY KEY, dob_used TEXT, race TEXT, field_office TEXT, state INTEGER, sex TEXT, crimes TEXT, reward INTEGER)")
    conn.commit()

# def create_state_table(cur, conn, states, state_abbr):
#     cur.execute("CREATE TABLE IF NOT EXISTS States (state_id INTEGER PRIMARY KEY, state_abbr TEXT)")
#     state_id = 1

#     for state in state_abbr:
#         cur.execute("INSERT OR IGNORE INTO States (state_id, state_abbr) VALUES (?, ?)", (state_id, state))
#         state_id += 1
#     conn.commit()

def create_race_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Races (race_id INTEGERY PRIMARY KEY, race TEXT)")
    race_id = 1
    race_lst = ['white', 'black', 'native', 'asian', 'hispanic']

    for race in race_lst:
        cur.execute("INSERT OR IGNORE INTO Races (race_id, race) VALUES (?, ?)", (race_id, race))
        race_id += 1
    conn.commit()

def add_criminals(cur, conn, states):
    cur.execute("SELECT COUNT (*) FROM Criminals")
    num_rows = cur.fetchone()[0]
    if num_rows < 20:
        page = 1
    elif num_rows < 40:
        page = 2
    elif num_rows < 60:
        page = 3
    elif num_rows < 80:
        page = 4
    elif num_rows < 100:
        page = 5
    elif num_rows < 120:
        page = 6
    elif num_rows < 140:
        page = 7
    elif num_rows < 160:
        page = 8
    elif num_rows < 180:
        page = 9
    elif num_rows < 190:
        page = 10

    params={'page': page}
    response = requests.get('https://api.fbi.gov/wanted/v1/list', params=params)
    data = json.loads(response.content)

    for item in data['items']:
        name = item['title']
        if item['dates_of_birth_used'] == None:
            dob_used = "no dob"
        else:
            dob_used = item['dates_of_birth_used'][0]
        if item['race'] == None:
            race = None
        else:
            cur.execute("SELECT Races.race_id FROM Races WHERE Races.race = ?", (item['race'],))
            race = int(cur.fetchone()[0])
        if item['field_offices'] == None:
            field_office = None
            state = None
        else:
            field_office = item['field_offices'][0]
            print(field_office)
            cur.execute("SELECT States.stateid FROM States WHERE States.abbreviation = ?", (states[item['field_offices'][0]],))
            state = int(cur.fetchone()[0])
            print(state)
        sex = item['sex']
        crimes = item['description']
        reward_ = re.findall(r'\$(\S+)', str(item['reward_text']))
        if len(reward_) > 0:
            reward = int(reward_[0].replace(',', ''))
        else:
            reward = None

        cur.execute("INSERT OR IGNORE INTO Criminals (name, dob_used, race, field_office, state, sex, crimes, reward) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (name, dob_used, race, field_office, state, sex, crimes, reward))

    conn.commit()

def get_field_offices(cur, conn):
    cur.execute("SELECT field_office FROM Criminals")
    offices = cur.fetchall()
    field_offices = {}
    for office in offices:
        if office not in field_offices:
            field_offices[office[0]] = ""




def main():
    cur, conn = setUpDatabase("main_data.db")
    states = {'washingtondc': 'DC', 'tampa': 'FL', 'philadelphia': 'PA', 'jacksonville': 'FL', 'albuquerque': 'NM', 'losangeles': 'CA', 'miami': 'FL', 'sanjuan': 'UT', 'cleveland': 'OH', 'newhaven': 'CT', 'seattle': 'WA', 'cincinnati': 'OH', 'portland': 'OR', 'phoenix': 'AZ', 'dallas': 'TX', 'minneapolis': 'MN', 'chicago': 'IL', 'newark': 'NJ', 'sanfrancisco': 'CA', 'newyork': 'NY', 'sacramento': 'CA', 'saltlakecity': 'UT', 'lasvegas': 'NV', 'louisville': 'KY', 'boston': 'MA', 'houston': 'TX', 'omaha': 'NE', 'pittsburgh': 'PA', 'atlanta': 'GA', 'columbia': 'SC', 'albany': 'NY', 'kansascity': 'KS', 'denver': 'CO', 'mobile': 'AL', 'buffalo': 'NY', 'elpaso': 'TX', 'littlerock': 'AR', 'sandiego': 'CA', 'detroit': 'MI', 'milwaukee': 'WI', 'richmond': 'VA', 'baltimore': 'MD', 'neworleans': 'LA', 'charlotte': 'NC', 'indianapolis': 'IN', 'oklahomacity': 'OK', 'norfolk': 'VA', 'stlouis': 'MO', 'knoxville': 'TN', 'birmingham': 'AL', 'springfield': 'OR', 'memphis': 'TN', 'jackson': 'MS', 'honolulu': 'HI', 'sanantonio': 'TX'}
    # state_abbr = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VE', 'VI', 'WA', 'WV', 'WI', 'WY']
    #Alabama is Al not AL in Huda's code
    # create_state_table(cur, conn, states, state_abbr)
    create_criminal_table(cur, conn)
    create_race_table(cur, conn)
    add_criminals(cur, conn, states)
    # get_field_offices(cur, conn)

if __name__ == '__main__':
    main()

