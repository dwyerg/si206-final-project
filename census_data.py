from bs4 import BeautifulSoup
import requests
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
    cur.execute("CREATE TABLE IF NOT EXISTS States (stateid INTEGER PRIMARY KEY, abbreviation TEXT, type TEXT, sex TEXT)")
    conn.commit()

"""sets up the table with the census data"""
def set_census_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Census (stateid INTEGER PRIMARY KEY, black PERCENT, native PERCENT, asian PERCENT, islander PERCENT, multiracial PERCENT, hispaniclatino PERCENT, white PERCENT)")
    conn.commit()

"""scrapes a website to get data for a dictionary of the states
to get a unique number id for each state in alphabetical order
ids 1 - 50 are for population data, 51 - 100 are for poverty data, 101 - 150 for female
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
        
        states[int(id)] = state.upper()

    for i in range(51,101):
        id = i - 50
        states[i] = states.get(id)

    for i in range(101,151):
        id = i - 100
        states[i] = states.get(id)
        
    return states

"""add state id and abbreviations to database"""
def add_states(cur, conn, states):
    for id in states.keys():
        if id < 51:
            label = 'population stats'
            sex = 'both'
        elif id < 101:
            label = 'poverty stats'
            sex = 'both'
        else:
            label = 'female stats'
            sex = 'f'
        
        cur.execute("INSERT OR REPLACE INTO States (stateid, abbreviation, type, sex) VALUES (?, ?, ?, ?)", (id, states[id], label, sex))

    conn.commit()

"""
takes in list of state abbreviations and list of state ids
function to use csv file of data on poverty rates
returns a list of dicts for each state
[{state:#, label:%, label:%}, {state:#, label:%, label:%}, {state:#, label:%, label:%}]
"""
def poverty_data_from_csv(states):
    out = []
    i = 0
    labels = ['location', 'white', 'black', 'hispaniclatino', 'asian', 'native', 'multiracial']

    with open('poverty_data.csv', 'r', encoding='utf-8-sig') as csv_file:
        csv_reader = csv.reader(csv_file)
        header = next(csv_reader)

        for row in csv_reader:
            stats = {}
            stats['state'] = states[i]

            for t in range(1, len(labels)):
                stats[labels[t]] = row[t]

            out.append(stats)

            i = i + 1

    return out

"""
function to collect data into a dictionary from quick facts page of census for each state
returns a list of dicts for each state
[{state:MI, sex:Both/F, label:%, label:%}, {state:MI, label:%, label:%}, {state:MI, label:%, label:%}]
https://www.census.gov/quickfacts/fact/table/[STATE]/PST045219
"""
def population_data(state):
    url = f'https://www.census.gov/quickfacts/fact/table/{state}/PST045219'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')

    out = []
    labels = ['black', 'native', 'asian', 'islander', 'multiracial','hispaniclatino', 'white']
    label = 0
    
    table = soup.find('div', class_='qf-facttable')
    race_section = table.find("tbody", {"data-topic" : "Race and Hispanic Origin"})
    rows = race_section.find_all('tr')
    race_data = {}
    race_data['state'] = state
    race_data['sex'] = 'Both'

    for row in rows[2:]:
        info = row.find_all('td')
        
        for i in range(len(info)):

            #check for positive index for title name
            if (i % 2 == 0):
                title = labels[label]
                if label == 6:
                    label = 0
                else:
                    label = label + 1
            else:
                percent = info[i].text.split('\n')[-1]
                race_data[title] = percent
    
    out.append(race_data)

    return out

"""
function to collect data into a dictionary from quick facts page of census for each state
returns a list of dicts for each state with female statistics
[stateid:{state:MI, sex:Both/F, label:%, label:%}, stateid:{state:MI, label:%, label:%}, stateid:{state:MI, label:%, label:%}]
https://www.census.gov/quickfacts/fact/table/[STATE]/SEX255219#SEX255219
"""
def female_population_data(state):
    url = f'https://www.census.gov/quickfacts/fact/table/{state}/SEX255219#SEX255219'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')

    out = []
    labels = ['black', 'native', 'asian', 'islander', 'multiracial','hispaniclatino', 'white']
    label = 0
    
    table = soup.find('div', class_='qf-facttable')
    race_section = table.find("tbody", {"data-topic" : "Race and Hispanic Origin"})
    rows = race_section.find_all('tr')
    race_data = {}
    race_data['state'] = state
    race_data['sex'] = 'F'

    for row in rows[2:]:
        info = row.find_all('td')
        
        for i in range(len(info)):

            #check for positive index for title name
            if (i % 2 == 0):
                title = labels[label]
                if label == 6:
                    label = 0
                else:
                    label = label + 1
            else:
                percent = info[i].text.split('\n')[-1]
                race_data[title] = percent
    
    out.append(race_data)

    return out

"""add stats to database"""
def add_population_data(cur, conn, data, states):
    #overall stats
    for i in range(len(states)):
            curr = data[i][0]
            stateid = states[i]
            black = curr['black']
            native = curr['native']
            asian = curr['asian']
            islander = curr['islander']
            multiracial = curr['multiracial']
            latino = curr['hispaniclatino']
            white = curr['white']
            
            cur.execute("INSERT INTO Census (stateid,black,native,asian,islander,multiracial,hispaniclatino,white) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (stateid, black,native,asian,islander,multiracial,latino,white))

    conn.commit()

def add_female_data(cur, conn, data, states):
    #female stats
    for i in range(len(states)):
        curr = data[i][0]
        stateid = states[i]
        black = curr['black']
        native = curr['native']
        asian = curr['asian']
        islander = curr['islander']
        multiracial = curr['multiracial']
        latino = curr['hispaniclatino']
        white = curr['white']

        cur.execute("INSERT INTO Census (stateid,black,native,asian,islander,multiracial,hispaniclatino,white) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (stateid,black,native,asian,islander,multiracial,latino,white))

    conn.commit()

def add_poverty_data(cur, conn, data, states):
    #poverty stats
    for i in range(len(states)):
        curr = data[i]
        stateid = states[i]
        black = curr['black']
        native = curr['native']
        asian = curr['asian']
        islander = 'N/A'
        multiracial = curr['multiracial']
        latino = curr['hispaniclatino']
        white = curr['white']
        
        cur.execute("INSERT INTO Census (stateid,black,native,asian,islander,multiracial,hispaniclatino,white) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (stateid,black,native,asian,islander,multiracial,latino,white))
    
    conn.commit()

def main():
    states_dict = numbered_states()
    all_state_ids = list(states_dict.keys())
    totalracedata = all_state_ids[:50]
    povertydata = all_state_ids[50:100]
    femaledata = all_state_ids[100:150]

    poverty_list = poverty_data_from_csv(povertydata)

    population_list = []
    for id in totalracedata:
        all = population_data(states_dict[id])
        population_list.append(all)

    female_list = []
    for id in femaledata:
        female = female_population_data(states_dict[id])
        female_list.append(female)

    cur, conn = setUpDatabase("census_data.db")
    set_census_table(cur, conn)
    set_states_table(cur, conn)
    add_states(cur, conn, states_dict)
    add_population_data(cur, conn, population_list, totalracedata)
    add_poverty_data(cur, conn, poverty_list, povertydata)
    add_female_data(cur, conn, female_list, femaledata)

main()
