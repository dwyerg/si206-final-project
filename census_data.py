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
    cur.execute("CREATE TABLE IF NOT EXISTS States (stateid INTEGER PRIMARY KEY, abbreviation TEXT, label TEXT)")
    conn.commit()

"""sets up the table with the census data"""
def set_census_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Census (stateid INTEGER PRIMARY KEY, white PERCENT, black PERCENT, native PERCENT, hispaniclatino PERCENT, asian PERCENT)")
    conn.commit()

"""to get a unique number id for each state in alphabetical order
ids 1 - 50 are for 2020 data, 51 - 100 are for 2018 data, 101 - 150 for poverty data
https://www.owogram.com/us-states-alphabetical-order/"""
def numbered_states():
    out = {}
    states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VE', 'VI', 'WA', 'WV', 'WI', 'WY']
    states_without_dc = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VE', 'VI', 'WA', 'WV', 'WI', 'WY']
    
    for i in range(len(states)):
        out[i] = states[i]
    for i in range(len(states)):
        out[i+51] = states[i]
    for i in range(len(states_without_dc)):
        out[i+102] = states_without_dc[i]

    return out

"""
function to collect 2018 data into a list of dictionaries
[statid:{WHITE:%, BLACK:%, NATIVE:%, HISPANICLATINO:%, ASIAN:%}, statid:{WHITE:%, BLACK:%, NATIVE:%, HISPANICLATINO:%, ASIAN:%}, statid:{WHITE:%, BLACK:%, NATIVE:%, HISPANICLATINO:%, ASIAN:%}]
https://en.wikipedia.org/wiki/Historical_racial_and_ethnic_demographics_of_the_United_States
"""
def population_data_2018(ids):
    url = 'https://en.wikipedia.org/wiki/Historical_racial_and_ethnic_demographics_of_the_United_States'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')

    white = []
    black = []
    native = []
    hispaniclatino = []
    asian = []
    out = []

    #list of each race percent %s
    #dictionary for each state

    main = soup.find('div', class_="mw-body")
    body = main.find('div', class_="vector-body")
    content = body.find('div', class_="mw-body-content mw-content-ltr")
    tags = content.find('div', class_="mw-parser-output")
    tables = tags.find_all('table', class_="wikitable sortable")

    whitetable = tables[6].find('tbody')
    whiterows = whitetable.find_all('tr')
    for row in whiterows[1:-1]:
        cells = row.find_all('th')
        percent = cells[-1].text
        white.append(percent)

    blacktable = tables[7].find('tbody')
    blackrows = blacktable.find_all('tr')
    for row in blackrows[6:-1]:
        cells = row.find_all('th')
        percent = cells[-2].text
        black.append(percent)

    nativechart = tags.find_all('table', class_="sortable wikitable outercollapse")
    nativetable = nativechart[0].find('tbody')
    nativerows = nativetable.find_all('tr')
    for row in nativerows[2:53]:
        cells = row.find_all('td')
        percent = cells[13].text
        native.append(percent)

    latinotable = tables[9].find('tbody')
    latinorows = latinotable.find_all('tr')
    for row in latinorows[6:-1]:
        cells = row.find_all('th')
        percent = cells[-2].text
        hispaniclatino.append(percent)

    asianchart = tags.find_all('table', class_="sortable wikitable outercollapse")
    asiantable = asianchart[-1].find('tbody')
    asianrows = asiantable.find_all('tr')
    for row in asianrows[2:-1]:
        cells = row.find_all('th')
        percent = cells[-2].text
        asian.append(percent)

    for i in range(len(ids)):
        curr = {}
        curr['white'] = white[i].strip('\n')
        curr['black'] = black[i].strip('\n')
        curr['native'] = native[i].strip('\n')
        curr['hispaniclatino'] = hispaniclatino[i].strip('\n')
        curr['asian'] = asian[i].strip('\n')

        state = {}
        state[ids[i]] = curr
        out.append(state)

    return out

"""
function to collect 2020 data into a list of dictionaries
[statid:{WHITE:%, BLACK:%, NATIVE:%, HISPANICLATINO:%, ASIAN:%}, statid:{WHITE:%, BLACK:%, NATIVE:%, HISPANICLATINO:%, ASIAN:%}, statid:{WHITE:%, BLACK:%, NATIVE:%, HISPANICLATINO:%, ASIAN:%}]
https://en.wikipedia.org/wiki/Historical_racial_and_ethnic_demographics_of_the_United_States
"""
def population_data_2020(ids):
    url = 'https://en.wikipedia.org/wiki/Historical_racial_and_ethnic_demographics_of_the_United_States'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')

    white = []
    black = []
    native = []
    hispaniclatino = []
    asian = []
    out = []

    #list of each race percent %s
    #dictionary for each state

    main = soup.find('div', class_="mw-body")
    body = main.find('div', class_="vector-body")
    content = body.find('div', class_="mw-body-content mw-content-ltr")
    tags = content.find('div', class_="mw-parser-output")
    tables = tags.find_all('table', class_="wikitable sortable")

    whitetable = tables[6].find('tbody')
    whiterows = whitetable.find_all('tr')
    for row in whiterows[1:-1]:
        cells = row.find_all('td')
        percent = cells[1].text
        white.append(percent)

    blacktable = tables[7].find('tbody')
    blackrows = blacktable.find_all('tr')
    for row in blackrows[6:-1]:
        cells = row.find_all('th')
        percent = cells[-1].text
        black.append(percent)

    nativechart = tags.find_all('table', class_="sortable wikitable outercollapse")
    nativetable = nativechart[0].find('tbody')
    nativerows = nativetable.find_all('tr')
    for row in nativerows[2:54]:
        cells = row.find_all('td')
        percent = cells[14].text
        native.append(percent)

    latinotable = tables[9].find('tbody')
    latinorows = latinotable.find_all('tr')
    for row in latinorows[6:-1]:
        cells = row.find_all('th')
        percent = cells[-1].text
        hispaniclatino.append(percent)

    asianchart = tags.find_all('table', class_="sortable wikitable outercollapse")
    asiantable = asianchart[-1].find('tbody')
    asianrows = asiantable.find_all('tr')
    for row in asianrows[2:-1]:
        cells = row.find_all('th')
        percent = cells[-1].text
        asian.append(percent)

    for i in range(len(ids)):
        curr = {}
        curr['white'] = white[i].strip('\n')
        curr['black'] = black[i].strip('\n')
        curr['native'] = native[i].strip('\n')
        curr['hispaniclatino'] = hispaniclatino[i].strip('\n')
        curr['asian'] = asian[i].strip('\n')

        state = {}
        state[ids[i]] = curr
        out.append(state)

    return out

"""
takes in list of state abbreviations and list of state ids
function to use csv file of data on poverty rates
returns a list of dicts for each state
[{state:#, label:%, label:%}, {state:#, label:%, label:%}, {state:#, label:%, label:%}]
"""
def poverty_data_from_csv(states):
    out = []
    i = 0
    labels = ['location', 'white', 'black', 'hispaniclatino', 'asian', 'native']

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

"""add state id and abbreviations to database"""
def add_states(cur, conn, states):
    for id in states.keys():
        if id < 51:
            label = '2020'
        elif id < 101:
            label = '2018'
        else:
            label = 'poverty'
        
        cur.execute("INSERT OR REPLACE INTO States (stateid, abbreviation, label) VALUES (?, ?, ?)", (id, states[id], label))

    conn.commit()

"""add poverty stats to database"""
def add_poverty_data(cur, conn, data, states):
    #poverty stats
    for i in range(len(states)):
        curr = data[i]
        stateid = states[i]
        white = curr['white']
        black = curr['black']
        native = curr['native']
        latino = curr['hispaniclatino']
        asian = curr['asian']
        
        cur.execute("INSERT INTO Census (stateid,white,black,native,hispaniclatino,asian) VALUES (?, ?, ?, ?, ?, ?)", (stateid,white,black,native,latino,asian))
    
    conn.commit()

"""add 2018 and 2020 stats to database"""
def add_population_data(cur, conn, olddata, recentdata):
    #2018 stats
    i = 0
    for statedict in olddata:
        stateid = list(statedict.keys())[0]
        curr = olddata[i][stateid]
        i = i + 1
        white = curr['white']
        black = curr['black']
        native = curr['native']
        latino = curr['hispaniclatino']
        asian = curr['asian']
            
        cur.execute("INSERT INTO Census (stateid,white,black,native,hispaniclatino,asian) VALUES (?, ?, ?, ?, ?, ?)", (stateid,white,black,native,latino,asian))

    #2020 stats
    for statedict in recentdata:
        stateid = list(statedict.keys())[0]
        curr = recentdata[stateid][stateid]
        white = curr['white']
        black = curr['black']
        native = curr['native']
        latino = curr['hispaniclatino']
        asian = curr['asian']
            
        cur.execute("INSERT INTO Census (stateid,white,black,native,hispaniclatino,asian) VALUES (?, ?, ?, ?, ?, ?)", (stateid,white,black,native,latino,asian))
    
    conn.commit()

def main():
    states_dict = numbered_states()
    all_state_ids = list(states_dict.keys())
    recentraceids = all_state_ids[:51]
    oldraceids = all_state_ids[51:102]
    povertyids = all_state_ids[102:152]

    poverty_data = poverty_data_from_csv(povertyids)
    recent_data = population_data_2020(recentraceids)
    old_data = population_data_2018(oldraceids)

    cur, conn = setUpDatabase("census_data.db")
    set_census_table(cur, conn)
    set_states_table(cur, conn)
    add_states(cur, conn, states_dict)
    add_population_data(cur, conn, old_data, recent_data)
    add_poverty_data(cur, conn, poverty_data, povertyids)

main()


"""OLD NUMBERED STATES FUNCTION"""
"""url = 'https://www.owogram.com/us-states-alphabetical-order/'
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
    
return states"""