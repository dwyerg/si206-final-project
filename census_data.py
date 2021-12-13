from bs4 import BeautifulSoup
import requests
import csv
import os
import sqlite3
import matplotlib.pyplot as plt
import numpy as np

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
    states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VE', 'WA', 'WV', 'WI', 'WY']
    states_without_dc = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VE', 'WA', 'WV', 'WI', 'WY']
    
    for i in range(len(states)):
        out[i] = states[i]
    for i in range(len(states)):
        out[i+51] = states[i]
    for i in range(len(states_without_dc)):
        out[i+102] = states_without_dc[i]

    return out

"""
input: unique state ids
output: list of dicts with demographics per state
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
input: unique state ids
output: list of dicts with demographics per state
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
input: unique state ids
output: list of dicts with percentages for each demographic per state
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

"""add state id and abbreviations to state table with state names and ids to serve as key in database"""
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

"""add poverty stats to census table in database"""
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

"""add 2018 and 2020 stats to census table database"""
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

"""calculates difference between poverty and general population stats for each racial group (poverty% - population%)
for California and New York (the 2 states with the most criminals) and writes the calculations into a txt file
returns list of dicts to be used for visualizations
list: [(ca race stats), (ca poverty stats), (ny race stats), (ny poverty stats)"""
def write_calculations (cur, filename):
    #POVERTY DATA (not enough data for natives in these states)
    #for California
    cur.execute("SELECT white, black, hispaniclatino, asian FROM Census WHERE stateid = 106")
    ca_povertydata = cur.fetchall()
    ca_povertystats = {'white':"{:.0%}".format(ca_povertydata[0][0]),
                    'black':"{:.0%}".format(ca_povertydata[0][1]),
                    'latino':"{:.0%}".format(ca_povertydata[0][2]),
                    'asian':"{:.0%}".format(ca_povertydata[0][3])}

    #for New York
    cur.execute("SELECT white, black, hispaniclatino, asian FROM Census WHERE stateid = 133")
    ny_povertydata = cur.fetchall()
    ny_povertystats = {'white':"{:.0%}".format(ny_povertydata[0][0]),
                    'black':"{:.0%}".format(ny_povertydata[0][1]),
                    'latino':"{:.0%}".format(ny_povertydata[0][2]),
                    'asian':"{:.0%}".format(ny_povertydata[0][3])}

    #RACE DATA
    #for California
    cur.execute("SELECT white, black, hispaniclatino, asian FROM Census WHERE stateid = 4")
    ca_racedata = cur.fetchall()
    ca_racestats = {'white':ca_racedata[0][0],'black':ca_racedata[0][1],
                    'latino':ca_racedata[0][2], 'asian':ca_racedata[0][3]}

    #for New York
    cur.execute("SELECT white, black, hispaniclatino, asian FROM Census WHERE stateid = 32")
    ny_racedata = cur.fetchall()
    ny_racestats = {'white':ny_racedata[0][0],'black':ny_racedata[0][1],
                    'latino':ny_racedata[0][2], 'asian':ny_racedata[0][3]}
    
    # calculations of poverty rate - 
    ca_calc = {}
    ny_calc={}
    txt_str = []
    for race in ca_povertystats.keys():
        ca_pov = float(ca_povertystats[race].strip('%'))
        ca_race = float(ca_racestats[race].strip('%'))
        ca_calc[race] = round((ca_pov - ca_race),2)


        ny_pov = float(ny_povertystats[race].strip('%'))
        ny_race = float(ny_racestats[race].strip('%'))
        ny_calc[race] = round((ny_pov - ny_race),2)
        
        #strings to write into file
        ca_str = f"While {race} people in California make up {ca_race}% of the population, they make up {ca_pov}% of the state's poverty population. (Difference: {ca_calc[race]}%)"
        ny_str = f"While {race} people in New York make up {ny_race}% of the population, they make up {ny_pov}% of the state's poverty population. (Difference: {ny_calc[race]}%)"


        txt_str.append(ca_str)
        txt_str.append(ny_str)
    
    with open(filename, 'w') as fileout:
        for line in txt_str:
            string = line.split(',')
            fileout.write(string[0])
            fileout.write('\n')
            fileout.write(string[1])
            fileout.write('\n\n')

    out = []
    out.append(ca_racestats)
    out.append(ca_povertystats)
    out.append(ny_racestats)
    out.append(ny_povertystats)
    return out

"""takes in list of dictionaries with population and poverty stats from write_calculations and the state name
makes bar chart comparing general population and poverty percentages for ethnic groups in the state
saves the visualization as a png"""
def createBarGraph(stats, statename): 
    groups = ['White', 'Black', 'Hispanic/Latino', 'Asian']
    population = []
    poverty = []

    for percent in stats[0].values():
        population.append(float(percent.strip('%')))

    for percent in stats[1].values():
        poverty.append(float(percent.strip('%')))

    indices = np.arange(len(groups))
    width = 0.20

    plt.bar(indices + width, poverty, width=width, color ='darkgoldenrod', label ='Poverty')
    plt.bar(indices, population, width=width, color ='darkseagreen', label ='Population')

    plt.xticks(ticks=indices, labels=groups)
    
    plt.xlabel("Demographic")
    plt.ylabel("Percent makeup")
    plt.title(f"Population vs Poverty makeup in {statename}")

    plt.legend()
    plt.show()

def main():
    cur, conn = setUpDatabase("main_data.db")

    #SET UP STATE IDS
    """states_dict = numbered_states()
    all_state_ids = list(states_dict.keys())
    recentraceids = all_state_ids[:51]
    oldraceids = all_state_ids[51:102]
    povertyids = all_state_ids[102:152]"""

    #SCRAPE DATA FROM WEBSITES INTO LISTS
    """poverty_data = poverty_data_from_csv(povertyids)
    recent_data = population_data_2020(recentraceids)
    old_data = population_data_2018(oldraceids)"""

    #SET UP TABLES AND PUT SCRAPED DATA IN DATABASE
    """set_census_table(cur, conn)
    set_states_table(cur, conn)
    add_states(cur, conn, states_dict)
    add_population_data(cur, conn, old_data, recent_data)
    add_poverty_data(cur, conn, poverty_data, povertyids)"""

    #CALCULATE DATA AND WRITE TO TXT FILE
    """ca_and_ny_stats = write_calculations (cur, 'race_stats.txt')"""

    #VISUALIZATIONS
    """ca_stats = ca_and_ny_stats[:2]
    ny_stats = ca_and_ny_stats[2:]

    createBarGraph(ca_stats, "California")
    createBarGraph(ny_stats, "New York")"""

main()