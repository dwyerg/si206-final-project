# Getting FBI Criminal API Data

import requests
import json
import re
import os
import sqlite3
import matplotlib.pyplot as plt


def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def create_criminal_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Criminals (name TEXT PRIMARY KEY, dob_used TEXT, race TEXT, field_office TEXT, state INTEGER, sex TEXT, crimes TEXT, reward INTEGER)")
    conn.commit()

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
        page = 3
    elif num_rows < 60:
        page = 5
    elif num_rows < 80:
        page = 7
    elif num_rows < 100:
        page = 9
    elif num_rows < 120:
        page = 11
    elif num_rows < 140:
        page = 13
    elif num_rows < 160:
        page = 15
    elif num_rows < 180:
        page = 17
    elif num_rows < 190:
        page = 19

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
            cur.execute("SELECT States.stateid FROM States WHERE States.abbreviation = ?", (states[item['field_offices'][0]],))
            state = int(cur.fetchone()[0])
        sex = item['sex']
        crimes = item['description']
        reward_ = re.findall(r'\$(\S+)', str(item['reward_text']))
        if len(reward_) > 0:
            reward = int(reward_[0].replace(',', ''))
        else:
            reward = None

        cur.execute("INSERT OR IGNORE INTO Criminals (name, dob_used, race, field_office, state, sex, crimes, reward) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (name, dob_used, race, field_office, state, sex, crimes, reward))

    conn.commit()

def get_field_offices(cur):
    cur.execute("SELECT field_office FROM Criminals")
    offices = cur.fetchall()
    field_offices = {}
    for office in offices:
        if office not in field_offices:
            field_offices[office[0]] = ""
    return field_offices

def criminals_by_state(cur, conn, filename):
    with open(filename, 'w') as fileout:
        fileout.write('STATE_ABBREVIATION,TOTAL_CRIMINALS\n')
        for i in range(0, 51):
            cur.execute("SELECT States.abbreviation FROM Criminals JOIN States ON Criminals.state = States.stateid WHERE Criminals.state = ?", (i,))
            tot = cur.fetchall()
            if len(tot) == 0:
                continue
            else:
                fileout.write(str(tot[0][0]) + ',' + str(len(tot)) + '\n')

def pie_charts(cur, filename):
    ca = "CA"
    cur.execute("SELECT Census.asian, Census.black, Census.hispaniclatino, Census.native, Census.white FROM States JOIN Census ON States.stateid = Census.stateid WHERE States.abbreviation = ?", (ca,))
    ca_raw_percentages = cur.fetchone()
    ca_percentages = []
    for percent in ca_raw_percentages:
        ca_percentages.append(float(percent[:-1]))
    labels = 'asian', 'black', 'hispanic', 'native', 'white'
    colors = ['#03254c', '#1167b1', '#187bcd', '#2a9df4', '#d0efff']
    fig1, ax1 = plt.subplots()
    ax1.pie(ca_percentages, labels=labels, colors=colors, autopct='%1.1f%%')
    plt.title("Racial Makeup of California")

    ny = "NY"
    cur.execute("SELECT Census.asian, Census.black, Census.hispaniclatino, Census.native, Census.white FROM States JOIN Census ON States.stateid = Census.stateid WHERE States.abbreviation = ?", (ny,))
    ny_raw_percentages = cur.fetchone()
    ny_percentages = []
    for percent in ny_raw_percentages:
        ny_percentages.append(float(percent[:-1]))
    fig2, ax2 = plt.subplots()
    ax2.pie(ny_percentages, labels=labels, colors=colors, autopct='%1.1f%%')
    plt.title("Racial Makeup of New York")

    cur.execute("SELECT Races.race FROM Criminals JOIN States ON States.stateid = Criminals.state JOIN Races ON Races.race_id = Criminals.race WHERE States.abbreviation = ?", (ca,))
    ca_criminal_races = cur.fetchall()
    ca_criminal_count = len(ca_criminal_races)
    ca_race_count = {}
    for criminal in ca_criminal_races:
        if criminal[0] not in ca_race_count:
            ca_race_count[criminal[0]] = 0
        ca_race_count[criminal[0]] += 1
    for race in ca_race_count.items():
        percentage = round(race[1]/ca_criminal_count*100, 1)
        ca_race_count[race[0]] = percentage
    ca_race_count = dict(sorted(ca_race_count.items()))
    ca_criminal_percentages = [0, 0] + list(ca_race_count.values())
    ca_labels =  ['asian/black', ''] + list(ca_race_count.keys())
    fig3, ax3 = plt.subplots()
    ax3.pie(ca_criminal_percentages, labels=ca_labels, colors=colors, autopct='%1.1f%%')
    plt.title("Criminal Racial Makeup of California")

    cur.execute("SELECT Races.race FROM Criminals JOIN States ON States.stateid = Criminals.state JOIN Races ON Races.race_id = Criminals.race WHERE States.abbreviation = ?", (ny,))
    ny_criminal_races = cur.fetchall()
    ny_criminal_count = len(ny_criminal_races)
    ny_race_count = {}
    for criminal in ny_criminal_races:
        if criminal[0] not in ny_race_count:
            ny_race_count[criminal[0]] = 0
        ny_race_count[criminal[0]] += 1
    for race in ny_race_count.items():
        percentage = round(race[1]/ny_criminal_count*100, 1)
        ny_race_count[race[0]] = percentage
    ny_race_count = dict(sorted(ny_race_count.items()))
    ny_criminal_percentages = list(ny_race_count.values())[:3] + [0] + [list(ny_race_count.values())[-1]]
    print(ny_criminal_percentages)
    ny_labels =  list(ny_race_count.keys())[:3] + ['native'] + [list(ny_race_count.keys())[-1]]
    print(ny_labels)
    fig4, ax4 = plt.subplots()
    ax4.pie(ny_criminal_percentages, labels=ny_labels, colors=colors, autopct='%1.1f%%')
    plt.title("Criminal Racial Makeup of New York")
    plt.show()


def main():
    cur, conn = setUpDatabase("main_data.db")
    # states = {'washingtondc': 'DC', 'tampa': 'FL', 'philadelphia': 'PA', 'jacksonville': 'FL', 'albuquerque': 'NM', 'losangeles': 'CA', 'miami': 'FL', 'sanjuan': 'UT', 'cleveland': 'OH', 'newhaven': 'CT', 'seattle': 'WA', 'cincinnati': 'OH', 'portland': 'OR', 'phoenix': 'AZ', 'dallas': 'TX', 'minneapolis': 'MN', 'chicago': 'IL', 'newark': 'NJ', 'sanfrancisco': 'CA', 'newyork': 'NY', 'sacramento': 'CA', 'saltlakecity': 'UT', 'lasvegas': 'NV', 'louisville': 'KY', 'boston': 'MA', 'houston': 'TX', 'omaha': 'NE', 'pittsburgh': 'PA', 'atlanta': 'GA', 'columbia': 'SC', 'albany': 'NY', 'kansascity': 'KS', 'denver': 'CO', 'mobile': 'AL', 'buffalo': 'NY', 'elpaso': 'TX', 'littlerock': 'AR', 'sandiego': 'CA', 'detroit': 'MI', 'milwaukee': 'WI', 'richmond': 'VA', 'baltimore': 'MD', 'neworleans': 'LA', 'charlotte': 'NC', 'indianapolis': 'IN', 'oklahomacity': 'OK', 'norfolk': 'VA', 'stlouis': 'MO', 'knoxville': 'TN', 'birmingham': 'AL', 'springfield': 'OR', 'memphis': 'TN', 'jackson': 'MS', 'honolulu': 'HI', 'sanantonio': 'TX'}
    # cur.execute("DROP TABLE Criminals")
    # create_criminal_table(cur, conn)
    # create_race_table(cur, conn)
    # add_criminals(cur, conn, states)
    # get_field_offices(cur)

    # criminals_by_state(cur, conn, 'criminals_by_state.txt')
    pie_charts(cur, 'criminals_by_state.txt')

if __name__ == '__main__':
    main()


