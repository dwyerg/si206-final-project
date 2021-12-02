# Getting FBI Criminal API Data

import requests
import json
import re
import os
import csv
import sqlite3

# url = "https://api.usa.gov/crime/fbi/sapi/api/data/nibrs/rape/offender/national/race?api_key=m4AGJ3DCZqhmgqCJrJd5cczZPtTSl6DmUrkRLJIX"

# response = requests.get(url)

# print(response.status_code)
# print(response.text)


def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def create_criminal_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Criminals (name TEXT PRIMARY KEY, dob_used TEXT, race TEXT, field_office TEXT, sex TEXT, crimes TEXT, reward INTEGER)")
    conn.commit()
    # make criminal_id into INTEGER PRIMARY KEY

def add_criminals(cur, conn):
    page = 1
    num_criminals = 0
    while num_criminals < 100:
        params={'page': page}
        response = requests.get('https://api.fbi.gov/wanted/v1/list', params=params)
        data = json.loads(response.content)
        page += 1

        for item in data['items']:
            num_criminals += 1
            name = item['title']
            if item['dates_of_birth_used'] == None:
                dob_used = "no dob"
            else:
                dob_used = item['dates_of_birth_used'][0]
            race = item['race']
            if item['field_offices'] == None:
                continue
            else:
                field_office = item['field_offices'][0]
            sex = item['sex']
            crimes = item['description']
            reward = item['reward_text']
    
            cur.execute("INSERT OR IGNORE INTO Criminals (name, dob_used, race, field_office, sex, crimes, reward) VALUES (?, ?, ?, ?, ?, ?, ?)", (name, dob_used, race, field_office, sex, crimes, reward))

    conn.commit()

def get_field_offices(cur, conn):
    cur.execute("SELECT field_office FROM Criminals")
    offices = cur.fetchall()
    field_offices = {}
    for office in offices:
        if office not in field_offices:
            field_offices[office[0]] = ""
    print(field_offices)

cur, conn = setUpDatabase("FBI_data.db")
# create_criminal_table(cur, conn)
# add_criminals(cur, conn)
get_field_offices(cur, conn)



