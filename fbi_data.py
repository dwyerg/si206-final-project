# Getting FBI Criminal API Data

import requests
import json
import re
import os
import csv
import sqlite3


# url = "https://api.usa.gov/crime/fbi/sapi/"

# querystring = {"api":"api", "nibrs":"nibrs", "offense":"robbery", "national":"national", "variable":"age"}

# params = {"api_key":"m4AGJ3DCZqhmgqCJrJd5cczZPtTSl6DmUrkRLJIX"}

# response = requests.request("GET", url, headers=headers)

# response = requests.get("https://api.usa.gov/crime/fbi/sapi/m4AGJ3DCZqhmgqCJrJd5cczZPtTSl6DmUrkRLJIX")

# api_key = 'm4AGJ3DCZqhmgqCJrJd5cczZPtTSl6DmUrkRLJIX'
# offense = 'rape'
# variable = 'age'

# url = f'https://crime-data-explorer.fr.cloud.gov/pages/docApi/api/nibrs/{offense}/offender/national/{variable}'

# url = f"https://crime-data-explorer.fr.cloud.gov/pages/docApi/v1.json?limit=1&api_key={api_key}&"

# url = f'https://{api_key}@crime-data-explorer.fr.cloud.gov/pages/docApi'

# response = requests.get(url)

# print(response.status_code)
# print(response.text)


# import requests

# url = "https://jailbase-jailbase.p.rapidapi.com/recent/"

# querystring = {"source_id":"<REQUIRED>"}

# headers = {
#     'x-rapidapi-key': "SIGN-UP-FOR-KEY",
#     'x-rapidapi-host': "jailbase-jailbase.p.rapidapi.com"
#     }

# response = requests.request("GET", url, headers=headers, params=querystring)

# print(response.text)

# params = {"field_offices":"washingtondc"}

# print(response.status_code)
# print(response.text)

# print(data)

# print(data['total'])
# print(len(data['items']))
# print(data.keys())

# print(len(data))
# print(data['items'][11])



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
    cur.execute("CREATE TABLE IF NOT EXISTS Criminals (criminal_id INTEGER, name TEXT, dob_used TEXT, race TEXT, field_office TEXT, sex TEXT, crimes TEXT, reward INTEGER)")
    conn.commit()
    # make criminal_id into INTEGER PRIMARY KEY

def add_criminal(cur, conn):
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



cur, conn = setUpDatabase("FBI_data.db")
create_criminal_table(cur, conn)
add_criminal(cur, conn)




