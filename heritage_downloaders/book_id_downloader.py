import re
import sqlite3

import requests
from parse import compile

db_conn = sqlite3.connect('data.db')
db_cursor = db_conn.cursor()
db_cursor.execute('CREATE TABLE IF NOT EXISTS publications (publication text PRIMARY KEY, parent text)')
db_cursor.execute('CREATE TABLE IF NOT EXISTS lists (list text PRIMARY KEY, parent text)')
db_cursor.execute('CREATE TABLE IF NOT EXISTS parsed_lists (list text PRIMARY KEY)')

base_url = 'http://e-heritage.ru'
start_url = '/unicollections/list.html?id=42033753'
list_pages_re = r'list.html\?start=[\d]+&amp;count=[\d]+&amp;id=[\d]+'
list_re = r'/unicollections/list.html\?id=[\d]+'
pub_re = r'/ras/view/publication/general.html\?id=[\d]+'
queue_passed_lists = {start_url}
queue_lists = [start_url]
queue_pubs = []

db_cursor.execute('SELECT * FROM parsed_lists')
db_parsed_lists = db_cursor.fetchall()
if len(db_parsed_lists) > 0:
    queue_passed_lists = set([x[0] for x in db_parsed_lists])
db_cursor.execute('SELECT * FROM lists')
db_lists = db_cursor.fetchall()
if len(db_lists) > 0:
    queue_lists = [x[0] for x in db_lists if x[0] not in queue_passed_lists]
db_cursor.execute('SELECT * FROM publications')
db_pubs = db_cursor.fetchall()
if len(db_pubs) > 0:
    queue_pubs = [x[0] for x in db_pubs]

pagination_f = 'list.html?start={}&amp;count={}&amp;id={}'
pagination_out = '/unicollections/list.html?start={}&count={}&id={}'
p = compile(pagination_f)
with requests.Session() as session:
    while len(queue_lists) > 0:
        cur_list = queue_lists[0]
        response = session.get(base_url+cur_list)

        if response.status_code == 200:
            # everything ok
            pass
        elif response.status_code == 404:
            print(f'404 for {cur_list}')
            queue_passed_lists.add(cur_list)
            db_cursor.execute('INSERT INTO parsed_lists VALUES (?)', (cur_list,))
            db_conn.commit()
            
            queue_lists = queue_lists[1:]
            continue
        else:
            print(f'timeout for {cur_list}')
            # put this list to the end, we'll deal with it later
            queue_lists = queue_lists[1:] + [cur_list]
            continue
        
        queue_passed_lists.add(cur_list)
        db_cursor.execute('INSERT INTO parsed_lists VALUES (?)', (cur_list,))
        db_conn.commit()

        found_lists = [x for x in re.findall(list_re, response.text) if x not in queue_passed_lists and x not in queue_lists]
        res = db_cursor.executemany('INSERT INTO lists VALUES (?,?)', [(x, cur_list) for x in found_lists])
        queue_lists += found_lists

        found_pagination = re.findall(list_pages_re, response.text)
        if len(found_pagination) > 0:
            parsed_last_found = p.parse(found_pagination[-1])
            max_start, list_count, list_id = int(parsed_last_found[0]), int(parsed_last_found[1]), parsed_last_found[2]

            full_pagination = [pagination_out.format(i, list_count, list_id) for i in range(0, max_start+1, list_count)]
            filtered_pagination = [x for x in full_pagination if x not in queue_passed_lists and x not in queue_lists]
            queue_lists += filtered_pagination
            db_cursor.executemany('INSERT INTO lists VALUES (?,?)', [(x, cur_list) for x in filtered_pagination])
        found_pubs = list(set([x for x in re.findall(pub_re, response.text) if x not in queue_pubs]))
        queue_pubs += found_pubs
        db_cursor.executemany('INSERT INTO publications VALUES (?,?)', [(x, cur_list) for x in found_pubs])

        db_conn.commit()
        print(f'parsed {cur_list}')
        queue_lists = queue_lists[1:]

queue_pubs = list(set(queue_pubs))
with open('ids.json', 'w+') as file:
    for pub in queue_pubs:
        file.write(pub+'\n')
