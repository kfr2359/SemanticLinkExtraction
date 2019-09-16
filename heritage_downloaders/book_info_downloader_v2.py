import json
import requests
import sqlite3
import time
import multiprocessing as mp
import xml.etree.ElementTree as ET
from parse import compile

ns = {
    'isp': 'http://umeta.ru/namespaces/platform/ixsp',
    'xsp-request': 'http://apache.org/xsp/request/2.0',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'dcterms': 'http://purl.org/dc/terms/',
    'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
    'isir': 'http://isir.ras.ru/namespace/',
    'aux': 'http://umeta.ru/namespaces/blocks/auxiliary/',
    'media': 'http://umeta.ru/namespaces/heritage/media/',
    'kernel': 'http://umeta.ru/namespaces/platform/kernel/'
}

descr_url_base = 'http://e-heritage.ru/ras/view/publication/general.xml?id={}'
contents_url_base = 'http://books.e-heritage.ru/Book/Book/getCt/15'

id_format = '/ras/view/publication/general.html?id={}'

def fetch_description(id: str, session: requests.Session):
    url = descr_url_base.format(id)
    response = session.get(url)
    while True:
        if response.status_code == 200:
            return response.text
        elif response.status_code == 404:
            return None
        else:
            response = session.get(url)
    

def fetch_contents(msc: str, session: requests.Session):
    url = contents_url_base
    variables = {
        'bok': msc
    }
    response = session.post(url, data=variables)
    while True:
        if response.status_code == 200:
            return response.content.decode('utf-8-sig')
        elif response.status_code == 404:
            return None
        else:
            response = session.post(url, data=variables)


def process_ids(ids):
    p = compile(id_format)
    db_conn = sqlite3.connect('data.db', timeout=20)
    db_cursor = db_conn.cursor()
    db_cursor.execute('SELECT id from raw_data_not_fixed')
    processed_ids = [x[0] for x in db_cursor.fetchall()]
    with requests.Session() as session:
        for id_line in ids:
            id = p.parse(id_line)[0]
            if id in processed_ids:
                continue
            description = fetch_description(id, session)
            msc = None
            contents = None
            if description is not None:
                try:
                    root = ET.fromstring(description)
                except Exception:
                    pass
                
                root = root[0]
                if root is not None:
                    msc_entry = root.find('msc', ns)
                    if msc_entry is not None:
                        msc = msc_entry.text
                        contents = fetch_contents(msc, session)
            db_cursor.execute('INSERT INTO raw_data_not_fixed VALUES(?,?,?,?)', (id, msc, description, contents))
            db_conn.commit()
            print(f'processed {id}')
                
    db_conn.close()


def do_parallel_work(input: list, worker_num, worker_func):
    split_idx = len(store) // worker_num
    d = [None for _ in range(worker_num)]
    d[0] = store[:split_idx]
    for i in range(1, worker_num-1):
        d[i] = store[split_idx*i:split_idx*(i+1)]
    d[worker_num-1] = store[split_idx*(worker_num-1):]
    
    processes = [mp.Process(target=worker_func, args=(d[i],)) for i in range(worker_num)]
    for p in processes:
        p.start()

    while any([p.is_alive() for p in processes]):
        time.sleep(5)


if __name__ == '__main__':
    db_conn = sqlite3.connect('data.db', timeout=10)
    db_cursor = db_conn.cursor()
    db_cursor.execute('CREATE TABLE IF NOT EXISTS raw_data_not_fixed (id integer PRIMARY KEY, msc integer, description text, contents text)')
    db_cursor.execute('SELECT publication from publications')
    store = [x[0] for x in db_cursor.fetchall()]
    db_conn.close()

    do_parallel_work(store, 16, process_ids)