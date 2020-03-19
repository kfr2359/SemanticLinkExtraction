import json
import sqlite3

if __name__ == '__main__':
    total_dict = {}
    for i in range(1,5):
        with open(f'out{i}.json', 'r', encoding='utf-8-sig') as file:
            total_dict.update(json.load(file)) 

    db_conn = sqlite3.connect('data_final.db')
    db_cursor = db_conn.cursor()

    for k, v in total_dict.items():
        db_cursor.execute('UPDATE `entries` SET contents_entries=? WHERE id=?', (v, k))
    db_conn.commit()