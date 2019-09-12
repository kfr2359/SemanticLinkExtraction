import sqlite3
import concurrent.futures as cf
from collections import Counter


def build_inverse_index(list_tuples: list) -> dict:
    result = {}
    for tuple_ in list_tuples:
        id_entry, id_wiki = tuple_
        result.setdefault(id_wiki, []).append(id_entry)
    return result


def divide_list(total: list, num_parts: int):
    parts = []
    split_idx = len(total) // num_parts
    for i in range(num_parts):
        parts.append(total[split_idx*(i) : split_idx*(i+1)])
    
    return parts


if __name__ == '__main__':
    db_conn = sqlite3.connect('data_final.db')
    db_cursor = db_conn.cursor()
    db_cursor.execute('select * from tags_entries_wiki')
    tags_entry_wiki = db_cursor.fetchall()

    inverse_index = {}
    executor = cf.ProcessPoolExecutor(max_workers=4)
    for inverse_index_part in executor.map(build_inverse_index, divide_list(tags_entry_wiki, 4)):
        for k, v in inverse_index_part.items():
            inverse_index.setdefault(k, []).extend(v)

    db_cursor.execute('drop table if exists tags_wiki_entry')
    db_conn.commit()
    db_cursor.execute('create table tags_wiki_entry (id_wiki integer, id_entry integer)')
    for id_wiki, list_id_entry in inverse_index.items():
        for id_entry in list_id_entry:
            db_cursor.execute('insert into tags_wiki_entry values(?,?)', (id_wiki, id_entry))
    db_conn.commit()
