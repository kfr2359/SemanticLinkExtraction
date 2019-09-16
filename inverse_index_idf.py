import sqlite3
import math
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


def calc_idf(set_id_entry: set, num_docs: int):
    return math.log10(num_docs / len(set_id_entry))


if __name__ == '__main__':
    db_conn = sqlite3.connect('data_final.db')
    db_cursor = db_conn.cursor()
    db_cursor.execute('select count(*) from entries_norm')
    num_docs = db_cursor.fetchall()[0][0]
    db_cursor.execute('select id_entry, id_wiki from tags_entries_wiki')
    tags_entry_wiki = db_cursor.fetchall()

    print('started inversing')
    inverse_index = {}
    executor = cf.ProcessPoolExecutor(max_workers=4)
    for inverse_index_part in executor.map(build_inverse_index, divide_list(tags_entry_wiki, 4)):
        for k, v in inverse_index_part.items():
            inverse_index.setdefault(k, []).extend(v)

    inverse_index_w_sets = {}
    for id_wiki, list_id_entry in inverse_index.items():
        inverse_index_w_sets[id_wiki] = set(list_id_entry)
    print('done inversing')

    db_cursor.execute('drop table if exists tags_wiki_entry')
    db_cursor.execute('drop table if exists wiki_title_idf')
    db_conn.commit()
    db_cursor.execute('create table tags_wiki_entry (id_wiki integer, id_entry integer)')
    db_cursor.execute('create table wiki_title_idf (id_wiki integer, idf real)')

    print('started calculating idf')
    id_wiki_idf_tuples = []

    for id_wiki, set_id_entry in inverse_index_w_sets.items():
        if not set_id_entry:
            continue
        idf = math.log10(num_docs / len(set_id_entry))
        id_wiki_idf_tuples.append((id_wiki, idf))
    print('idf calculateded')

    for id_wiki, idf in id_wiki_idf_tuples:
        db_cursor.execute('insert into wiki_title_idf values(?,?)', (id_wiki, idf))

    db_conn.commit()
    
    for id_wiki, set_id_entry in inverse_index_w_sets.items():
        if not set_id_entry:
            continue
        for id_entry in set_id_entry:
            db_cursor.execute('insert into tags_wiki_entry values(?,?)', (id_wiki, id_entry))
    db_conn.commit()
