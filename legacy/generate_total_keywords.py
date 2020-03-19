import json
import sqlite3

if __name__ == '__main__':
    db_conn = sqlite3.connect('data_final.db')
    db_cursor = db_conn.cursor()
    db_cursor.execute('select id, GRNTI as keywords from entries')
    data_tuples = db_cursor.fetchall()
    entry_gold_keywords = {k: v for k, v in data_tuples}

    db_cursor.execute('select id_entry, title, tf_idf.tf_idf from tf_idf join wiki_titles on wiki_titles.id = id_wiki')
    data_tuples = db_cursor.fetchall()
    data_dict = {}
    for id_entry, wiki_title, tf_idf in data_tuples:
        data_dict.setdefault(id_entry, {})
        data_dict[id_entry][wiki_title] = tf_idf

    medians = {}

    for id_entry, keywords in data_dict.items():
        # medians[id_entry] = statistics.median(keywords.values())
        tf_idf_medians = sorted(keywords.values(), reverse=True)
        try:
            medians[id_entry] = tf_idf_medians[4]
        except IndexError:
            medians[id_entry] = tf_idf_medians[-1]

    for id_entry, median in medians.items():
        data_dict[id_entry] = [k for k, v in data_dict[id_entry].items() if v > median]

    with open('gold_keywords.json', 'w+') as fout:
        json.dump(entry_gold_keywords, fout, ensure_ascii=False)

    with open('result_keywords.json', 'w+') as fout:
        json.dump(data_dict, fout, ensure_ascii=False)

    