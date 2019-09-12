import sqlite3

if __name__ == '__main__':
    db_conn = sqlite3.connect('data_final.db')
    db_cursor = db_conn.cursor()

    db_cursor.execute('select id_entry, id_wiki, tf from tags_entries_wiki')
    entry_wiki_tf = db_cursor.fetchall()

    db_cursor.execute('select id_wiki, idf from wiki_title_idf')
    wiki_idf_tuples = db_cursor.fetchall()
    wiki_idf = dict(wiki_idf_tuples)
    
    print('started tf-idf calculation')
    for id_entry, id_wiki, tf in entry_wiki_tf:
        tf_idf = tf * wiki_idf[id_wiki]
        db_cursor.execute('update tags_entries_wiki set tf_idf=? where id_entry=? and id_wiki=?', (tf_idf, id_entry, id_wiki))

    db_conn.commit()