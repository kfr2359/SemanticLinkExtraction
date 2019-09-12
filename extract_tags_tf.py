import json
import string
import pymorphy2
import sqlite3
import string
import concurrent.futures as cf
from flashtext import KeywordProcessor


stop_words_path = 'stop_words.txt'

stop_words = set()
morph = pymorphy2.MorphAnalyzer()
wiki_titles = KeywordProcessor(case_sensitive=True)
wiki_titles.non_word_boundaries = set('абвгдежзийклмнопрстуфхцчшщъыьэуя' + string.digits)


def load_stop_words(path: str):
    stop_words = []
    with open(path, 'r') as file:
        stop_words = file.read().splitlines()
    return stop_words


def remove_specsymbols(line: str) -> str:
    mypunctuation = string.punctuation + '«»№§$'
    return line.translate(str.maketrans('', '', mypunctuation))


#################
def is_roman_numeral(word: str) -> bool:
    roman_letters = set(('m', 'd', 'c', 'l', 'x', 'v', 'i'))
    for letter in word:
        if letter not in roman_letters:
            return False
    return True


def is_numeral(word: str) -> bool:
    return word.isnumeric()


def is_stop_word(word: str) -> bool:
    return word in stop_words


filter_funcs = [
    is_roman_numeral,
    is_numeral,
    is_stop_word
]


def is_filtered_out(word: str) -> bool:
    for func in filter_funcs:
        if func(word):
            return True
    return False


def extact_tags(entries_norm: list) -> dict:
    result = {}
    for id_entry, title, contents_entries in entries_norm:
        result[id_entry] = wiki_titles.extract_keywords(title)
        result[id_entry] += wiki_titles.extract_keywords(contents_entries)
    return result


def divide_list(total: list, num_parts: int):
    parts = []
    split_idx = len(total) // num_parts
    for i in range(num_parts):
        parts.append(total[split_idx*(i) : split_idx*(i+1)])
    
    return parts


def calc_tf(list_id_wiki: list, target_id_wiki: int):
    return list_id_wiki.count(target_id_wiki) / len(list_id_wiki)


if __name__ == '__main__':
    db_conn = sqlite3.connect('data_final.db')
    db_cursor = db_conn.cursor()
    
    db_cursor.execute('drop table if exists tags_entries_wiki')
    db_conn.commit()
    db_cursor.execute('create table tags_entries_wiki (id_entry integer, id_wiki integer, tf real, tf_idf real)')

    db_cursor.execute('select * from entries_norm')
    entries_norm = db_cursor.fetchall()
    db_cursor.execute('select * from wiki_titles')
    wiki_titles_raw = db_cursor.fetchmany(500000)
    while len(wiki_titles_raw) > 0:
        wiki_titles = KeywordProcessor(case_sensitive=True)
        wiki_titles.non_word_boundaries = set('абвгдежзийклмнопрстуфхцчшщъыьэуя' + string.digits)
        for id_wiki, _, title_norm in wiki_titles_raw:
            wiki_titles.add_keyword(title_norm, id_wiki)

        entries_norm_parts = divide_list(entries_norm, 4)
        print(f'started tagging with {len(wiki_titles_raw)} titles')
        tags_entries_wiki = {}
        tags_entries_wiki.update(extact_tags(entries_norm))
        executor = cf.ProcessPoolExecutor(max_workers=4)
        for tags_entries_wiki_part in executor.map(extact_tags, entries_norm_parts):
            tags_entries_wiki.update(tags_entries_wiki)
        print('done tagging')
        db_ins_cursor = db_conn.cursor()
        for id_entry, list_id_wiki in tags_entries_wiki.items():
            if not list_id_wiki:
                continue
            for id_wiki in list_id_wiki:
                tf = calc_tf(list_id_wiki, id_wiki)
                db_ins_cursor.execute('insert into tags_entries_wiki values(?,?,?,?)', (id_entry, id_wiki, tf, 0.0))
        db_conn.commit()

        wiki_titles_raw = db_cursor.fetchmany(500000)

    db_conn.commit()