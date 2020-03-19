import json
import string
import pymorphy2
import sqlite3
import concurrent.futures as cf


stop_words_path = 'stop_words.txt'

stop_words = set()
morph = pymorphy2.MorphAnalyzer()


def load_stop_words(path: str):
    with open(path, 'r') as file:
        stop_words = set(file.read().splitlines())
    return stop_words


def remove_specsymbols(line: str) -> str:
    mypunctuation = string.punctuation + '«»№§$' + string.digits
    return line.translate(str.maketrans('', '', mypunctuation))


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


def normalize_strings(strings: list) -> list:
    result = []
    for entry in strings:
        norm_entry_pre = remove_specsymbols(entry.lower().replace('—', '-').replace('  ', ' '))
        list_words = [x for x in norm_entry_pre.split(' ') if not is_filtered_out(x)]
        norm_entry = ' '.join([morph.parse(x)[0].normal_form for x in list_words])
        if len(norm_entry) == 0:
            continue
        result.append((entry, norm_entry))

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
    stop_words = load_stop_words(stop_words_path)
    with open('ruwiki_fixed.txt', 'r') as fin:
        wiki_titles = list(set(fin.read().splitlines()))

    wiki_titles_parts = divide_list(wiki_titles, 4)
    print('starting normalizing')
    wiki_titles_norm = []
    executor = cf.ProcessPoolExecutor(max_workers=4)
    for wiki_titles_norm_part in executor.map(normalize_strings, wiki_titles_parts):
        wiki_titles_norm += wiki_titles_norm_part
    print('done normalizing')
    
    db_cursor.execute('drop table if exists wiki_titles')
    db_conn.commit()
    db_cursor.execute('create table wiki_titles (id integer primary key, title text, title_norm text)')
    for i, title_tuple in enumerate(wiki_titles_norm):
        db_cursor.execute('insert into wiki_titles values(?,?,?)', (i, title_tuple[0], title_tuple[1]))

    db_conn.commit()
    