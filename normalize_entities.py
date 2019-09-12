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


def normalize_tuple_list(tuples: list, indices: []) -> list:
    result_dict = {}
    for i in range(len(tuples[0])):
        subinput = [x[i] for x in tuples]
        subresult = normalize_strings(subinput) if i in indices else subinput
        result_dict[i] = subresult
    
    result = []
    for i in range(len(tuples)):
        entry = []
        for i_tuple in range(len(tuples[0])):
            entry.append(result_dict[i_tuple][i])
        result.append(tuple(entry))

    return result
            

def normalize_strings(strings: list) -> list:
    result = []
    for entry in strings:
        norm_entry_pre = remove_specsymbols(entry.lower().replace('—', '-').replace('  ', ' '))
        norm_entry = ''
        for line in norm_entry_pre.split('\n'):
            list_words = [x for x in line.split(' ') if not is_filtered_out(x)]
            norm_entry += ' '.join([morph.parse(x)[0].normal_form for x in list_words]) + '\n'
        if not norm_entry:
            continue
        norm_entry = norm_entry[:-1]
        result.append(norm_entry)

    return result


def divide_list(total: list, num_parts: int):
    parts = []
    split_idx = len(total) // num_parts
    for i in range(num_parts):
        parts.append(total[split_idx*(i) : split_idx*(i+1)])
    
    return parts


if __name__ == '__main__':
    stop_words = load_stop_words(stop_words_path)
    db_conn = sqlite3.connect('data_final.db')
    db_cursor = db_conn.cursor()
    db_cursor.execute('select * from entries')
    entries = db_cursor.fetchall()

    entries_parts = divide_list(entries, 4)
    print('starting normalizing')
    norm_entries = []
    executor = cf.ProcessPoolExecutor(max_workers=4)
    for norm_entries_part in executor.map(normalize_tuple_list, entries_parts, [[2,4] for _ in range(4)]):
        norm_entries += norm_entries_part
    print('done normalizing')
    
    db_cursor.execute('drop table if exists entries_norm')
    db_conn.commit()
    db_cursor.execute('create table entries_norm (id integer primary key, title text, contents_entries text)')
    for i, entry_tuple in enumerate(norm_entries):
        db_cursor.execute('insert into entries_norm values(?,?,?)', (entry_tuple[0], entry_tuple[2], entry_tuple[4]))

    db_conn.commit()
    