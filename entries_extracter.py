import xml.etree.ElementTree as ET
import json
import sqlite3
import string


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


def has_russian(input: str) -> bool:
    alphabet = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
    for letter in input.lower():
        if letter in alphabet:
            return True
    return False


def has_old_russian(input: str) -> bool:
    alphabet = 'ѣѵѳ'
    result = False
    for letter in input.lower():
        if letter in alphabet:
            result = True
            break

    if not result:
        for letter in input:
            if letter == 'i':
                result = True
                break

    return result


def has_western(input: str) -> bool:
    alphabet_not_num = 'abefghjknopqrstuwyzäöüßæâçéèêëîïôœùûüÿ'
    
    has_num = False

    for letter in input.lower():
        if letter in alphabet_not_num:
            return True

    return False


def get_lang(contents_entries: list) -> str:
    contents_str = ''.join(contents_entries)
    if has_russian(contents_str) and not has_old_russian(contents_str):
        return 'russian'
    elif has_russian(contents_str) and has_old_russian(contents_str):
        return 'slavenica'
    elif has_western(contents_str):
        return 'western'
    else:
        return 'nolang'


def extract_info(store: list, db_cursor: sqlite3.Cursor):
    for id, msc, description, contents in store:
        title = ''
        GRNTIs = []
        if description is None or description == '':
            continue

        try:
            root = ET.fromstring(description)
            root = root[0]
        except Exception:
            root = None
        
        if root is None:
            print(f'description for id {id} isn\'t parsed')
        else:
            # book title
            title_entry = root.find('isir:mainTitle/rdf:value', ns)
            title = title_entry.text if title_entry is not None else ''

            # GRNTI
            for is_gathered_into in root.findall('aux:isGatheredInto', ns):
                GRNTI = is_gathered_into.find('dc:title/rdf:value', ns).text
                GRNTIs.append(GRNTI)
        
        contents_entries = extract_contents_entries(contents, id)
        lang = get_lang(contents_entries)
        db_cursor.execute('INSERT INTO entries VALUES(?,?,?,?,?,?)', (id, msc, title, '\n'.join(GRNTIs), '\n'.join(contents_entries), lang))


def _find_rec(node, element):
    for item in node.findall(element):
        yield item
        for child in _find_rec(item, element):
            yield child


def extract_contents_entries(contents: str, id: str) -> list:
    contents_entries = []
    if not contents:
        print(f'contents for id {id} aren\'t parsed: contents is empty')
        return contents_entries

    try:
        root = ET.fromstring(contents)
    except Exception as e:
        print(f'contents for id {id} aren\'t parsed: {e}')
        return contents_entries

    root = root.find('content')
    if root is None:
        print(f'contents for id {id} aren\'t parsed: root is None')
        return contents_entries
    
    for element in _find_rec(root, 'el'):
        if element.text is not None:
            contents_entries.append(element.text)
    
    return contents_entries

    
if __name__ == '__main__':
    db_conn = sqlite3.connect('data_final.db')
    db_cursor = db_conn.cursor()

    db_cursor.execute('drop table if exists entries')
    db_conn.commit()
    db_cursor.execute('CREATE TABLE IF NOT EXISTS entries (id integer PRIMARY KEY, msc integer, title text, GRNTI text, contents_entries text, lang text)')
    db_conn.commit()

    with open('cyberleninka.json', 'r') as fin:
        input_texts = json.load(fin)
    for i, item in enumerate(input_texts):
        db_cursor.execute('INSERT INTO entries VALUES(?,?,?,?,?,?)', (i, 0, item.get('title', ''), '\n'.join(item.get('keywords', [])), item.get('abstract', ''), 'russian'))

    # db_cursor.execute('SELECT * from `raw_data`')
    # store = db_cursor.fetchall()
    
    # extract_info(store, db_cursor)
    db_conn.commit()