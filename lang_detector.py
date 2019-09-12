import sqlite3
import xml.etree.ElementTree as ET
import string


def has_russian(input: str) -> bool:
    alphabet = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
    for letter in input:
        if letter in alphabet:
            return True
    return False


def has_old_russian(input: str) -> bool:
    alphabet = 'іѣѵѳ'
    has_common_russian = has_russian(input)
    if not has_common_russian:
        return False
    for letter in input:
        if letter in alphabet:
            return True
    return False

def has_western(input: str) -> bool:
    alphabet = 'äöüßàâæçéèêëîïôœùûüÿ' + string.ascii_lowercase
    for letter in input:
        if letter in alphabet:
            return True
    return False


def _find_rec(node, element):
    for item in node.findall(element):
        yield item
        for child in _find_rec(item, element):
            yield child


if __name__ == '__main__':
    db_conn = sqlite3.connect('data.db')
    db_cursor = db_conn.cursor()

    db_cursor.execute('SELECT * from `raw_data`')
