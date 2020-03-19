import json
import multiprocessing as mp
import os
import sqlite3
import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

USER_AGENT = 'Mozilla/5.0 (Linux; U; Android 2.3; en-us) AppleWebKit/999+ (KHTML, like Gecko) Safari/999.9'
GECKODRIVER_PATH = './geckodriver'

INPUT_SELECTOR = 'slovo'
OUTPUT_SELECTOR = 'otvet'
FROM_LANG_SELECTOR = 's_chosen'
TO_LANG_SELECTOR = 'na_chosen'
CONVERT_ID = 'post'
PETRO_SELECTOR = 'div#s_chosen > div > ul > li[data-option-array-index=\'30\']'
NOV_SELECTOR = 'div#na_chosen > div > ul > li[data-option-array-index=\'34\']'

xml_step = 2


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


def get_id_contents_descr():
    db_conn = sqlite3.connect('data_final.db')
    db_cursor = db_conn.cursor()

    db_cursor.execute('SELECT id, contents_entries from `entries` WHERE lang="slavenica"')
    store = db_cursor.fetchall()
    result = {k: v for k, v in store if has_old_russian(v)}
    return result


def process_contents(store, path):
    pid = os.getpid()
    print(f'process {path} - {pid}')

    START_FROM = 0
    cnt = 0
    input_str = ''
    # ids_list = []
    driver = None
    for id in store.keys():
        if cnt % 30 == 0:
            if driver is not None:
                driver.close()

            options = Options()
            options.add_argument('--headless')
            driver = webdriver.Firefox(executable_path=GECKODRIVER_PATH, options=options)
            
        driver.get('http://slavenica.com/')

        spellchecker_elem = driver.find_element_by_id('P')
        spellchecker_elem.click()

        from_lang_elem = driver.find_element_by_id(FROM_LANG_SELECTOR)
        from_lang_elem.click()
        wait = WebDriverWait(driver, 5)
        petro_elem = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, PETRO_SELECTOR)))
        petro_elem.click()

        to_lang_elem = driver.find_element_by_id(TO_LANG_SELECTOR)
        to_lang_elem.click()
        nov_elem = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, NOV_SELECTOR)))
        nov_elem.click()

        output_elem = driver.find_element_by_id(OUTPUT_SELECTOR)
        post_elem = driver.find_element_by_id(CONVERT_ID)

        input_str = store[id]

        input_elem = wait.until(EC.element_to_be_clickable((By.ID, INPUT_SELECTOR)))
        input_elem.click()
        input_elem.send_keys(Keys.CONTROL, 'a')
        input_elem.send_keys(Keys.BACKSPACE)
        input_elem.send_keys(input_str)

        while output_elem.text == '' and input_str:
            time.sleep(0.5)
            
        store[id] = output_elem.text

        cnt += 1

        if cnt % 5 == 0:
            print(f'{path} processed {cnt}')

    with open(path, 'w+') as file:
        json.dump(store, file, indent=2)
    driver.close()
 

if __name__ == '__main__':
    store = get_id_contents_descr()
    print(len(store))
    split_idx = len(store) // 4
    d1 = dict(list(store.items())[:split_idx])
    d2 = dict(list(store.items())[split_idx:split_idx*2])
    d3 = dict(list(store.items())[split_idx*2:split_idx*3])
    d4 = dict(list(store.items())[split_idx*3:])

    p1 = mp.Process(target=process_contents, args=(d1,'out1.json'))
    p2 = mp.Process(target=process_contents, args=(d2,'out2.json'))
    p3 = mp.Process(target=process_contents, args=(d3,'out3.json'))
    p4 = mp.Process(target=process_contents, args=(d4,'out4.json'))
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    while p1.is_alive() or p2.is_alive() or p3.is_alive() or p4.is_alive():
        time.sleep(5)

    pass