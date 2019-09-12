import requests
import json
import sqlite3

# menkeev
# KEY = 'trnsl.1.1.20190908T130258Z.9d6fbc747c6a5f10.72976d74dc56e00bb859db9b960543cf80b79571'
KEY = 'trnsl.1.1.20190908T130258Z.9d6fbc747c6a5f10.72976d74dc56e00bb859db9b960543cf80b79571'

import aiohttp
import asyncio

async def translate(input: str, session: aiohttp.ClientSession) -> str:
    url = 'https://translate.yandex.net/api/v1.5/tr.json/translate'
    params = {
        'key': KEY,
        'text': input,
        'lang': 'ru',
        'format': 'html'
    }
    async with session.post(url, data=params) as resp:
        try:
            text = await resp.text()
            result = json.loads(text)
        except Exception as e:
            print(f'{text}|{resp.status}{e}')
            return input
        return result['text'][0]

async def bound_fetch(sem, id, input, session) -> str:
    async with sem:
        return await translate(input, session), id

async def run(store):
    print('started')
    sem = asyncio.Semaphore(5)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for entry in store:
            tasks.append(asyncio.create_task(bound_fetch(sem, entry[0], entry[2], session)))
        return await asyncio.gather(*tasks)


if __name__ == '__main__':
    db_conn = sqlite3.connect('data_final.db')
    db_cursor = db_conn.cursor()

    db_cursor.execute('SELECT * from `entries` WHERE lang="nolang"')
    store = db_cursor.fetchall()

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(run(store))
    for entry in result:
        db_cursor.execute('UPDATE `entries` SET title=? WHERE id=?', entry)
    db_conn.commit()
