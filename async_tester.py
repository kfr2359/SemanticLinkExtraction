import aiohttp
import asyncio

KEY = 'trnsl.1.1.20190528T171228Z.2d44022cc37b4e9d.9857d7bbf22d82b2202e4f4fc1f320b9e6dd0c51'
TEXT = 'here we go again\nhere we go again\nhere we go again\nhere we go again\nhere we go again\n'

async def run():
    async with aiohttp.ClientSession() as session:
        url = 'https://translate.yandex.net/api/v1.5/tr.json/translate'
        params = {
            'key': KEY,
            'text': TEXT,
            'lang': 'ru',
            'format': 'html'
        }
        async with session.get(url, params=params) as resp:
            print(resp.status)
            print(await resp.text())

loop = asyncio.get_event_loop()

future = asyncio.ensure_future(run())
loop.run_until_complete(future)