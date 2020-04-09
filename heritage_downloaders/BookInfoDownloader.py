import os
from concurrent.futures import ThreadPoolExecutor as PoolExecutor

import requests

listIdsFilename = '/home/kfr2359/msufldr/specsem 11 term/pub_id_msc.txt'
descrURLBase = 'http://e-heritage.ru/ras/view/publication/general.xml?id='
contentsURLBase = 'http://books.e-heritage.ru/Book/Book/getCt/15'

book_contents_path_str = 'book_contents/{}.xml'
book_description_path_str = 'book_description/{}.rdf'


def get_id_msc_pairs():
    idMSCpairs = {}
    with open(listIdsFilename, "r") as fListIds:
        lines = fListIds.readlines()

        # remove first line because it's the name of column
        lines = lines[1:-1]
        
        for line in lines:
            pair = line.split('\t')
            pair[1] = pair[1].replace('\n', '')
            idMSCpairs[pair[0]] = pair[1]
    return idMSCpairs


def fetch_description(id):
    url = descrURLBase + id
    filename = book_description_path_str.format(id)
    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        with open(filename, 'w+') as file:
            file.write(requests.get(url).text)
    

def fetch_contents(msc):
    url = contentsURLBase
    filename = book_contents_path_str.format(msc)
    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        variables = {
            'bok': msc
        }
        with open(filename, 'w+') as file:
            file.write(requests.post(url, data=variables).content.decode('utf-8'))


def download_book_info(id_msc_pairs):    
    print(f'total: {len(id_msc_pairs)}')
    with PoolExecutor(max_workers=8) as executor:
        for _ in executor.map(fetch_description, id_msc_pairs.keys()):
            pass
    
    with PoolExecutor(max_workers=8) as executor:
        for _ in executor.map(fetch_contents, id_msc_pairs.values()):
            pass


if __name__ == '__main__':
    id_msc_pairs = get_id_msc_pairs()

    download_book_info(id_msc_pairs)
