import BookInfoDownloader as bid
import xml.etree.ElementTree as ET
import json

id_contents_descr_path = 'id_contents_descr.json'

def unite_book_info_files():
    id_msc_pairs = bid.get_id_msc_pairs()
    id_contents_descr_dict = dict()

    print(f'total: {len(id_msc_pairs)}')
    i = 0
    for id, msc in id_msc_pairs.items():
        contents_path = bid.book_contents_path_str.format(msc)
        description_path = bid.book_description_path_str.format(id)
        
        with open(contents_path, 'r', encoding='utf-8-sig') as contents_file, open(description_path, 'r') as description_file:
            id_contents_descr_dict[id] = {
                'msc': msc,
                'contents': contents_file.read(),
                'description': description_file.read()
            }
        
        i = i + 1
        if i % 100 == 0:
            print(f'processed {i}')

    with open(id_contents_descr_path, 'w+', encoding='utf-8-sig') as output:
        json.dump(id_contents_descr_dict, output, indent=2)


if __name__ == '__main__':
    unite_book_info_files()