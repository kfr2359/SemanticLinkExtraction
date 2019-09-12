import json

entries_w_tags_path = 'info_w_tags.json'
tags_stat_path = 'tags_stat.json'
tags_count_path = 'tags_stat_count.json'

def extract_grnti_id(input_dict: dict, output_dict: dict):
    for id, entry in input_dict.items():
        if entry.get('GRNTI') is None:
            continue
        for grnti in entry['GRNTI']:
            grnti = grnti[:8]
            output_dict['GRNTI'].setdefault(grnti, []).append(id)
    pass


def extract_tags_id_count(input_dict: dict, output_dict: dict):
    for id, entry in input_dict.items():
        for tag in entry['tags']:
            output_dict['tags'].setdefault(tag, {})
            if output_dict['tags'][tag].get(id) is None:
                output_dict['tags'][tag][id] = 1
            else:
                output_dict['tags'][tag][id] += 1
        

def count_total_tag_count(output_dict: dict):
    for tag, value in output_dict['tags'].items():
        count = 0
        for sub_count in value.values():
            count += sub_count
        output_dict['tags'][tag]['count'] = count


if __name__ == '__main__':
    with open(entries_w_tags_path, 'r', encoding='utf-8-sig') as file:
        input_dict = json.load(file)
    
    stat_dict = {
        'GRNTI': {},
        'tags': {}
    }
    extract_grnti_id(input_dict, stat_dict)
    extract_tags_id_count(input_dict, stat_dict)
    count_total_tag_count(stat_dict)
    with open(tags_stat_path, 'w+') as file:
        json.dump(stat_dict, file, indent=2)

    count_dict = {
        'GRNTI': {},
        'tags': {}
    }
    for k, v in stat_dict['GRNTI'].items():
        count_dict['GRNTI'].setdefault(len(v), []).append(k)
    
    for k, v in stat_dict['tags'].items():
        count_dict['tags'].setdefault(v['count'], []).append(k)

    count_dict['GRNTI'] = dict(sorted(count_dict['GRNTI'].items(), reverse=True))
    count_dict['tags'] = dict(sorted(count_dict['tags'].items(), reverse=True))
    with open(tags_count_path, 'w+') as file:
        json.dump(count_dict, file, indent=2)