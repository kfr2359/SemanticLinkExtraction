import json

tags_stat_path = 'tags_stat.json'
tags_count_path = 'tags_stat_count.json'

with open(tags_stat_path, 'r', encoding='utf-8-sig') as file:
    stat_dict = json.load(file)


with open(tags_count_path, 'r', encoding='utf-8-sig') as file:
    count_dict = json.load(file)

a=1