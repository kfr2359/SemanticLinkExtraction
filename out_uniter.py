import json

total_dict = {}
for i in range(1,5):
    with open(f'out{i}.json', 'r', encoding='utf-8-sig') as file:
        total_dict.update(json.load(file)) 

with open('out.json' ,'w+') as file:
    json.dump(total_dict, file, indent=2)