import json

if __name__ == '__main__':
    with open('gold_keywords.json', 'r') as fin:
        gold_keywords = json.load(fin)

    with open('result_keywords.json', 'r') as fin:
        result_keywords = json.load(fin)

    numberCorrect = 0
    numberExtracted = 0
    numberTotal = sum(len(v.split('\n')) for v in gold_keywords.values())

    for id_entry, keywords in result_keywords.items():
        numberExtracted += len(keywords)
        for k in keywords:
            if k.lower() in gold_keywords[id_entry].lower():
                numberCorrect += 1
    
    print(numberCorrect / numberExtracted * 100)
    print(numberCorrect / numberTotal * 100)