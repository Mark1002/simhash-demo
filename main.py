"""Main file for demo."""
import hashlib
import json
from simhash import SimhashIndex, Simhash
from typing import List


def load_documents() -> List[dict]:
    """Load documents from json file."""
    with open('docs.json', 'r') as file:
        docs = json.loads(file.read())
    return docs


def make_vals_hash(doc: dict) -> str:
    """Make redis MD5ID hash values for update condition."""
    dic_copy = doc.copy()
    del dic_copy['fetched_time']
    del dic_copy['updated_time']
    vals_hash = str(dic_copy.values()).encode('utf-8')
    vals_hash = hashlib.md5(vals_hash).hexdigest()
    print(f'vals_hash {vals_hash}')
    return vals_hash


def make_simhash():
    docs = load_documents()
    for doc in docs:
        simhash = Simhash(doc['content'])
        print(simhash.value)


def load_hash_table():
    """load corpus."""
    # TODO change to load from redis
    result = {
        'md5_1': '早上8點，已從股市退休的政府4大基金前代操經理人黃豐凱，喝著香醇的咖啡',
        'md5_2': '北部一名20歲女大生昨（29）日在台中市精武車站，期間和男友講電話，不料突然從北上月台落軌',
        'md5_3': '給定一段語句，進行分詞，得到有效的特徵向量，然後為每一個特徵向量設置1-5等5個級別的權重',
    }
    result = [(k, Simhash(v)) for k, v in result.items()]
    return result


def perform_simhash_filter(doc: dict, k: int):
    """perform simhash filter."""
    result = load_hash_table()
    index = SimhashIndex(result, k=k)
    s1 = Simhash(doc['content'])
    dup_list = index.get_near_dups(s1)

    if len(dup_list) > 0:
        print(f"{doc['md5_id']} is near duplicate!")


if __name__ == '__main__':
    perform_simhash_filter(doc={
        'md5_id': 'md5_4',
        'content': '早上8點，已從股市退休的政府4大基金前代操經理人黃伯伯，喝著香醇的咖啡',
    }, k=3)
