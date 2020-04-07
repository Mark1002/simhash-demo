"""Main file for demo."""
import datetime
import json
from redis import StrictRedis
from simhash import Simhash
from typing import List

r_md5_id_client = StrictRedis(db=2)
r_simhash_client = StrictRedis(db=3)


def load_documents() -> List[dict]:
    """Load documents from json file."""
    with open('docs.json', 'r') as file:
        docs = json.loads(file.read())
    return docs


def convert_simhash_bin_code(doc: str) -> str:
    """Convert to simhash bin code."""
    bin_code = format(Simhash(doc).value, '064b')
    return bin_code


def compute_hmm_distance(bin1: str, bin2: str) -> int:
    """compute hmm distance."""
    assert len(bin1) == len(bin2)
    dis = sum([int(bit1 != bit2) for bit1, bit2 in zip(bin1, bin2)])
    return dis


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


def perform_simhash_filter(doc: dict):
    """perform simhash filter."""
    if r_md5_id_client.exists(doc['md5_id']):
        return
    r_md5_id_client.setex(doc['md5_id'], datetime.timedelta(hours=24), 1)
    bin_code = convert_simhash_bin_code(doc['content'])
    bucket_len = len(bin_code) // 4
    for i in range(0, len(bin_code), bucket_len):
        bucket = bin_code[i:i+bucket_len]
        print(f'bucket{i//bucket_len}:{bucket}')
        if r_simhash_client.exists(bucket):
            for i in range(r_simhash_client.llen(bucket)):
                bin_code = r_simhash_client.lindex(bucket, i)
                bin_code = bin_code.decode()
                print(bin_code)
        else:
            r_simhash_client.lpush(bucket, bin_code)
            r_simhash_client.expire(bucket, datetime.timedelta(hours=24))
    pass


if __name__ == '__main__':
    docs = load_documents()
    for doc in docs[:1]:
        perform_simhash_filter(doc)
