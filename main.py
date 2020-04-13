# -*- coding: utf-8 -*-
"""Main file for demo."""
import datetime
import json
from redis import StrictRedis
from simhash import Simhash
from typing import List

from fake_data import fake_doc_generator

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


def perform_simhash_filter(doc: dict):
    """perform simhash filter."""
    if r_md5_id_client.exists(doc['md5_id']):
        print('find exist md5_id!')
        return
    r_md5_id_client.setex(doc['md5_id'], datetime.timedelta(hours=24), 1)
    bin_code = convert_simhash_bin_code(
        f'{doc["content"]}:{doc["created_time"]}'
    )
    print(f'new coming bin code: {bin_code}')
    bucket_len = len(bin_code) // 4
    is_duplicate = False
    for i in range(0, len(bin_code), bucket_len):
        if is_duplicate:
            break
        bucket = bin_code[i:i+bucket_len]
        if r_simhash_client.exists(bucket):
            print(f'hit bin code partition: {bucket}')
            for i in range(r_simhash_client.llen(bucket)):
                bin_code_old = r_simhash_client.lindex(bucket, i)
                bin_code_old = bin_code_old.decode()
                print(f'old exist bin code: {bin_code_old}')
                dis = compute_hmm_distance(bin_code, bin_code_old)
                if dis <= 3:
                    print('find near duplicate!')
                    # remove near duplicate doc
                    # TODO
                    is_duplicate = True
                    break
        else:
            r_simhash_client.lpush(bucket, bin_code)
            r_simhash_client.expire(bucket, datetime.timedelta(hours=24))
            print(f'create bucket{i//bucket_len}:{bucket}')


if __name__ == '__main__':
    # docs = load_documents()
    docs = fake_doc_generator(3)
    for doc in docs:
        perform_simhash_filter(doc)
