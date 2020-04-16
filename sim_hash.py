# -*- coding: utf-8 -*-
"""Main file for demo."""
import datetime
import json
from redis import StrictRedis
from simhash import Simhash

from fake_data import fake_doc_generator, load_documents
from urllib.parse import urlparse

r_md5_id_client = StrictRedis(db=2)
r_simhash_client = StrictRedis(db=3)


def convert_simhash_bin_code(doc: str) -> str:
    """Convert to simhash bin code."""
    bin_code = format(Simhash(doc).value, '064b')
    return bin_code


def compute_hmm_distance(bin1: str, bin2: str) -> int:
    """compute hmm distance."""
    assert len(bin1) == len(bin2)
    dis = sum([int(bit1 != bit2) for bit1, bit2 in zip(bin1, bin2)])
    return dis


def perform_simhash_filter(doc: dict) -> int:
    """perform simhash filter."""
    print(f'created_time: {doc["created_time"]}')
    if r_md5_id_client.exists(doc['md5_id']):
        print('find exist md5_id!')
        return 0
    r_md5_id_client.setex(doc['md5_id'], datetime.timedelta(hours=24), 1)
    bin_code = convert_simhash_bin_code(
        f'{doc["content"]}:{doc["created_time"]}'
    )
    print(f'new coming bin code: {bin_code}')
    # channel net url
    netloc = urlparse(doc['link']).netloc
    bucket_len = len(bin_code) // 4
    is_duplicate = False
    # query all 4 parts 16 bits partition
    for i in range(0, len(bin_code), bucket_len):
        if is_duplicate:
            return 1
        bucket = bin_code[i:i+bucket_len]
        # hit this 16 bits bucket
        if r_simhash_client.exists(bucket):
            print(f'hit bin code partition: {bucket}')
            # find this 16 bits bucket one by one
            for i in range(r_simhash_client.llen(bucket)):
                d = json.loads(r_simhash_client.lindex(bucket, i))
                bin_code_old = d['bin_code']
                print(f'old exist bin code: {bin_code_old}')
                dis = compute_hmm_distance(bin_code, bin_code_old)
                if dis <= 3 and d['netloc'] == netloc:
                    print('find near duplicate!')
                    # remove near duplicate doc
                    r_md5_id_client.delete(doc['md5_id'])
                    # TODO
                    is_duplicate = True
                    break
        else:
            d = {
                'bin_code': bin_code,
                'netloc': netloc
            }
            r_simhash_client.lpush(bucket, json.dumps(d))
            r_simhash_client.expire(bucket, datetime.timedelta(hours=24))
            print(f'create bucket{i//bucket_len}:{bucket}')
    # miss all 16 bits bucket
    return 0


if __name__ == '__main__':
    dup_num = 0
    # docs = load_documents()
    docs = fake_doc_generator(num=100)
    for doc in docs:
        dup_num += perform_simhash_filter(doc)
    print(f'near duplicate number: {dup_num}')
