# -*- coding: utf-8 -*-
"""Main file for demo."""
import datetime
import json
from log_service import logging
from redis import StrictRedis
from simhash import Simhash

from fake_data import load_documents, fake_doc_generator
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


def perform_simhash_filter(doc: dict, hmm_dis: int = 3) -> int:
    """Perform simhash filter."""
    if r_md5_id_client.exists(doc['md5_id']):
        logging.debug('find exist md5_id!')
        return 0
    r_md5_id_client.setex(doc['md5_id'], datetime.timedelta(hours=24), 1)
    # channel net url
    netloc = urlparse(doc['link']).netloc
    bin_code = convert_simhash_bin_code(
        f'{doc["created_time"]}:{doc["content"]}:{doc["article_layer"]}:{doc["source"]}:{doc["uid"]}:{doc["author_id"]}:{netloc}:{doc["title"]}' # noqa
    )
    logging.debug(f'new coming bin code: {bin_code}')
    bucket_len = len(bin_code) // 4
    # query all 4 parts 16 bits partition
    for i in range(0, len(bin_code), bucket_len):
        bucket = bin_code[i:i + bucket_len]
        # hit this 16 bits bucket
        if r_simhash_client.exists(bucket):
            logging.debug(f'hit bin code partition: {bucket}')
            # find this 16 bits bucket one by one
            for i in range(r_simhash_client.llen(bucket)):
                d = json.loads(r_simhash_client.lindex(bucket, i))
                bin_code_old = d['bin_code']
                logging.debug(f'old exist bin code: {bin_code_old}')
                dis = compute_hmm_distance(bin_code, bin_code_old)
                logging.debug(f'distance: {dis}')
                if dis <= hmm_dis and d['netloc'] == netloc:
                    logging.warning(f"find near duplicate on md5_id: {d['md5_id']}") # noqa
                    # remove near duplicate doc
                    r_md5_id_client.delete(doc['md5_id'])
                    if r_md5_id_client.setnx('dup_counter', 0):
                        r_md5_id_client.expire('dup_counter', datetime.timedelta(hours=24)) # noqa
                    r_md5_id_client.incr('dup_counter')
                    logging.warning(f"duplicate count: {r_md5_id_client.get('dup_counter').decode()}") # noqa
                    return 1
        else:
            d = {
                'bin_code': bin_code,
                'netloc': netloc,
                'md5_id': doc['md5_id']
            }
            r_simhash_client.lpush(bucket, json.dumps(d))
            r_simhash_client.expire(bucket, datetime.timedelta(hours=24))
            logging.debug(f'create bucket{i//bucket_len}:{bucket}')
    # miss all 16 bits bucket
    return 0


if __name__ == '__main__':
    dup_num = 0
    docs = load_documents(400, 500)
    docs = fake_doc_generator(docs=docs, num=100)
    for doc in docs:
        dup_num += perform_simhash_filter(doc)
    logging.debug(f'near duplicate number: {dup_num}')
