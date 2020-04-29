# -*- coding: utf-8 -*-
"""Fake data for near duplicate."""
import copy
import datetime
import json
import random
import secrets

from typing import List


def load_documents(
    word_lower_size: int, word_upper_size: int
) -> List[dict]:
    """Load documents from json file."""
    with open('data/docs.json', 'r') as file:
        docs = json.loads(file.read())
    results = []
    for doc in docs:
        if word_lower_size < len(str(doc['content'])) < word_upper_size:
            results.append(doc)
    return results


def gen_datetime(min_year=1900, max_year=datetime.datetime.now().year):
    """Random gen datetime."""
    start = datetime.datetime(min_year, 1, 1, 00, 00, 00)
    years = max_year - min_year + 1
    end = start + datetime.timedelta(days=365 * years)
    return start + (end - start) * random.random()


def gen_random_fid() -> str:
    """Gen random fid."""
    fids = ['wm', 'yahoo', 'lt', 'apple']
    return fids[random.randint(0, len(fids)-1)]


def fake_doc_generator(
    docs: list, num: int,
    same_doc: bool = False,
) -> iter:
    """Fake gen."""
    doc = docs[random.randint(0, len(docs)-1)]
    for _ in range(num):
        if same_doc:
            doc = copy.copy(doc)
        else:
            doc = docs[random.randint(0, len(docs)-1)]
        doc['created_time'] = gen_datetime(min_year=2019).strftime("%Y-%m-%d %H:%M:%S") # noqa
        doc['md5_id'] = secrets.token_hex(nbytes=16)
        yield doc
