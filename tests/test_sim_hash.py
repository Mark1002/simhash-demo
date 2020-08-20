"""Tests for simhash filter."""
import secrets
from redis import StrictRedis
import logging
from .fake_data import fake_doc_generator, load_documents
from sim_hash_demo import perform_simhash_filter


def setup_function(function):
    """Set up function."""
    r = StrictRedis()
    r.flushall()


def test_same_hash_but_differ_netloc():
    """Test the same hash but differ netloc."""
    dup_num = 0
    doc = list(fake_doc_generator(docs=load_documents(0, 9999), num=1))[0]
    dup_num += perform_simhash_filter(doc)
    # make up differ netloc
    doc['link'] = 'https://www.yahoo.com/post123'
    doc['md5_id'] = secrets.token_hex(nbytes=16)
    dup_num += perform_simhash_filter(doc)
    assert dup_num == 0


def _test_duplicate_docs(lower: int, upper: int):
    """Test fake duplicate docs logic."""
    # add unique docs
    docs = load_documents(lower, upper)
    for doc in docs:
        perform_simhash_filter(doc, hmm_dis=3)
    # add near duplicate docs
    dup_counter = 0
    dup_num = 1000
    docs = fake_doc_generator(docs=docs, num=dup_num)
    for doc in docs:
        dup_counter += perform_simhash_filter(doc, hmm_dis=3)
    logging.debug(f'near duplicate number: {dup_counter}, hit rate: {dup_counter / dup_num}') # noqa


def test_duplicate_long_docs():
    """Test for long duplicate docs."""
    _test_duplicate_docs(600, 9999)


def test_duplicate_short_docs():
    """Test for short duplicate docs."""
    _test_duplicate_docs(0, 100)


def teardown_function(function):
    """Tear down function."""
    r = StrictRedis()
    r.flushall()
