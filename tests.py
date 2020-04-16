"""Tests for simhash filter."""
import secrets
from redis import StrictRedis
from fake_data import fake_doc_generator
from sim_hash import perform_simhash_filter


def setup_function(function):
    """Set up function."""
    r = StrictRedis()
    r.flushall()


def test_same_hash_but_differ_netloc():
    """Test the same hash but differ netloc."""
    dup_num = 0
    doc = list(fake_doc_generator(1))[0]
    dup_num += perform_simhash_filter(doc)
    # make up differ netloc
    doc['link'] = 'https://www.yahoo.com/post123'
    doc['md5_id'] = secrets.token_hex(nbytes=16)
    dup_num += perform_simhash_filter(doc)
    assert dup_num == 0


def teardown_function(function):
    """Tear down function."""
    r = StrictRedis()
    r.flushall()
