# -*- coding: utf-8 -*-
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
        print('find exist md5_id!')
        return
    r_md5_id_client.setex(doc['md5_id'], datetime.timedelta(hours=24), 1)
    bin_code = convert_simhash_bin_code(doc['content'])
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
    docs = [
        {
            'uid': 'wm',
            'link': 'https://www.wealth.com.tw/home/articles/20906',
            'author': ' 尚清林',
            'author_id': ' 尚清林',
            'poster': ' 尚清林',
            'title': '政府基金操盤手 傳授K線抓轉折的選股術',
            'category': ' 投資高手',
            'content': '早上8點，已從股市退休的政府4大基金前代操經理人黃豐凱，喝著香醇的咖啡，從101大樓內俯視信義區逐漸熱鬧的街頭，讓內心沉澱下來，帶著好心情迎接即將到來的開盤。和往常一樣，黃豐凱打開了筆電，把開盤前的想法，透過LINE族群，和投資朋友打招呼。\n自創投資軟體\u30003面向綜合選股\n曾經當過產業研究員、投信基金與政府4大基金代操經理人，累積了20幾年法人圈資歷的黃豐凱說：「放緩腳步，有時候反而想得更清楚更全面。」退去經理人的緊張忙碌，他現在的生活多一些時間留給家人，前些日子還和全家一起參加鐵人3項。在置身事外的視角下，黃豐凱反而看得更清晰，提早一步提醒投資人，躲過了這一回的美中貿易戰。\n黃豐凱會成為投資理財老師，純屬機緣巧合。退休時，有其他教學老師看中他在業界的經歷，請他一起開課，但他沒有太大興趣，只想當個快樂的「自營商」。這段期間，他還將過去操盤經驗，從總體經濟、大盤、產業趨勢與公司題材3大觀察面向，歸納成一套「GO Rich投資護照」軟體，從中判斷多空轉折。\n後來周遭的好友們建議說，不如把這套系統拿出來開課，有沒有賺錢是其次，至少可以讓朋友們懂得建立正確的投資觀。幫忙黃豐凱設計軟體的游先生就說，由於過去工作的關係，黃豐凱拜訪過很多上市櫃公司，對於每一家公司的優劣狀況大致都了解，這一點是其他老師... 閱讀全文', # noqa
            'created_time': '2019-05-29 00:00:00',
            'article_layer': 0,
            'source': 'NEWS',
            'like_count': 0,
            'dislike_count': 0,
            'share_count': 0,
            'tag': '股市,台股,K線,黃豐凱,投資術,美利達',
            'comment_count': 0,
            'doc_id': None,
            'fetched_time': '2020-04-01 15:17:38',
            'image_url': None,
            'view_count': 0,
            'md5_id': 'fa6088f5af48f2cc8e01f1daf513c3cg'
        },
        {
            'uid': 'wm',
            'link': 'https://www.wealth.com.tw/home/articles/20906',
            'author': ' 尚清林',
            'author_id': ' 尚清林',
            'poster': ' 尚清林',
            'title': '政府基金操盤手 傳授K線抓轉折的選股術',
            'category': ' 投資高手',
            'content': '早上8點，已從股市退休的政府4大基金前代操經理人黃豐凱，喝著香醇的咖啡，從101大樓內俯視信義區逐漸熱鬧的街頭，讓內心沉澱下來，帶著好心情迎接即將到來的開盤。和往常一樣，黃豐凱打開了筆電，把開盤前的想法，透過LINE族群，和投資朋友打招呼。\n自創投資軟體\u30003面向綜合選股\n曾經當過產業研究員、投信基金與政府4大基金代操經理人，累積了20幾年法人圈資歷的黃豐凱說：「放緩腳步，有時候反而想得更清楚更全面。」退去經理人的緊張忙碌，他現在的生活多一些時間留給家人，前些日子還和全家一起參加鐵人3項。在置身事外的視角下，黃豐凱反而看得更清晰，提早一步提醒投資人，躲過了這一回的美中貿易戰。\n黃豐凱會成為投資理財老師，純屬機緣巧合。退休時，有其他教學老師看中他在業界的經歷，請他一起開課，但他沒有太大興趣，只想當個快樂的「自營商」。這段期間，他還將過去操盤經驗，從總體經濟、大盤、產業趨勢與公司題材3大觀察面向，歸納成一套「GO Rich投資護照」軟體，從中判斷多空轉折。\n後來周遭的好友們建議說，不如把這套系統拿出來開課，有沒有賺錢是其次，至少可以讓朋友們懂得建立正確的投資觀。幫忙黃豐凱設計軟體的游先生就說，由於過去工作的關係，黃豐凱拜訪過很多上市櫃公司，對於每一家公司的優劣狀況大致都了解，這一點是其他老師... 閱讀全文', # noqa
            'created_time': '2019-05-29 00:00:00',
            'article_layer': 0,
            'source': 'NEWS',
            'like_count': 0,
            'dislike_count': 0,
            'share_count': 0,
            'tag': '股市,台股,K線,黃豐凱,投資術,美利達',
            'comment_count': 0,
            'doc_id': None,
            'fetched_time': '2020-04-01 15:17:38',
            'image_url': None,
            'view_count': 0,
            'md5_id': 'fa6088f5af48f2cc8e01f1daf513c3cf'
        }
    ]
    for doc in docs[:1]:
        perform_simhash_filter(doc)
