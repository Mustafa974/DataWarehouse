# settings of headers
ACCEPT = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8'
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) ' \
             'Chrome/69.0.3497.81 Safari/537.36'
ACCEPT_ENCODING = 'gzip, deflate, br'
ACCEPT_LANGUAGE = 'zh-CN,zh;q=0.9'
CACHE_CONTROL = 'max-age=0'
HEADERS = {
    'accept': ACCEPT,
    'user-agent': USER_AGENT,
    'accept-encoding': ACCEPT_ENCODING,
    'accept-language': ACCEPT_LANGUAGE,
    'cache-control': CACHE_CONTROL
}


# proxy pool
PROXY_POOL_URL = 'http://127.0.0.1:5000/get'


# mongodb
# htl
HLT_USER_str = '192.168.1.101'
HLT_DB_str = 'DWClass'
COL_MovieWithAttr_str = 'MovieWithAttr'
# wtx
WTX_USER_str = '192.168.1.100'
WTX_DB_str = 'amazon'
# WTX_DB_str = 'backup'
# collections
COL_INDEX_str = 'index'
COL_TXT_str = 'txt'
COL_SOURCE_str = 'source'
COL_OTHER_PAGE_str = 'other_page'
COL_NOT_MOVIE_str = 'not_movie'
COL_NOT_FOUND_str = 'not_found'
COL_DETAIL_ALL_str = 'detail_all'
COL_VERSION_str = 'version'

# hlt keys
COMMENTS = 'Comments'
TYPE = 'type'
MOVIE_NAME = 'movieName'
RELEASE_TIME = 'releaseTime'
# wtx keys
REVIEW = 'review'
PAGE = 'page'
TITLE = 'title'
TIME = 'time'
COUNT = 'count'
REASON = 'reason'
MARK = 'mark'
CHECKED = 'checked'
REF_URL = 'ref_url'
# same keys
_ID = '_id'
URL = 'url'
ACTORS = 'actors'
DIRECTORS = 'directors'
GENRES = 'genres'
RATED = 'rated'


# kinds of page
WHITE = 0
BLACK = 1
OTHER = 2
ROBOT = 3
DOG = 4
WHITEw = 'W'
BLACKb = 'B'

# mysql configuration
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306
MYSQL_USR = 'root'
MYSQL_PWD = 'Mustafa.17'
MYSQL_DB = 'AmazonMovie'








