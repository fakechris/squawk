
from squawk.parsers.access_log import AccessLogParser
from squawk.parsers.csvparser import CSVParser
from squawk.parsers.apache import CookieAccessLogParser
from squawk.parsers.sqlite import SqliteReader
from squawk.parsers.sqlite2 import SqliteReader2, SqliteReader3, sql_conn, sql_columns

parsers = dict(
    access_log = AccessLogParser,
    apache = AccessLogParser,
    apache2 = AccessLogParser,
    nginx = AccessLogParser,    
    csv = CSVParser,
    cookie_log = CookieAccessLogParser,
    sqlite = SqliteReader,
    sqlite2 = SqliteReader2,
)
