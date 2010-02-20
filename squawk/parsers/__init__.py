
from squawk.parsers.access_log import AccessLogParser
from squawk.parsers.csvparser import CSVParser
from squawk.parsers.apache import CookieAccessLogParser
from squawk.parsers.sqlite import SqliteReader

parsers = dict(
    access_log = AccessLogParser,
    apache = AccessLogParser,
    apache2 = AccessLogParser,
    nginx = AccessLogParser,
    csv = CSVParser,
    sqlite = SqliteReader,
)
