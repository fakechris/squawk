
from squawk.parsers.csvparser import CSVParser
from squawk.parsers.nginx import AccessLogParser
from squawk.parsers.apache import CookieAccessLogParser

parsers = dict(
    access_log = AccessLogParser,
    apache = AccessLogParser,
    nginx = AccessLogParser,
    csv = CSVParser,
    cookie_log = CookieAccessLogParser,
)
