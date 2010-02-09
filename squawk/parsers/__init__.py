
from squawk.parsers.access_log import AccessLogParser
from squawk.parsers.csvparser import CSVParser
from squawk.parsers.apache import CookieAccessLogParser

parsers = dict(
    access_log = AccessLogParser,
    apache = AccessLogParser,
    apache2 = AccessLogParser,
    nginx = AccessLogParser,
    csv = CSVParser,
    cookie_log = CookieAccessLogParser,
)
