
from squawk.parsers.csvparser import CSVParser
from squawk.parsers.nginx import AccessLogParser
from squawk.parsers.apache import ApacheAccessLogParser

parsers = dict(
    access_log = AccessLogParser,
    apache = ApacheAccessLogParser,
    nginx = AccessLogParser,
    csv = CSVParser,
)
