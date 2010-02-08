import re

log_re = re.compile(
    r'^(?P<remote_addr>\S+)'
    r" -"
    r" (?P<remote_user>[^\s]+)"
    r" \[(?P<time>[^\]]+)\]"
    r'\s+"(?P<method>\w{3,4}) '
    r'(?P<request>[^ ]+)'    
	r' [^"]+"'	
    r" (?P<status>[^\s]+)"
    r" (?P<bytes>[^\s]+)"
    r'\s+"(?P<referrer>[^"]*)"'
    r'\s+"(?P<user_agent>[^"]*)"'
    r'\s+(?P<user_cookie>\S+)'
    r".*$")

class CookieAccessLogParser(object):
    def __init__(self, file):
        if isinstance(file, basestring):
            self.fp = open(file, "rb")
        else:
            self.fp = file

    def __iter__(self):
        for line in self.fp:
            m = log_re.match(line.strip())
            d = m.groupdict()
            try:    
                d['bytes'] = int(d['bytes'])
            except:
                d['bytes'] = 0
            d['status'] = int(d['status'])
            yield d

    def all_fields(self):
        return "remote_addr, remote_user, time, method, request, status, bytes, referrer, user_agent, user_cookie"
