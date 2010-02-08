import re, time

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
        
        self.columns = [x[0] for x in sorted(log_re.groupindex.items(), key=lambda g:g[1])]

    def __iter__(self):
        for line in self.fp:
            m = log_re.match(line.strip())
            d = m.groupdict()
            d['time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(d['time'], '%d/%b/%Y:%H:%M:%S +0800'))
            try:    
                d['bytes'] = int(d['bytes'])
            except:
                d['bytes'] = 0
            d['status'] = int(d['status'])
            yield d

