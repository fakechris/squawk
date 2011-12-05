
from sqlite3 import dbapi2 as sqlite
import re

RE_SQL_CREATE = re.compile('^create\s+table\s+\w+\s*\(([^\)]+)\)$', re.I)

# 'CREATE TABLE data (a,b,c,d)'
def sqlite_schema(conn, table_name):
    sql = 'select sql from sqlite_master where type="table" and tbl_name="%s"' % table_name
    rs = conn.execute(sql).fetchall()
    return RE_SQL_CREATE.findall(rs[0][0])[0].split(',')

class SqliteReader(object):
    def __init__(self, file, tablename, sql):
        self.sql = sql
        self.conn = sqlite.connect(file)
        self.conn.text_factory = str
        self.conn.execute('pragma cache_size=100000')
        self.conn.execute('pragma synchronous=OFF')
        self.columns = [x.lower() for x in sqlite_schema(self.conn, tablename)]
        
    def __del__(self):
        self.conn.close()
        
    def __iter__(self):
        cursor = self.conn.cursor()
        c = cursor.execute(self.sql)
        while 1:
            results = c.fetchmany(1024)
            if not results:
                break
            for r in results:
                yield dict(zip(self.columns, r))
        cursor.close()
