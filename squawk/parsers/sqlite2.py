
from sqlite3 import dbapi2 as sqlite
import re

RE_SQL_CREATE = re.compile('^create\s+table\s+\w+\s*\(([^\)]+)\)$', re.I)

# 'CREATE TABLE data (a,b,c,d)'
def sqlite_schema(conn, table_name):
    sql = 'select sql from sqlite_master where type="table" and tbl_name="%s"' % table_name
    rs = conn.execute(sql).fetchall()
    return RE_SQL_CREATE.findall(rs[0][0])[0].split(',')

def sql_conn(file):
    conn = sqlite.connect(file)
    conn.text_factory = str
    conn.execute('pragma cache_size=100000')
    conn.execute('pragma synchronous=OFF')
    return conn

def sql_columns(conn, tablename):
    return [x.lower() for x in sqlite_schema(conn, tablename)]

def init_sqlite_table(cur, cols, uniqkey, idxlst, tablename = "data"):
    """init table, index, unique key
    Arguments:
    - cur:
    - cols: "f1,f2,f3,f4"  or ["f1", "f2", "f3", "f4"]
    - uniqkey: "f1,f2,f3,f4"  or ["f1", "f2", "f3", "f4"]
    - idxlst: "f1.f2,f3.f4" or ["f1.f2", "f3.f4"]
    """
    if isinstance(cols, str):
        cols = cols.split(",")
    try:
        cur.execute('create table %s (%s)'%(tablename, ','.join(cols)))
    except Exception,e:
        if e.message != 'table %s already exists' % tablename:
            print e.message
            raise e

    if uniqkey:
        if isinstance(uniqkey, str):
            uniqkey = uniqkey.split(",")
        cur.execute('drop index if exists %s.idx ' % (tablename))
        uniq_sql = ('create unique index if not exists '
                    'uniq_idx on %s (%s)'
                    ) % (tablename, ','.join(uniqkey))

        cur.execute(uniq_sql)

    if isinstance(idxlst, str):
        idxlst = idxlst.split(",")
    for idx in idxlst:
        sql = 'create index if not exists idx_%s on %s (%s)' % (idx.replace('.' , '_'),
                                                                tablename,
                                                                ','.join(idx.split('.'))
                                                                )
        cur.execute(sql)

class SqliteReader(object):
    def __init__(self, file, tablename, sql):
        self.sql = sql
        self.conn = sql_conn(file)        
        self.columns = sql_columns(self.conn, tablename)

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
                yield r
        cursor.close()

class SqliteReader2(object):
    def __init__(self, conn, cols, sql):
        self.sql = sql
        self.conn = conn        
        self.columns = cols

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
                yield r
        cursor.close()

