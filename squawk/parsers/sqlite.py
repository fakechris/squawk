
from sqlite3 import dbapi2 as sqlite

#TODO:
def sqlite_schema(table_name):
    conn.execute('select sql from sqlite_master where type="table" and tbl_name="data"').fetchall()

class SqliteReader(object):
    def __init__(self, file, sql):
        self.sql = sql
        self.conn = sqlite.connect(file)
        self.conn.text_factory = str
        self.conn.execute('pragma cache_size=100000')
        self.conn.execute('pragma synchronous=OFF')
        
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
                yield 
        cursor.close()
