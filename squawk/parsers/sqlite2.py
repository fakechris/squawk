
from sqlite3 import dbapi2 as sqlite
from squawk.parsers.sqlite import sqlite_schema

def sql_conn(file):
    conn = sqlite.connect(file)
    conn.text_factory = str
    conn.execute('pragma cache_size=100000')
    conn.execute('pragma synchronous=OFF')
    return conn

def sql_columns(conn, tablename):
    return [x.lower() for x in sqlite_schema(conn, tablename)]

class SqliteReader2(object):
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

class SqliteReader3(object):
    def __init__(self, conn, columns, sql):
        self.sql = sql
        self.conn = conn        
        self.columns = columns
        
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
