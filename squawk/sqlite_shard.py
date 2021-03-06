
from query2 import *
from db_shards_query import *
from parsers import *
from squawk.sql2 import sql_parser
import glob, os

class SqliteShard(object):
    def __init__(self, files=None):
        self.conns = []
        if not files:
            files = [os.path.join(os.getcwd(),p1) for p1 in glob.glob("*") if p1.endswith(".db")]
#            files = [os.path.join(os.getcwd(),p1) for p1 in glob.glob("*") if not p1.endswith("-journal")]
        for f in files:
            self.conns.append(sql_conn(f))

    def __del__(self):
        for c in self.conns:
            c.close()

    def _query(self, sql):
        tokens = sql_parser.parseString(sql)
        all_fields = []
        if tokens.columns == '*':
            usePrefix = len(tokens.tables) > 1
            for t in tokens.tables:
                columns = sql_columns(self.conns[0], t[0])
                if usePrefix:
                    columns = ["%s.%s"%(t[0],c) for c in columns]
                all_fields.extend(columns)
        return DbShardsQuery(sql, all_fields), all_fields

    def cols(self, sql):
        q, all_fields = self._query(sql)
        results = []
        for c in q.column_classes:
            results.append(c().name)
        return results

    def query(self, sql):
        q, all_fields = self._query(sql)
        sources = [SqliteReader2(c, all_fields, sql) for c in self.conns]
        return q(sources)

    def fetch_all(self, sql):
        return list(self.query(sql))
