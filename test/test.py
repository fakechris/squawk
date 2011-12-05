
from squawk.sqlite_shard import SqliteShard
from squawk.parsers import sql_conn, init_sqlite_table

from sqlite3 import dbapi2 as sqlite


def test(id):
    con = sql_conn("test%s.db" % id)
    cur = con.cursor()
    init_sqlite_table(cur, "media_id,campaign_id,adgroup_id", "", "media_id")
    cur.execute('insert into data values (%s,2,3)' % id)
    cur.close()
    del cur
    con.commit()
    con.close()
    del con

def shard_test():
    q = SqliteShard()
    print q.cols("select * from data")
    print q.fetch_all("select * from data")

def main():
#    test(1)
#    test(2)
    shard_test()

if __name__ == "__main__":
    main()
