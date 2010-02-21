
from shards_query import NotSubQuerySupportException, Merge
from query import LimitOffset, Filter
from query2 import Query2, GroupBy2, OrderBy2

class DbShardsQuery(Query2):
    def __init__(self, sql, all_fields=None):
        super(DbShardsQuery, self).__init__(sql, all_fields)

    def __call__(self, sources): 
        if self._table_subquery:
            raise NotSubQuerySupportException()
        executors = sources
        merge_executor = None           
        for p in self._parts:   
            # normal parts
            if p.func == Filter: # result set is already filtered by database sql engine 
                continue
            elif p.func not in [LimitOffset, GroupBy2, OrderBy2]:
                assert not merge_executor
                for i, source in enumerate(sources):           
                    executors[i] = p(source=executors[i])
            else: # merge parts            
                if not merge_executor:
                    merge_executor = Merge(executors)
                merge_executor = p(source=merge_executor)
        if not merge_executor:
            merge_executor = Merge(executors)
        merge_executor.columns = [c().name for c in self.column_classes] if self.column_classes else sources[0].columns
        return merge_executor