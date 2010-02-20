
from shards_query import ShardsQuery, NotSubQuerySupportException, Merge
from query import Query, GroupBy, OrderBy, LimitOffset, Filter

class DbShardsQuery(ShardsQuery):
    def __init__(self, sql):
        super(DbShardsQuery, self).__init__(sql)

    def __call__(self, sources): 
        if self._table_subquery:
            raise NotSubQuerySupportException()
        executors = sources
        merge_executor = None           
        for p in self._parts:   
            # normal parts
            if p.func == Filter: # result set is already filtered by database sql engine 
                continue
            elif p.func not in [LimitOffset, GroupBy, OrderBy]:
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