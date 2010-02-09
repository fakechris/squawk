
from query import Query, GroupBy, OrderBy, LimitOffset

class NotSubQuerySupportException(Exception):    
    def __str__(self):
        return "Shards does not support subquery."

class Merge(object):
    def __init__(self, sources):
        self.sources = sources

    def __iter__(self):
        for source in self.sources:
            for row in source:
                yield row

class ShardsQuery(Query):
    def __init__(self, sql):
        super(ShardsQuery, self).__init__(sql)

    def __call__(self, sources): 
        if self._table_subquery:
            raise NotSubQuerySupportException()
        executors = sources
        merge_executor = None           
        for p in self._parts:   
            # normal parts
            if p.func not in [LimitOffset, GroupBy, OrderBy]:
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
