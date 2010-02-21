
from __future__ import division
from functools import partial
import re

from squawk.aggregates2 import aggregate_functions2
from squawk.sql2 import sql_parser

from query import OPERATOR_MAPPING, sql_like, LimitOffset, Filter

def reverse_idx(s):
    assert type(s) == list or type(s) == tuple
    return dict(zip(s,range(len(s))))

class Column2(object):
    def __init__(self, column, name=None):
        self.column = column.lower()
        self.name = (name or column).lower()
        self._value = None

    def update(self, v):
        self._value = v

    def value(self):
        return self._value
    
class GroupBy2(object):
    def __init__(self, source, group_by, columns):
        self.source = source
        self.group_by = group_by
        self._columns = columns
        self._idx = {}
        for i, c in enumerate(columns):
            self._idx[c().name] = i         

    def __iter__(self):
        groups = {}
        for row in self.source:
            key = tuple(row[self._idx[k]] for k in self.group_by)
            if key not in groups:
                groups[key] = [x() for x in self._columns]
            for s in groups[key]:
                s.update(row[self._idx[s.name]])
        
        for key, row in groups.iteritems():
            yield tuple([r.value() for r in row])
            #yield dict((r.name, r.value()) for r in row)            

class OrderBy2(object):
    def __init__(self, source, order_by, columns, descending=False):
        self.source = source
        self.order_by = order_by.lower()
        self.descending = descending
        self._idx = {}
        for i, c in enumerate(columns):
            self._idx[c().name] = i         

    def __iter__(self):
        results = list(self.source)        
        results.sort(key=lambda row:row[self._idx[self.order_by]], reverse=self.descending)
        for r in results:
            yield r
            
class Selector2(object):
    def __init__(self, source, columns):
        self.source = source
        self._columns = [(n.lower(), (a or n).lower()) for n, a in columns] if columns else None

    def __iter__(self):
        for row in self.source:
            yield row

class Aggregator2(object):
    def __init__(self, source, columns):
        self.source = source
        self._columns = columns
        self._idx = {}
        for i, c in enumerate(self._columns):
            self._idx[c().name] = i         

    def __iter__(self):
        columns = [c() for c in self._columns]
        for row in self.source:
            for c in columns:
                c.update(row[self._idx[c.name]])
        yield tuple([c.value() for c in columns])
        #yield dict((c.name, c.value()) for c in columns)

class Query2(object):
    def __init__(self, sql, all_fields=None):
        self.tokens = sql_parser.parseString(sql) if isinstance(sql, basestring) else sql
        self.column_classes = None
        self._table_subquery = None
        self._all_fields = all_fields
        self._idx = {}
        self._parts = self._generate_parts()        

    def _reverse_index(self):
        for i, c in enumerate(self.column_classes):
            self._idx[c().name] = i         

    def _generate_parts(self):
        """Return a list of callables that can be composed to build a query generator"""
        tokens = self.tokens
        parts = []

        self.column_classes = [self._column_builder(c) for c in tokens.columns] if tokens.columns != '*' else None
        if not self.column_classes and self._all_fields:
            assert tokens.columns == '*'
            self.column_classes = [self._column_builder(c) for c in self._all_fields]
        assert self.column_classes
        self._reverse_index()        

        if not isinstance(tokens.tables[0][0], basestring):
            self._table_subquery = Query2(tokens.tables[0][0])

        if tokens.where:
            #func = eval("lambda row:"+self._filter_builder(tokens.where))
            func = eval("lambda row:True") # ignore where clause
            parts.append(partial(Filter, function=func))
        if tokens.groupby:
            # Group by query
            parts.append(partial(GroupBy2,
                    group_by = [c[0] for c in tokens.groupby],
                    columns = self.column_classes))
        elif self.column_classes and tokens.columns != '*' and any(len(c.name)>1 for c in tokens.columns):
            # Aggregate query
            parts.append(partial(Aggregator2, columns=self.column_classes))
        else:
            # Basic select
            parts.append(partial(Selector2, columns=[(c.name[0], c.alias) for c in tokens.columns] if tokens.columns != '*' else None))
        if tokens.orderby:
            order = tokens.orderby
            parts.append(partial(OrderBy2, 
                    order_by=order[0][0], 
                    descending=order[1]=='DESC' if len(order) > 1 else False,
                    columns = self.column_classes))
        if tokens.limit or tokens.offset:
            parts.append(partial(LimitOffset,
                limit = int(tokens.limit) if tokens.limit else None,
                offset = int(tokens.offset) if tokens.offset else 0))

        return parts

    def _filter_builder(self, where):
        """Return a Python expression from a tokenized 'where' filter"""
        l = []
        for expr in where:
            if expr[0] == '(':
                l.append("(")
                l.append(self._filter_builder(expr[1:-1]))
                l.append(")")
            else:
                if isinstance(expr, basestring):
                    l.append(expr)
                elif len(expr) == 3 and expr[1] == "like":
                    l.append('re.match(%s, row[%s])' % (sql_like(expr[2]), self._idx[expr[0].lower()]))
                elif len(expr) == 3:
                    if expr[1] == "like":
                        l.append('re.match(%s, row[%s])' % (sql_like(expr[2]), self._idx[expr[0].lower()]))
                    elif expr[1] in ("~", '~*', '!~', '!~*'):
                        neg = "not " if expr[1][0] == '!' else ""
                        flags = re.I if expr[1][-1] == '*' else 0
                        l.append('%sre.match(r%s, row[%s], %d)' % (neg, expr[2], self._idx[expr[0].lower()], flags))
                    else:
                        op = OPERATOR_MAPPING[expr[1]]
                        l.append('(row[%s] %s %s)' % (self._idx[expr[0].lower()], op, expr[2]))
                elif expr[1] == "in":
                    l.append('(row[%s] in %r)' % (self._idx[expr[0].lower()], expr[3:-1]))
                else:
                    raise Exception("Don't understand expression %s in where clause" % expr)
        return " ".join(l)

    def _column_builder(self, col):
        """Return a callable that builds a column or aggregate object"""
        if type(col) == str:
            return lambda:Column2(col, col)
        elif len(col.name) > 1:
            # Aggregate
            try:
                aclass = aggregate_functions2[col.name[0]]
            except KeyError:
                raise KeyError("Unknown aggregate function %s" % col.name[0])
            return lambda:aclass(col.name[1], col.alias if col.alias else '%s(%s)' % (col.name[0], col.name[1]))
        else:
            # Column
            return lambda:Column2(col.name[0], col.alias)

    def __call__(self, source):
        executor = self._table_subquery(source) if self._table_subquery else source
        for p in self._parts:
            executor = p(source=executor)
        executor.columns = [c().name for c in self.column_classes] if self.column_classes else source.columns
        return executor
    