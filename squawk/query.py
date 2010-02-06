#!/usr/bin/env python

from __future__ import division
from functools import partial
import logging

from squawk.aggregates import aggregate_functions
from squawk.sql import sql_parser

class Column(object):
    def __init__(self, column, name=None):
        self.column = column.lower()
        self.name = (name or column).lower()
        self._value = None

    def update(self, row):
        self._value = row[self.column]

    def value(self):
        return self._value

class Limit(object):
    def __init__(self, source, limit):
        self.source = source
        self.limit = limit

    def __iter__(self):
        for i, row in enumerate(self.source):
            yield row
            if i+1 >= self.limit:
                return

class OrderBy(object):
    def __init__(self, source, order_by, descending=False):
        self.source = source
        self.order_by = order_by.lower()
        self.descending = descending

    def __iter__(self):
        results = list(self.source)
        results.sort(key=lambda row:row[self.order_by], reverse=self.descending)
        for r in results:
            yield r

class GroupBy(object):
    def __init__(self, source, group_by, columns):
        self.source = source
        self.group_by = group_by.lower()
        self.columns = columns

    def __iter__(self):
        groups = {}
        for row in self.source:
            value = row[self.group_by]
            if value not in groups:
                groups[value] = [x() for x in self.columns]
            for s in groups[value]:
                s.update(row)
        for key, row in groups.iteritems():
            yield dict((r.name, r.value()) for r in row)

class Filter(object):
    def __init__(self, source, function):
        self.source = source
        self.function = function

    def __iter__(self):
        for row in self.source:
            if self.function(row):
                yield row

class Selector(object):
    def __init__(self, source, columns):
        self.source = source
        self.columns = [(n.lower(), (a or n).lower()) for n, a in columns]

    def __iter__(self):
        for row in self.source:
            yield dict((alias, row[name]) for name, alias in self.columns)

class Aggregator(object):
    def __init__(self, source, columns):
        self.source = source
        self.columns = columns

    def __iter__(self):
        columns = [c() for c in self.columns]
        for row in self.source:
            for c in columns:
                c.update(row)
        yield dict((c.name, c.value()) for c in columns)

class Query(object):
    def __init__(self, sql):
        self.tokens = sql_parser.parseString(sql)
        self.columns = []
        self._parts = []
        self._generate_parts()

    def _generate_parts(self):
        tokens = self.tokens

        self.columns = [self._column_builder(c)().name for c in tokens.columns]

        if tokens.where:
            func = eval("lambda row:"+self._filter_builder(tokens.where))
            self._parts.append(partial(Filter, function=func))
        if tokens.groupby:
            # Group by query
            self._parts.append(partial(GroupBy,
                    group_by = tokens.groupby[0][0],
                    columns = [self._column_builder(c) for c in tokens.columns]))
        elif any(len(c.name)>1 for c in tokens.columns):
            # Aggregate query
            self._parts.append(partial(Aggregator,
                columns = [self._column_builder(c) for c in tokens.columns]))
        else:
            # Basic select
            self._parts.append(partial(Selector, columns=[(c.name[0], c.alias) for c in tokens.columns]))
        if tokens.orderby:
            order = tokens.orderby
            self._parts.append(partial(OrderBy, order_by=order[0][0], descending=order[1]=='DESC'))
        if tokens.limit:
            self._parts.append(partial(Limit, limit=int(tokens.limit)))

    def _filter_builder(self, where):
        l = []
        for expr in where:
            if expr[0] == '(':
                l.append("(")
                l.append(self._filter_builder(expr[1:-1]))
                l.append(")")
            else:
                if isinstance(expr, basestring):
                    l.append(expr)
                elif len(expr) == 3:
                    op = {
                        '<>': '!=',
                        '!=': '!=',
                        '=': '==',
                        '<': '<',
                        '>': '>',
                        '<=': '<=',
                        '>=': '>=',
                    }[expr[1]]
                    l.append('(row["%s"] %s %s)' % (expr[0].lower(), op, expr[2]))
                else:
                    raise Exception("Don't understand expression %s in where clause" % expr)
        return " ".join(l)

    def _column_builder(self, col):
        if len(col.name) > 1:
            # Aggregate
            try:
                aclass = aggregate_functions[col.name[0]]
            except KeyError:
                raise KeyError("Unknown aggregate function %s" % col.name[0])
            return lambda:aclass(col.name[1], col.alias if col.alias else '%s(%s)' % (col.name[0], col.name[1]))
        else:
            # Column
            return lambda:Column(col.name[0], col.alias)

    def execute(self, source):
        executor = source
        for p in self._parts:
            executor = p(source=executor)
        return executor
