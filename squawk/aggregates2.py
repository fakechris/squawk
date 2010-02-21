"""
An aggregate class is expected to except two values at
instantiation: 'column' and 'name', and the class
must have two methods 'update(self, row)' and 'value(self)'.
The 'update' method is called for each row, and the 'value'
must return the final result of the aggregation.
"""

class AvgAggregate2(object):
    """Calculate the average value for a column"""

    def __init__(self, column, name=None):
        self.column = column.lower()
        self.name = (name or column).lower()
        self.sum = 0
        self.count = 0

    def update(self, v):
        self.sum += v
        self.count += 1

    def value(self):
        if self.count == 0:
            return None
        return self.sum / self.count

class CountAggregate2(object):
    """Count the number of rows"""

    def __init__(self, column, name=None):
        self.column = column.lower()
        self.name = (name or column).lower()
        self.count = 0

    def update(self, v):
        self.count += v

    def value(self):
        return self.count

class MaxAggregate2(object):
    """Calculate the maximum value for a column"""

    def __init__(self, column, name=None):
        self.column = column.lower()
        self.name = (name or column).lower()
        self.max = None

    def update(self, v):
        if self.max is None:
            self.max = v
        else:
            self.max = max(self.max, v)

    def value(self):
        return self.max

class MinAggregate2(object):
    """Calculate the minimum value for a column"""

    def __init__(self, column, name=None):
        self.column = column.lower()
        self.name = (name or column).lower()
        self.min = None

    def update(self, v):
        if self.min is None:
            self.min = v
        else:
            self.min = min(self.min, v)

    def value(self):
        return self.min

class SumAggregate2(object):
    """Calculate the sum of values for a column"""

    def __init__(self, column, name=None):
        self.column = column.lower()
        self.name = (name or column).lower()
        self.sum = 0

    def update(self, v):
        self.sum += v

    def value(self):
        return self.sum

aggregate_functions2 = dict(
    avg = AvgAggregate2,
    count = CountAggregate2,
    max = MaxAggregate2,
    min = MinAggregate2,
    sum = SumAggregate2,
)
