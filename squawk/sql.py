#!/usr/bin/env python

# This file is camelCase to match pyparsing

__all__ = ["sql_parser"]

from pyparsing import Literal, CaselessLiteral, Word, Upcase, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, upcaseTokens, Suppress, stringEnd, Regex, NotAny

selectToken  = Keyword("select", caseless=True)
fromToken    = Keyword("from", caseless=True)
whereToken   = Keyword("where", caseless=True)
groupByToken = Keyword("group", caseless=True) + Keyword("by", caseless=True)
orderByToken = Keyword("order", caseless=True) + Keyword("by", caseless=True)
limitToken   = Keyword("limit", caseless=True)

selectStmt  = Forward()

ident          = Word(alphas, alphanums + "_$").setName("identifier")
# ident          = Regex(r'"?(?!^from$|^where$)[A-Za-z][A-Za-z0-9_$]*"?').setName("identifier")
filename       = Word(alphanums+"/._-").setName("filename")
columnName     = delimitedList(ident, ".", combine=True).setParseAction(upcaseTokens)

aggregateFunction = (
    (CaselessLiteral("count") | CaselessLiteral("sum") |
     CaselessLiteral("min") | CaselessLiteral("max") | CaselessLiteral("avg"))
    + Suppress("(") + (columnName | oneOf("1 *")) + Suppress(")"))
columnDef      = Group(aggregateFunction | columnName).setResultsName("name")
aliasDef       = Optional(Optional(Suppress(CaselessLiteral("AS"))) + NotAny(whereToken | fromToken) + columnName.setResultsName("alias"))
# aliasDef       = Optional(Suppress(CaselessLiteral("AS")) | columnName.setResultsName("alias")) | Optional(columnName.setResultsName("alias"))

tableName      = delimitedList(ident, ".", combine=True).setParseAction(upcaseTokens)
subQuery       = "(" + selectStmt + ")"
tableDef       = subQuery | tableName
tableNameList  = Group(delimitedList(tableDef + aliasDef))

whereExpression = Forward()
and_ = Keyword("and", caseless=True)
or_ = Keyword("or", caseless=True)
in_ = Keyword("in", caseless=True)

E = CaselessLiteral("E")
binop = oneOf("= != <> < > >= <= eq ne lt le gt ge", caseless=True)
arithSign = Word("+-", exact=1)
realNum = (Combine(
    Optional(arithSign) + (
        Word(nums) + "." + Optional(Word(nums)) | ("." + Word(nums))
    ) + Optional(E + Optional(arithSign) + Word(nums)))
    .setName("real")
    .setParseAction(lambda s,l,toks: float(toks[0])))
intNum = (Combine(Optional(arithSign) + Word(nums) +
    Optional(E + Optional("+") + Word(nums)))
    .setName("integer")
    .setParseAction(lambda s,l,toks: int(toks[0])))

# WHERE

columnRval = realNum | intNum | quotedString | columnName # need to add support for alg expressions
whereCondition = Group(
        (columnName + binop + columnRval) |
        (columnName + in_ + "(" + delimitedList(columnRval) + ")") |
        (columnName + in_ + "(" + selectStmt + ")") |
        ("(" + whereExpression + ")")
    )
whereExpression << whereCondition + ZeroOrMore((and_ | or_) + whereExpression) 

# GROUP BY

groupByExpression = Group(delimitedList(columnDef))

# ORDER BY

orderByExpression = Group(delimitedList(columnDef + Optional(CaselessLiteral("DESC") | CaselessLiteral("ASC"))))

# LIMIT

limitExpression = intNum

# define the grammar
selectColumnList = Group(delimitedList(Group(columnDef + aliasDef)))
selectStmt << (
    selectToken + 
    ('*' | selectColumnList).setResultsName("columns") + 
    fromToken + tableNameList.setResultsName("tables") + 
    Optional(whereToken + whereExpression.setResultsName("where"), "") +
    Optional(groupByToken + groupByExpression.setResultsName("groupby"), "") +
    Optional(orderByToken + orderByExpression.setResultsName("orderby"), "") +
    Optional(limitToken + limitExpression.setResultsName("limit"), ""))

sql_parser = selectStmt + stringEnd

sqlComment = "--" + restOfLine # ignore comments
sql_parser.ignore(sqlComment)
