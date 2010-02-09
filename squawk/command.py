
from __future__ import with_statement

import sys
from optparse import OptionParser
from squawk.query import Query
from squawk.output import output_formats
from squawk.parsers import parsers
from squawk.sql import sql_parser

def get_table_names(tokens):
    if not isinstance(tokens.tables[0][0], basestring):
        return get_table_names(tokens.tables[0][0])
    return [tokens.tables[0][0]]

class Combiner(object):
    def __init__(self, files, parser_class):
        self.files = files
        self.parser_class = parser_class
        self.index = 0
        self.next_file()

    def open_file(self, fname):
        if fname.endswith(".gz"):
            import gzip
            return gzip.open(fname, "r")
        elif fname.endswith(".bz2"):
            import bz2
            return bz2.BZ2File(fname, "r")
        elif fname.endswith(".zip"):
            import zipfile
            return zipfile.ZipFile(fname, "r")
        else:
            return open(fname, "r")

    def next_file(self):
        if self.index >= len(self.files):
            raise StopIteration()
        fname = self.files[self.index]
        self.parser = self.parser_class(sys.stdin if fname == '-' else self.open_file(fname))
        self.parser_iter = iter(self.parser)
        self.columns = self.parser.columns
        self.index += 1

    def __iter__(self):
        return self

    def next(self):
        while True:
            try:
                row = self.parser_iter.next()
            except StopIteration:
                self.next_file()
            else:
                return row

def build_opt_parser():
    parser = OptionParser()
    parser.add_option("-p", "--parser", dest="parser",
                      help="name of parser for input")
    parser.add_option("-f", "--format", dest="format", default="tabular",
                      help="write output in FORMAT format", metavar="FORMAT")
    #parser.add_option("-x", "--extract", dest="extract", 
    #                  help="extract from compression format (zip, gz, bz2)")
    # parser.add_option("-q", "--quiet",
    #                   action="store_false", dest="verbose", default=True,
    #                   help="don't print status messages to stdout")
    return parser

def main():
    parser = build_opt_parser()
    (options, args) = parser.parse_args()

    sql = ' '.join(args)
    files = get_table_names(sql_parser.parseString(sql))

    parser_name = options.parser
    if parser_name:
        parser = parsers[parser_name]
    else:
        fn = files[0]
        if fn.rsplit('/', 1)[-1] == 'access.log':
            parser = parsers['access_log']
        elif fn.endswith('.csv'):
            parser = parsers['csv']
        else:
            sys.stderr.write("Can't figure out parser for input")
            sys.exit(1)

    source = Combiner(files, parser)
    query = Query(sql)

    output = output_formats[options.format]

    output(query(source))

if __name__ == "__main__":
    main()
