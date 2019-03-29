#!/usr/bin/env python
"""
Command line tool used to extract the data from metastock files and save it
in text format.
"""

import sys
from optparse import OptionParser

from metastock.files import MSEMasterFile

Usage = """usage: %prog [options] [symbol1] [symbol2] ....

Examples:
    %prog -p 2 --all        extract all symbols from EMASTER file
    %prog FW20 "S&P500"     extract FW20 and S&P500 from EMASTER file
"""

def main():
    """
    launched when running this file
    """

    parser = OptionParser(usage=Usage)
    parser.add_option("-l", "--list", action="store_true", dest="list",
                  help="list all the symbols from EMASTER file")
    parser.add_option("-a", "--all", action="store_true", dest="all",
                      help="extract all the symbols from EMASTER file")
    parser.add_option("-p", "--precision", type="int", dest="precision",
                      help="round the floating point numbers to PRECISION digits after the decimal point (default: 2)")
    parser.add_option("-e", "--encoding", type="str", dest="encoding", default="ascii",
                     help="character encoding of the stock names like cp1250 or cp1256 (default: ascii)")


    (options, args) = parser.parse_args()

    # check if the options are valid
    if not (options.all or options.list or len(args) > 0):
        parser.print_help()
        sys.exit(0)

    em_file = MSEMasterFile('EMASTER', options.encoding, options.precision)
    # list the symbols or extract the data
    if options.list:
        em_file.list_all_symbols()
    else:
        em_file.output_ascii(options.all, args)

if __name__ == "__main__":
    main()
