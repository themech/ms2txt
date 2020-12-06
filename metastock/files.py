"""
Reading metastock files.
"""

import os
import struct
import re
import traceback

from .utils import fmsbin2ieee, float2date, float2time, int2date, paddedString


class DatFile:
    stock = None
    reg = re.compile('\"(.+)\",.+', re.IGNORECASE)
    columns = None

    def __init__(self, stock):
        self.stock = stock

    def dump(self):
        try:
            self.load_columns()
            self.dump_candles()
        except Exception:
            print("Error while converting symbol", self.stock.stock_symbol)
            traceback.print_exc()

    def load_columns(self):
        """
        Try to read columns names from the DOP file
        """
        filename = 'F%d.DOP' % self.stock.file_number
        if not os.path.isfile(filename):
            print("No DOP file found, assuming default columns set")
            assert(self.stock.fields in (7, 8))
            if self.stock.fields == 7:
                self.columns = [
                    'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL', 'OI'
                ]
            else:
                self.columns = [
                    'DATE', 'TIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL', 'OI'
                ]
        else:
            file_handle = open(filename, 'r')
            lines = file_handle.read().split()
            file_handle.close()
            assert(len(lines) == self.stock.fields)
            self.columns = []
            for line in lines:
                match = self.reg.search(line)
                colname = match.groups()[0]
                self.columns.append(colname)
        print(self.columns)

    class Column:
        """
        This is a base class for classes reading metastock data for a specific
        columns. The read method is called when reading a decode the column
        value
        @ivar dataSize: number of bytes is the data file that is used to store
                        a single value
        @ivar name: column name
        """
        dataSize = 4
        name = None

        def __init__(self, name):
            self.name = name

        def read(self, bytes):
            """Read and return a column value"""

        def format(self, value):
            """
            Return a string containing a value returned by read method
            """
            return str(value)

    class DateColumn(Column):
        """A date column"""
        def read(self, bytes):
            """Convert from MBF to date string"""
            return float2date(fmsbin2ieee(bytes))

        def format(self, value):
            if value is not None:
                return value.strftime('%Y%m%d')
            return DatFile.Column.format(self, value)

    class TimeColumn(Column):
        """A time column"""
        def read(self, bytes):
            """Convert read bytes from MBF to time string"""
            return float2time(fmsbin2ieee(bytes))

        def format(self, value):
            if value is not None:
                return value.strftime('%H%M%S')
            return DatFile.Column.format(self, value)

    class FloatColumn(Column):
        """
        A float column
        @ivar precision: round floats to n digits after the decimal point
        """
        precision = 2

        def read(self, bytes):
            """Convert bytes containing MBF to float"""
            return fmsbin2ieee(bytes)

        def format(self, value):
            return ("%0."+str(self.precision)+"f") % value

    class IntColumn(Column):
        """An integer column"""
        def read(self, bytes):
            """Convert MBF bytes to an integer"""
            return int(fmsbin2ieee(bytes))

    # we map a metastock column name to an object capable reading it
    knownMSColumns = {
        'DATE': DateColumn('Date'),
        'TIME': TimeColumn('Time'),
        'OPEN': FloatColumn('Open'),
        'HIGH': FloatColumn('High'),
        'LOW': FloatColumn('Low'),
        'CLOSE': FloatColumn('Close'),
        'VOL': IntColumn('Volume'),
        'OI': IntColumn('Oi'),
    }
    unknownColumnDataSize = 4    # assume unknown column data is 4 bytes long

    max_recs = 0
    last_rec = 0

    def dump_candles(self):
        """
        Load metastock DAT file and write the content
        to a text file
        """
        file_handle = None
        outfile = None
        try:
            file_handle = open('%s%s' % (self.stock.filename, self.stock.datafile_ext), 'rb')
            self.max_recs = struct.unpack("H", file_handle.read(2))[0]
            self.last_rec = struct.unpack("H", file_handle.read(2))[0]

            # not sure about this, but it seems to work
            file_handle.seek((self.stock.fields - 1) * 4, os.SEEK_CUR)

            #print("Expecting %d candles in file %s. num_fields : %d" % \
            #   (self.last_rec - 1, self.stock.filename, self.stock.fields))
            outfile = open('%s.TXT' % self.stock.stock_symbol, 'w')
            # write the header line, for example:
            # "Name","Date","Time","Open","High","Low","Close","Volume","Oi"
            outfile.write('"Name"')
            columns = []
            for ms_col_name in self.columns:
                column = self.knownMSColumns.get(ms_col_name)
                if column is not None:
                    outfile.write(',"%s"' % column.name)
                columns.append(column)  # we append None if the column is unknown
            outfile.write('\n')

            # we have (self.last_rec - 1) candles to read
            for _ in range(self.last_rec - 1):
                outfile.write(self.stock.stock_symbol)
                for col in columns:
                    if col is None:  # unknown column?
                        # ignore this column
                        file_handle.seek(self.unknownColumnDataSize, os.SEEK_CUR)
                    else:
                        # read the column data
                        bytes = file_handle.read(col.dataSize)
                        # decode the data
                        value = col.read(bytes)
                        # format it
                        value = col.format(value)

                        outfile.write(',%s' % value)

                outfile.write('\n')
        finally:
            if outfile is not None:
                outfile.close()
            if file_handle is not None:
                file_handle.close()


def dump_stock_to_file(stock):
    print("Processing %s (fileNo %d)" % (stock.stock_symbol, stock.file_number))
    try:
        file = DatFile(stock)
        file.dump()
    except Exception:
        print("Error while converting symbol", stock.stock_symbol)
        traceback.print_exc()


class Stock:
    file_number = 0
    record_length = 0
    fields = 0
    stock_symbol = ''
    stock_name = ''
    first_date = None
    last_date = None
    time_frame = 'D'
    datafile_ext = ''
    filename = ''

    def __str__(self):
        return u'symbol: %s, name: %s, filename: %s, start: %s, end: %s, frame: %s' % (
            self.stock_symbol, self.stock_name, self.filename, self.first_date, self.last_date, self.time_frame
        )


class MSMasterFile:
    reconds_count = 0
    file_handle = None

    def __init__(self, encoding):
        self.encoding = encoding

    def load(self):
        if os.path.isfile('MASTER'):
            self.file_handle = open('MASTER', 'rb')
            self.reconds_count = struct.unpack("H", self.file_handle.read(2))[0]

    def close(self):
        if self.file_handle is not None:
            self.file_handle.close()
            self.file_handle = None

    def load_symbol(self, i):
        symbol = Stock()
        self.file_handle.seek( (i+1)*53)
        symbol.file_number = struct.unpack("B", self.file_handle.read(1))[0]
        symbol.filename = 'F%d' % symbol.file_number
        symbol.datafile_ext = '.DAT'
        self.file_handle.seek(2, os.SEEK_CUR)
        symbol.record_length = struct.unpack("B", self.file_handle.read(1))[0]
        # czy potrzebuje field_bitset?
        symbol.fields = struct.unpack("B", self.file_handle.read(1))[0]
        self.file_handle.seek(2, os.SEEK_CUR)
        name = self.file_handle.read(16)
        symbol.stock_name = paddedString(name, self.encoding)

        self.file_handle.seek(2, os.SEEK_CUR)
        symbol.first_date = float2date(fmsbin2ieee(self.file_handle.read(4)))
        symbol.last_date = float2date(fmsbin2ieee(self.file_handle.read(4)))

        symbol.time_frame = struct.unpack("c", self.file_handle.read(1))[0].decode('ascii')
        self.file_handle.seek(2, os.SEEK_CUR)
        name = self.file_handle.read(14)
        symbol.stock_symbol = paddedString(name, 'ascii')
        return symbol


class MSEMasterFile:
    """
    Metastock extended index file
    """

    def __init__(self, encoding):
        self.encoding = encoding
        self.reconds_count = 0
        self.file_handle = None

    def load(self):
        """
        The whole file is read while creating this object
        @param filename: name of the file to open (usually 'EMASTER')
        """
        if os.path.isfile('EMASTER'):
            self.file_handle = open('EMASTER', 'rb')
            self.reconds_count = struct.unpack("H", self.file_handle.read(2))[0] # czy moze 1 bajt i B?
            self.last_file = struct.unpack("H", self.file_handle.read(2))[0]
        return

    def close(self):
        if self.file_handle is not None:
            self.file_handle.close()
            self.file_handle = None

    def load_symbol(self, i):
        symbol = Stock()
        self.file_handle.seek( (i+1)*192)

        self.file_handle.seek(2, os.SEEK_CUR)
        symbol.file_number = struct.unpack("B", self.file_handle.read(1))[0]
        if symbol.file_number == 0:
            return symbol
        symbol.filename = 'F%d' % symbol.file_number
        symbol.datafile_ext = '.DAT'
        self.file_handle.seek(3, os.SEEK_CUR)
        symbol.fields = struct.unpack("B", self.file_handle.read(1))[0]
        self.file_handle.seek(4, os.SEEK_CUR)
        name = self.file_handle.read(14)
        symbol.stock_symbol = paddedString(name, 'ascii')
        self.file_handle.seek(7, os.SEEK_CUR)
        name = self.file_handle.read(16)
        symbol.stock_name = paddedString(name, self.encoding)
        self.file_handle.seek(12, os.SEEK_CUR)
        symbol.time_frame = struct.unpack("c", self.file_handle.read(1))[0].decode('ascii')
        self.file_handle.seek(3, os.SEEK_CUR)
        symbol.first_date = float2date(fmsbin2ieee(self.file_handle.read(4)))
        self.file_handle.seek(4, os.SEEK_CUR)
        symbol.last_date = float2date(fmsbin2ieee(self.file_handle.read(4)))
        return symbol


class MSXMsterFile:
    reconds_count = 0
    file_handle = None

    def __init__(self, encoding):
        self.encoding = encoding

    def load(self):
        if os.path.isfile('XMASTER'):
            self.file_handle = open('XMASTER', 'rb')
            self.file_handle.seek(10, os.SEEK_SET)
            self.reconds_count = struct.unpack("H", self.file_handle.read(2))[0]

    def close(self):
        if self.file_handle is not None:
            self.file_handle.close()
            self.file_handle = None

    def load_symbol(self, i):
        symbol = Stock()
        self.file_handle.seek( (i+1)*150 )
        self.file_handle.seek(1, os.SEEK_CUR)
        name = self.file_handle.read(14)
        symbol.stock_symbol = paddedString(name, 'ascii')

        self.file_handle.seek(1, os.SEEK_CUR)
        name = self.file_handle.read(45)
        symbol.stock_name = paddedString(name, self.encoding)

        self.file_handle.seek(1, os.SEEK_CUR)
        symbol.time_frame = struct.unpack("c", self.file_handle.read(1))[0].decode('ascii')
        self.file_handle.seek(2, os.SEEK_CUR) # intraday timeframe?
        symbol.file_number = struct.unpack("H", self.file_handle.read(2))[0]
        symbol.filename = 'F%d' % symbol.file_number
        symbol.datafile_ext = '.MWD'
        self.file_handle.seek(3, os.SEEK_CUR)
        self.file_handle.seek(1, os.SEEK_CUR) # bitset
        self.file_handle.seek(33, os.SEEK_CUR)
        self.file_handle.seek(4, os.SEEK_CUR) # collection date?
        symbol.first_date = int2date(struct.unpack("I", self.file_handle.read(4))[0])
        self.file_handle.seek(4, os.SEEK_CUR)  # first time?
        #symbol.last_date = float2date(fmsbin2ieee(self.file_handle.read(4)))
        symbol.last_date = int2date(struct.unpack("I", self.file_handle.read(4))[0])
        self.file_handle.seek(4, os.SEEK_CUR)  # last time?

        return symbol


class MetastockFiles:
    def __init__(self, encoding, precision=None):
        if precision is not None:
            DatFile.FloatColumn.precision = precision
        emaster = MSEMasterFile(encoding)
        master = MSMasterFile(encoding)
        xmaster = MSXMsterFile(encoding)

        self.symbols = {}

        master.load()
        if master.reconds_count > 0:
            for i in range(master.reconds_count):
                s = master.load_symbol(i)
                if s.file_number > 0:
                    self.symbols[s.file_number] = s
        master.close()
        emaster.load()
        if emaster.reconds_count > 0:
            for i in range(emaster.reconds_count):
                s = emaster.load_symbol(i)

                # -- debug --
                #if s.file_number > 0 and s.file_number not in self.symbols:
                #    print("Cannot find file %d in MASTER data" % s.file_number)
                #else:
                #    if s.file_number > 0 and len(s.stock_name) > 0 and s.stock_name != self.symbols[s.file_number].stock_name:
                #        print("EMASTER wants to overwrite %s with %s" % (self.symbols[s.file_number].stock_name, s.stock_name))

                if s.file_number > 0 and len(s.stock_name) > 0 and s.file_number in self.symbols:
                    self.symbols[s.file_number].stock_name = s.stock_name
        emaster.close()

        # xmaster support is not ready yet, commented out
        """
        xmaster.load()
        if xmaster.reconds_count > 0:
            for i in range(xmaster.reconds_count):
                s = xmaster.load_symbol(i)
                if s.file_number > 0:
                    self.symbols[s.file_number] = s
        xmaster.close()
        """

    def list_all_symbols(self):
        """
        Lists all the symbols from metastock index file and writes it
        to the output
        """
        print('Number of available symbols: %d' % len(self.symbols))
        for stock in self.symbols.values():
            print("symbol: %s, name: %s, file number: %s" % \
                (stock.stock_symbol, stock.stock_name, stock.file_number))


    def output_ascii(self, all_symbols, symbols):
        """
        Read all or specified symbols and write them to text
        files (each symbol in separate file)
        @param all_symbols: when True, all symbols are processed
        @type all_symbols: C{bool}
        @param symbols: list of symbols to process
        """
        for stock in self.symbols.values():
            if all_symbols or (str(stock.stock_symbol) in symbols):
                dump_stock_to_file(stock)

