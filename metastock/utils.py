"""
Helper methods
"""

import struct
import datetime

def fmsbin2ieee(data):
    """
    Convert an array of 4 bytes containing Microsoft Binary floating point
    number to IEEE floating point format (which is used by Python)
    """
    as_int = struct.unpack("i", data)
    if not as_int:
        return 0.0
    man = int(struct.unpack('H', data[2:])[0])
    if not man:
        return 0.0
    exp = (man & 0xff00) - 0x0200
    man = man & 0x7f | (man << 8) & 0x8000
    man |= exp >> 1

    data2 = bytes(data[:2])
    if type(data2) is str:
        # python2
        data2 += chr(man & 255)
        data2 += chr((man >> 8) & 255)
    else:
        # python3
        data2 += bytes([man & 255])
        data2 += bytes([(man >> 8) & 255])
    return struct.unpack("f", data2)[0]

def float2date(date):
    """
    Metastock stores date as a float number.
    Here we convert it to a python datetime.date object.
    """
    date = int(date)
    if date < 101:
        date = 101
    year = 1900 + (date // 10000)
    month = (date % 10000) // 100
    day = date % 100
    return datetime.date(year, month, day)

def int2date(date):
    year = (date // 10000)
    month = (date % 10000) // 100
    day = date % 100
    return datetime.date(year, month, day)

def float2time(time):
    """
    Metastock stores date as a float number.
    Here we convert it to a python datetime.time object.
    """
    time = int(time)
    hour = time // 10000
    minute = (time % 10000) // 100
    return datetime.time(hour, minute)

def paddedString(s, encoding):
    # decode and trim zero/space padded strings
    zeroPadding = 0
    if type(s) is str:
        #python 2
        zeroPadding = '\x00'
    end = s.find(zeroPadding)
    if end >= 0:
        s = s[:end]
    try:
        return s.decode(encoding).rstrip(' ')
    except Exception as e:
        print("Error while reading the stock name. Did you specify the correct encoding?\n" +
              "Current encoding: %s, error message: %s" % (encoding, e))
        raise
