"""
Helper methods
"""

import struct
import datetime

def fmsbin2ieee(bytes):
    """
    Convert an array of 4 bytes containing Microsoft Binary floating point
    number to IEEE floating point format (which is used by Python)
    """
    as_int = struct.unpack("i", bytes)
    if not as_int:
        return 0.0
    man = long(struct.unpack('H', bytes[2:])[0])
    exp = (man & 0xff00) - 0x0200
    if (exp & 0x8000 != man & 0x8000):
        return 1.0
        #raise ValueError('exponent overflow')
    man = man & 0x7f | (man << 8) & 0x8000
    man |= exp >> 1

    bytes2 = bytes[:2]
    bytes2 += chr(man & 255)
    bytes2 += chr((man >> 8) & 255)
    return struct.unpack("f", bytes2)[0]

def float2date(date):
    """
    Metastock stores date as a float number.
    Here we convert it to a python datetime.date object.
    """
    date = int(date)
    year = 1900 + (date / 10000)
    month = (date % 10000) / 100
    day = date % 100
    return datetime.date(year, month, day)

def float2time(time):
    """
    Metastock stores date as a float number.
    Here we convert it to a python datetime.time object.
    """
    time = int(time)
    hour = time / 10000
    minute = (time % 10000) / 100
    return datetime.time(hour, minute)
