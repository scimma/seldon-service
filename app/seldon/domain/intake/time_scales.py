"""Rubin's time scale and its offset from UTC.

Rubin times are MJD in TAI. TAI has been 37 seconds ahead of UTC since the
leap second of 2017-01-01, and stays so until the next leap second, which
none is scheduled for.
"""

#: Julian date of MJD 0.
MJD_EPOCH_JD = 2_400_000.5

#: TAI minus UTC, in seconds.
TAI_MINUS_UTC_SECONDS = 37
