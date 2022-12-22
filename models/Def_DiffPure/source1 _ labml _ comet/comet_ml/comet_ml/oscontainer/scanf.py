# -*- coding: utf-8 -*-
# *******************************************************
#   ____                     _               _
#  / ___|___  _ __ ___   ___| |_   _ __ ___ | |
# | |   / _ \| '_ ` _ \ / _ \ __| | '_ ` _ \| |
# | |__| (_) | | | | | |  __/ |_ _| | | | | | |
#  \____\___/|_| |_| |_|\___|\__(_)_| |_| |_|_|
#
#  Sign up for free at http://www.comet.ml
#  Copyright (C) 2015-2021 Comet ML INC
#  This file can not be copied and/or distributed
#  without the express permission of Comet ML Inc.
# *******************************************************
# The MIT License (MIT)
#
# Copyright (c) 2016 joshburnett
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

"""
Small scanf implementation.

Python has powerful regular expressions, but sometimes they are totally overkill
when you just want to parse a simple-formatted string.
C programmers use the scanf-function for these tasks (see link below).

This implementation of scanf translates the simple scanf-format into
regular expressions. Unlike C you can be sure that there are no buffer overflows
possible.

For more information see
  * http://www.python.org/doc/current/lib/node49.html
  * http://en.wikipedia.org/wiki/Scanf

Original code from:
    http://code.activestate.com/recipes/502213-simple-scanf-implementation/

Modified original to make the %f more robust, as well as added %* modifier to
skip fields.

Adapted from:
    https://github.com/joshburnett/scanf
"""
import re

from comet_ml._typing import Any, Optional, Tuple

try:
    from functools import lru_cache
except ImportError:
    from backports.functools_lru_cache import lru_cache  # type: ignore

# As you can probably see it is relatively easy to add more format types.
# Make sure you add a second entry for each new item that adds the extra
# few characters needed to handle the field omission.
scanf_translate = [
    (re.compile(_token), _pattern, _cast)
    for _token, _pattern, _cast in [
        (r"%c", r"(.)", lambda x: x),
        (r"%\*c", r"(?:.)", None),
        (r"%(\d)c", r"(.{%s})", lambda x: x),
        (r"%\*(\d)c", r"(?:.{%s})", None),
        (r"%(\d)[di]", r"([+-]?\d{%s})", int),
        (r"%\*(\d)[di]", r"(?:[+-]?\d{%s})", None),
        (r"%[di]", r"([+-]?\d+)", int),
        (r"%\*[di]", r"(?:[+-]?\d+)", None),
        (r"%u", r"(\d+)", int),
        (r"%\*u", r"(?:\d+)", None),
        (r"%[fgeE]", r"([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)", float),
        (r"%\*[fgeE]", r"(?:[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)", None),
        (r"%s", r"(\S+)", lambda x: x),
        (r"%\*s", r"(?:\S+)", None),
        (r"%([xX])", r"(0%s[\dA-Za-f]+)", lambda x: int(x, 16)),
        (r"%\*([xX])", r"(?:0%s[\dA-Za-f]+)", None),
        (r"%o", r"(0[0-7]*)", lambda x: int(x, 8)),
        (r"%\*o", r"(?:0[0-7]*)", None),
    ]
]

# Cache formats
SCANF_CACHE_SIZE = 1000


def scanf(format_str, line, collapse_whitespace=True):
    # type: (str, str, bool) -> Optional[Tuple[Any, ...]]
    """
    scanf supports the following formats:
      %c        One character
      %5c       5 characters
      %d, %i    int value
      %7d, %7i  int value with length 7
      %f        float value
      %o        octal value
      %X, %x    hex value
      %s        string terminated by whitespace

    Any pattern with a * after the % (e.g., '%*f') will result in scanf matching the pattern but omitting the matched
    portion from the results. This is helpful when parts of the input string may change but should be ignored.

    Examples:

    >>> scanf("%s - %d errors, %d warnings", "/usr/sbin/sendmail - 0 errors, 4 warnings")
    ('/usr/sbin/sendmail', 0, 4)
    >>> scanf("%o %x %d", "0123 0x123 123")
    (83, 291, 123)
    >>> scanf("%o %*x %d", "0123 0x123 123")
    (83, 123)

    :param format_str: the scanf-compatible format string comprised of plain text and tokens from the table above.
    :param line: the text line to be parsed.
    :param collapse_whitespace: if True, performs a greedy match with whitespace in the input string,
    allowing for easy parsing of text that has been formatted to be read more easily. This enables better matching
    in log files where the data has been formatted for easier reading. These cases have variable amounts of whitespace
    between the columns, depending on the number of characters in the data itself.
    :return: a tuple of found values or None if the format does not match.
    """

    format_re, casts = _scanf_compile(format_str, collapse_whitespace)

    found = format_re.search(line)
    if found:
        groups = found.groups()
        return tuple([casts[i](groups[i]) for i in range(len(groups))])

    return None


@lru_cache(maxsize=SCANF_CACHE_SIZE)
def _scanf_compile(format_str, collapse_whitespace=True):
    """
    Compiles the format into a regular expression. Compiled formats are cached for faster reuse.

    For example:
    >>> format_re_compiled, casts = _scanf_compile('%s - %d errors, %d warnings')
    >>> print format_re_compiled.pattern

    """

    format_pat = ""
    cast_list = []
    i = 0
    length = len(format_str)
    while i < length:
        found = None
        for token, pattern, cast in scanf_translate:
            found = token.match(format_str, i)
            if found:
                if cast is not None:
                    cast_list.append(cast)
                groups = found.groupdict() or found.groups()
                if groups:
                    pattern = pattern % groups
                format_pat += pattern
                i = found.end()
                break
        if not found:
            char = format_str[i]
            # escape special characters
            if char in "|^$()[]-.+*?{}<>\\":
                format_pat += "\\"
            format_pat += char
            i += 1

    if collapse_whitespace:
        format_pat = re.sub(r"\s+", r"\\s+", format_pat)

    format_re = re.compile(format_pat)
    return format_re, cast_list
