from behave import *
from agate import Table

import hashlib
import os

def hash_value(x=None):
    if x is None:
        x = os.urandom(32)
    try:
        return hashlib.md5(x.encode('utf-8')).hexdigest()[:8]
    except AttributeError:
        return hashlib.md5(x).hexdigest()[:8]


def behave2agate(table):
    column_names = table.headings
    rows = table.rows
    rows_dict = [dict(zip(column_names, row)) for row in rows]
    return Table.from_object(rows_dict)

