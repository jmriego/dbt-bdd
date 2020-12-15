from behave import *

import hashlib
import os

def hash_value(x=None):
    if x is None:
        x = os.urandom(32)
    try:
        return hashlib.md5(x.encode('utf-8')).hexdigest()[:8]
    except AttributeError:
        return hashlib.md5(x).hexdigest()[:8]
