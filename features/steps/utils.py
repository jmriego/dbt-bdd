from behave import *

import hashlib

def hash_value(x):
    return hashlib.md5(x.encode('utf-8')).hexdigest()[:8]
