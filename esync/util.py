import datetime
import hashlib


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def sha1(data):
    return hashlib.sha1(data).hexdigest()


def hash_file(path, hasher=sha256):
    return hasher(open(path, 'rb').read())


def now(current_datetime=datetime.datetime.utcnow):
    return current_datetime().isoformat()
