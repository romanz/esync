from .. import util

def test_hash():
    assert util.sha1(b'') == 'da39a3ee5e6b4b0d3255bfef95601890afd80709'
