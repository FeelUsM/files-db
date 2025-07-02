from pprint import pprint
import sys
import io

#pprint(b'\xc1\xcb'.decode('utf8',errors="surrogatepass")) # error
pprint(b'\xc1\xcb'.decode('utf8',errors="surrogateescape"))

print('---------------------')

import re
import codecs

class CustomIncrementalDecoder(codecs.IncrementalDecoder):
    def decode(self, b, final=False):
        return b.decode("utf8", errors="surrogatepass")

def decode_custom(b, errors='strict'):
	return b.decode("utf8", errors="surrogatepass"), len(b)

class CustomStreamReader(codecs.StreamReader):
    def decode(self, b, errors='strict'):
        return b.decode("utf8", errors="surrogatepass"), len(b)

if sys.platform == 'win32':
	ESCAPE_SYMBOL = '/'
else:
	ESCAPE_SYMBOL = '\\'

def surrogate_escape(s):
	outs = ''
	while m := re.search('[\ud800-\udfff]', s):
		assert m.end() - m.start() ==1
		outs += s[:m.start()] + ESCAPE_SYMBOL + 'u' + hex(ord(s[m.start()]))[2:]
		s = s[m.end():]
	outs+=s
	return outs.encode('utf8')

class CustomIncrementalEncoder(codecs.IncrementalEncoder):
    def encode(self, s, final=False):
        return surrogate_escape(s)

def encode_custom(s, errors='strict'):
	return surrogate_escape(s), len(s)

class CustomStreamWriter(codecs.StreamWriter):
    def encode(self, s, errors='strict'):
        return surrogate_escape(s), len(s)

def search(name):
    if name == "utf8_custom":
        return codecs.CodecInfo(
            name="utf8-custom",
            encode=lambda s, errors='strict': (surrogate_escape(s), len(s)),
            decode=decode_custom,
            incrementalencoder=CustomIncrementalEncoder,
            incrementaldecoder=CustomIncrementalDecoder,
            streamreader=CustomStreamReader,
            streamwriter=CustomStreamWriter
        )
    else:
    	print('serch result is None')

codecs.register(search)

s = 'ф ы в а \ud801 ф ы в а \ud801 ф ы в а \ud801 ф ы в а \ud801 ф ы в а '
b = s.encode('utf8-custom')
pprint(b)
pprint(b.decode('utf8'))

sys.stdout.reconfigure(encoding='utf8-custom')
sys.stderr.reconfigure(encoding='utf8-custom')

print(s)
print(123)

print('---------------------')

import sqlite3

def decode_row_factory(cursor, row):
    return tuple(
        col.decode('utf8', errors="surrogatepass") if isinstance(col, bytes) else col
        for col in row
    )

def str_adapter(s: str) -> str|bytes:
    try:
        s.encode("utf8")
        return s
    except Exception:
        return s.encode("utf8", errors="surrogatepass")

sqlite3.register_adapter(str, str_adapter)
con = sqlite3.connect(":memory:")
con.row_factory = decode_row_factory
cur = con.cursor()

# Тест
cur.execute("CREATE TABLE tx (t TEXT)")
cur.execute("INSERT INTO tx (t) VALUES (?)", ("фыва",))
cur.execute("INSERT INTO tx (t) VALUES (?)", ("\ud800",))

pprint(cur.execute("SELECT t FROM tx").fetchall())
pprint(cur.execute("SELECT typeof(t) FROM tx").fetchall())

for (s,) in cur.execute("SELECT t FROM tx").fetchall():
    print(s)
