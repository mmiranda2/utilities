import os
import json
import requests
from functools import wraps
from base64 import b64encode, b64decode, urlsafe_b64encode


def decorator(f, before=None, after=None):
    @wraps(f)
    def wrapper(*args, **kwargs):
        b = before(f, *args, **kwargs) if before else None
        result = f(*args, **kwargs)
        a = after(f, result, b, *args, **kwargs) if after else None
        if a:
            return a
        return result
    return wrapper


def utfencode(s: str):
	return s.encode('utf-8')


def utfdecode(b: bytes):
	return b.decode('utf-8')


def utfreflect(x):
	'''
	Do UTF-8 reflection on x
	'''
	utf_map = {
		str: utfencode,
		bytes: utfdecode
	}
	func = utf_map[type(x)]
	return func(x)


def raise_or_cast_to_bytes(bs):
	if not isinstance(bs, bytes):
		if isinstance(bs, str):
			return utfencode(bs)
		else:
			raise TypeError('Argument should be bytes or str')


def base64d(b64_bs) -> bytes:
	b64_bytes = raise_or_cast_to_bytes(b64_bs)
	bs = b64decode(b64_bytes)
	return bs


def base64e(bs) -> bytes:
	raise_or_cast_to_bytes(bs)
	b64_bytes = b64encode(bs)
	return b64_bytes


def stringify_binary_content(byte_content) -> str:
	b64_content = base64e(byte_content)
	return utfdecode(b64_content)


def rebuild_binary_content(s) -> bytes:
	b64_bs = utfencode(s)
	bs = b64decode(b64_bs)
	return bs


def write_f(filename, content, mode):
	with open(filename, mode) as f:
		f.write(content)


write_text = lambda *x, **y: write_f(*x, **y, mode='w')
write_bytes = lambda *x, **y: write_f(*x, **y, mode='wb')


def iget_f(filename, mode='r'):
	with open(filename, mode) as f:
		yield f.readline()


def get_f(filename, mode, **kwargs):
	with open(filename, mode) as f:
		content = f.read()
	if kwargs.get('base64', False):
		content = base64d(content)
	return content


get_text = lambda *x, **y: get_f(*x, **y, mode='r')
get_bytes = lambda *x, **y: get_f(*x, **y, mode='rb')


def put(content, filename, mode='wb', xattrs={}):
	with open(filename, mode) as f:
		f.write(content)
		if xattrs:
			put_file_attrs(filename, xattrs)


def put_file_attrs(filename, xattrs):
	for k in xattrs:
		obj = xattrs[k]
		val = str(obj)
		if type(obj) is dict:
			val = json.dumps(obj)
		attr = 'user.' + str(k)
		os.setxattr(filename, attr, utfencode(val))


def json_obj(filename, encoded=False):
	if not encoded:
		with open(filename, 'r') as f:
			obj = json.load(f)
	else:
		file_text = get_text(filename)
		decoded_file_text = base64d(file_text)
		obj = json.loads(decoded_file_text)
	return obj


def json_dump(obj, filename, mode='w'):
	with open(filename, mode) as f:
		json.dump(obj, f)


def get_config(section=None):
	config = os.environ.get('QUICKSERVICE_CONFIG', './paths.json')
	config = json_obj(config)
	if section:
		config = config[section]
	return config


def get(url):
	resp = requests.get(url)
	return resp.content


def get_sources(content, sep='\n', tag=''):
	lines = enumerate(line.strip() for line in content.split(sep) if tag in line)
	return lines


def do_url(src, dest):
	c = get(src)
	put(c, dest)


def do_urls(src, dest):
	if not os.path.exists(dest):
		os.mkdir(dest)
	c = get_f(src, mode='r')
	l = get_sources(c)
	for i, this_url in l:
		out_fp = dest + str(i)
		do_url(this_url, out_fp)


def do_html_to_urls(src, dest):
	c = get_f(src, mode='r')
	l = get_sources(c, tag='<source')
	out = ''
	for line in l:
		row = line.split('src="')
		if len(row) > 1:
			out += row[1].split('" ')[0]
			out += '\n'
	put(out, dest, mode='w')


def run(get_url, get_urls, html_to_urls):
	paths = json_obj('../paths.json')
	if get_url:
		do_url(paths['url'], url_fp)
	if get_urls:
		do_urls(paths['urls_fp'], paths['urls_content_folder'])
	if html_to_urls:
		do_html_to_urls(paths['html_fp'], html_to_urls_fp)


def str_64(s):
	x = s

	# convert x to bytes
	if type(s) is str:
		x = utfdecode(s)
	
	x_64 = urlsafe_b64encode(x)
	x_64_str = utfdecode(x_64)
	return x_64_str


def str_hash(s):
	return str(hash(s))


def make_filename(url):
	from base64 import urlsafe_b64encode
	'''
	url_bytes = url.replace('https://', '').replace('http://', '').encode('utf-8')
	simple_filename = urlsafe_b64encode(url_bytes).decode('utf-8')
	if len(simple_filename) > 50:
		cut_filename = '/'.join(simple_filename.split('/')[1:])
		if len(cut_filename) > 50:
			hashed_filename = str(hash(url))
			hashed_filename_64 = urlsafe_b64encode(hashed_filename.encode('utf-8')).decode('utf-8')
			if len(hashed_filename_64) > 50:
				return hashed_filename
			else:
				return hashed_filename_64
		else:
			return cut_filename
	'''
	filename_64 = str_64(url)
	if len(filename_64) > 50:
		return str_hash(filename_64)
	return filename_64


def store_payload(payload):
	paths = json_obj('../paths.json')
	payload_fp = paths['payload_fp']
	filename = make_filename(payload['url']) + '.json'
	json_dump(payload, payload_fp + filename)
