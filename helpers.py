import os
import sys
import json
import requests
import traceback


def guessmode(content, mode='r'):
	suffix = '' if type(content) is str else 'b'
	return mode + suffix


def join2d(arr, newline='\n', sep=','):
	return '\n'.join([','.join(row) for row in arr])


def parsecsv(filename=None, sep=',', newline='\n', content=None, replace_file=[], replace_val=[], replace_line=[], dictionary=False, trim=False):
	if content:
		s = content
	else:
		s = readf(filename)
	if replace_file or replace_val or replace_line:
		replace_file = replace_file or ['', '']
		replace_val = replace_val or ['', '']
		replace_line = replace_line or ['', '']
		rows = [[val.replace(*replace_val) for val in line.replace(*replace_line).split(sep)] for line in s.replace(*replace_file).split(newline) if not trim or (sep in line or len(line.strip()))]
	else:
		rows = [line.split(sep) for line in s.split(newline) if not trim or (sep in line or len(line.strip()))]
	if dictionary:
		return {key: [row[i] if i < len(row) else '' for row in rows[1:]] for i, key in enumerate(rows[0])}
	else:
		return rows


def readf(filename, mode='r', loader_f=lambda fio: fio.read()):
	try:
		with open(filename, mode=mode) as f:
			x = loader_f(f)
		return x
	except Exception as e:
		if os.path.exists(filename) and  mode == 'r':
			return readf(filename, mode='rb', loader_f=loader_f)
		raise e


def repairpsqloutput(filename):
    content = readf(filename, mode='w')
    arr = parsecsv(content=content, sep='|', replace_val=[' ', ''])
    return arr

def dumpcontent(content, file_obj):
	file_obj.write(content)


def writef(content, filename, mode='w', writer_func=dumpcontent):
	mode = guessmode(content, mode)
	f = open(filename, mode)
	try:
		dumpcontent(content, f)
	except:
		print(traceback.format_exc())
	finally:
		f.close()
	return filename


def loadjsonfile(filename, **kwargs):
	_kwargs = {'mode': 'r', 'loader_f': json.load}
	_kwargs.update(kwargs)
	return readf(filename, **_kwargs)


def loadjsonstring(jsonstring):
	return json.loads(jsonstring)


def loadjson(somestring):
	if os.path.exists(somestring):
		try:
			return loadjsonfile(somestring)
		except Exception as e:
			print('Arg filepath exists and failed to load ::\n retrying arg as jsonstring', e)
			try:
				return loadjsonstring(somestring)
			except Exception as e2:
				print('failed to guess and parse json string on retry::\n', e2, '\n\t-----')
				raise e
	try:
		# assume some string is a json string ... if it is a filename, we go to except block
		return loadjsonstring(somestring)
	except json.decoder.JSONDecodeError as e:
		print('Arg filepath does not exist and parsed as jsonstring')
		raise e


def dumpjsontofile(content, filename, **kwargs):
	_kwargs = {'mode': 'w', 'writer_f': json.dump}
	_kwargs.update(kwargs)
	writef(content, filename, **_kwargs)


def dumpjson(obj, compact=False, indent=4):
	if compact:
		return json.dumps(obj)
	else:
		return json.dumps(obj, indent=indent)


def ptjson(*args, **kwargs):
	print(dumpjson(*args, **kwargs))


def geturl(url, *args, **kwargs):
    resp = requests.get(url, *args, **kwargs)
    return resp


def write_f(*args, **kwargs):
	if len(args) > 2:
		args[2] = args[2].replace('w', 'a')
	else:
		if 'mode' in kwargs:
			kwargs['mode'] = kwargs['mode'].replace('w', 'a')
		else:
			kwargs['mode'] = 'a'
	return writef(*args, **kwargs)


guess_mode = guessmode
join_2d = join2d
parse_csv = parsecsv
read_f = readf
repair_psql_output = repairpsqloutput
dump_content = dumpcontent
load_json_file = loadjsonfile
load_json_string = loadjsonstring
load_json = loadjson
dump_json_to_file = dumpjsontofile
dump_json = dumpjson
pt_json = ptjson
get_url = geturl


exported = [
    guess_mode,
    join_2d,
    parse_csv,
    read_f,
    repair_psql_output,
    dump_content,
    write_f,
    load_json_file,
    load_json_string,
    load_json,
    dump_json_to_file,
    dump_json,
    pt_json,
    get_url
]
