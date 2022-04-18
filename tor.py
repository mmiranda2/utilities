import requests
import time
from functools import wraps
from utils import utfencode, utfdecode, get_config, get_text, decorator, get_f
from stem import Signal
from stem.control import Controller


def decorator(func):
	@functools.wraps(func)
	def wrapper_decorator(*args, **kwargs):
		# Do something before
		value = func(*args, **kwargs)
		# Do something after
		return value
	return wrapper_decorator


class TorCaller:
	def __init__(self, config):
		self.config = config.copy()
		self.secrets_store = self.config['secrets_fp']
		self.relay_port = 9050
		self.control_port = 9051
		self.address = '127.0.0.1'
		self.controller = Controller.from_port(address=self.address, port=self.control_port)
		self.controller.authenticate(password=self.get_auth())
		self.check_ip_url = 'https://icanhazip.com'
		self.current_ip = self.get_current_ip()

	def __enter__(self):
		return self

	def __exit__(self, exit_type, value, traceback):
		self.controller.close()

	def get_auth(self):
		password = get_f(self.secrets_store, mode='r', base64=True).strip()
		return password
	
	def pause_for_renew(self):
		is_ready = lambda: self.controller.is_newnym_available()
		counter = 0
		while not is_ready():
			if counter > 3:
				return False
			wait_time = self.controller.get_newnym_wait() + 0.2
			time.sleep(wait_time)
			counter += 1
		return True

	def renew_ip(self):
		self.controller.signal(Signal.NEWNYM)
		return self.pause_for_renew()

	def request(self, *args, **kwargs):
		kwargs['proxies'] = self.get_proxy_dict(self.address, self.relay_port)
		resp = requests.request(*args, **kwargs)
		return resp
	
	def get_current_ip(self):
		return self.request('GET', self.check_ip_url).text.strip()
	
	@staticmethod
	def get_proxy_dict(address, relay_port):
		return {
			'http': f'socks5h://{address}:{relay_port}',
			'https': f'socks5h://{address}:{relay_port}'
		}


def test_tor_caller():
	base_ip = requests.get('https://icanhazip.com').text.strip()
	print('Base IP:', base_ip)
	with TorCaller(config=get_config('tor')) as tor_caller:
		first_ip = tor_caller.get_current_ip()
		print('First IP:', first_ip)
		print('Renewing...')
		print(tor_caller.renew_ip())
		second_ip = tor_caller.get_current_ip()
		print('Second IP:', second_ip)
		print('Done')
	return


if __name__ == '__main__':
	print("Tor is running version %s" % controller.get_version())
	print("Tor is running on pid {}".format(controller.get_pid()))