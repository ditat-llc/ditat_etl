from urllib.parse import urlparse
import re
import os

from concurrent.futures import ThreadPoolExecutor
import requests

from ..time import TimeIt

filedir = os.path.abspath(os.path.dirname(__file__))
ignored_domains_path = os.path.join(filedir, 'domains_ignored.txt')

with open(ignored_domains_path, 'r') as f:
	ignored_domains = f.read().splitlines()


def extract_domain(
	url_or_email,
	ignore_domains=False,
	ignored_domains=ignored_domains,
	isemail=False

):
	url_or_email = url_or_email if type(url_or_email) == str else str(url_or_email)

	result = None

	if '@' in url_or_email:
		regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
		if re.fullmatch(regex, url_or_email):
			if isemail:
				result = url_or_email

			else:
				domain = url_or_email.split('@')[1]
				if domain:
					result = domain
	else:
		url_or_email = f'http://{url_or_email}' \
			if not url_or_email.startswith('http') else url_or_email
		domain = urlparse(url_or_email.replace('www.', '')).netloc
		if domain and '.' in domain:
			result = domain

	if ignore_domains and result in ignored_domains: 
		return None

	return result



@TimeIt
def eval_url(
	url: str or list,
	max_workers: int=10000,
	timeout=10,
):
	url  = [url] if isinstance(url, str) else url

	total = len(url) 
	current = 1

	def f(url):
		if not url:
			return None

		nonlocal current

		if not url.startswith('http'):
			url2 = 'https://' + url
			url = 'http://' + url
		try:
			r = requests.get(url, timeout=timeout)

			stm = f"Processed: {current} / {total}"
			print(stm, end='\r')

			current += 1
			status_code = r.status_code

			if status_code != 200:
				return False

			fmt_resp_url = extract_domain(r.url)

			return fmt_resp_url

		except:
			try:
				r = requests.get(url2, timeout=timeout)

				stm = f"Processed: {current} / {total}"
				print(stm, end='\r')

				current += 1
				status_code = r.status_code

				if status_code != 200:
					return False

				fmt_resp_url = extract_domain(r.url)

				return fmt_resp_url

			except:
				stm = f"Processed: {current} / {total}"
				print(stm, end='\r')
				current += 1
				return None

	max_workers = min(len(url), max_workers) 
	print(f'Initializing {max_workers} workers.')

	with ThreadPoolExecutor(max_workers=max_workers) as ex:
		iterables = {i: ex.submit(f, url=i) for i in url}
		results = {i: j.result() for i, j in iterables.items()}

	return results
