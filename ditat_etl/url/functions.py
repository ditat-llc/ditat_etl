from urllib.parse import urlparse


def extract_domain(url):
	url = f'http://{url}' if not url.startswith('http') else url
	domain = urlparse(url.replace('www.', '')).netloc
	return domain