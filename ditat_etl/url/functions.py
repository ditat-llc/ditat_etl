from urllib.parse import urlparse
import re

def extract_domain(url_or_email):
	url_or_email = str(url_or_email)
	if '@' in url_or_email:
		regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
		if re.fullmatch(regex, url_or_email):
			domain = url_or_email.split('@')[1]
			return domain
	else:
		url_or_email = f'http://{url_or_email}' if not url_or_email.startswith('http') else url_or_email
		domain = urlparse(url_or_email.replace('www.', '')).netloc
		return domain