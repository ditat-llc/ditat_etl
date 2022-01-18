from urllib.parse import urlparse
import re

from concurrent.futures import ThreadPoolExecutor
import requests

from ..utils.time_functions import time_it


def extract_domain(url_or_email):
    url_or_email = str(url_or_email)
    if '@' in url_or_email:
        regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        if re.fullmatch(regex, url_or_email):
            domain = url_or_email.split('@')[1]
            if domain:
                return domain
    else:
        url_or_email = f'http://{url_or_email}' if not url_or_email.startswith('http') else url_or_email
        domain = urlparse(url_or_email.replace('www.', '')).netloc
        if domain and '.' in domain:
            return domain


@time_it()
def eval_url(
    url: str or list,
    max_workers: int=10000,
    timeout=60,
):
    '''
        This function can later be moved to class Url()
    '''
    url  = [url] if isinstance(url, str) else url

    @time_it()
    def f(url):
        if not url.startswith('http'):
            url2 = 'https://' + url
            url = 'http://' + url
        try:
            r = requests.get(url, timeout=timeout)
            return r.status_code
        except:
            try:
                r = requests.get(url2, timeout=timeout)
                return r.status_code
            except:
                return None

    with ThreadPoolExecutor(max_workers=min(len(url), max_workers)) as ex:
        iterables = {i: ex.submit(f, url=i) for i in url}
        results = {i: j.result() for i, j in iterables.items()}

    return results
