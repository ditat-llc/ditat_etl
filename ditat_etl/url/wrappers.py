from .url import Url


'''
Wrapper for fast applications.


For more information check ditat_etl.url.url.py --> class Url()
'''

debug_level = 'WARNING'


def request(method, url, debug_level=debug_level, **kwargs):
	url_instance = Url(debug_level=debug_level, **kwargs)
	response = url_instance.request(queue=url, method=method, **kwargs)
	return response


def get(url, debug_level=debug_level, **kwargs):
	url_instance = Url(debug_level=debug_level, **kwargs)
	response = url_instance.request(queue=url, method='get', **kwargs)
	return response


def post(url, debug_level=debug_level, **kwargs):
	url_instance = Url(debug_level=debug_level, **kwargs)
	response = url_instance.request(queue=url, method='post', **kwargs)
	return response


def update(url, debug_level=debug_level, **kwargs):
	url_instance = Url(debug_level=debug_level, **kwargs)
	response = url_instance.request(queue=url, method='update', **kwargs)
	return response


def delete(url, debug_level=debug_level, **kwargs):
	url_instance = Url(debug_level=debug_level, **kwargs)
	response = url_instance.request(queue=url, method='delete', **kwargs)
	return response