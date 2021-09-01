'''
Test stuff
'''

# from url import Url

# url_instance = Url()

# resp = url_instance.request('https://google.com', n_times=5, use_proxy=True)

# print(resp)

import url

resp = url.get('https://google.com', use_proxy=True, debug_level='DEBUG')
resp = url.request(method='GET','https://google.com', use_proxy=True, debug_level='DEBUG')
print(resp)
