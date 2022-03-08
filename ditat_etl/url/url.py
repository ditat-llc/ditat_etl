import os
import json
from typing import Union, Any
import random
import logging

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


filedir = os.path.abspath(os.path.dirname(__file__))


class Url:
    '''
    - IP_URL: Ip getter. It could also be used to verify that the proxy
        given is working by comparing the result to self.ip

    - PROXY_API: Url to get new proxies. Support for other urls is not yet
        avialable due to the response.json() formatting.

    - VALIDATION_URL: This url can be anything. It is used to self.eval_proxy
        and get a status_code == 200.

    - MAX_RETRIES: Used for self.request and others.
    '''
    IP_URL = 'https://api.ipify.org'
    PROXY_API = 'https://gimmeproxy.com/api/getProxy'
    VALIDATION_URL = 'https://google.com'
    MAX_RETRIES = 50

    def __init__(
        self,
        timeout: int=15,
        debug_level: str='DEBUG',
        max_workers: int=min(32, os.cpu_count() + 4),
        proxies_filepath: str= os.path.join(filedir, 'proxies.json'),
        add_proxies: int=None,
        **kwargs
        ):
        '''
        Args:
            - timeout (int, default=15): You can overwrite the timeout on individual methods.
            - debug_level (str, default='DEBUG'): self.load_logger for more details.
            - max_workers (int, default=min(32, os.cpu_count() + 4)): Many methods use
                multithreading. Documentation suggests this default according to server's capabilities.
            - proxies_filepath (str, default=os.path.join(filedir, 'proxies.json'):
                Dynamic file for proxies. Used for property "proxies"
            - add_proxies (int, default=2): Using the local server ip, add n proxies to self.proxies
        Returns:
            - None
        '''
        self.timeout = timeout
        self.max_workers = max_workers
        self.proxies_filepath = proxies_filepath
        self.load_logger(debug_level=debug_level)

        if add_proxies:
            self.add_proxies(add_proxies)

    def load_logger(
        self,
        debug_level: str='DEBUG'
        ):
        '''
        Complete setup for built-in logging module. This is a local implementation.

        Args:
            - debug_level (str, default='INFO'): One of DEBUG_LEVELS according to logging
                package.
        '''
        DEBUG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

        _raise = False

        if debug_level not in DEBUG_LEVELS:
            _raise = True           
            debug_level = 'DEBUG'

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.addHandler(logging.StreamHandler())
        self.logger.setLevel(getattr(logging, debug_level))

        if _raise:
            self.logger.warning(f'Debug level specified not valid. Setting to {debug_level}.')

    @property
    def proxies(self):
        '''
        Property "proxies" is always read from file.
        '''
        with open(self.proxies_filepath, 'r') as f:
            proxies_ = json.loads(f.read())
            random.shuffle(proxies_)
            # self.logger.info(f'Read proxies n={len(proxies)}')
            return proxies_

    @proxies.setter
    def proxies(self, proxies):
        '''
        Property "proxies" setter has an extra step if set incorrectly
        and keeping the previous values.
        '''
        proxies_ = self.proxies.copy()
        with open(self.proxies_filepath, 'w') as f:
            try:
                proxies = list(set(proxies))
                random.shuffle(proxies)
                proxies_json = json.dumps(proxies)
                f.write(proxies_json)
                self.logger.info(f'Set proxies n={len(proxies)}')
            except Exception as e:
                self.logger.warning('Could not set proxies with new values. Keeping old values.')
                f.write(json.dumps(proxies_))

    def append_proxies(self, proxies):
        '''
        Combination of setter and getter of "proxies".
        '''
        proxies = proxies if isinstance(proxies, list) else [proxies]
        _proxies = self.proxies.copy()
        self.logger.info(f'Appended proxies (n={len(proxies)}) are (first 10): {proxies[: 10]}')
        _proxies.extend(proxies)
        self.proxies = _proxies

    def _request(
        self,
        url: str,
        method: Union['get', 'post', 'update', 'delete']='get',
        proxy: str=None,
        timeout: int=False,
        _raise=False,
        extra_print: Any=None,
        expected_status_code: int=None,
        **kwargs
        ):
        '''
        Internal core method for http request.

        Note:
            - Only use self.request

        Args:
            - url (str)

            - method (str, default='get'): One of HTTP methods.

            - proxy (str, default=None): Optional usage of proxy for request.

            - timeout (int, default=False): It uses self.default. You can set
                an integer or None explicity to avoid having a timeout.

            - _raise (bool, default=False)

            - extra_print(Any, defualt=None): Along with the logger printing
                of the main response execution, you can add anything and it will
                be printed after the original message response.url f"{response.url}
                 - STATUS CODE: {response.status_code}".

            - expected_status_code (int, default=None): Shortcut to flag responses as
                successful or failed.

            - **kwargs: key, value pairs that are passed to the main requests.method

        Returns:
            - response (requests.models.Response): returns False if not successful.
        '''
        f_payload = {
            'url': url,
            'proxies': {'http': proxy, 'https': proxy},
            'timeout': self.timeout if timeout is False else timeout
        }

        try:
            f = getattr(requests, method.lower())
            response = f(**f_payload, **kwargs)
            msg = f"{response.url} - STATUS CODE: {response.status_code}"

            if extra_print:
                msg += f" - {extra_print}"

            self.logger.info(msg)

            if expected_status_code and expected_status_code != response.status_code:
                return False

            return response

        except Exception as e:
            if _raise:
                raise ValueError(e)

            err_msg = f"{url} - ERROR"

            self.logger.info(err_msg)
            self.logger.debug(e)

            return False

    def get_local_ip(self, url=None):
        '''
        Args:
            - url (str, default=None): Using the default Url.IP_URL if not provided.

        Returns:
            - result (str): If successful, returning the local ip.
        '''
        url  = url or Url.IP_URL

        resp = self._request(url=url, expected_status_code=200)

        if resp:
            result = resp.text
            self.logger.info(f"Local Ip is {result}")

            return result

        self.logger.warning('Could not get Local Ip')

    def request(
        self,
        queue: str or list,
        expected_status_code: int=None,
        n_times: int=1,
        max_retries: int=None,
        use_proxy=False,
        _raise=True,
        **kwargs
        ):
        '''
        Parallel execution of self.request using proxies
        and trying different proxies until successful (until max_retries)

        Args:
            - queue (str or list): url(s) to be requested.

            - expected_status_code(int, default=200): Any of the Http codes.

            - n_times (int, default=1): If type(queue) == str and n_times is provided
                queue is transformed to: [queue] * n_times.

            - max_retries (int, default=None): Proxy retrial. If use_proxy is False,
                max_retries = 1

            - use_proxy (bool, default=True)

            - _raise (bool, default=True): Set to False if partial results are ok.

        Returns:
            - results (list(requests.models.Response))
        '''
        # Setting the queue in the right format
        queue = [queue] * n_times if isinstance(queue, str) else queue
        queue = {index: value for index, value in enumerate(queue)}
        queue_len = len(queue)
        
        # Retry logic
        retries = 1
        max_retries = min(max_retries or len(self.proxies), Url.MAX_RETRIES) if use_proxy else 1

        result_dict = {}
        proxy_iter = iter(self.proxies)

        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            # Trying until queue is emtpy or reach max_tries looping through proxies
            while len(result_dict) < queue_len and retries <= max_retries:
                proxy_ = next(proxy_iter) if use_proxy else None
                self.logger.info(f"Trying proxy ({retries}/{max_retries}): {proxy_}")

                # Futures dictionary
                f_dict = {
                    ex.submit(
                        fn=self._request,
                        url=v,
                        proxy=proxy_,
                        expected_status_code=expected_status_code,
                        extra_print=f"Item n: {k}",
                        **kwargs
                    ): k for k, v in queue.items()
                }
                # Complete futures and add to result_dict
                for f in as_completed(f_dict):
                    result = f.result()

                    if result is not False:
                        result_dict[f_dict[f]] = result
                        del queue[f_dict[f]]

                retries += 1

        if _raise and len(result_dict) < queue_len:
            raise ImportError('Could not complete the bulk request!')

        elif len(result_dict) < queue_len:
            self.logger.warning('Could not complete the bulk request! Only returning successful.')

        # Sort according to initial queue order
        results = [kv[1] for kv in sorted(result_dict.items(), key=lambda x: x[0])]

        if queue_len == 1:
            results = results[0]

        return results

    def eval_proxy(
        self,
        proxy: str,
        url: str=None
        ):
        '''
        Evaluate if a given proxy is valid by hitting a certain url and
        expecting a 200 status_code

        Args:
            - proxy (str)
            - url (str, default=Url.VALIDATION_URL)

        Returns
            - proxy or False (depending on success)
        '''
        url = url or Url.VALIDATION_URL

        resp = self._request(url=url, proxy=proxy, expected_status_code=200)

        if resp:
            self.logger.info(f'Valid proxy: {proxy}')
            return proxy

        self.logger.info(f"Invalid proxy: {proxy}")

        return False

    def clean_proxies(self):
        '''
        Automatic process to clean self.proxies (parallel)
        '''
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = executor.map(self.eval_proxy, self.proxies)

        self.proxies = [proxy for proxy in results if proxy]

    def add_proxies(
        self,
        n: int=1,
        url: str=None,
        use_proxy: bool=False,
        max_retries: int =None
        ):
        '''
        Retrive proxies from the web and add them to self.proxies.

        * It does not validate whether or not the proxy(ies) is (are) valid.
        To do so, call self.clean_proxies()

        Args:
            - n (int, default=1): Number of proxies to be requested.

            - url (str, default=None): It uses Url.PROXY_API as default.

            - use_proxy (bool, default=False): Request from local ip or use proxies.
                This parameter as True hasn't been successful. In testing.

            - max_retries (int, default=None): If use_prixy is False, then one,
                else it uses the internal mechanism to return the max_retries with queries
                if max_retries is None.

        Notes:
            - use_proxy as True has not proved successful.

            - Real default for url == Url.PROXY_API. When parsing the json
                result of the successful response, there is some custom logic,
                therefore not allowing for a different url.
        '''
        url = url or Url.PROXY_API

        proxies = self.request(
            queue=url,
            expected_status_code=200,
            n_times=n,
            _raise=False,
            use_proxy=use_proxy,
            max_retries=max_retries
        )

        proxies = [resp.json()['curl'] for resp in proxies]
        self.append_proxies(proxies)

        return proxies


if __name__ == '__main__':
    u = Url()


