import os
from random import uniform
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


from .url import Url


filedir = os.path.abspath(os.path.dirname(__file__))


class Base:
    LOCATORS = {
        'class': 'CLASS_NAME',
        'id': 'ID',
        'css': 'CSS_SELECTOR',
        'link': 'LINK_TEXT',
        'name': 'NAME',
        'partial_link': 'PARTIAL_LINK_TEXT',
        'tag': 'TAG_NAME',
        'xpath': 'XPATH'
    }

    def __init__(self):
        '''
        '''
        self.base_path = filedir
        self.url = Url()

    def start_driver(
        self,
        width=2560,
        height=1440,
        headless=False,
        driver_folder='drivers',
        driver_name='chromedriver',
        proxy=False
        ):
        '''
        Driver init.

        Args:
            - width (int, default=2560)
            - height (int, default=1440)
            - headless(bool, default=False): See what happens
            - driver_folder (str, default='drivers'): Where to save chromedriver.
            - driver_name (str, default='chromedriver'): Name of driver.
            - proxy (bool, default=False): Use proxy for driver (from self.url.proxies)
        '''
        self.driver_path = os.path.join(
            self.base_path,
            driver_folder,
            driver_name
        )
        self.driver_options = Options()
        self.driver_options.headless = headless
        if proxy:
            self.driver_options.add_argument(
                f'--proxy-server={self.url.proxies[0]}'
            )
        self.driver = webdriver.Chrome(
            executable_path=self.driver_path,
            options=self.driver_options
            )
        self.driver.set_window_size(width, height)

    @staticmethod
    def rand_sleep(
        min=1,
        max=3,
        **kwargs
        ):
        '''
        Random sleep for each action to avoid getting caught.

        Args:
            - min (int, default=1)
            - max (int, default=3)
        '''
        s = time.sleep(uniform(min, max))

    def nav(
        self,
        path,
        **kwargs
        ):
        '''
        Main navigation method.

        Args:
            - path (str)
        '''
        self.driver.get(path)
        type(self).rand_sleep(**kwargs)

    def click(
        self,
        element,
        **kwargs
        ):
        element.click()
        type(self).rand_sleep(**kwargs)

    def get_element_by(
        self,
        by,
        by_element,
        many=False,
        wait=True,
        wait_time=5,
        close_driver=True
        ):
        if wait:
            wait_response = self.wait(
                by_element=by_element,
                by=by,
                wait=wait_time,
                close_driver=close_driver
            )
            if not wait_response:
                return None
        if by not in type(self).LOCATORS:
            by_element = f"*[{by}='{by_element}']"
            by = 'css'
        elements = self.driver.find_elements(
            getattr(By, type(self).LOCATORS[by]),
            by_element
        )
        if not elements:
            return None
        elements = elements if many else elements[0]
        return elements

    def wait(
        self,
        by_element,
        by,
        wait=5,
        close_driver=True
        ):
        if by not in type(self).LOCATORS:
            by_element = f"*[{by}='{by_element}']"
            by = 'css'
        try:
            wait_element =  WebDriverWait(self.driver, wait).until(
                EC.presence_of_element_located((
                    getattr(By, type(self).LOCATORS[by]),
                    by_element
                ))
            )
            return True
        except Exception as e:
            print(e)
            if close_driver:
                self.driver.close()
            return False


