import time
import re
from functools import wraps


def time_it(text=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            '''
            Simple timer for functions, methods, allowing optional text.

            Args:
                - text (str, default=None): If given, it is appended in the output of the timer.

            Returns:
                - result: Function of method invoked being decorated.
            '''
            t1 = time.time()
            result = func(*args, **kwargs)
            time_out = f"{func.__qualname__} time: {round(time.time() - t1, 3)}"
            if text:
                time_out += ", " + str(text)
            print(time_out)
            return result
        return wrapper
    return decorator



####### MOVE TO ANOTHER FILE ###########
def int_to_chunks(number, n):
    l = [int(number / n)] * n
    diff = number % n
    for i in range(diff):
        l[i] += 1
    return l

########################################
