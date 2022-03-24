import time
from functools import wraps, update_wrapper


class TimeIt:
    def __init__(self, func=None, decimals=4, text=None):
        '''
        Utility class to time:
            1. Callables
            2. Indented blocks

        Args:
            - func (callable, default=None)
            - decimals (int, default=4)
            - text(str, default=None): Use text for indented block

        Examples:

            A)
                @TimeIt
                def f():
                    pass

                f()

                >> f takes: 0.0006 sec.

            B) 
                @TimeIt(decimals=8)
                def f():
                    pass

                f()

                >> f takes: 0.00000003 sec.

            C) 
                with TimeIt():
                    time.sleep(0.3)

                >> indented block takes: 0.3005 sec.

            D) 
                class Foo:
                @TimeIt() # It has to be a callable for classes
                def m(self):
                    pass

                Foo().m()

                >> m takes: 0.0 sec.

        '''
        update_wrapper(self, func)

        self.func = func
        self.decimals = decimals
        self.block_text = text or 'indented block'

    def __call__(self, func=None, *args, **kwargs):
        start = time.time()

        # When decorating without callable
        if self.func:
            # the func argument work as the first positional argument
            if func:

                args = [func] + list(args)

            result = self.func(*args, **kwargs)

            t = time.time() - start

            print(f"{self.func.__name__} takes: {round(t, self.decimals)} sec.")

            return result

        # When decorating with callable
        else:
            @wraps(func)
            def wrapper(*args, **kwargs):

                result = func(*args, **kwargs)

                t = time.time() - start

                print(f"{func.__name__} takes: {round(t, self.decimals)} sec.")

                return result

            return wrapper
    
    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        start = time.time()

        t = time.time() - self.start

        print(f"{self.block_text} takes: {round(t, self.decimals)} sec.")
