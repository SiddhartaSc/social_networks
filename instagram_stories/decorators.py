import time
import logging

def exponential_backoff_retry(some_function):
    """
    Tries to execute the (void) funcion, if it succeeds, then it returns the value.
    If not it tries 3 times with exponential backoff.
    usage:
        @exponential_backoff_retry
        some_function()
    """
    def wrapper(*args, **kwargs):
        timeout_ms = 100
        while True:
            try:
                some_function(*args, **kwargs)
                return True
            except:
                logging.exception('retry: {} Timeout: {}'.format(some_function.__name__,str(timeout_ms)))
                if timeout_ms == 800:
                    logging.exception(some_function.__name__ + ' failed after exponential retry!')
                    return False
                time.sleep(timeout_ms/1000)
                timeout_ms *= 2

    return wrapper

def exponential_backoff_retry_return_fvalue(some_function):
    """
    Tries to execute the  funcion, if it succeeds, then it returns the value.
    If not it tries 3 times with exponential backoff.
    usage:
        @exponential_backoff_retry
        some_function()
    """
    def wrapper(*args, **kwargs):
        timeout_ms = 100
        while True:
            try:
                value = some_function(*args, **kwargs)
                return value
            except:
                logging.exception('retry: {} Timeout: {}'.format(some_function.__name__,str(timeout_ms)))
                if timeout_ms == 800:
                    logging.exception(some_function.__name__ + ' failed after exponential retry!')
                    return False
                time.sleep(timeout_ms/1000)
                timeout_ms *= 2

    return wrapper