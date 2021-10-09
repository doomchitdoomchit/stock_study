from logging.config import dictConfig
import logging

dictConfig({
    'version': 1,
    'formatters': {
        'default': {
            'format': '[%(asctime)s] %(message)s',
        }
    },
    'handlers': {
        'file': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'filename': 'debug.log',
            'formatter': 'default',
        },
    },
    'root': {
        'level': 'DEBUG',
        'handlers': ['file']
    }
})


class LogWriter(object):
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        func_name = self.func.__name__
        logger = logging.getLogger()
        logging.debug(f"{func_name}가 시작되었습니다.")
        try:
            result = self.func(*args, **kwargs)
            logging.debug(f"{func_name}가 종료되었습니다.")
            return result
        except Exception as e:
            logger.exception(e)
            logging.error(e)
            raise
