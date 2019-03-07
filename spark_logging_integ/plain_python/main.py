import logging
import logging.config
# load my module
import my_module

# load the logging configuration
import coloredlogs
class CustomLoggingFormatter(logging.Formatter):
    data = {}

    def __init__(self,fmt,x_name):
        super(CustomLoggingFormatter, self).__init__(fmt,None)


    def format(self, record):
        return super(CustomLoggingFormatter, self).format(record)
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'console': {
            '()': 'coloredlogs.ColoredFormatter',
            'format': '%(asctime)s  %(process)d  %(name)s %(levelname)s - %(message)s ',
        },
        'logzioFormat': {
            'format': '{"process_id": %(process)d }'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'console'
        },
        'logzio': {
            'class': 'logzio.handler.LogzioHandler',
            'level': 'INFO',
            'formatter': 'logzioFormat',
            'token': 'GAnICDvFUktzANXoLVcdASeqKyGEMrXr',
            'logzio_type': "my_custom_type",
            'logs_drain_timeout': 5,
            'url': 'https://listener.logz.io:8071',
            'debug': True
        },
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': 'DEBUG'
        },
        'urllib3': {
            'level': 'WARN'
        }
    }
}
logging.config.dictConfig(LOGGING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

my_module.foo()
bar = my_module.Bar()
bar.bar()
import time
time.sleep(5)