import configparser
import os
import logging
import logging.handlers
import json
import copy
import datetime

config = configparser.RawConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'resources', 'config.cfg'))
# api_version = (0,10) could just connect without confirm if connected
# local_producer = KafkaProducer(bootstrap_servers=site.config.get('local', 'bootstrap_servers'),
#                                retries=60, retry_backoff_ms=60000, compression_type='gzip')
# redis_client_producer = redis.Redis(host=site.config.get('local', 'redis_server'),
#                                         port=site.config.get('local', 'redis_port'),
#                                         password=site.config.get('local', 'redis_password'))


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class LogDetails:
    site_name = None
    service = None
    level = None
    log = None
    time = None


def log_init(log_file, site_name, service):
    date_fmt = '%a, %d %b %Y %H:%M:%S'
    format_str = '%(asctime)s %(levelname)s %(message)s'
    formatter = logging.Formatter(format_str, date_fmt)
    if not os.path.exists(os.path.dirname(log_file)):
        os.makedirs(os.path.dirname(log_file))

    handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=1024 * 1024 * 5, backupCount=2,encoding='utf-8')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    global log_template
    log_template = LogDetails()
    log_template.site_name = site_name
    log_template.service = service


def log(level, msg):
    # noinspection PyBroadException
    log_details = copy.copy(log_template)
    log_details.log = msg
    log_details.level = level
    log_details.time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        if level == 'info':
            logger.info(json.dumps(log_details.__dict__))
        elif level == 'warning':
            logger.warning(json.dumps(log_details.__dict__))
        elif level == 'error':
            logger.error(json.dumps(log_details.__dict__))
        elif level == 'critical':
            logger.critical(json.dumps(log_details.__dict__))
        else:
            print('Log type error! ')
        # if link is not None:
        #     link.lpush('log', pickle.dumps(log_details))

    except Exception as e:
        # just print if unexpected exception happened.
        # actually str(e) works in python3. it could be some coding errors in python2.
        print('Log queue error! ' + str(e))
