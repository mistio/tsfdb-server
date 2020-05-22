import dateparser
import re
import requests
import logging
import json
import time
from datetime import datetime, timedelta
from tsfdb_server_v1.models.error import Error  # noqa: E501

log = logging.getLogger(__name__)
log.setLevel(logging.ERROR)


def round_base(x, precision, base):
    return round(base * round(float(x)/base), precision)


def log2slack(log_entry):
    if not config('TSFDB_NOTIFICATIONS_WEBHOOK'):
        return

    response = requests.post(
        config('TSFDB_NOTIFICATIONS_WEBHOOK'),
        data=json.dumps({'text': log_entry}),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        log.error(
            'Request to slack returned an error %s, the response is:'
            '\n%s' % (response.status_code, response.text)
        )


def error(code, error_msg, traceback=None, request=None):
    if code >= 500:
        if traceback and log.getEffectiveLevel() <= logging.INFO:
            error_msg += ("\nTRACEBACK: %s" % traceback)
        if request and log.getEffectiveLevel() <= logging.DEBUG:
            error_msg += ("\nREQUEST: %s" % request)
        log.error("ERROR: %s" % error_msg)
        log2slack(error_msg)
    return Error(code, error_msg)


def metric_to_dict(metric, metric_type):
    return {
        metric: {
            "id": metric,
            "name": metric,
            "column": metric,
            "measurement": metric,
            "max_value": None,
            "min_value": None,
            "priority": 0,
            "unit": "",
            "type": metric_type
        }
    }


def parse_start_stop_params(start, stop):
    """Helper method which parses the start/stop params
       from relative values(sec,min,hour, etc..) to datetime
       and returns them in an array.
    """

    #  set start/stop params if they do not exist
    if not start:
        start = datetime.now() - timedelta(minutes=10)
    else:
        # Convert "y" to "years" since dateparser doesn't support it
        # e.g. -2y => -2years
        start = re.sub("y$", "years", start)
        start = dateparser.parse(start)

    if not stop:
        stop = datetime.now()
    else:
        stop = re.sub("y$", "years", stop)
        stop = dateparser.parse(stop)

    #  round down start and stop time
    start = start.replace(microsecond=0)
    stop = stop.replace(microsecond=0)

    return start, stop


def is_regex(string):
    return not bool(re.match("^[a-zA-Z0-9.]+$", string))


def decrement_time(dt, resolution):
    if resolution == "minute":
        return dt - timedelta(minutes=1)
    elif resolution == "hour":
        return dt - timedelta(hours=1)
    return dt - timedelta(days=1)


def generate_metric(tags, measurement):
    tags.pop("machine_id", None)
    tags.pop("host", None)
    metric = measurement
    # First sort the tags in alphanumeric order
    tags = sorted(tags.items())
    # Then promote the tags which have the same name as the measurement
    tags = sorted(tags, key=lambda item: item[0] == measurement, reverse=True)
    for tag, value in tags:
        processed_tag = tag.replace(measurement, '')
        processed_value = value.replace(measurement, '')
        # Ignore the tag if it is empty
        if processed_tag:
            metric += (".%s" % processed_tag)
        # Ignore the value if it is empty
        if processed_value and processed_tag:
            metric += ("-%s" % processed_value)
        # Accomodate for the possibility
        # that there is a value with an empty tag
        elif processed_value:
            metric += (".%s" % processed_value)

    metric = metric.replace('/', '-')
    metric = metric.replace('.-', '.')
    metric = re.sub(r'\.+', ".", metric)
    return metric


def div_datapoints(datapoints1, datapoints2):
    return [[d1/d2, t1]
            for ((d1, t1), (d2, t2)) in zip(datapoints1, datapoints2)
            if t1 == t2]


def profile(func):
    def wrap(*args, **kwargs):
        begin = time.time()
        func(*args, **kwargs)
        end = time.time()
        print(("Function %s took %d msecs") %
              (func.__name__, int((end - begin)*1000)))

    return wrap


def config(name):
    config_dict = {
        'AGGREGATE_MINUTE': True,
        'AGGREGATE_HOUR': True,
        'AGGREGATE_DAY': True,
        'DO_NOT_CACHE_FDB_DIRS': False,
        'TRANSACTION_RETRY_LIMIT': 0,
        'TRANSACTION_TIMEOUT': 2000,
        'CHECK_DUPLICATES': False,
        'TSFDB_URI': "http://localhost:8080",
        'TSFDB_NOTIFICATIONS_WEBHOOK': "",
        'ACQUIRE_TIMEOUT': 30,
        'CONSUME_TIMEOUT': 1,
        'QUEUE_RETRY_TIMEOUT': 5,
        'WRITE_IN_QUEUE': True,
        'SECONDS_RANGE': 1,
        'MINUTES_RANGE': 48,
        'HOURS_RANGE': 1440
    }
    return config_dict.get(name)
