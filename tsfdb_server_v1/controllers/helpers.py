import dateparser
import re
import requests
import logging
import json
import time
import os
from datetime import datetime, timedelta
from line_protocol_parser import parse_line
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


def metric_to_dict(metric, metric_type, timestamp=0):
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
            "type": metric_type,
            "last_updated": timestamp
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
        start = parse_time(start)

    if not stop:
        stop = datetime.now()
    else:
        stop = parse_time(stop)

    #  round down start and stop time
    start = start.replace(microsecond=0)
    stop = stop.replace(microsecond=0)

    return start, stop


def parse_time(dt):
    # Convert "y" to "years" since dateparser doesn't support it
    # e.g. -2y => -2years
    dt = re.sub("y$", "years", dt)
    if re.match(".*ms", dt):
        dt = dt.replace("ms", "")
        dt = "%ds" % int(float(dt) / 1000)
    return dateparser.parse(dt)


def parse_relative_time_to_seconds(dt):
    return round((datetime.now() - parse_time(dt)).total_seconds())


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
    datapoints1_dict = {t1: d1 for (d1, t1) in datapoints1}
    datapoints2_dict = {t2: d2 for (d2, t2) in datapoints2}
    datapoints = []
    for _, t1 in datapoints1:
        if (datapoints1_dict.get(t1) is not None
                and datapoints2_dict.get(t1) is not None):
            if datapoints2_dict[t1] == 0:
                datapoints.append([0, t1])
            else:
                datapoints.append(
                    [datapoints1_dict[t1]/datapoints2_dict[t1], t1])
    return datapoints


def profile(func):
    def wrap(*args, **kwargs):
        begin = time.time()
        func(*args, **kwargs)
        end = time.time()
        dt = int((end - begin)*1000)
        timestamp = str(int(datetime.now().timestamp())) + 9 * '0'
        print(("Function %s took %d msecs") %
              (func.__name__, dt))
        if config('STATS_LOG_RATE') > 0:
            if len(args) >= 1 and args[0] != "tsfdb":
                if int(datetime.now().timestamp()) % \
                        config('STATS_LOG_RATE') == 0:
                    line = ((
                        "stats,machine_id=tsfdb,func=%s" +
                        " latency=%f %s") %
                        (func.__name__, dt, timestamp))
                    from tsfdb_server_v1.controllers.db import write_in_kv
                    write_in_kv("tsfdb", line + "\n")

    return wrap


def print_trace(func):
    import asyncio
    if asyncio.iscoroutinefunction(func):
        async def wrap(*args, **kwargs):
            import traceback
            import sys
            try:
                return await func(*args, **kwargs)
            except:
                traceback.print_exc(file=sys.stdout)
                raise
    else:
        def wrap(*args, **kwargs):
            import traceback
            import sys
            try:
                return func(*args, **kwargs)
            except:
                traceback.print_exc(file=sys.stdout)
                raise
    return wrap


def config(name):
    config_dict = {
        'AGGREGATE_MINUTE': (os.getenv('AGGREGATE_MINUTE', 'True') == 'True'),
        'AGGREGATE_HOUR': (os.getenv('AGGREGATE_HOUR', 'True') == 'True'),
        'AGGREGATE_DAY': (os.getenv('AGGREGATE_DAY', 'True') == 'True'),
        'DO_NOT_CACHE_FDB_DIRS':
        (os.getenv('DO_NOT_CACHE_FDB_DIRS', 'False') == 'True'),
        'TRANSACTION_RETRY_LIMIT':
        int(os.getenv('TRANSACTION_RETRY_LIMIT', 0)),
        'TRANSACTION_TIMEOUT': int(os.getenv('TRANSACTION_TIMEOUT', 2000)),
        'CHECK_DUPLICATES': (os.getenv('CHECK_DUPLICATES', 'False') == 'True'),
        'TSFDB_URI': os.getenv('TSFDB_URI', "http://localhost:8080"),
        'TSFDB_NOTIFICATIONS_WEBHOOK':
        os.getenv('TSFDB_NOTIFICATIONS_WEBHOOK'),
        'ACQUIRE_TIMEOUT': float(os.getenv('ACQUIRE_TIMEOUT', 30)),
        'CONSUME_TIMEOUT': float(os.getenv('CONSUME_TIMEOUT', 1)),
        'QUEUE_RETRY_TIMEOUT': int(os.getenv('QUEUE_RETRY_TIMEOUT', 5)),
        'QUEUE_TRANSACTION_RETRY_LIMIT':
        int(os.getenv('QUEUE_TRANSACTION_RETRY_LIMIT', 3)),
        'WRITE_IN_QUEUE': (os.getenv('WRITE_IN_QUEUE', 'True') == 'True'),
        'SECONDS_RANGE': int(os.getenv('SECONDS_RANGE', 1)),
        'MINUTES_RANGE': int(os.getenv('MINUTES_RANGE', 48)),
        'HOURS_RANGE': int(os.getenv('HOURS_RANGE', 1440)),
        'QUEUES': int(os.getenv('QUEUES', -1)),
        'STATS_LOG_RATE': int(os.getenv('STATS_LOG_RATE', -1)),
        'DATAPOINTS_PER_READ': int(os.getenv('DATAPOINTS_PER_READ', 200)),
        'ACTIVE_METRIC_MINUTES': int(os.getenv('ACTIVE_METRIC_MINUTES', 60))
    }
    return config_dict.get(name)


def time_range_to_resolution(time_range_in_hours):
    if time_range_in_hours <= config('SECONDS_RANGE'):
        return 'second'
    elif time_range_in_hours <= config('MINUTES_RANGE'):
        return 'minute'
    elif time_range_in_hours <= config('HOURS_RANGE'):
        return 'hour'
    return 'day'


def get_fallback_resolution(resolution):
    fallback_resolutions = {
        'second': 'minute',
        'minute': 'hour',
        'hour': 'day'
    }
    return fallback_resolutions.get(resolution)


def seperate_metrics(data):
    data = data.split('\n')
    # Get rid of all empty lines
    data = [line for line in data if line != ""]
    data_tsfdb = []
    data_rest = []
    for line in data:
        if parse_line(line)["tags"]["machine_id"] == "tsfdb":
            data_tsfdb.append(line)
        else:
            data_rest.append(line)
    return '\n'.join(data_tsfdb), '\n'.join(data_rest)


def get_machine_id(data):
    data = data.split('\n')
    # Get rid of all empty lines
    data = [line for line in data if line != ""]
    return parse_line(data[0])["tags"]["machine_id"]


def get_queue_id(data):
    machine_id = get_machine_id(data)
    if config('QUEUES') == -1:
        return machine_id
    return 'q' + str(hash(machine_id) % config('QUEUES'))


def filter_artifacts(start, stop, datapoints):
    return [[val, dt] for val, dt in datapoints
            if start <= datetime.fromtimestamp(dt) <= stop]
