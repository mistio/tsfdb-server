import fdb
import fdb.tuple
import datetime
import dateparser
import re
import numpy as np
import logging

from tsfdb_server_v1.models.error import Error  # noqa: E501

fdb.api_version(620)

log = logging.getLogger(__name__)


def create_key_tuple_second(dt, machine, metric):
    return create_key_tuple_minute(dt, machine, metric) + (dt.second,)


def create_key_tuple_minute(dt, machine, metric):
    return create_key_tuple_hour(dt, machine, metric) + (dt.minute,)


def create_key_tuple_hour(dt, machine, metric):
    return create_key_tuple_day(dt, machine, metric) + (dt.hour,)


def create_key_tuple_day(dt, machine, metric):
    return (
        machine,
        metric,
        dt.year,
        dt.month,
        dt.day,
    )


def create_timestamp_second(tuple_key):
    # The last 6 items of the tuple contain the date up to the second
    # (year, month, day, hour, minute, second)
    return int(datetime.datetime(*tuple_key[-6:]).timestamp())


def create_timestamp_minute(tuple_key):
    # The last 5 items of the tuple contain the date up to the minute
    # (year, month, day, hour, minute)
    return int(datetime.datetime(*tuple_key[-5:]).timestamp())


def create_timestamp_hour(tuple_key):
    # The last 4 items of the tuple contain the date up to the hour
    # (year, month, day, hour)
    return int(datetime.datetime(*tuple_key[-4:]).timestamp())


def create_timestamp_day(tuple_key):
    # The last 3 items of the tuple contain the date up to the day
    # (year, month, day)
    return int(datetime.datetime(*tuple_key[-3:]).timestamp())


def open_db():
    db = fdb.open()
    db.options.set_transaction_retry_limit(3)
    db.options.set_transaction_timeout(1000)
    return db


def error(code, error_msg):
    log.error(error_msg)
    return Error(code, error_msg)


def create_start_stop_key_tuples(
    time_range_in_hours, monitoring, machine, metric, start, stop
):
    # if time range is less than an hour, we create the keys for getting the
    # datapoints per second
    if time_range_in_hours <= 1:
        # delta compensates for the range function of foundationdb which
        # for start, stop returns keys in [start, stop). We convert it to
        # the range [start, stop]
        delta = datetime.timedelta(seconds=1)
        return [
            monitoring.pack(create_key_tuple_second(start, machine, metric)),
            monitoring.pack(create_key_tuple_second(
                stop + delta, machine, metric)),
        ]
    # if time range is less than 2 days, we create the keys for getting the
    # summarizeddatapoints per minute
    elif time_range_in_hours <= 48:
        delta = datetime.timedelta(minutes=1)
        return [
            monitoring["metric_per_minute"].pack(
                create_key_tuple_minute(start, machine, metric)
            ),
            monitoring["metric_per_minute"].pack(
                create_key_tuple_minute(stop + delta, machine, metric)
            ),
        ]
    # if time range is less than 2 months, we create the keys for getting
    # the summarized datapoints per hour
    elif time_range_in_hours <= 1440:
        delta = datetime.timedelta(hours=1)
        return [
            monitoring["metric_per_hour"].pack(
                create_key_tuple_hour(start, machine, metric)
            ),
            monitoring["metric_per_hour"].pack(
                create_key_tuple_hour(stop + delta, machine, metric)
            ),
        ]
    # if time range is more than 2 months, we create the keys for getting
    # the summarized datapoints per day
    else:
        delta = datetime.timedelta(hours=24)
        return [
            monitoring["metric_per_day"].pack(
                create_key_tuple_day(start, machine, metric)
            ),
            monitoring["metric_per_day"].pack(
                create_key_tuple_day(stop + delta, machine, metric)
            ),
        ]


def create_timestamp(time_range_in_hours, tuple_key):
    # if time range is less than an hour, we create the timestamp per second
    if time_range_in_hours <= 1:
        timestamp = create_timestamp_second(tuple_key)
    # if time range is less than 2 days, we create the timestamp per minute
    elif time_range_in_hours <= 48:
        timestamp = create_timestamp_minute(tuple_key)
    # if time range is less than 2 months, we create the timestamp per hour
    elif time_range_in_hours <= 1440:
        timestamp = create_timestamp_hour(tuple_key)
    # if time range is more than 2 months, we create the timestamp per day
    else:
        timestamp = create_timestamp_day(tuple_key)
    return timestamp


def create_metric(data_tuple):
    return {
        data_tuple[1] +
        "." +
        data_tuple[2]: {
            "id": data_tuple[1] + "." + data_tuple[2],
            "name": data_tuple[1] + "." + data_tuple[2],
            "column": data_tuple[1] + "." + data_tuple[2],
            "measurement": data_tuple[1] + "." + data_tuple[2],
            "max_value": None,
            "min_value": None,
            "priority": 0,
            "unit": "",
        }
    }


def parse_start_stop_params(start, stop):
    """Helper method which parses the start/stop params
       from relative values(sec,min,hour, etc..) to datetime
       and returns them in an array.
    """

    #  set start/stop params if they do not exist
    if not start:
        start = datetime.datetime.now() - datetime.timedelta(minutes=10)
    else:
        # Convert "y" to "years" since dateparser doesn't support it
        # e.g. -2y => -2years
        start = re.sub("y$", "years", start)
        start = dateparser.parse(start)

    if not stop:
        stop = datetime.datetime.now()
    else:
        stop = re.sub("y$", "years", stop)
        stop = dateparser.parse(stop)

    #  round down start and stop time
    start = start.replace(second=0, microsecond=0)
    stop = stop.replace(second=0, microsecond=0)

    return start, stop


def create_datapoint(time_range_in_hours, tuple_value, tuple_key):
    timestamp = create_timestamp(time_range_in_hours, tuple_key)
    # if the range is less than an hour, we create the appropriate
    # datapoint [value, timestamp]
    if time_range_in_hours <= 1:
        return [tuple_value[0], timestamp]
    # else we need to use the summarized values [sum, count, min, max]
    # and convert them to a datapoint [value, timestamp]
    else:
        sum_values = tuple_value[0]
        count = tuple_value[1]
        return [sum_values / count, timestamp]


def is_regex(string):
    return not bool(re.match("^[a-zA-Z0-9.]+$", string))


@fdb.transactional
def get_metrics(tr, monitoring, resource):
    metrics = {}
    for k, v in tr[monitoring["available_metrics"][resource].range()]:
        data_tuple = monitoring["available_metrics"][resource].unpack(k)
        metrics.update(create_metric(data_tuple))

    return metrics


def find_metrics(resource):
    try:
        db = open_db()
        if fdb.directory.exists(db, "monitoring"):
            monitoring = fdb.directory.open(db, "monitoring")
        else:
            error_msg = "Monitoring directory doesn't exist."
            return error(503, error_msg)

        return get_metrics(db, monitoring, resource)
    except fdb.FDBError as err:
        return error(503, str(err.description, 'utf-8'))


@fdb.transactional
def get_resources(tr, monitoring, regex_resources):
    resources = []
    for k, v in tr[monitoring["available_resources"].range()]:
        candidate = monitoring["available_resources"].unpack(k)
        if re.match("^%s$" % regex_resources, candidate):
            resources.append(k)

    return resources


def find_resources(regex_resources):
    try:
        db = open_db()
        if fdb.directory.exists(db, "monitoring"):
            monitoring = fdb.directory.open(db, "monitoring")
            return get_resources(db, monitoring, regex_resources)

        else:
            error_msg = "Monitoring directory doesn't exist."
            return error(503, error_msg)
    except fdb.FDBError as err:
        return error(503, str(err.description, 'utf-8'))


@fdb.transactional
def get_datapoints(tr, start, stop, time_range_in_hours):
    datapoints = []
    for k, v in tr[start:stop]:

        tuple_key = list(fdb.tuple.unpack(k))
        tuple_value = list(fdb.tuple.unpack(v))

        datapoints.append(
            create_datapoint(
                time_range_in_hours, tuple_value, tuple_key
            )
        )
    return datapoints


def get_data(resource, start, stop, metrics):
    try:
        db = open_db()
        data = {}
        # Open the monitoring directory if it exists
        if fdb.directory.exists(db, "monitoring"):
            monitoring = fdb.directory.open(db, ("monitoring",))
        else:
            error_msg = "Monitoring directory doesn't exist."
            return error(503, error_msg)

        start, stop = parse_start_stop_params(start, stop)
        time_range = stop - start
        time_range_in_hours = round(time_range.total_seconds() / 3600, 2)

        for metric in metrics:
            (
                key_timestamp_start,
                key_timestamp_stop,
            ) = create_start_stop_key_tuples(
                time_range_in_hours, monitoring, resource, metric, start, stop
            )

            datapoints = get_datapoints(
                db, key_timestamp_start, key_timestamp_stop,
                time_range_in_hours)

            data.update({("%s.%s" % (resource, metric)): datapoints})

        return data
    except fdb.FDBError as err:
        return error(503, str(err.description, 'utf-8'))


def fetch(resources_and_metrics, start="", stop="", step=""):
    # We take for granted that all metrics start with the id and that
    # it ends on the first occurence of a dot, e.g id.system.load1
    data = {}
    resources, metrics = resources_and_metrics.split(".", 1)
    if is_regex(resources):
        # At the moment we ignore the cases where the resource is a regex
        # resources = find_resources(resources)
        return {}
    else:
        resources = [resources]

    for resource in resources:
        if is_regex(metrics):
            regex_metric = metrics
            metrics = []
            all_metrics = find_metrics(resource)
            if isinstance(all_metrics, Error):
                return all_metrics
            if regex_metric == "*":
                metrics = [candidate for candidate in all_metrics]
            else:
                for candidate in all_metrics:
                    if re.match("^%s$" % regex_metric, candidate):
                        metrics.append(candidate)
            if len(metrics) == 0:
                error_msg = (
                    "No metrics for regex: \"%s\" where found" % regex_metric
                )
                return error(400, str(error_msg, 'utf-8'))
        else:
            metrics = [metrics]
        current_data = get_data(resource, start, stop, metrics)
        if isinstance(current_data, Error):
            return current_data
        data.update(current_data)

    return data


def deriv(data):
    if not isinstance(data, dict) or not data:
        return {}
    for metric, datapoints in data.items():
        if not datapoints or len(datapoints) < 2:
            data[metric] = []
            continue
        values = np.asarray([value for value, _ in datapoints])
        timestamps = np.asarray([timestamp for _, timestamp in datapoints])
        values_deriv = np.gradient(values, timestamps)
        data[metric] = [
            [value_deriv.tolist(), timestamp.tolist()]
            for value_deriv, timestamp in zip(
                np.nditer(values_deriv), np.nditer(timestamps)
            )
        ]
    return data
