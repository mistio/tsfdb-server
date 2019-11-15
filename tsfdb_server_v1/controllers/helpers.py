import fdb
import fdb.tuple
import datetime
import dateparser
import re

from tsfdb_server_v1.models.error import Error  # noqa: E501

fdb.api_version(610)


def create_key_tuple_second(dt, machine, metric):
    return (
        create_key_tuple_minute(dt, machine, metric) +
        (dt.second,)
    )


def create_key_tuple_minute(dt, machine, metric):
    return (
        create_key_tuple_hour(dt, machine, metric) +
        (dt.minute,)
    )


def create_key_tuple_hour(dt, machine, metric):
    return (
        create_key_tuple_day(dt, machine, metric) +
        (dt.hour,)
    )


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


def create_start_stop_key_tuples(
    time_range_in_hours, monitoring, machine, metric, start, stop
):
    # if time range is less than an hour, we create the keys for getting the
    # datapoints per second
    if time_range_in_hours <= 1:
        return [
            monitoring.pack(create_key_tuple_second(start, machine, metric)),
            monitoring.pack(create_key_tuple_second(stop, machine, metric)),
        ]
    # if time range is less than 2 days, we create the keys for getting the
    # summarizeddatapoints per minute
    elif time_range_in_hours <= 48:
        return [
            monitoring["metric_per_minute"].pack(
                create_key_tuple_minute(start, machine, metric)
            ),
            monitoring["metric_per_minute"].pack(
                create_key_tuple_minute(stop, machine, metric)
            ),
        ]
    # if time range is less than 2 months, we create the keys for getting
    # the summarized datapoints per hour
    elif time_range_in_hours <= 1440:
        return [
            monitoring["metric_per_hour"].pack(
                create_key_tuple_hour(start, machine, metric)
            ),
            monitoring["metric_per_hour"].pack(
                create_key_tuple_hour(stop, machine, metric)
            ),
        ]
    # if time range is more than 2 months, we create the keys for getting
    # the summarized datapoints per day
    else:
        return [
            monitoring["metric_per_day"].pack(
                create_key_tuple_day(start, machine, metric)
            ),
            monitoring["metric_per_day"].pack(
                create_key_tuple_day(stop, machine, metric)
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
        start = dateparser.parse(start)

    if not stop:
        stop = datetime.datetime.now()
    else:
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


def find_metrics(resource):
    db = fdb.open()
    metrics = {}
    if fdb.directory.exists(db, "monitoring"):
        monitoring = fdb.directory.open(db, "monitoring")

        for k, v in db[monitoring["available_metrics"][resource].range()]:
            data_tuple = monitoring["available_metrics"][resource].unpack(k)
            metrics.update(create_metric(data_tuple))

        return metrics

    else:
        return Error(message="Machine doesn't have the metrics directory.")


def find_resources(regex_resources):
    db = fdb.open()
    resources = []
    if fdb.directory.exists(db, "monitoring"):
        monitoring = fdb.directory.open(db, "monitoring")

        for k, v in db[monitoring["available_resources"].range()]:
            candidate = monitoring["available_resources"].unpack(k)
            if re.match("^%s$" % regex_resources, candidate):
                resources.append(k)

        return resources

    else:
        return Error(message="Machine doesn't have the metrics directory.")


def get_data(resource, start, stop, metrics):
    db = fdb.open()
    data = {}

    # Open the monitoring directory if it exists
    if fdb.directory.exists(db, "monitoring"):
        monitoring = fdb.directory.open(db, ("monitoring",))
    else:
        return Error(
            message="The directory you are trying to read does not exist."
        )

    start, stop = parse_start_stop_params(start, stop)
    time_range = stop - start
    time_range_in_hours = round(time_range.total_seconds() / 3600, 2)

    for metric in metrics:
        datapoints = []
        (
            key_timestamp_start,
            key_timestamp_stop,
        ) = create_start_stop_key_tuples(
            time_range_in_hours, monitoring, resource, metric, start, stop
        )
        try:
            for k, v in db[key_timestamp_start:key_timestamp_stop]:

                tuple_key = list(fdb.tuple.unpack(k))
                tuple_value = list(fdb.tuple.unpack(v))

                datapoints.append(
                    create_datapoint(
                        time_range_in_hours, tuple_value, tuple_key
                    )
                )

        except fdb.FDBError as error:
            return Error(message="db error." + str(error))
            # db.on_error(error).wait()
        data.update({("%s.%s" % (resource, metric)): datapoints})

    return data


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
            for candidate in all_metrics:
                if re.match("^%s$" % regex_metric, candidate):
                    metrics.append(candidate)
            if len(metrics) == 0:
                return Error(
                    message=(
                        "No metrics for this regex where found. metric = %s"
                        % regex_metric
                    )
                )
        else:
            metrics = [metrics]
        current_data = get_data(resource, start, stop, metrics)
        if isinstance(current_data, Error):
            return current_data
        data.update(current_data)

    return data
