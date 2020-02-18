import logging
from .helpers import error
from datetime import datetime, timedelta

log = logging.getLogger(__name__)


def key_tuple_second(dt, machine, metric):
    return key_tuple_minute(dt, machine, metric) + (dt.second,)


def key_tuple_minute(dt, machine, metric):
    return key_tuple_hour(dt, machine, metric) + (dt.minute,)


def key_tuple_hour(dt, machine, metric):
    return key_tuple_day(dt, machine, metric) + (dt.hour,)


def key_tuple_day(dt, machine, metric):
    return (
        machine,
        metric,
        dt.year,
        dt.month,
        dt.day,
    )


def start_stop_key_tuples(
    db, time_range_in_hours, monitoring, machine, metric, start, stop
):
    # if time range is less than an hour, we create the keys for getting the
    # datapoints per second
    if time_range_in_hours <= 1:
        # delta compensates for the range function of foundationdb which
        # for start, stop returns keys in [start, stop). We convert it to
        # the range [start, stop]
        delta = timedelta(seconds=1)
        return [
            monitoring.pack(key_tuple_second(start, machine, metric)),
            monitoring.pack(key_tuple_second(
                stop + delta, machine, metric)),
        ]
    # if time range is less than 2 days, we create the keys for getting the
    # summarizeddatapoints per minute
    elif time_range_in_hours <= 48:
        delta = timedelta(minutes=1)
        if monitoring.exists(db, "metric_per_minute"):
            monitoring_sum = monitoring.open(db, ("metric_per_minute",))
        else:
            error_msg = "metric_per_minute directory doesn't exist."
            return error(503, error_msg, log)
        return [
            monitoring_sum.pack(
                key_tuple_minute(start, machine, metric)
            ),
            monitoring_sum.pack(
                key_tuple_minute(stop + delta, machine, metric)
            ),
        ]
    # if time range is less than 2 months, we create the keys for getting
    # the summarized datapoints per hour
    elif time_range_in_hours <= 1440:
        delta = timedelta(hours=1)
        if monitoring.exists(db, "metric_per_hour"):
            monitoring_sum = monitoring.open(db, ("metric_per_hour",))
        else:
            error_msg = "metric_per_hour directory doesn't exist."
            return error(503, error_msg, log)
        return [
            monitoring_sum.pack(
                key_tuple_hour(start, machine, metric)
            ),
            monitoring_sum.pack(
                key_tuple_hour(stop + delta, machine, metric)
            ),
        ]
    # if time range is more than 2 months, we create the keys for getting
    # the summarized datapoints per day
    else:
        delta = timedelta(hours=24)
        if monitoring.exists(db, "metric_per_day"):
            monitoring_sum = monitoring.open(db, ("metric_per_day",))
        else:
            error_msg = "metric_per_day directory doesn't exist."
            return error(503, error_msg, log)
        return [
            monitoring_sum.pack(
                key_tuple_day(start, machine, metric)
            ),
            monitoring_sum.pack(
                key_tuple_day(stop + delta, machine, metric)
            ),
        ]


def tuple_to_timestamp(time_range_in_hours, tuple_key):
    # if time range is less than an hour, we create the timestamp per second
    # The last 6 items of the tuple contain the date up to the second
    # (year, month, day, hour, minute, second)
    if time_range_in_hours <= 1:
        return int(datetime(*tuple_key[-6:]).timestamp())
    # if time range is less than 2 days, we create the timestamp per minute
    # The last 5 items of the tuple contain the date up to the minute
    # (year, month, day, hour, minute)
    if time_range_in_hours <= 48:
        return int(datetime(*tuple_key[-5:]).timestamp())
    # if time range is less than 2 months, we create the timestamp per hour
    # The last 4 items of the tuple contain the date up to the hour
    # (year, month, day, hour)
    if time_range_in_hours <= 1440:
        return int(datetime(*tuple_key[-4:]).timestamp())
    # if time range is more than 2 months, we create the timestamp per day
    # The last 3 items of the tuple contain the date up to the day
    # (year, month, day)
    return int(datetime(*tuple_key[-3:]).timestamp())


def tuple_to_datapoint(time_range_in_hours, tuple_value, tuple_key):
    timestamp = tuple_to_timestamp(time_range_in_hours, tuple_key)
    # if the range is less than an hour, we create the appropriate
    # datapoint [value, timestamp]
    if time_range_in_hours <= 1:
        return [tuple_value[0], timestamp]
    # else we need to use the summarized values [sum, count, min, max]
    # and convert them to a datapoint [value, timestamp]
    sum_values = tuple_value[0]
    count = tuple_value[1]
    return [sum_values / count, timestamp]


def time_aggregate_tuple(machine, metric, dt, resolution):
    if resolution == "minute":
        return key_tuple_minute(dt, machine, metric)
    elif resolution == "hour":
        return key_tuple_hour(dt, machine, metric)
    return key_tuple_day(dt, machine, metric)
