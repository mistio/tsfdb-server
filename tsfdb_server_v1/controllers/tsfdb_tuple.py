import logging
import struct
from .helpers import config
from datetime import datetime, timedelta

log = logging.getLogger(__name__)


def key_tuple_second(dt, metric, stat=None):
    return key_tuple_minute(dt, metric, stat) + (dt.second,)


def key_tuple_minute(dt, metric, stat=None):
    return key_tuple_hour(dt, metric, stat) + (dt.minute,)


def key_tuple_hour(dt, metric, stat=None):
    return key_tuple_day(dt, metric, stat) + (dt.hour,)


def key_tuple_day(dt, metric, stat=None):
    if stat:
        return (
            metric,
            stat,
            dt.year,
            dt.month,
            dt.day,
        )
    return (
        metric,
        dt.year,
        dt.month,
        dt.day,
    )


def start_stop_key_tuples(
        db, time_range_in_hours, resource, machine_dirs,
        resolutions_dirs, metric, start, stop, stat=None):
    # if time range is less than an hour, we create the keys for getting the
    # datapoints per second
    if time_range_in_hours <= config('SECONDS_RANGE'):
        # delta compensates for the range function of foundationdb which
        # for start, stop returns keys in [start, stop). We convert it to
        # the range [start, stop]
        delta = timedelta(seconds=1)
        # Open the monitoring directory if it exists
        return [
            key_tuple_second(start, metric),
            key_tuple_second(stop + delta, metric)
        ]

    # if time range is less than 2 days, we create the keys for getting the
    # summarized datapoints per minute
    elif time_range_in_hours <= config('MINUTES_RANGE'):
        delta = timedelta(minutes=1)
        return [
            key_tuple_minute(start, metric, stat),
            key_tuple_minute(stop + delta, metric, stat)
        ]
    # if time range is less than 2 months, we create the keys for getting
    # the summarized datapoints per hour
    elif time_range_in_hours <= config('HOURS_RANGE'):
        delta = timedelta(hours=1)
        return [
            key_tuple_hour(start, metric, stat),
            key_tuple_hour(stop + delta, metric, stat)
        ]
    # if time range is more than 2 months, we create the keys for getting
    # the summarized datapoints per day
    delta = timedelta(hours=24)
    return [
        key_tuple_day(start, metric, stat),
        key_tuple_day(stop + delta, metric, stat)
    ]


def tuple_to_timestamp(time_range_in_hours, tuple_key):
    # if time range is less than an hour, we create the timestamp per second
    # The last 6 items of the tuple contain the date up to the second
    # (year, month, day, hour, minute, second)
    if time_range_in_hours <= config('SECONDS_RANGE'):
        return int(datetime(*tuple_key[-6:]).timestamp())
    # if time range is less than 2 days, we create the timestamp per minute
    # The last 5 items of the tuple contain the date up to the minute
    # (year, month, day, hour, minute)
    if time_range_in_hours <= config('MINUTES_RANGE'):
        return int(datetime(*tuple_key[-5:]).timestamp())
    # if time range is less than 2 months, we create the timestamp per hour
    # The last 4 items of the tuple contain the date up to the hour
    # (year, month, day, hour)
    if time_range_in_hours <= config('HOURS_RANGE'):
        return int(datetime(*tuple_key[-4:]).timestamp())
    # if time range is more than 2 months, we create the timestamp per day
    # The last 3 items of the tuple contain the date up to the day
    # (year, month, day)
    return int(datetime(*tuple_key[-3:]).timestamp())


def tuple_to_datapoint(time_range_in_hours, tuple_value, tuple_key,
                       metric_type, stat):
    timestamp = tuple_to_timestamp(time_range_in_hours, tuple_key)
    # if the range is less than an hour, we create the appropriate
    # datapoint [value, timestamp]
    if time_range_in_hours <= config('SECONDS_RANGE'):
        return [tuple_value[0], timestamp]
    # else we need to use the summarized values [sum, count, min, max]
    # and convert them to a datapoint [value, timestamp]
    value = struct.unpack_from('<q', tuple_value)[0]
    if metric_type == "float" and stat != "count":
        value /= 1000
    return [value, timestamp]


def time_aggregate_tuple(metric, stat, dt, resolution):
    if resolution == "minute":
        return key_tuple_minute(dt, metric, stat)
    elif resolution == "hour":
        return key_tuple_hour(dt, metric, stat)
    return key_tuple_day(dt, metric, stat)
