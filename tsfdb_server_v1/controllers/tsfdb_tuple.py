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
    db, resolution, resource, metric, start, stop, stat=None,
        limit=None):
    time_boundaries = split_time_range(resolution, start, stop, limit)
    # if time range is less than an hour, we create the keys for getting the
    # datapoints per second
    if resolution == 'second':
        # delta compensates for the range function of foundationdb which
        # for start, stop returns keys in [start, stop). We convert it to
        # the range [start, stop]
        return [key_tuple_second(time_boundary, metric) for time_boundary
                in time_boundaries]

    # if time range is less than 2 days, we create the keys for getting the
    # summarized datapoints per minute
    elif resolution == 'minute':
        return [key_tuple_minute(time_boundary, metric, stat) for time_boundary
                in time_boundaries]
    # if time range is less than 2 months, we create the keys for getting
    # the summarized datapoints per hour
    elif resolution == 'hour':
        return [key_tuple_hour(time_boundary, metric, stat) for time_boundary
                in time_boundaries]
    # if time range is more than 2 months, we create the keys for getting
    # the summarized datapoints per day
    return [key_tuple_day(time_boundary, metric, stat) for time_boundary
            in time_boundaries]


def split_time_range(resolution, start, stop, limit):
    if resolution == 'second':
        delta = timedelta(seconds=1)
    elif resolution == 'minute':
        delta = timedelta(minutes=1)
    elif resolution == 'hour':
        delta = timedelta(hours=1)
    else:
        delta = timedelta(hours=24)
    if not limit or start == stop:
        return [start, stop + delta]
    time_ranges = []
    time_boundary = start
    while time_boundary < stop:
        time_ranges.append(time_boundary)
        time_boundary += delta * limit
    if time_ranges[-1] < stop + delta:
        time_ranges.append(stop + delta)
    return time_ranges


def tuple_to_timestamp(resolution, tuple_key):
    # if time range is less than an hour, we create the timestamp per second
    # The last 6 items of the tuple contain the date up to the second
    # (year, month, day, hour, minute, second)
    if resolution == 'second':
        return int(datetime(*tuple_key[-6:]).timestamp())
    # if time range is less than 2 days, we create the timestamp per minute
    # The last 5 items of the tuple contain the date up to the minute
    # (year, month, day, hour, minute)
    elif resolution == 'minute':
        return int(datetime(*tuple_key[-5:]).timestamp())
    # if time range is less than 2 months, we create the timestamp per hour
    # The last 4 items of the tuple contain the date up to the hour
    # (year, month, day, hour)
    elif resolution == 'hour':
        return int(datetime(*tuple_key[-4:]).timestamp())
    # if time range is more than 2 months, we create the timestamp per day
    # The last 3 items of the tuple contain the date up to the day
    # (year, month, day)
    return int(datetime(*tuple_key[-3:]).timestamp())


def tuple_to_datapoint(resolution, tuple_value, tuple_key,
                       metric_type, stat):
    timestamp = tuple_to_timestamp(resolution, tuple_key)
    # if the range is less than an hour, we create the appropriate
    # datapoint [value, timestamp]
    if resolution == 'second':
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


def round_start(start, resolution):
    if resolution == 'minute':
        if start.second > 0:
            start.replace(second=0)
            start.replace(minute=start.minute+1)
    elif resolution == 'hour':
        if start.minute > 0:
            start.replace(minute=0)
            start.replace(hour=start.hour+1)
    elif resolution == 'day':
        if start.hour > 0:
            start.replace(hour=0)
            start.replace(day=start.day+1)
    return start
