import logging
import fdb
from .helpers import error
from datetime import datetime, timedelta

log = logging.getLogger(__name__)


def key_tuple_second(dt, metric):
    return key_tuple_minute(dt, metric) + (dt.second,)


def key_tuple_minute(dt, metric):
    return key_tuple_hour(dt, metric) + (dt.minute,)


def key_tuple_hour(dt, metric):
    return key_tuple_day(dt, metric) + (dt.hour,)


def key_tuple_day(dt, metric):
    return (
        metric,
        dt.year,
        dt.month,
        dt.day,
    )


def start_stop_key_tuples_per_resolution(db, start, stop, resource, metric,
                                         resolution, resolutions_dirs, delta):
    if not (resolutions_dirs.get(resource) and
            resolutions_dirs[resource].get(resolution)):
        if not resolutions_dirs.get(resource):
            resolutions_dirs[resource] = {}
        if fdb.directory.exists(db, ("monitoring",
                                     ("metric_per_" + resolution),
                                     resource)):
            resolutions_dirs[resource][resolution] = fdb.directory.open(
                db, ("monitoring", ("metric_per_" + resolution),
                     resource))
        else:
            error_msg = (
                "Resource directory: %s with resolution: %s doesn't exist."
                % (resource, resolution))
            return error(503, error_msg)
    return [
        resolutions_dirs[resource][resolution].pack(
            key_tuple_minute(start, metric)
        ),
        resolutions_dirs[resource][resolution].pack(
            key_tuple_minute(stop + delta, metric)
        ),
    ]


def start_stop_key_tuples(
    db, time_range_in_hours, resource, machine_dirs,
    resolutions_dirs, metric, start, stop
):
    # if time range is less than an hour, we create the keys for getting the
    # datapoints per second
    if time_range_in_hours <= 1:
        # delta compensates for the range function of foundationdb which
        # for start, stop returns keys in [start, stop). We convert it to
        # the range [start, stop]
        delta = timedelta(seconds=1)
        # Open the monitoring directory if it exists
        if not machine_dirs.get(resource):
            if fdb.directory.exists(db, ("monitoring", resource)):
                machine_dirs[resource] = fdb.directory.open(
                    db, ("monitoring", resource))
            else:
                error_msg = (
                    "Resource directory: %s doesn't exist." % resource)
                return error(503, error_msg)
        return [
            machine_dirs[resource].pack(key_tuple_second(start, metric)),
            machine_dirs[resource].pack(key_tuple_second(
                stop + delta, metric)),
        ]
    # if time range is less than 2 days, we create the keys for getting the
    # summarizeddatapoints per minute
    elif time_range_in_hours <= 48:
        delta = timedelta(minutes=1)
        return start_stop_key_tuples_per_resolution(db, start, stop, resource,
                                                    metric, "minute",
                                                    resolutions_dirs, delta)
    # if time range is less than 2 months, we create the keys for getting
    # the summarized datapoints per hour
    elif time_range_in_hours <= 1440:
        delta = timedelta(hours=1)
        return start_stop_key_tuples_per_resolution(db, start, stop, resource,
                                                    metric, "hour",
                                                    resolutions_dirs, delta)
    # if time range is more than 2 months, we create the keys for getting
    # the summarized datapoints per day
    delta = timedelta(hours=24)
    return start_stop_key_tuples_per_resolution(db, start, stop, resource,
                                                metric, "day",
                                                resolutions_dirs, delta)


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


def time_aggregate_tuple(metric, dt, resolution):
    if resolution == "minute":
        return key_tuple_minute(dt, metric)
    elif resolution == "hour":
        return key_tuple_hour(dt, metric)
    return key_tuple_day(dt, metric)
