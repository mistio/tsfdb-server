import asyncio
import fdb
import fdb.tuple
import re
import logging
import traceback
import struct
from .tsfdb_tuple import tuple_to_datapoint, start_stop_key_tuples, \
    time_aggregate_tuple, key_tuple_second
from .helpers import metric_to_dict, error, parse_start_stop_params, \
    generate_metric, div_datapoints, profile
from .queue import Queue, Subspace
from line_protocol_parser import parse_line
from datetime import datetime
from tsfdb_server_v1.models.error import Error  # noqa: E501

fdb.api_version(620)

log = logging.getLogger(__name__)

AGGREGATE_MINUTE = True
AGGREGATE_HOUR = True
AGGREGATE_DAY = True

DO_NOT_CACHE_FDB_DIRS = False

TRANSACTION_RETRY_LIMIT = 0
# timeout in ms
TRANSACTION_TIMEOUT = 2000

fdb_dirs = {}
machine_dirs = {}
resolutions = ("minute", "hour", "day")
resolutions_dirs = {}
resolutions_options = {"minute": AGGREGATE_MINUTE,
                       "hour": AGGREGATE_HOUR, "day": AGGREGATE_DAY}

struct_types = (int, float)


def open_db():
    db = fdb.open()
    db.options.set_transaction_retry_limit(TRANSACTION_RETRY_LIMIT)
    db.options.set_transaction_timeout(TRANSACTION_TIMEOUT)
    return db


def open_db_async():
    db = fdb.open(event_model="asyncio")
    db.options.set_transaction_retry_limit(TRANSACTION_RETRY_LIMIT)
    db.options.set_transaction_timeout(TRANSACTION_TIMEOUT)
    return db


@fdb.transactional
def find_metrics_from_db(tr, available_metrics, resource):
    metrics = {}
    for k, v in tr[available_metrics[resource].range()]:
        metric = available_metrics[resource].unpack(k)[0]
        value = fdb.tuple.unpack(v)[0]
        metrics.update(metric_to_dict(metric, value))

    return metrics


def find_metrics(resource):
    try:
        db = open_db()
        if DO_NOT_CACHE_FDB_DIRS or not fdb_dirs.get('available_metrics'):
            if fdb.directory.exists(db, ('monitoring', 'available_metrics')):
                fdb_dirs['available_metrics'] = fdb.directory.open(
                    db, ('monitoring', 'available_metrics'))
            else:
                error_msg = "Monitoring directory doesn't exist."
                return error(404, error_msg)

        return find_metrics_from_db(
            db, fdb_dirs['available_metrics'], resource)
    except fdb.FDBError as err:
        error_msg = ("%s on find_metrics(resource) with resource_id: %s" % (
            str(err.description, 'utf-8'),
            resource))
        return error(503, error_msg, traceback=traceback.format_exc(),
                     request=resource)


@fdb.transactional
def find_resources_from_db(tr, monitoring, regex_resources):
    resources = []
    for k, v in tr[monitoring["available_resources"].range()]:
        candidate = monitoring["available_resources"].unpack(k)
        if re.match("^%s$" % regex_resources, candidate):
            resources.append(k)

    return resources


def find_resources(regex_resources):
    try:
        db = open_db()
        if DO_NOT_CACHE_FDB_DIRS or not fdb_dirs.get('monitoring'):
            if fdb.directory.exists(db, "monitoring"):
                fdb_dirs['monitoring'] = fdb.directory.open(db, "monitoring")
            else:
                error_msg = "Monitoring directory doesn't exist."
                return error(404, error_msg)
        return find_resources_from_db(
            db, fdb_dirs['monitoring'], regex_resources)
    except fdb.FDBError as err:
        error_msg = (
            "%s on find_resources(regex_resources) with regex_resources: %s"
            % (
                str(err.description, 'utf-8'),
                regex_resources))
        return error(503, error_msg, traceback=traceback.format_exc(),
                     request=regex_resources)


@fdb.transactional
def find_datapoints_from_db(tr, start, stop, time_range_in_hours, resource,
                            metric, stat):

    if DO_NOT_CACHE_FDB_DIRS or not fdb_dirs.get('available_metrics'):
        if fdb.directory.exists(tr, ('monitoring', 'available_metrics')):
            fdb_dirs['available_metrics'] = fdb.directory.open(
                tr, ('monitoring', 'available_metrics'))
        else:
            error_msg = "Monitoring directory doesn't exist."
            return error(404, error_msg)
    if not tr[fdb_dirs['available_metrics'].pack(
            (resource, metric))].present():
        error_msg = "Metric type: %s for resource: %s doesn't exist." % (
            resource, metric)
        return error(404, error_msg)
    metric_type_tuple = tr[fdb_dirs['available_metrics'].pack(
        (resource, metric))]
    metric_type = fdb.tuple.unpack(metric_type_tuple)[0]

    datapoints = []
    for k, v in tr[start:stop]:

        tuple_key = list(fdb.tuple.unpack(k))
        if time_range_in_hours <= 1:
            tuple_value = list(fdb.tuple.unpack(v))
        else:
            tuple_value = v

        datapoints.append(
            tuple_to_datapoint(
                time_range_in_hours, tuple_value, tuple_key, metric_type, stat
            )
        )
    return datapoints


def _find_datapoints_per_metric(db, time_range_in_hours, resource,
                                machine_dirs, resolutions_dirs, metric,
                                start, stop):
    stats = (None,)
    datapoints_per_stat = {}
    if time_range_in_hours > 1:
        stats = ("count", "sum")

    for stat in stats:
        tuples = start_stop_key_tuples(
            db, time_range_in_hours,
            resource, machine_dirs, resolutions_dirs, metric, start,
            stop, stat
        )

        if isinstance(tuples, Error):
            return tuples

        key_timestamp_start, key_timestamp_stop = tuples

        datapoints_per_stat[stat] = find_datapoints_from_db(
            db, key_timestamp_start, key_timestamp_stop,
            time_range_in_hours, resource, metric, stat)

        if isinstance(datapoints_per_stat[stat], Error):
            return datapoints_per_stat[stat]

    if time_range_in_hours > 1:
        datapoints = div_datapoints(list(
            datapoints_per_stat["sum"]),
            list(datapoints_per_stat["count"]))
    else:
        datapoints = list(datapoints_per_stat[None])

    return {("%s.%s" % (resource, metric)): datapoints}


async def find_datapoints(resource, start, stop, metrics):
    try:
        loop = asyncio.get_event_loop()
        db = open_db_async()
        data = {}
        start, stop = parse_start_stop_params(start, stop)
        time_range = stop - start
        time_range_in_hours = round(time_range.total_seconds() / 3600, 2)

        metrics_data = [
            loop.run_in_executor(None, _find_datapoints_per_metric, *
                                 (db,
                                  time_range_in_hours, resource,
                                  machine_dirs, resolutions_dirs,
                                  metric, start, stop))
            for metric in metrics
        ]

        metrics_data = await asyncio.gather(*metrics_data)

        for metric_data in metrics_data:
            if isinstance(metric_data, Error):
                return metric_data
            if metric_data:
                data.update(metric_data)

        return data
    except fdb.FDBError as err:
        error_msg = (
            ("% s on find_datapoints(resource, start, stop"
                + ", metrics) with resource_id: % s") % (
                str(err.description, 'utf-8'), resource))
        return error(503, error_msg, traceback=traceback.format_exc(),
                     request=str((resource, start, stop, metrics)))


def queue_test():
    db = open_db()
    queue = Queue(Subspace(('queue',)))
    print(queue.peek(db))
    return
    item = queue.pop(db)
    while item:
        print(item)
        item = queue.pop(db)


def write_tuple_full_resolution(tr, machine_dir, key, value):
    if not tr[machine_dir.pack(key)].present():
        tr[machine_dir.pack(key)] = fdb.tuple.pack((value,))
        return True
    log.warning("key: %s already exists" % str(key))
    return False


def update_metric(tr, available_metrics, metric, metric_type):
    if not tr[available_metrics.pack(metric)].present():
        tr[available_metrics.pack(metric)] = fdb.tuple.pack((metric_type,))


def apply_time_aggregation(tr, monitoring, machine,
                           metric, dt, value, resolutions, resolutions_dirs,
                           resolutions_options):
    if type(value) not in struct_types:
        log.warning("Unsupported aggregation value type: %s" %
                    str(type(value)))
        return
    if type(value) is float:
        value *= 1000
        value = int(value)
    for resolution in resolutions:
        if not resolutions_options[resolution]:
            continue
        if not (resolutions_dirs.get(machine) and
                resolutions_dirs[machine].get(resolution)):
            if not resolutions_dirs.get(machine):
                resolutions_dirs[machine] = {}
            resolutions_dirs[machine][resolution] = resolutions_dirs[
                resolution].create_or_open(
                tr, (machine,))
        monitoring_time = resolutions_dirs[machine][resolution]
        tr.add(monitoring_time.pack(
            time_aggregate_tuple(metric, "count", dt, resolution)),
            struct.pack('<q', 1))
        tr.add(monitoring_time.pack(
            time_aggregate_tuple(metric, "sum", dt, resolution)),
            struct.pack('<q', value))
        tr.min(monitoring_time.pack(
            time_aggregate_tuple(metric, "min", dt, resolution)),
            struct.pack('<q', value))
        tr.max(monitoring_time.pack(
            time_aggregate_tuple(metric, "max", dt, resolution)),
            struct.pack('<q', value))


def write_line(tr, monitoring, available_metrics, line, metrics,
               machine_dirs, resolutions, resolutions_dirs,
               resolutions_options):
    dict_line = parse_line(line)
    machine = dict_line["tags"]["machine_id"]
    if not machine_dirs.get(machine):
        machine_dirs[machine] = monitoring.create_or_open(
            tr, (machine,))
    if not metrics.get(machine):
        machine_metrics = find_metrics_from_db(
            tr, available_metrics, machine)
        metrics[machine] = {m for m in machine_metrics.keys()}
    metric = generate_metric(
        dict_line["tags"], dict_line["measurement"])
    dt = datetime.fromtimestamp(int(str(dict_line["time"])[:10]))
    for field, value in dict_line["fields"].items():
        machine_metric = "%s.%s" % (metric, field)
        if write_tuple(tr, machine_dirs[machine], key_tuple_second(
                dt, machine_metric), value):
            if not (machine_metric in metrics.get(machine)):
                update_metric(tr, available_metrics,
                              (machine, machine_metric),
                              type(value).__name__)
            apply_time_aggregation(tr, monitoring, machine,
                                   machine_metric, dt, value,
                                   resolutions, resolutions_dirs,
                                   resolutions_options)


def write_tuple(tr, machine_dirs, dt, machine_metric, value, metrics,
                available_metrics, machine, monitoring, resolutions,
                resolutions_dirs, resolutions_options):
    if write_tuple_full_resolution(tr, machine_dirs[machine], key_tuple_second(
            dt, machine_metric), value):
        if not (machine_metric in metrics.get(machine)):
            update_metric(tr, available_metrics,
                          (machine, machine_metric),
                          type(value).__name__)
        apply_time_aggregation(tr, monitoring, machine,
                               machine_metric, dt, value,
                               resolutions, resolutions_dirs,
                               resolutions_options)


async def write_lines(tr, monitoring, available_metrics, lines):
    metrics = {}
    loop = asyncio.get_event_loop()
    for resolution in resolutions:
        if DO_NOT_CACHE_FDB_DIRS or not resolutions_dirs.get(resolution):
            resolutions_dirs[resolution] = monitoring.create_or_open(
                tr, ('metric_per_' + resolution,))
    writers = []

    for line in lines:
        dict_line = parse_line(line)
        machine = dict_line["tags"]["machine_id"]
        if not machine_dirs.get(machine):
            machine_dirs[machine] = monitoring.create_or_open(
                tr, (machine,))
        if not metrics.get(machine):
            machine_metrics = find_metrics_from_db(
                tr, available_metrics, machine)
            metrics[machine] = {m for m in machine_metrics.keys()}
        metric = generate_metric(
            dict_line["tags"], dict_line["measurement"])
        dt = datetime.fromtimestamp(int(str(dict_line["time"])[:10]))
        for field, value in dict_line["fields"].items():
            machine_metric = "%s.%s" % (metric, field)
            writers.append(loop.run_in_executor(None, write_tuple, *
                                                (tr, machine_dirs, dt,
                                                 machine_metric, value,
                                                 metrics,
                                                 available_metrics, machine,
                                                 monitoring, resolutions,
                                                 resolutions_dirs,
                                                 resolutions_options)))

    await asyncio.gather(*writers, return_exceptions=True)


def write_in_queue(data):
    try:
        db = open_db()
        queue = Queue(Subspace(('queue',)))
        queue.push(db, data)
        print("Pushed %d bytes" % len(data.encode('utf-8')))
    except fdb.FDBError as err:
        error_msg = ("%s on write_in_queue(data) with resource_id: %s" % (
            str(err.description, 'utf-8')))
        return error(503, error_msg, traceback=traceback.format_exc(),
                     request=str(data))


@fdb.transactional
def _write_in_kv(tr, fdb_dirs, data):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        write_lines(tr, fdb_dirs['monitoring'],
                    fdb_dirs['available_metrics'], data))


@profile
def write_in_kv(data):
    try:
        db = open_db_async()

        if DO_NOT_CACHE_FDB_DIRS or not fdb_dirs.get('monitoring'):
            fdb_dirs['monitoring'] = fdb.directory.create_or_open(
                db, ('monitoring',))
        if DO_NOT_CACHE_FDB_DIRS or not fdb_dirs.get('available_metrics'):
            fdb_dirs['available_metrics'] = fdb_dirs[
                'monitoring'].create_or_open(
                db, ('available_metrics',))
        # Create a list of lines
        data = data.split('\n')
        # Get rid of all empty lines
        data = [line for line in data if line != ""]

        metrics = set()
        total_datapoints = 0
        machine = ""
        for line in data:
            dict_line = parse_line(line)
            machine = dict_line["tags"]["machine_id"]
            total_datapoints += len(dict_line["fields"].items())
            metric = generate_metric(
                dict_line["tags"], dict_line["measurement"])
            for field, _ in dict_line["fields"].items():
                metrics.add(machine + "-" + metric + "-" + field)

        log.warning(("Request for resource: %s, number of metrics: %d," +
                     " number of datapoints: %d") % (
            machine, len(metrics), total_datapoints))

        _write_in_kv(db, fdb_dirs, data)

    except fdb.FDBError as err:
        error_msg = ("%s on write_in_kv(data) with resource_id: %s" % (
            str(err.description, 'utf-8'),
            parse_line(data[0])["tags"]["machine_id"]))
        return error(503, error_msg, traceback=traceback.format_exc(),
                     request=str(data))
