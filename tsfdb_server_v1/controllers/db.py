import fdb
import fdb.tuple
import re
import logging
import traceback
from .tsfdb_tuple import tuple_to_datapoint, start_stop_key_tuples, \
    time_aggregate_tuple, key_tuple_second
from .helpers import metric_to_dict, error, parse_start_stop_params, \
    decrement_time, generate_metric
from .queue import Queue, Subspace
from .tsfdb_dirs import create_or_open_fdb_dir, create_or_open_metric_dir
from line_protocol_parser import parse_line
from datetime import datetime
from tsfdb_server_v1.models.error import Error  # noqa: E501

fdb.api_version(620)

log = logging.getLogger(__name__)

AGGREGATE_MINUTE = 1
AGGREGATE_HOUR = 2
AGGREGATE_DAY = 2

DO_NOT_CACHE_FDB_DIRS = False

TRANSACTION_RETRY_LIMIT = 0
# timeout in ms
TRANSACTION_TIMEOUT = 2000

fdb_dirs = {}
machine_dirs = {}
cached_dirs = {}
resolutions = ("minute", "hour", "day")
resolutions_dirs = {}
resolutions_options = {"minute": AGGREGATE_MINUTE,
                       "hour": AGGREGATE_HOUR, "day": AGGREGATE_DAY}


def open_db():
    db = fdb.open()
    db.options.set_transaction_retry_limit(TRANSACTION_RETRY_LIMIT)
    db.options.set_transaction_timeout(TRANSACTION_TIMEOUT)
    return db


@fdb.transactional
def find_metrics_from_db(tr, resource):
    # need an open only
    available_metrics = create_or_open_fdb_dir(
        tr, ('monitoring', 'available_metrics',), cached_dirs)
    metrics = {}
    for k, v in tr[available_metrics[resource].range()]:
        data_tuple = available_metrics[resource].unpack(k)
        metrics.update(metric_to_dict(data_tuple[1]))

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
def find_datapoints_from_db(tr, start, stop, time_range_in_hours):
    datapoints = []
    for k, v in tr[start:stop]:

        tuple_key = list(fdb.tuple.unpack(k))
        tuple_value = list(fdb.tuple.unpack(v))

        datapoints.append(
            tuple_to_datapoint(
                time_range_in_hours, tuple_value, tuple_key
            )
        )
    return datapoints


def find_datapoints(resource, start, stop, metrics):
    try:
        db = open_db()
        data = {}
        start, stop = parse_start_stop_params(start, stop)
        time_range = stop - start
        time_range_in_hours = round(time_range.total_seconds() / 3600, 2)

        for metric in metrics:
            tuples = start_stop_key_tuples(
                db, time_range_in_hours,
                resource, machine_dirs, resolutions_dirs, metric, start, stop
            )

            if isinstance(tuples, Error):
                return tuples

            key_timestamp_start, key_timestamp_stop = tuples

            datapoints = find_datapoints_from_db(
                db, key_timestamp_start, key_timestamp_stop,
                time_range_in_hours)

            data.update({("%s.%s" % (resource, metric)): datapoints})

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


def write_tuple(tr, machine, key, value):
    machine_dir = create_or_open_fdb_dir(
        tr, ('monitoring', 'metric_per_second', machine,),
        cached_dirs)
    if not tr[machine_dir.pack(key)].present():
        tr[machine_dir.pack(key)] = fdb.tuple.pack((value,))
        return True
    saved_value = fdb.tuple.unpack(tr[machine_dir.pack(key)])[0]
    if saved_value != value:
        log.error("key: %s already exists with a different value" % str(key))
    else:
        log.warning("key: %s already exists with the same value" % str(key))
    return False


def update_metric(tr, metric):
    available_metrics = create_or_open_fdb_dir(
        tr, ('monitoring', 'available_metrics',), cached_dirs)
    if not tr[available_metrics.pack(metric)].present():
        tr[available_metrics.pack(metric)] = fdb.tuple.pack(("",))


def apply_time_aggregation(tr, machine,
                           metric, dt, value, resolutions,
                           resolutions_options):
    new_aggregation = False
    last_tuple = None
    last_dt = None
    for resolution in resolutions:
        if resolutions_options[resolution] == 0 or \
                (resolutions_options[resolution] == 2 and
                 not new_aggregation):
            continue
        if resolutions_options[resolution] == 2:
            sum_dt = last_dt
        else:
            sum_dt = dt
        monitoring_time = create_or_open_fdb_dir(
            tr, ('monitoring', 'metric_per_' + resolution, machine,),
            cached_dirs)
        sum_tuple = tr[monitoring_time.pack(
            time_aggregate_tuple(metric, sum_dt, resolution))]
        if sum_tuple.present():
            sum_tuple = fdb.tuple.unpack(sum_tuple)
            sum_value, count, min_value, max_value = sum_tuple
            new_aggregation = False
            if resolutions_options[resolution] == 2:
                last_tuple = fdb.tuple.unpack(last_tuple)
                last_sum_value, last_count, last_min_value, last_max_value \
                    = last_tuple
                sum_value += last_sum_value
                count += last_count
                min_value = min(last_min_value, min_value)
                max_value = max(last_max_value, max_value)
            else:
                sum_value += value
                count += 1
                min_value = min(value, min_value)
                max_value = max(value, max_value)
        else:
            if resolutions_options[resolution] == 2:
                last_tuple = fdb.tuple.unpack(last_tuple)
                sum_value, count, min_value, max_value \
                    = last_tuple
            else:
                sum_value, count, min_value, max_value = value, 1, value, value
            last_dt = decrement_time(dt, resolution)
            last_tuple = tr[monitoring_time.pack(
                time_aggregate_tuple(metric, last_dt, resolution))]
            new_aggregation = last_tuple.present()
        tr[monitoring_time.pack(
            time_aggregate_tuple(metric, sum_dt, resolution))] \
            = fdb.tuple.pack((sum_value, count, min_value, max_value))


@fdb.transactional
def write_lines(tr, lines):
    metrics = {}
    for line in lines:
        dict_line = parse_line(line)
        machine = dict_line["tags"]["machine_id"]
        if not metrics.get(machine):
            machine_metrics = find_metrics_from_db(
                tr, machine)
            metrics[machine] = {m for m in machine_metrics.keys()}
        metric = generate_metric(
            dict_line["tags"], dict_line["measurement"])
        dt = datetime.fromtimestamp(int(str(dict_line["time"])[:10]))
        for field, value in dict_line["fields"].items():
            machine_metric = "%s.%s" % (metric, field)
            if write_tuple(tr, machine, key_tuple_second(
                    dt, machine_metric), value):
                if not (machine_metric in metrics.get(machine)):
                    update_metric(tr, (machine, type(
                        value).__name__, machine_metric))
                apply_time_aggregation(tr, machine,
                                       machine_metric, dt, value,
                                       resolutions,
                                       resolutions_options)


def write_in_queue(data):
    try:
        db = open_db()
        queue = Queue(Subspace(('queue',)))
        queue.push(db, data)
        print("Pushed %d bytes" % len(data.encode('utf-8')))
    except fdb.FDBError as err:
        error_msg = ("%s on write(data) with resource_id: %s" % (
            str(err.description, 'utf-8')))
        return error(503, error_msg, traceback=traceback.format_exc(),
                     request=str(data))


def write_in_kv(data):
    try:
        db = open_db()

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

        write_lines(db, data)
    except fdb.FDBError as err:
        error_msg = ("%s on write(data) with resource_id: %s" % (
            str(err.description, 'utf-8'),
            parse_line(data[0])["tags"]["machine_id"]))
        return error(503, error_msg, traceback=traceback.format_exc(),
                     request=str(data))
