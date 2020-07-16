import asyncio
import fdb
import fdb.tuple
import re
import logging
import traceback
from .tsfdb_tuple import key_tuple_second
from .helpers import error, parse_start_stop_params, \
    generate_metric, profile, is_regex, config, get_queue_id, \
    time_range_to_resolution
from .queue import Queue
from line_protocol_parser import parse_line
from datetime import datetime
from tsfdb_server_v1.models.error import Error  # noqa: E501
from .time_series_layer import TimeSeriesLayer

fdb.api_version(620)

log = logging.getLogger(__name__)

resolutions = ("minute", "hour", "day")


def open_db():
    db = fdb.open()
    db.options.set_transaction_retry_limit(config('TRANSACTION_RETRY_LIMIT'))
    db.options.set_transaction_timeout(config('TRANSACTION_TIMEOUT'))
    return db


time_series = TimeSeriesLayer()


def find_metrics(org, resource):
    try:
        db = open_db()
        return time_series.find_metrics(db, org, resource)
    except fdb.FDBError as err:
        error_msg = ("%s on find_metrics(resource) with resource_id: %s" % (
            str(err.description, 'utf-8'),
            resource))
        return error(503, error_msg, traceback=traceback.format_exc(),
                     request=resource)


def find_resources(org, regex_resources, authorized_resources=None):
    try:
        db = open_db()
        return time_series.find_resources(db, org, regex_resources,
                                          authorized_resources)
    except fdb.FDBError as err:
        error_msg = (
            "%s on find_resources(regex_resources) with regex_resources: %s"
            % (
                str(err.description, 'utf-8'),
                regex_resources))
        return error(503, error_msg, traceback=traceback.format_exc(),
                     request=regex_resources)


async def async_find_datapoints(org, resource, start, stop, metrics):
    try:
        loop = asyncio.get_event_loop()
        db = open_db()
        data = {}
        start, stop = parse_start_stop_params(start, stop)

        time_range = stop - start
        time_range_in_hours = round(time_range.total_seconds() / 3600, 2)

        resolution = time_range_to_resolution(time_range_in_hours)
        available_metrics = fdb.directory.create_or_open(
            db, ('monitoring', org, 'available_metrics'))
        datapoints_dir = fdb.directory.create_or_open(
            db, ('monitoring', org, resource, resolution))

        metrics_data = [
            loop.run_in_executor(None, time_series.find_datapoints,
                                 *(db, org, resource, metric, start, stop,
                                   datapoints_dir, available_metrics))
            for metric in metrics
        ]

        metrics_data = await asyncio.gather(*metrics_data,
                                            return_exceptions=True)
        exceptions = 0
        last_exception = None
        for metric_data in metrics_data:
            if isinstance(metric_data, Error):
                return metric_data
            if isinstance(metric_data, Exception):
                exceptions += 1
                last_exception = metric_data
            elif metric_data:
                data.update(metric_data)
        if last_exception:
            if not data:
                raise last_exception
            error(
                500,
                "Got exceptions on %d time_series.find_datapoints() instances"
                % exceptions, traceback=str(last_exception))
        return data
    except fdb.FDBError as err:
        error_msg = (
            ("% s on async_find_datapoints(resource, start, stop"
                + ", metrics) with resource_id: % s") % (
                str(err.description, 'utf-8'), resource))
        return error(503, error_msg, traceback=traceback.format_exc(),
                     request=str((resource, start, stop, metrics)))


@fdb.transactional
def write_lines(tr, org, lines):
    metrics = {}
    datapoints_dir = {}
    for line in lines:
        dict_line = parse_line(line)
        machine = dict_line["tags"]["machine_id"]
        metric = generate_metric(
            dict_line["tags"], dict_line["measurement"])
        dt = datetime.fromtimestamp(int(str(dict_line["time"])[:10]))
        for field, value in dict_line["fields"].items():
            machine_metric = "%s.%s" % (metric, field)
            if not datapoints_dir.get("second"):
                datapoints_dir["second"] = fdb.directory.create_or_open(
                    tr, ('monitoring', org, machine, 'second'))
            if time_series.write_datapoint(tr, org, machine, key_tuple_second(
                dt, machine_metric), value,
                    datapoints_dir=datapoints_dir['second']):
                if not metrics.get(machine):
                    metrics[machine] = set()
                metrics[machine].add((machine_metric, type(value).__name__))
                for resolution in resolutions:
                    if not datapoints_dir.get(resolution):
                        datapoints_dir[resolution] = \
                            fdb.directory.create_or_open(
                            tr, ('monitoring', org, machine, resolution))
                    time_series.write_datapoint_aggregated(
                        tr, org, machine, machine_metric,
                        dt, value, resolution,
                        datapoints_dir=datapoints_dir[resolution])
    return metrics


@profile
def write_in_queue(org, data):
    try:
        if not data:
            return
        db = open_db()
        queue = Queue(get_queue_id(data))
        queue.push(db, (org, data))
        print("Pushed %d bytes" % len(data.encode('utf-8')))
    except fdb.FDBError as err:
        error_msg = ("%s on write_in_queue(data)" % (
            str(err.description, 'utf-8')))
        return error(503, error_msg, traceback=traceback.format_exc(),
                     request=str(data))


@profile
def write_in_kv(org, data):
    try:
        if not data:
            return
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

        metrics = write_lines(db, org, data)
        update_metrics(db, org, metrics)
    except fdb.FDBError as err:
        error_msg = ("%s on write_in_kv(data) with resource_id: %s" % (
            str(err.description, 'utf-8'),
            parse_line(data[0])["tags"]["machine_id"]))
        return error(503, error_msg, traceback=traceback.format_exc(),
                     request=str(data))


@fdb.transactional
def update_metrics(tr, org, new_metrics):
    for machine, metrics in new_metrics.items():
        current_metrics = time_series.find_metrics(
            tr, org, machine)
        current_metrics = {m for m in current_metrics.keys()}
        for metric, metric_type in metrics:
            if not (metric in current_metrics):
                time_series.add_metric(tr, org,
                                       (machine, metric),
                                       metric_type)


async def async_fetch_list(org, multiple_resources_and_metrics, start="",
                           stop="", authorized_resources=None):
    data = {}
    loop = asyncio.get_event_loop()
    data_list = [
        loop.run_in_executor(None, fetch_item, *
                             (org, resources_and_metrics, start, stop,
                              authorized_resources))
        for resources_and_metrics in multiple_resources_and_metrics
    ]

    data_list = await asyncio.gather(*data_list)

    for data_item in data_list:
        if isinstance(data_item, Error):
            return data_item
        data.update(data_item)

    return data


def fetch_item(org, resources_and_metrics, start="", stop="",
               authorized_resources=None):
    resources, metrics = resources_and_metrics.split(".", 1)
    if is_regex(resources):
        regex_resources = resources
        resources = find_resources(
            org, regex_resources, authorized_resources)
        if isinstance(resources, Error):
            return resources
    else:
        resources = [resources]

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    data = loop.run_until_complete(
        async_fetch_item(org, resources, start, stop, metrics))
    return data


async def async_fetch_item(org, resources, start, stop, metrics):
    data = {}
    loop = asyncio.get_event_loop()
    data_list = [
        loop.run_in_executor(None, find_datapoints_per_resource, *
                             (org, resource, start, stop, metrics))
        for resource in resources
    ]

    data_list = await asyncio.gather(*data_list)
    last_error = None
    for data_item in data_list:
        if isinstance(data_item, Error):
            if data_item.code // 100 != 4:
                return data_item
            last_error = data_item
        else:
            data.update(data_item)

    if data == {} and last_error:
        return last_error
    return data


def find_datapoints_per_resource(org, resource, start, stop, metrics):
    db = open_db()
    data = {}
    if is_regex(metrics):
        regex_metric = metrics
        metrics = []
        all_metrics = time_series.find_metrics(db, org, resource)
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
            return error(400, error_msg, log)
    else:
        metrics = [metrics]
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    current_data = loop.run_until_complete(
        async_find_datapoints(org, resource, start, stop, metrics))
    if isinstance(current_data, Error):
        return current_data
    data.update(current_data)
    return data
