import asyncio
import fdb
import fdb.tuple
import re
import logging
import traceback
import os
from .tsfdb_tuple import key_tuple_second
from .helpers import error, parse_start_stop_params, \
    generate_metric, profile, is_regex, config
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
                                          authorized_resources=None)
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

        metrics_data = [
            loop.run_in_executor(None, time_series.find_datapoints,
                                 *(db, org, resource, metric, start, stop))
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
            ("% s on async_find_datapoints(resource, start, stop"
                + ", metrics) with resource_id: % s") % (
                str(err.description, 'utf-8'), resource))
        return error(503, error_msg, traceback=traceback.format_exc(),
                     request=str((resource, start, stop, metrics)))


@fdb.transactional
def write_lines(tr, org, lines):
    metrics = {}
    for line in lines:
        dict_line = parse_line(line)
        machine = dict_line["tags"]["machine_id"]
        if not metrics.get(machine):
            machine_metrics = time_series.find_metrics(
                tr, org, machine)
            metrics[machine] = {m for m in machine_metrics.keys()}
        metric = generate_metric(
            dict_line["tags"], dict_line["measurement"])
        dt = datetime.fromtimestamp(int(str(dict_line["time"])[:10]))
        for field, value in dict_line["fields"].items():
            machine_metric = "%s.%s" % (metric, field)
            if time_series.write_datapoint(tr, org, machine, key_tuple_second(
                    dt, machine_metric), value):
                if not (machine_metric in metrics.get(machine)):
                    time_series.add_metric(tr, org,
                                           (machine, machine_metric),
                                           type(value).__name__)
                for resolution in resolutions:
                    time_series.write_datapoint_aggregated(
                        tr, org, machine, machine_metric,
                        dt, value, resolution)


def write_in_queue(org, data):
    try:
        if not data:
            return
        db = open_db()
        queue = Queue(os.uname()[1])
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

        write_lines(db, org, data)
    except fdb.FDBError as err:
        error_msg = ("%s on write_in_kv(data) with resource_id: %s" % (
            str(err.description, 'utf-8'),
            parse_line(data[0])["tags"]["machine_id"]))
        return error(503, error_msg, traceback=traceback.format_exc(),
                     request=str(data))


async def async_fetch_list(org, multiple_resources_and_metrics, start="",
                           stop="", step=""):
    data = {}
    loop = asyncio.get_event_loop()
    data_list = [
        loop.run_in_executor(None, fetch_item, *
                             (org, resources_and_metrics, start, stop, step))
        for resources_and_metrics in multiple_resources_and_metrics
    ]

    data_list = await asyncio.gather(*data_list)

    for data_item in data_list:
        if isinstance(data_item, Error):
            return data_item
        data.update(data_item)

    return data


def fetch_item(org, resources_and_metrics, start="", stop="", step=""):
    data = {}
    resources, metrics = resources_and_metrics.split(".", 1)
    db = open_db()
    if is_regex(resources):
        regex_resources = resources
        resources = find_resources(
            org, regex_resources, authorized_resources=None)
        if isinstance(resources, Error):
            return resources
    else:
        resources = [resources]

    for resource in resources:
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
