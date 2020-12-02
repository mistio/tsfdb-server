import asyncio
import connexion
import re
import numpy as np
import logging
import json
from .helpers import round_base, error, parse_relative_time_to_seconds, \
    parse_start_stop_params
from datetime import datetime
from .db import DBOperations
from tsfdb_server_v1.models.error import Error  # noqa: E501

log = logging.getLogger(__name__)
stats = ("value", "count", "sum", "max", "min")


def roundX(data, precision=0, base=1):
    if not isinstance(data, dict) or not data:
        return {}
    for metric, datapoints in data.items():
        for stat in stats:
            if not data[metric].get(stat):
                continue
            data[metric][stat] = [
                [round_base(x, precision, base), y]
                for x, y in datapoints[stat]
            ]
    return data


def roundY(data, precision=0, base=1):
    if not isinstance(data, dict) or not data:
        return {}
    for metric, datapoints in data.items():
        for stat in stats:
            if not data[metric].get(stat):
                continue
            data[metric][stat] = [
                [x, round_base(y, precision, base)]
                for x, y in datapoints[stat]
            ]
    return data


def mean(data):
    if not isinstance(data, dict) or not data:
        return {}
    for metric, datapoints in data.items():
        for stat in stats:
            if not datapoints.get(stat):
                continue
            mean_data = {}
            for value, timestamp in datapoints[stat]:
                if not mean_data.get(timestamp):
                    mean_data[timestamp] = []
                mean_data[timestamp].append(value)
            data[metric][stat] = []
            for timestamp, values in mean_data.items():
                data[metric][stat].append(
                    [sum(values)/len(values), timestamp])
    return data


def fetch(db_ops, resources_and_metrics, start="", stop="", step=""):
    # We take for granted that all metrics start with the id and that
    # it ends on the first occurence of a dot, e.g id.system.load1
    start, stop = parse_start_stop_params(start, stop)
    if start > stop:
        return Error(code=400, message="Invalid time range")
    start = str(int(datetime.timestamp(start)))
    stop = str(int(datetime.timestamp(stop)))
    data = {}
    org = connexion.request.headers['x-org-id']
    authorized_resources = connexion.request.headers.get('x-allowed-resources')
    if authorized_resources:
        authorized_resources = json.loads(authorized_resources)

    if isinstance(resources_and_metrics, str):
        multiple_resources_and_metrics = [resources_and_metrics]
    else:
        multiple_resources_and_metrics = resources_and_metrics

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    data = loop.run_until_complete(
        db_ops.async_fetch_list(
            org, multiple_resources_and_metrics, start, stop,
            authorized_resources))
    if not isinstance(data, Error) and step:
        return mean(roundY(data, base=parse_relative_time_to_seconds(step)))
    return data


def fetch_monitoring(resources_and_metrics, start="", stop="", step="",
                     resolution=""):
    db_ops = DBOperations(resolution=resolution)
    return fetch(db_ops, resources_and_metrics, start, stop, step)


def fetch_metering(resources_and_metrics, start="", stop="", step="",
                   resolution=""):
    db_ops = DBOperations(series_type="metering", resolution=resolution)
    return fetch(db_ops, resources_and_metrics, start, stop, step)


def deriv(data):
    if not isinstance(data, dict) or not data:
        return {}
    for metric, datapoints in data.items():
        for stat in stats:
            if not datapoints.get(stat) or len(datapoints.get(stat)) < 2:
                data[metric][stat] = []
                continue
            values = np.asarray([value for value, _ in datapoints[stat]])
            timestamps = np.asarray(
                [timestamp for _, timestamp in datapoints[stat]])
            values_deriv = np.gradient(values, timestamps)
            data[metric][stat] = [
                [value_deriv.tolist(), timestamp.tolist()]
                for value_deriv, timestamp in zip(
                    np.nditer(values_deriv), np.nditer(timestamps)
                )
            ]
    return data


def topk(data, k=20, stat="value"):
    if not isinstance(data, dict) or not data:
        return {}
    sum_data = {}
    top_data = {}
    for metric, datapoints in data.items():
        if datapoints[stat]:
            sum_data[metric] = sum(
                x for x, y in datapoints[stat])/len(datapoints[stat])
        else:
            sum_data[metric] = 0
    for metric, _ in sorted(sum_data.items(),
                            key=lambda item: item[1],
                            reverse=True)[0:k]:
        top_data[metric] = data[metric]
    return top_data
